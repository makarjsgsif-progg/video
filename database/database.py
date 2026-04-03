import ssl
import string
import random
from datetime import datetime, timedelta

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    BigInteger, Text, ForeignKey, select, update, delete, func,
    create_engine,
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, AsyncSession, async_sessionmaker,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool

from config.config import settings

# ---------------------------------------------------------------------------
_is_postgres = "postgresql" in settings.DATABASE_URL or "postgres" in settings.DATABASE_URL


def _to_asyncpg_url(url: str) -> str:
    url = url.replace("postgres://", "postgresql://", 1)
    if "postgresql+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def _to_psycopg2_url(url: str) -> str:
    url = url.replace("postgres://", "postgresql://", 1)
    url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    if "postgresql+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


# ---------------------------------------------------------------------------
# Async engine — SQLAlchemy 2.0.36+ корректно передаёт statement_cache_size=0
# в asyncpg.connect(), что полностью отключает prepared statements.
# ---------------------------------------------------------------------------
def _make_async_engine(url: str):
    if not _is_postgres:
        return create_async_engine(url, echo=False)

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    return create_async_engine(
        _to_asyncpg_url(url),
        echo=False,
        connect_args={
            "ssl": ssl_ctx,
            "statement_cache_size": 0,   # отключает prepared statements в asyncpg
        },
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        pool_reset_on_return="rollback",
    )


engine = _make_async_engine(settings.DATABASE_URL)

async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


# ---------------------------------------------------------------------------
# init_db — psycopg2 (sync) чтобы DDL не шёл через PgBouncer asyncpg-путём
# ---------------------------------------------------------------------------
async def init_db():
    import asyncio

    def _create_tables():
        if _is_postgres:
            sync_engine = create_engine(
                _to_psycopg2_url(settings.DATABASE_URL),
                connect_args={"sslmode": "require"},
                poolclass=NullPool,
            )
        else:
            sync_engine = create_engine(settings.DATABASE_URL, poolclass=NullPool)

        try:
            with sync_engine.begin() as conn:
                Base.metadata.create_all(conn)
        finally:
            sync_engine.dispose()

    await asyncio.to_thread(_create_tables)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True)
    language = Column(String(5), default="en")
    is_premium = Column(Boolean, default=False)
    premium_until = Column(DateTime, nullable=True)
    is_banned = Column(Boolean, default=False)
    registered_at = Column(DateTime, server_default=func.now())

    referral_code = Column(String(16), unique=True, nullable=True)
    referred_by = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    referral_count = Column(Integer, default=0)


class Download(Base):
    __tablename__ = "downloads"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.id"))
    platform = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())


class Ad(Base):
    __tablename__ = "ads"

    id = Column(Integer, primary_key=True)
    message_text = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gen_ref_code() -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=8))


# ---------------------------------------------------------------------------
# Repositories
# ---------------------------------------------------------------------------

class AdRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_ad(self, text: str) -> Ad:
        ad = Ad(message_text=text)
        self.session.add(ad)
        await self.session.commit()
        return ad

    async def get_active_ads(self) -> list[Ad]:
        result = await self.session.execute(select(Ad).where(Ad.is_active == True))
        return result.scalars().all()

    async def get_all_ads(self) -> list[Ad]:
        result = await self.session.execute(select(Ad))
        return result.scalars().all()

    async def remove_ad(self, ad_id: int):
        await self.session.execute(delete(Ad).where(Ad.id == ad_id))
        await self.session.commit()

    async def toggle_ad(self, ad_id: int, active: bool):
        await self.session.execute(
            update(Ad).where(Ad.id == ad_id).values(is_active=active)
        )
        await self.session.commit()


class DownloadRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_download(self, user_id: int, platform: str):
        download = Download(user_id=user_id, platform=platform)
        self.session.add(download)

    async def get_total_downloads(self) -> int:
        result = await self.session.execute(select(func.count(Download.id)))
        return result.scalar()

    async def get_user_downloads_today(self, user_id: int) -> int:
        today = datetime.now().date()
        result = await self.session.execute(
            select(func.count(Download.id)).where(
                Download.user_id == user_id,
                Download.created_at >= today,
            )
        )
        return result.scalar()


class UserRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_user(self, user_id: int) -> User | None:
        result = await self.session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    async def get_user_by_referral_code(self, code: str) -> User | None:
        result = await self.session.execute(
            select(User).where(User.referral_code == code.upper())
        )
        return result.scalar_one_or_none()

    async def create_user(
        self, user_id: int, language: str = "en", referred_by: int | None = None
    ) -> User:
        code = _gen_ref_code()
        while True:
            existing = await self.get_user_by_referral_code(code)
            if not existing:
                break
            code = _gen_ref_code()

        user = User(
            id=user_id,
            language=language,
            referral_code=code,
            referred_by=referred_by,
        )
        self.session.add(user)
        await self.session.commit()
        return user

    async def set_referred_by(self, user_id: int, referrer_id: int):
        await self.session.execute(
            update(User).where(User.id == user_id).values(referred_by=referrer_id)
        )

    async def set_language(self, user_id: int, language: str):
        await self.session.execute(
            update(User).where(User.id == user_id).values(language=language)
        )
        await self.session.commit()

    async def set_premium(self, user_id: int, days: int):
        until = datetime.now() + timedelta(days=days)
        await self.session.execute(
            update(User).where(User.id == user_id).values(
                is_premium=True, premium_until=until
            )
        )
        await self.session.commit()

    async def remove_premium(self, user_id: int):
        await self.session.execute(
            update(User).where(User.id == user_id).values(
                is_premium=False, premium_until=None
            )
        )
        await self.session.commit()

    async def ban_user(self, user_id: int):
        await self.session.execute(
            update(User).where(User.id == user_id).values(is_banned=True)
        )
        await self.session.commit()

    async def unban_user(self, user_id: int):
        await self.session.execute(
            update(User).where(User.id == user_id).values(is_banned=False)
        )
        await self.session.commit()

    async def increment_referral_count(self, referrer_id: int):
        await self.session.execute(
            update(User)
            .where(User.id == referrer_id)
            .values(referral_count=User.referral_count + 1)
        )

    async def get_all_users_count(self) -> int:
        result = await self.session.execute(select(func.count(User.id)))
        return result.scalar()

    async def get_active_users_today(self) -> int:
        today = datetime.now().date()
        result = await self.session.execute(
            select(func.count(func.distinct(Download.user_id))).where(
                Download.created_at >= today
            )
        )
        return result.scalar()

    async def get_all_user_ids(self) -> list[int]:
        result = await self.session.execute(select(User.id))
        return result.scalars().all()