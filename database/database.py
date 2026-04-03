import ssl  # ДОБАВЬ ЭТО
from datetime import datetime, timedelta

from sqlalchemy import Column, Integer, String, Boolean, DateTime, BigInteger, Text, ForeignKey, select, update, delete, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

from config.config import settings

# --- ЭТОТ БЛОК НУЖЕН ДЛЯ SUPABASE ---
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
# ------------------------------------

# === Engine & Session ===
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    connect_args={
        "ssl": ssl_context,
        "prepared_statement_cache_size": 0,
        "statement_cache_size": 0
    }
)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# === Models ===
class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True)
    language = Column(String(2), default="en")
    is_premium = Column(Boolean, default=False)
    premium_until = Column(DateTime, nullable=True)
    is_banned = Column(Boolean, default=False)
    registered_at = Column(DateTime, server_default=func.now())

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

# === Repositories ===
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
        await self.session.execute(update(Ad).where(Ad.id == ad_id).values(is_active=active))
        await self.session.commit()

class DownloadRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_download(self, user_id: int, platform: str):
        download = Download(user_id=user_id, platform=platform)
        self.session.add(download)
        await self.session.commit()

    async def get_total_downloads(self) -> int:
        result = await self.session.execute(select(func.count(Download.id)))
        return result.scalar()

    async def get_user_downloads_today(self, user_id: int) -> int:
        today = datetime.now().date()
        result = await self.session.execute(select(func.count(Download.id)).where(
            Download.user_id == user_id,
            Download.created_at >= today
        ))
        return result.scalar()

class UserRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_user(self, user_id: int) -> User | None:
        result = await self.session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    async def create_user(self, user_id: int, language: str = "en") -> User:
        user = User(id=user_id, language=language)
        self.session.add(user)
        await self.session.commit()
        return user

    async def set_language(self, user_id: int, language: str):
        await self.session.execute(update(User).where(User.id == user_id).values(language=language))
        await self.session.commit()

    async def set_premium(self, user_id: int, days: int):
        until = datetime.now() + timedelta(days=days)
        await self.session.execute(update(User).where(User.id == user_id).values(is_premium=True, premium_until=until))
        await self.session.commit()

    async def remove_premium(self, user_id: int):
        await self.session.execute(update(User).where(User.id == user_id).values(is_premium=False, premium_until=None))
        await self.session.commit()

    async def ban_user(self, user_id: int):
        await self.session.execute(update(User).where(User.id == user_id).values(is_banned=True))
        await self.session.commit()

    async def unban_user(self, user_id: int):
        await self.session.execute(update(User).where(User.id == user_id).values(is_banned=False))
        await self.session.commit()

    async def get_all_users_count(self) -> int:
        # FIX: использовать COUNT(*) вместо загрузки всех записей в память
        result = await self.session.execute(select(func.count(User.id)))
        return result.scalar()

    async def get_active_users_today(self) -> int:
        # FIX: использовать COUNT(DISTINCT user_id) вместо загрузки всех записей в память
        today = datetime.now().date()
        result = await self.session.execute(
            select(func.count(func.distinct(Download.user_id))).where(Download.created_at >= today)
        )
        return result.scalar()