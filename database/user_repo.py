from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import User
from datetime import datetime, timedelta


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
        result = await self.session.execute(select(User))
        return len(result.scalars().all())

    async def get_active_users_today(self) -> int:
        today = datetime.now().date()
        from database.models import Download
        result = await self.session.execute(select(Download.user_id).where(Download.created_at >= today))
        return len(set(result.scalars().all()))