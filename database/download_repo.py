from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import Download
from datetime import datetime


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