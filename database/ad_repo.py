from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import Ad


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