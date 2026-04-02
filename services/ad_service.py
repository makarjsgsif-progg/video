from database.db import async_session_maker
from database.ad_repo import AdRepo

class AdService:
    async def get_active_ads(self):
        async with async_session_maker() as session:
            repo = AdRepo(session)
            return await repo.get_active_ads()