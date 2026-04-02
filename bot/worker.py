import asyncio
import logging
from services.queue_service import QueueService
from services.downloader import Downloader
from services.ad_service import AdService
from services.limit_service import LimitService
from database.user_repo import UserRepo
from database.download_repo import DownloadRepo
from database.db import async_session_maker
from bot.loader import bot


class Worker:
    def __init__(self):
        self.queue_service = QueueService()
        self.downloader = Downloader()
        self.ad_service = AdService()
        self.limit_service = LimitService()

    async def process_task(self, task: dict):
        user_id = task["user_id"]
        url = task["url"]
        platform = task["platform"]

        async with async_session_maker() as session:
            user_repo = UserRepo(session)
            download_repo = DownloadRepo(session)
            user = await user_repo.get_user(user_id)

            if not user or user.is_banned:
                return

            is_premium = user.is_premium
            limit_ok = await self.limit_service.check_and_increment(user_id, is_premium)
            if not limit_ok and not is_premium:
                await bot.send_message(user_id, "❌ Daily limit reached. Upgrade to premium.")
                return

            retries = 0
            video_bytes = None
            while retries < 3:
                video_bytes, error = await self.downloader.download(url)
                if video_bytes:
                    break
                retries += 1
                await asyncio.sleep(2)

            if video_bytes:
                await bot.send_video(user_id, video=video_bytes)
                await download_repo.add_download(user_id, platform)

                if not is_premium:
                    ads = await self.ad_service.get_active_ads()
                    if ads:
                        import random
                        ad = random.choice(ads)
                        await bot.send_message(user_id, f"📢 {ad.message_text}")
            else:
                await bot.send_message(user_id, f"❌ Failed to download: {error}")

    async def run(self):
        while True:
            task = await self.queue_service.pop_task()
            if task:
                await self.process_task(task)
            await asyncio.sleep(0.5)