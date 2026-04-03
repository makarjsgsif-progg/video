import asyncio
import json
from io import BytesIO

import redis.asyncio as redis
import yt_dlp
import httpx

from config.config import settings
from database.database import async_session_maker, AdRepo


class AdService:
    async def get_active_ads(self):
        async with async_session_maker() as session:
            repo = AdRepo(session)
            return await repo.get_active_ads()


class Downloader:
    def __init__(self):
        self.opts = {
            'format': 'best[height<=720]',
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'noplaylist': True,
        }

    async def download(self, url: str) -> tuple[BytesIO | None, str | None]:
        def sync_download():
            try:
                with yt_dlp.YoutubeDL(self.opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    url_to_download = info['url']
                    with httpx.Client(timeout=30) as client:
                        resp = client.get(url_to_download)
                        if resp.status_code == 200:
                            return BytesIO(resp.content), None
                        else:
                            return None, "HTTP error"
            except Exception as e:
                return None, str(e)

        return await asyncio.to_thread(sync_download)


class LimitService:
    def __init__(self):
        if settings.REDIS_URL.startswith("rediss://") or "upstash.io" in settings.REDIS_URL:
            self.redis = redis.from_url(settings.REDIS_URL, ssl=True, decode_responses=True)
        else:
            self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)

    async def check_and_increment(self, user_id: int, is_premium: bool) -> bool:
        if is_premium:
            return True
        key = f"daily_limit:{user_id}"
        current = await self.redis.get(key)
        if current is None:
            await self.redis.setex(key, 86400, 1)
            return True
        if int(current) < settings.DEFAULT_DAILY_LIMIT:
            await self.redis.incr(key)
            return True
        return False


class QueueService:
    def __init__(self):
        if settings.REDIS_URL.startswith("rediss://") or "upstash.io" in settings.REDIS_URL:
            self.redis = redis.from_url(settings.REDIS_URL, ssl=True, decode_responses=True)
        else:
            self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.queue_key = "download_queue"

    async def push_task(self, user_id: int, url: str, platform: str):
        task = json.dumps({"user_id": user_id, "url": url, "platform": platform})
        await self.redis.lpush(self.queue_key, task)

    async def pop_task(self) -> dict | None:
        task = await self.redis.rpop(self.queue_key)
        if task:
            return json.loads(task)
        return None