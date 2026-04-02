import json
import redis.asyncio as redis
from config.config import settings

class QueueService:
    def __init__(self):
        # Поддержка TLS для Upstash
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