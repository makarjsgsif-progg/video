import redis.asyncio as redis
from config.config import settings

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