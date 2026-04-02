import time
from typing import Callable, Dict, Any, Awaitable
import redis.asyncio as redis
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from config.config import settings

redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

class ThrottleMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id
        key = f"throttle:{user_id}"
        last_call = await redis_client.get(key)
        now = time.time()
        if last_call and now - float(last_call) < 0.5:
            if isinstance(event, Message):
                await event.answer("⏳ Too many requests. Slow down.")
            return
        await redis_client.setex(key, 1, str(now))
        return await handler(event, data)