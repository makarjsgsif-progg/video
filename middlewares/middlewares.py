import time
from typing import Callable, Dict, Any, Awaitable

from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery

from database.database import async_session_maker, UserRepo
from utils.i18n import get_text

# Очищаем записи старше этого порога (секунды)
THROTTLE_TTL = 60


class UserMiddleware(BaseMiddleware):
    async def __call__(
            self,
            handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
            event: Message | CallbackQuery,
            data: Dict[str, Any]
    ) -> Any:
        user_id = None
        if isinstance(event, Message):
            user_id = event.from_user.id
        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id

        if user_id:
            async with async_session_maker() as session:
                repo = UserRepo(session)
                user = await repo.get_user(user_id)
                if not user:
                    user = await repo.create_user(user_id)
                data["user_db"] = user

        return await handler(event, data)


class ThrottleMiddleware(BaseMiddleware):
    def __init__(self):
        self.last_calls: dict[int, float] = {}

    async def __call__(
            self,
            handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
            event: Message | CallbackQuery,
            data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id
        now = time.time()
        last = self.last_calls.get(user_id, 0)

        if now - last < 0.5:
            if isinstance(event, Message):
                await event.answer("⏳ Too many requests. Slow down.")
            return

        self.last_calls[user_id] = now

        # FIX: периодическая очистка устаревших записей во избежание утечки памяти
        if len(self.last_calls) > 10_000:
            cutoff = now - THROTTLE_TTL
            self.last_calls = {uid: ts for uid, ts in self.last_calls.items() if ts > cutoff}

        return await handler(event, data)


class I18nMiddleware(BaseMiddleware):
    async def __call__(
            self,
            handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
            event: Message | CallbackQuery,
            data: Dict[str, Any]
    ) -> Any:
        user = data.get("user_db")
        lang = user.language if user else "en"
        data["gettext"] = lambda key, **kwargs: get_text(lang, key, **kwargs)
        return await handler(event, data)