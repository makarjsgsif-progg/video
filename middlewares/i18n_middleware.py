from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from utils.i18n import get_text

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