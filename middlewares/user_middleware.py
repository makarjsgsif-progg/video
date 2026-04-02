from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from database.db import async_session_maker
from database.user_repo import UserRepo


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