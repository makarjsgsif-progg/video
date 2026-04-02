from aiogram import Router, F
from aiogram.types import CallbackQuery
from database.db import async_session_maker
from database.user_repo import UserRepo
from utils.i18n import get_text

router = Router()

@router.callback_query(F.data.startswith("lang_"))
async def set_language_callback(call: CallbackQuery):
    lang = call.data.split("_")[1]
    async with async_session_maker() as session:
        repo = UserRepo(session)
        await repo.set_language(call.from_user.id, lang)
    await call.message.delete()
    await call.message.answer(get_text(lang, "language_set"))
    await call.answer()