from aiogram import Router, F
from aiogram.types import CallbackQuery

from database.database import async_session_maker, UserRepo
from services.services import LimitService
from utils.i18n import languages, get_text

router = Router()

limit_service = LimitService()


@router.callback_query(F.data.startswith("lang_"))
async def set_language_callback(callback: CallbackQuery, user_db):
    lang_code = callback.data.split("_", 1)[1]

    if lang_code not in languages:
        await callback.answer("❌ Unknown language", show_alert=True)
        return

    async with async_session_maker() as session:
        repo = UserRepo(session)
        await repo.set_language(callback.from_user.id, lang_code)

    text = get_text(lang_code, "language_set")
    await callback.message.edit_text(f"✅ {text}")
    await callback.answer()