from aiogram import Router, F
from aiogram.types import CallbackQuery
from database.database import async_session_maker, UserRepo
from utils.i18n import get_text

router = Router()

@router.callback_query(F.data.startswith("lang_"))
async def set_language_callback(call: CallbackQuery, user_db):
    lang = call.data.split("_")[1]
    async with async_session_maker() as session:
        repo = UserRepo(session)
        await repo.set_language(call.from_user.id, lang)

    # FIX: call.message может быть None если сообщение старше 48ч или удалено Telegram'ом
    if call.message:
        try:
            await call.message.delete()
        except Exception:
            pass
        await call.message.answer(get_text(lang, "language_set"))

    await call.answer(get_text(lang, "language_set"))