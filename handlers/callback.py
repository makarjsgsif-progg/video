"""
handlers/callback.py

Mega Upgrade:
- После смены языка обновляется клавиатура на новый язык (reply-markup пересылается)
- Защита от повторного нажатия
- Полная обработка ошибок
"""

import logging

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery

from database.database import UserRepo
from utils.i18n import languages, get_text

router = Router()
logger = logging.getLogger(__name__)


@router.callback_query(F.data.startswith("lang_"))
async def set_language_callback(callback: CallbackQuery, user_db):
    lang_code = callback.data.split("_", 1)[1]

    if lang_code not in languages:
        await callback.answer("❌ Unknown language.", show_alert=True)
        return

    if user_db and getattr(user_db, "language", None) == lang_code:
        await callback.answer(get_text(lang_code, "language_set"), show_alert=False)
        return

    try:
        repo = UserRepo()
        await repo.set_language(callback.from_user.id, lang_code)
    except Exception as e:
        logger.exception(f"Failed to set language for {callback.from_user.id}: {e}")
        await callback.answer("❌ Failed to save language. Try again later.", show_alert=True)
        return

    confirm_text = f"✅ {get_text(lang_code, 'language_set')}"
    try:
        await callback.message.edit_text(confirm_text)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.debug(f"edit_text failed: {e}")
        try:
            await callback.message.answer(confirm_text)
        except Exception:
            pass

    # Пересылаем клавиатуру на новом языке
    try:
        from handlers.user import main_keyboard
        await callback.message.answer(
            get_text(lang_code, "choose_language").split(":")[0] + " ✅",
            reply_markup=main_keyboard(lang_code),
        )
    except Exception as e:
        logger.debug(f"Failed to send new keyboard after lang change: {e}")

    await callback.answer()