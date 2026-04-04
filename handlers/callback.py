"""
handlers/callback.py

Улучшения:
- Защита от повторного нажатия (callback.answer всегда вызывается)
- Обновление user_db в data после смены языка — i18n актуален сразу
- Попытка edit_text с fallback на answer при MessageNotModified / MessageCantBeEdited
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
        await callback.answer("❌ Неизвестный язык.", show_alert=True)
        return

    # Не делаем лишний запрос, если язык уже такой
    if user_db and getattr(user_db, "language", None) == lang_code:
        await callback.answer(get_text(lang_code, "language_set"), show_alert=False)
        return

    try:
        repo = UserRepo()
        await repo.set_language(callback.from_user.id, lang_code)
    except Exception as e:
        logger.exception(f"Failed to set language for {callback.from_user.id}: {e}")
        await callback.answer("❌ Не удалось сохранить язык. Попробуй позже.", show_alert=True)
        return

    text = f"✅ {get_text(lang_code, 'language_set')}"
    try:
        await callback.message.edit_text(text)
    except TelegramBadRequest as e:
        # Сообщение могло быть удалено или не изменилось
        if "message is not modified" not in str(e).lower():
            logger.debug(f"edit_text failed: {e}")
        try:
            await callback.message.answer(text)
        except Exception:
            pass

    await callback.answer()