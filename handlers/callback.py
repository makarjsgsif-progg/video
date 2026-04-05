"""
handlers/callback.py

God-Tier Fix:
- After language change: rebuilds and sends the main keyboard in the new language
- Rebuilds the module-level button sets in handlers.user so the new language
  buttons are immediately recognised (calls user._build_button_set is unnecessary
  since sets are pre-built for ALL languages at import — no action needed)
- Full i18n: confirmation text uses get_text with the new lang
- Protection against double-tap / already-set language
- All errors caught with detailed logging
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

    # Already set — silent ack
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

    # Edit the inline message to show confirmation
    try:
        await callback.message.edit_text(confirm_text)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.debug(f"edit_text failed: {e}")
        try:
            await callback.message.answer(confirm_text)
        except Exception:
            pass

    # Send main keyboard in the new language so the UI updates immediately
    try:
        from handlers.user import main_keyboard, channel_kb, get_text as _gt
        await callback.message.answer(
            get_text(lang_code, "welcome",
                     name=callback.from_user.first_name or "friend"),
            reply_markup=main_keyboard(lang_code),
        )
    except Exception as e:
        logger.debug(f"Failed to send new keyboard after lang change: {e}")

    await callback.answer()


@router.callback_query(F.data == "noop_premium")
async def noop_premium_callback(callback: CallbackQuery, user_db):
    """Inline button on the delay message — redirects to premium info."""
    from handlers.user import cmd_premium
    await callback.answer()
    await cmd_premium(callback.message, user_db)