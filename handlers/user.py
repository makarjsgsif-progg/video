from aiogram import Router, F
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command
from utils.platform_detector import detect_platform
from services.services import QueueService
from utils.i18n import languages

router = Router()

# FIX: инициализация на уровне модуля при импорте до загрузки настроек —
# заменено на ленивую инициализацию через функцию
_queue_service: QueueService | None = None

def get_queue_service() -> QueueService:
    global _queue_service
    if _queue_service is None:
        _queue_service = QueueService()
    return _queue_service


@router.message(Command("start"))
async def cmd_start(message: Message, gettext, user_db):
    await message.answer(gettext("welcome", name=message.from_user.first_name))
    kb = []
    for code, name in languages.items():
        kb.append([InlineKeyboardButton(text=name, callback_data=f"lang_{code}")])
    await message.answer(gettext("choose_language"), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.message(Command("set_language"))
async def cmd_set_language(message: Message, gettext):
    kb = []
    for code, name in languages.items():
        kb.append([InlineKeyboardButton(text=name, callback_data=f"lang_{code}")])
    await message.answer(gettext("choose_language"), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.message(Command("premium"))
async def cmd_premium(message: Message, gettext, user_db):
    if user_db.is_premium:
        until = user_db.premium_until.strftime("%Y-%m-%d") if user_db.premium_until else "forever"
        await message.answer(gettext("premium_active", until=until))
    else:
        await message.answer(gettext("premium_info"))

@router.message(F.text)
async def handle_url(message: Message, gettext, user_db):
    url = message.text.strip()
    platform = detect_platform(url)
    if not platform:
        await message.answer(gettext("unsupported_url"))
        return

    if user_db.is_banned:
        await message.answer(gettext("banned"))
        return

    await get_queue_service().push_task(message.from_user.id, url, platform)
    await message.answer(gettext("processing"))