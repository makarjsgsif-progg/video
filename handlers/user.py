"""
handlers/user.py

Улучшения:
- /start: реферальная система теперь поддерживает оба формата приглашения:
  числовой user_id (?start=12345) и буквенный referral_code (?start=ABC123XY).
  Раньше обрабатывался только числовой формат — реферальные коды игнорировались.
- /status: новая команда — показывает текущий лимит, статус Premium и очередь.
  Полезна пользователям, у которых «зависла» задача.
- handle_url: извлекает URL из текста с помощью regex — работает даже если
  пользователь прислал ссылку вместе с текстом (например, «вот видео: https://...»)
- Кнопка «📥 Скачать видео» показывает список поддерживаемых платформ
  с актуальным лимитом пользователя.
- Профиль: поле реферального кода (для шаринга), форматирование дат.
- Все хендлеры: специфические except, профессиональное логирование.
"""

import logging
import re
from urllib.parse import urlparse

from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from database.database import UserRepo
from services.services import LimitService, QueueService
from utils.i18n import languages
from utils.platform_detector import detect_platform

router = Router()
logger = logging.getLogger(__name__)

# Regex для извлечения URL из произвольного текста
_URL_RE = re.compile(r"https?://[^\s]+", re.I)

# --------------------------------------------------------------------------- #
#  Синглтоны сервисов                                                          #
# --------------------------------------------------------------------------- #

_queue_service: QueueService | None = None
_limit_service: LimitService | None = None


def get_queue_service() -> QueueService:
    global _queue_service
    if _queue_service is None:
        _queue_service = QueueService()
    return _queue_service


def get_limit_service() -> LimitService:
    global _limit_service
    if _limit_service is None:
        _limit_service = LimitService()
    return _limit_service


# --------------------------------------------------------------------------- #
#  Клавиатура                                                                  #
# --------------------------------------------------------------------------- #

def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📥 Скачать видео"), KeyboardButton(text="👤 Мой профиль")],
            [KeyboardButton(text="🔗 Реферальная ссылка"), KeyboardButton(text="💎 Премиум")],
            [KeyboardButton(text="🌍 Язык"), KeyboardButton(text="ℹ️ Помощь")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Вставь ссылку на видео…",
    )


_KEYBOARD_BUTTONS = frozenset({
    "📥 Скачать видео",
    "👤 Мой профиль",
    "🔗 Реферальная ссылка",
    "💎 Премиум",
    "🌍 Язык",
    "ℹ️ Помощь",
})


# --------------------------------------------------------------------------- #
#  Утилиты                                                                     #
# --------------------------------------------------------------------------- #

def _is_valid_url(text: str) -> bool:
    """Быстрая проверка: начинается ли строка с http/https и имеет домен."""
    try:
        result = urlparse(text.strip())
        return result.scheme in ("http", "https") and bool(result.netloc)
    except Exception:
        return False


def _extract_url(text: str) -> str | None:
    """
    Извлекает первый URL из произвольного текста.
    Работает даже если пользователь написал «вот видос https://tiktok.com/...».
    """
    match = _URL_RE.search(text)
    return match.group(0).rstrip(".,!?)>»") if match else None


# --------------------------------------------------------------------------- #
#  /start                                                                      #
# --------------------------------------------------------------------------- #

@router.message(CommandStart())
async def cmd_start(message: Message, gettext, user_db, bot: Bot):
    args = message.text.split(maxsplit=1)
    ref_arg = args[1].strip() if len(args) > 1 else None

    # Реферальная система: принимаем и числовой ID, и буквенный referral_code
    if ref_arg and getattr(user_db, "referred_by", None) is None:
        await _process_referral(message, user_db, ref_arg, bot)

    await message.answer(
        gettext("welcome", name=message.from_user.first_name),
        reply_markup=main_keyboard(),
    )


async def _process_referral(message: Message, user_db, ref_arg: str, bot: Bot):
    """
    Обрабатывает реферальный аргумент /start.
    Поддерживает формат числового user_id и буквенного referral_code.
    """
    repo = UserRepo()
    referrer = None

    # Формат 1: числовой user_id (?start=123456)
    try:
        referrer_id = int(ref_arg)
        if referrer_id != message.from_user.id:
            referrer = await repo.get_user(referrer_id)
    except ValueError:
        pass

    # Формат 2: буквенный referral_code (?start=ABC123XY)
    if referrer is None and ref_arg.isalnum() and not ref_arg.isdigit():
        try:
            referrer = await repo.get_user_by_referral_code(ref_arg)
        except Exception as e:
            logger.debug(f"Referral code lookup failed for {ref_arg!r}: {e}")

    if referrer is None:
        return

    referrer_id = referrer.id
    if referrer_id == message.from_user.id:
        return  # Нельзя пригласить самого себя

    try:
        await repo.set_referred_by(message.from_user.id, referrer_id)
        await repo.increment_referral_count(referrer_id)
        await get_limit_service().add_referral_bonus(referrer_id, amount=5)

        try:
            await bot.send_message(
                referrer_id,
                f"🎉 <b>По твоей ссылке зарегистрировался новый пользователь!</b>\n\n"
                f"Тебе начислено <b>+5 бонусных загрузок</b> 🚀\n\n"
                f"Продолжай делиться — каждый друг = ещё +5!",
            )
        except Exception:
            pass  # Пользователь мог заблокировать бота — не критично

    except Exception as e:
        logger.exception(f"Referral processing error for {message.from_user.id}: {e}")


# --------------------------------------------------------------------------- #
#  Кнопка «Скачать видео»                                                     #
# --------------------------------------------------------------------------- #

@router.message(F.text == "📥 Скачать видео")
async def btn_download(message: Message, user_db):
    try:
        ls = get_limit_service()
        used, limit = await ls.get_usage(message.from_user.id)
        is_premium = getattr(user_db, "is_premium", False)

        limit_line = (
            "💎 Безлимитные загрузки (Premium)" if is_premium
            else f"⬇️ Загрузок сегодня: <b>{used}/{limit}</b>"
        )

        await message.answer(
            f"🔗 <b>Отправь ссылку на видео</b>\n\n"
            f"Поддерживаемые платформы:\n"
            f"🎵 TikTok · 📸 Instagram · 🐦 Twitter/X\n"
            f"🤖 Reddit · 👤 Facebook · 🎬 Vimeo\n"
            f"🎮 Twitch · 📌 Pinterest · 👻 Snapchat и другие!\n\n"
            f"{limit_line}\n\n"
            f"Просто вставь ссылку — скачаю мгновенно ⚡"
        )
    except Exception as e:
        logger.exception(f"btn_download error for {message.from_user.id}: {e}")
        await message.answer(
            "🔗 <b>Отправь ссылку на видео</b>\n\n"
            "Просто вставь ссылку — скачаю мгновенно ⚡"
        )


# --------------------------------------------------------------------------- #
#  Профиль                                                                     #
# --------------------------------------------------------------------------- #

@router.message(F.text == "👤 Мой профиль")
async def btn_profile(message: Message, user_db):
    try:
        ls = get_limit_service()
        used, limit = await ls.get_usage(message.from_user.id)

        premium_status = "✅ Активен" if user_db.is_premium else "❌ Нет"
        premium_line = ""
        if user_db.is_premium and getattr(user_db, "premium_until", None):
            premium_line = f"\n📅 Действует до: <b>{user_db.premium_until.strftime('%d.%m.%Y')}</b>"

        reg_line = ""
        if getattr(user_db, "registered_at", None):
            reg_line = f"\n📆 В боте с: <b>{user_db.registered_at.strftime('%d.%m.%Y')}</b>"

        bonus = 0
        try:
            raw = await ls.redis.get(f"referral_bonus:{message.from_user.id}")
            bonus = int(raw or 0)
        except Exception:
            pass

        ref_code = getattr(user_db, "referral_code", None) or "—"

        await message.answer(
            f"👤 <b>Твой профиль</b>\n\n"
            f"🆔 ID: <code>{message.from_user.id}</code>{reg_line}\n"
            f"⬇️ Загрузок сегодня: <b>{used}/{limit}</b>\n"
            f"🎁 Реферальных бонусов: <b>+{bonus}</b>\n"
            f"💎 Премиум: {premium_status}{premium_line}\n"
            f"👥 Приглашено друзей: <b>{getattr(user_db, 'referral_count', 0) or 0}</b>\n"
            f"🔗 Твой код: <code>{ref_code}</code>"
        )
    except Exception as e:
        logger.exception(f"btn_profile error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось загрузить профиль. Попробуй позже.")


# --------------------------------------------------------------------------- #
#  Реферальная ссылка                                                          #
# --------------------------------------------------------------------------- #

@router.message(F.text == "🔗 Реферальная ссылка")
@router.message(Command("referral"))
async def cmd_referral(message: Message, user_db, bot: Bot):
    try:
        me = await bot.get_me()
        user_id = message.from_user.id

        # Предпочитаем буквенный referral_code — выглядит аккуратнее
        ref_param = getattr(user_db, "referral_code", None) or str(user_id)
        ref_link = f"https://t.me/{me.username}?start={ref_param}"
        count = getattr(user_db, "referral_count", 0) or 0

        bonus = 0
        try:
            ls = get_limit_service()
            raw = await ls.redis.get(f"referral_bonus:{user_id}")
            bonus = int(raw or 0)
        except Exception:
            pass

        share_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="📤 Поделиться ссылкой",
                url=(
                    f"https://t.me/share/url?url={ref_link}"
                    f"&text=Скачивай видео с TikTok, Instagram и других платформ за секунды!"
                ),
            )
        ]])

        await message.answer(
            f"🔗 <b>Твоя реферальная ссылка:</b>\n"
            f"<code>{ref_link}</code>\n\n"
            f"👥 Приглашено друзей: <b>{count}</b>\n"
            f"🎁 Накоплено бонусных загрузок: <b>+{bonus}</b>\n\n"
            f"За каждого приглашённого — <b>+5 загрузок</b> навсегда!\n"
            f"Чем больше друзей — тем больше видео 🚀",
            reply_markup=share_kb,
        )
    except Exception as e:
        logger.exception(f"cmd_referral error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось загрузить реферальную ссылку.")


# --------------------------------------------------------------------------- #
#  Премиум                                                                     #
# --------------------------------------------------------------------------- #

@router.message(F.text == "💎 Премиум")
@router.message(Command("premium"))
async def cmd_premium(message: Message, gettext, user_db):
    try:
        if user_db.is_premium:
            until = (
                user_db.premium_until.strftime("%d.%m.%Y")
                if getattr(user_db, "premium_until", None)
                else "∞"
            )
            await message.answer(gettext("premium_active", until=until))
        else:
            await message.answer(gettext("premium_info"))
    except Exception as e:
        logger.exception(f"cmd_premium error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось загрузить информацию о Премиуме.")


# --------------------------------------------------------------------------- #
#  Статус задачи                                                               #
# --------------------------------------------------------------------------- #

@router.message(Command("status"))
async def cmd_status(message: Message, user_db):
    """
    Показывает текущий статус: лимит загрузок, Premium, длину очереди.
    Полезна пользователям, которые думают, что задача «зависла».
    """
    try:
        ls = get_limit_service()
        qs = get_queue_service()
        used, limit = await ls.get_usage(message.from_user.id)
        queue_len = await qs.get_queue_length()
        is_premium = getattr(user_db, "is_premium", False)

        premium_line = "💎 Premium: ✅ Активен (безлимит)" if is_premium else f"📊 Лимит: <b>{used}/{limit}</b> загрузок сегодня"

        queue_status = (
            "✅ Очередь пуста" if queue_len == 0
            else f"⏳ В очереди: <b>{queue_len}</b> задач"
        )

        await message.answer(
            f"📋 <b>Статус сервиса</b>\n\n"
            f"{premium_line}\n"
            f"{queue_status}\n\n"
            f"Если видео долго не приходит — попробуй отправить ссылку ещё раз."
        )
    except Exception as e:
        logger.exception(f"cmd_status error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось получить статус.")


# --------------------------------------------------------------------------- #
#  Язык                                                                        #
# --------------------------------------------------------------------------- #

@router.message(F.text == "🌍 Язык")
@router.message(Command("set_language"))
async def cmd_set_language(message: Message, gettext):
    try:
        items = list(languages.items())
        kb = []
        for i in range(0, len(items), 2):
            row = [InlineKeyboardButton(text=items[i][1], callback_data=f"lang_{items[i][0]}")]
            if i + 1 < len(items):
                row.append(
                    InlineKeyboardButton(text=items[i + 1][1], callback_data=f"lang_{items[i + 1][0]}")
                )
            kb.append(row)

        await message.answer(
            gettext("choose_language"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
        )
    except Exception as e:
        logger.exception(f"cmd_set_language error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось открыть выбор языка.")


# --------------------------------------------------------------------------- #
#  Помощь                                                                      #
# --------------------------------------------------------------------------- #

@router.message(F.text == "ℹ️ Помощь")
@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ <b>Помощь</b>\n\n"
        "<b>Как скачать видео?</b>\n"
        "1. Скопируй ссылку из приложения\n"
        "2. Вставь её в этот чат\n"
        "3. Получи видео без водяного знака!\n\n"
        "<b>Поддерживаемые платформы:</b>\n"
        "🎵 TikTok · 📸 Instagram · 🐦 Twitter/X\n"
        "🤖 Reddit · 👤 Facebook · 🎬 Vimeo\n"
        "🎮 Twitch · 📌 Pinterest · 👻 Snapchat\n\n"
        "<b>Лимиты:</b>\n"
        "• Бесплатно: 5 загрузок в день\n"
        "• 💎 Премиум: безлимитно\n"
        "• 👥 Реферал: +5 загрузок за каждого друга\n\n"
        "<b>Команды:</b>\n"
        "/start — главное меню\n"
        "/status — статус очереди и лимитов\n"
        "/referral — реферальная ссылка\n"
        "/premium — информация о Премиуме\n"
        "/set_language — сменить язык\n"
        "/help — эта справка"
    )


# --------------------------------------------------------------------------- #
#  Обработчик URL (основной)                                                   #
# --------------------------------------------------------------------------- #

@router.message(F.text, ~F.text.startswith("/"))
async def handle_url(message: Message, gettext, user_db):
    text = message.text.strip()

    # Фильтр кнопок клавиатуры
    if text in _KEYBOARD_BUTTONS:
        return

    # Пробуем извлечь URL из текста (на случай если пользователь добавил текст к ссылке)
    url = _extract_url(text)

    if not url:
        await message.answer(
            "🤔 <b>Это не похоже на ссылку</b>\n\n"
            "Отправь мне ссылку, начинающуюся с <code>http://</code> или <code>https://</code>.\n"
            "Например: <code>https://www.tiktok.com/...</code>"
        )
        return

    platform = detect_platform(url)
    if not platform:
        await message.answer(gettext("unsupported_url"))
        return

    try:
        await get_queue_service().push_task(message.from_user.id, url, str(platform))
        await message.answer(gettext("processing"))
    except Exception as e:
        logger.exception(f"Failed to push task for {message.from_user.id}: {e}")
        await message.answer(
            "⚠️ <b>Сервис временно недоступен</b>\n\n"
            "Не удалось поставить задачу в очередь. Попробуй через несколько секунд."
        )