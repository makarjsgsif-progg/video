"""
handlers/user.py

Mega Upgrade:
- Полная i18n: все кнопки клавиатуры, тексты и алерты переведены по языку user_db
- Профиль в точном формате ТЗ с реферальной ссылкой через bot_username
- Premium: 3 тарифа (Неделя / Месяц / Навсегда), профессиональный раздел
- Активный Premium: красивое сообщение с датой, без кнопки "купить"
- Link detection: ссылка → скачивание; текст → поддержка + главное меню
- Inline "Наш канал" прикреплён ко всем ключевым ответам
- Вирусный копирайтинг на всех языках
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
from utils.i18n import languages, get_text
from utils.platform_detector import detect_platform

router = Router()
logger = logging.getLogger(__name__)

# Regex для извлечения URL из произвольного текста
_URL_RE = re.compile(r"https?://[^\s]+", re.I)

# Куда форвардить support-сообщения
SUPPORT_USERNAME = "@YourBotPayments"
CHANNEL_URL = "https://t.me/downloaddq"

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
#  Inline-кнопки                                                               #
# --------------------------------------------------------------------------- #

def channel_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    """Inline-клавиатура с кнопкой «Наш канал» — на языке пользователя."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL),
    ]])


def channel_and_support_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    """Канал + поддержка в одной строке."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL),
        InlineKeyboardButton(
            text=get_text(lang, "btn_support"),
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        ),
    ]])


def contact_support_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=get_text(lang, "btn_contact_support"),
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        ),
    ]])


def premium_buy_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=get_text(lang, "btn_buy_tariffs"),
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        )],
        [InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL)],
    ])


def premium_active_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL),
    ]])


def share_kb(ref_link: str, lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=get_text(lang, "btn_share"),
            url=(
                f"https://t.me/share/url?url={ref_link}"
                f"&text=🔥 Скачивай видео из TikTok, Instagram и 50+ платформ бесплатно!"
            ),
        ),
    ]])


def donate_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="💸 " + ("Поддержать проект" if lang == "ru" else "Support project"),
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        )],
        [InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL)],
    ])


# --------------------------------------------------------------------------- #
#  Локализованная Reply-клавиатура                                             #
# --------------------------------------------------------------------------- #

def main_keyboard(lang: str = "ru") -> ReplyKeyboardMarkup:
    """Главная клавиатура с кнопками на языке пользователя."""
    t = lambda key: get_text(lang, key)
    placeholder = {
        "ru": "Вставь ссылку — скачаю за 5 сек ⚡",
        "en": "Paste link — downloaded in 5 sec ⚡",
        "es": "Pega el enlace — descargo en 5 seg ⚡",
        "pt": "Cole o link — baixo em 5 seg ⚡",
        "de": "Link einfügen — in 5 Sek. heruntergeladen ⚡",
        "fr": "Colle le lien — téléchargé en 5 sec ⚡",
        "hi": "लिंक पेस्ट करें — 5 सेकंड में डाउनलोड ⚡",
        "ar": "الصق الرابط — تحميل في 5 ثوانٍ ⚡",
    }.get(lang, "Paste link — downloaded in 5 sec ⚡")

    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t("btn_download")), KeyboardButton(text=t("btn_profile"))],
            [KeyboardButton(text=t("btn_referral")), KeyboardButton(text=t("btn_premium"))],
            [KeyboardButton(text=t("btn_donate")), KeyboardButton(text=t("btn_language"))],
            [KeyboardButton(text=t("btn_support"))],
        ],
        resize_keyboard=True,
        input_field_placeholder=placeholder,
    )


def _get_keyboard_buttons(lang: str) -> frozenset:
    """Возвращает набор текстов кнопок для фильтрации."""
    keys = [
        "btn_download", "btn_profile", "btn_referral", "btn_premium",
        "btn_donate", "btn_language", "btn_support",
    ]
    return frozenset(get_text(lang, k) for k in keys)


# --------------------------------------------------------------------------- #
#  Утилиты                                                                     #
# --------------------------------------------------------------------------- #

def _extract_url(text: str) -> str | None:
    match = _URL_RE.search(text)
    return match.group(0).rstrip(".,!?)>»") if match else None


def _get_lang(user_db) -> str:
    return getattr(user_db, "language", "ru") or "ru"


# --------------------------------------------------------------------------- #
#  /start                                                                      #
# --------------------------------------------------------------------------- #

@router.message(CommandStart())
async def cmd_start(message: Message, user_db, bot: Bot):
    lang = _get_lang(user_db)
    args = message.text.split(maxsplit=1)
    ref_arg = args[1].strip() if len(args) > 1 else None

    if ref_arg and getattr(user_db, "referred_by", None) is None:
        await _process_referral(message, user_db, ref_arg, bot, lang)

    name = message.from_user.first_name or "друг"
    await message.answer(
        get_text(lang, "welcome", name=name),
        reply_markup=main_keyboard(lang),
    )
    await message.answer(
        get_text(lang, "channel_subscribe"),
        reply_markup=channel_kb(lang),
    )


async def _process_referral(message: Message, user_db, ref_arg: str, bot: Bot, lang: str):
    repo = UserRepo()
    referrer = None

    try:
        referrer_id = int(ref_arg)
        if referrer_id != message.from_user.id:
            referrer = await repo.get_user(referrer_id)
    except ValueError:
        pass

    if referrer is None and ref_arg.isalnum() and not ref_arg.isdigit():
        try:
            referrer = await repo.get_user_by_referral_code(ref_arg)
        except Exception as e:
            logger.debug(f"Referral code lookup failed for {ref_arg!r}: {e}")

    if referrer is None:
        return

    referrer_id = referrer.id
    if referrer_id == message.from_user.id:
        return

    try:
        await repo.set_referred_by(message.from_user.id, referrer_id)
        await repo.increment_referral_count(referrer_id)
        await get_limit_service().add_referral_bonus(referrer_id, amount=5)

        ref_lang = getattr(referrer, "language", "ru") or "ru"
        try:
            await bot.send_message(
                referrer_id,
                get_text(ref_lang, "referral_bonus_notify"),
            )
        except Exception:
            pass

    except Exception as e:
        logger.exception(f"Referral processing error for {message.from_user.id}: {e}")


# --------------------------------------------------------------------------- #
#  Кнопка «Скачать видео»                                                      #
# --------------------------------------------------------------------------- #

@router.message(F.func(lambda m: m.text and m.text in _all_btn_download_texts()))
async def btn_download(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        ls = get_limit_service()
        used, limit = await ls.get_usage(message.from_user.id)
        is_premium = getattr(user_db, "is_premium", False)

        if is_premium:
            limit_line = get_text(lang, "limit_premium")
        else:
            remaining = max(0, limit - used)
            limit_line = get_text(lang, "limit_free", remaining=remaining, limit=limit)

        await message.answer(
            get_text(lang, "download_hint", limit_line=limit_line),
            reply_markup=channel_kb(lang),
        )
    except Exception as e:
        logger.exception(f"btn_download error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


def _all_btn_download_texts() -> set:
    return {get_text(lang, "btn_download") for lang in languages}


# --------------------------------------------------------------------------- #
#  Профиль                                                                     #
# --------------------------------------------------------------------------- #

@router.message(F.func(lambda m: m.text and m.text in _all_btn_profile_texts()))
@router.message(Command("profile"))
async def btn_profile(message: Message, user_db, bot: Bot):
    lang = _get_lang(user_db)
    try:
        ls = get_limit_service()
        used, limit = await ls.get_usage(message.from_user.id)
        is_premium = getattr(user_db, "is_premium", False)
        me = await bot.get_me()

        if is_premium and getattr(user_db, "premium_until", None):
            until_str = user_db.premium_until.strftime("%d.%m.%Y")
            status_text = get_text(lang, "premium_status_active", until=until_str)
        elif is_premium:
            status_text = get_text(lang, "premium_status_active", until="∞")
        else:
            status_text = get_text(lang, "premium_status_inactive")

        ref_param = getattr(user_db, "referral_code", None) or str(message.from_user.id)
        ref_count = getattr(user_db, "referral_count", 0) or 0

        lang_display = languages.get(lang, lang.upper())

        await message.answer(
            get_text(
                lang, "profile_text",
                lang=lang_display,
                used=used,
                limit=limit,
                status_text=status_text,
                ref_count=ref_count,
                bot_username=me.username,
                ref_param=ref_param,
            ),
            reply_markup=channel_kb(lang),
        )
    except Exception as e:
        logger.exception(f"btn_profile error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


def _all_btn_profile_texts() -> set:
    return {get_text(lang, "btn_profile") for lang in languages}


# --------------------------------------------------------------------------- #
#  Реферальная ссылка                                                          #
# --------------------------------------------------------------------------- #

@router.message(F.func(lambda m: m.text and m.text in _all_btn_referral_texts()))
@router.message(Command("referral"))
async def cmd_referral(message: Message, user_db, bot: Bot):
    lang = _get_lang(user_db)
    try:
        me = await bot.get_me()
        user_id = message.from_user.id

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

        await message.answer(
            get_text(
                lang, "referral_text",
                bot_username=me.username,
                ref_param=ref_param,
                count=count,
                bonus=bonus,
            ),
            reply_markup=share_kb(ref_link, lang),
        )
    except Exception as e:
        logger.exception(f"cmd_referral error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


def _all_btn_referral_texts() -> set:
    return {get_text(lang, "btn_referral") for lang in languages}


# --------------------------------------------------------------------------- #
#  Premium                                                                     #
# --------------------------------------------------------------------------- #

@router.message(F.func(lambda m: m.text and m.text in _all_btn_premium_texts()))
@router.message(Command("premium"))
async def cmd_premium(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        is_premium = getattr(user_db, "is_premium", False)

        if is_premium:
            until = (
                user_db.premium_until.strftime("%d.%m.%Y")
                if getattr(user_db, "premium_until", None)
                else "∞"
            )
            await message.answer(
                get_text(lang, "premium_active_text", until=until),
                reply_markup=premium_active_kb(lang),
            )
            return

        await message.answer(
            get_text(lang, "premium_tariffs"),
            reply_markup=premium_buy_kb(lang),
        )
    except Exception as e:
        logger.exception(f"cmd_premium error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


def _all_btn_premium_texts() -> set:
    return {get_text(lang, "btn_premium") for lang in languages}


# --------------------------------------------------------------------------- #
#  Донат                                                                       #
# --------------------------------------------------------------------------- #

@router.message(F.func(lambda m: m.text and m.text in _all_btn_donate_texts()))
@router.message(Command("donate"))
async def cmd_donate(message: Message, user_db):
    lang = _get_lang(user_db)
    await message.answer(
        get_text(lang, "donate_text", support=SUPPORT_USERNAME),
        reply_markup=donate_kb(lang),
    )


def _all_btn_donate_texts() -> set:
    return {get_text(lang, "btn_donate") for lang in languages}


# --------------------------------------------------------------------------- #
#  Статус задачи                                                               #
# --------------------------------------------------------------------------- #

@router.message(Command("status"))
async def cmd_status(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        ls = get_limit_service()
        qs = get_queue_service()
        used, limit = await ls.get_usage(message.from_user.id)
        queue_len = await qs.get_queue_length()
        is_premium = getattr(user_db, "is_premium", False)

        if is_premium:
            premium_line = get_text(lang, "status_premium_line")
        else:
            premium_line = get_text(lang, "status_limit_line", used=used, limit=limit)

        if queue_len == 0:
            queue_status = get_text(lang, "status_queue_empty")
        else:
            queue_status = get_text(lang, "status_queue_busy", count=queue_len)

        await message.answer(
            get_text(lang, "status_text", premium_line=premium_line, queue_status=queue_status),
        )
    except Exception as e:
        logger.exception(f"cmd_status error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


# --------------------------------------------------------------------------- #
#  Язык                                                                        #
# --------------------------------------------------------------------------- #

@router.message(F.func(lambda m: m.text and m.text in _all_btn_language_texts()))
@router.message(Command("set_language"))
async def cmd_set_language(message: Message, user_db):
    lang = _get_lang(user_db)
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
            get_text(lang, "choose_language"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
        )
    except Exception as e:
        logger.exception(f"cmd_set_language error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


def _all_btn_language_texts() -> set:
    return {get_text(lang, "btn_language") for lang in languages}


# --------------------------------------------------------------------------- #
#  Поддержка                                                                   #
# --------------------------------------------------------------------------- #

@router.message(F.func(lambda m: m.text and m.text in _all_btn_support_texts()))
@router.message(Command("support"))
async def cmd_support(message: Message, user_db):
    lang = _get_lang(user_db)
    await message.answer(
        get_text(lang, "support_prompt", support=SUPPORT_USERNAME),
        reply_markup=contact_support_kb(lang),
    )


def _all_btn_support_texts() -> set:
    return {get_text(lang, "btn_support") for lang in languages}


# --------------------------------------------------------------------------- #
#  Помощь                                                                      #
# --------------------------------------------------------------------------- #

@router.message(Command("help"))
async def cmd_help(message: Message, user_db):
    lang = _get_lang(user_db)
    await message.answer(
        get_text(lang, "help_text"),
        reply_markup=channel_kb(lang),
    )


# --------------------------------------------------------------------------- #
#  Обработчик URL и текста (основной роутер)                                   #
# --------------------------------------------------------------------------- #

@router.message(F.text, ~F.text.startswith("/"))
async def handle_url(message: Message, user_db, bot: Bot):
    text = message.text.strip()
    lang = _get_lang(user_db)

    # Фильтруем нажатия кнопок клавиатуры (на любом языке)
    if text in _all_keyboard_button_texts():
        return

    # Пробуем извлечь URL
    url = _extract_url(text)

    if not url:
        # Текст без ссылки → поддержка + показываем главное меню
        await _forward_to_support(message, bot, text, lang)
        return

    platform = detect_platform(url)
    if not platform:
        await message.answer(
            get_text(lang, "unsupported_url"),
            reply_markup=channel_kb(lang),
        )
        return

    try:
        await get_queue_service().push_task(message.from_user.id, url, str(platform))
        await message.answer(get_text(lang, "processing"))
    except Exception as e:
        logger.exception(f"Failed to push task for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


def _all_keyboard_button_texts() -> set:
    """Все тексты кнопок на всех языках — для фильтрации в handle_url."""
    keys = [
        "btn_download", "btn_profile", "btn_referral", "btn_premium",
        "btn_donate", "btn_language", "btn_support",
    ]
    result = set()
    for lang in languages:
        for k in keys:
            result.add(get_text(lang, k))
    return result


# --------------------------------------------------------------------------- #
#  Форвардинг в поддержку                                                      #
# --------------------------------------------------------------------------- #

async def _forward_to_support(message: Message, bot: Bot, user_text: str, lang: str):
    """
    Пересылает сообщение пользователя администратору в SUPPORT_USERNAME.
    Пользователь получает подтверждение + главное меню.
    """
    user = message.from_user
    username_part = f"@{user.username}" if user.username else f"id: <code>{user.id}</code>"
    name_part = f"{user.first_name or ''} {user.last_name or ''}".strip() or "—"

    # Пробуем переслать администратору
    try:
        support_chat = await bot.get_chat(SUPPORT_USERNAME)
        await bot.send_message(
            support_chat.id,
            f"📩 <b>Новое обращение в поддержку</b>\n\n"
            f"👤 Имя: <b>{name_part}</b>\n"
            f"🔗 Username: {username_part}\n"
            f"🆔 ID: <code>{user.id}</code>\n"
            f"🌍 Язык: {lang}\n\n"
            f"<b>Сообщение:</b>\n{user_text}",
        )
        logger.info(f"Support message forwarded from {user.id}")
    except Exception as e:
        logger.warning(f"Could not forward support message to {SUPPORT_USERNAME}: {e}")

    # Подтверждение пользователю + показываем главное меню
    from database.database import UserRepo
    user_db_obj = None
    try:
        repo = UserRepo()
        user_db_obj = await repo.get_user(user.id)
    except Exception:
        pass

    _lang = lang

    await message.answer(
        get_text(_lang, "support_accepted", support=SUPPORT_USERNAME),
        reply_markup=contact_support_kb(_lang),
    )
    # Показываем главное меню повторно
    await message.answer(
        "👇",
        reply_markup=main_keyboard(_lang),
    )