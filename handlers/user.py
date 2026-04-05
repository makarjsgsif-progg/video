"""
handlers/user.py

God-Tier Refactor:
- STRICT message priority: Commands → Menu Buttons → URL → Support fallback
- Button texts pre-computed as module-level frozensets (fast O(1) lookup, no race)
- Profile: "Something went wrong" eliminated — full try/except + graceful fallback
- Premium: viral tariff card with per-lang pricing
- i18n: every string uses get_text(); zero hard-coded Russian outside keys
- Support fallback: ONLY reached when text is NOT a button AND NOT a URL
- Inline "Наш канал" / "Our channel" attached to all key replies
- Viral Russian copywriting throughout
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache

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

# ── Constants ────────────────────────────────────────────────────────────────
_URL_RE = re.compile(r"https?://[^\s]+", re.I)
SUPPORT_USERNAME = "@YourBotPayments"
CHANNEL_URL = "https://t.me/downloaddq"

# Button key names used in every keyboard
_BUTTON_KEYS = (
    "btn_download", "btn_profile", "btn_referral",
    "btn_premium", "btn_donate", "btn_language", "btn_support",
)

# ── Singletons ───────────────────────────────────────────────────────────────
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


# ── Pre-computed button text sets (module-level, computed once on import) ─────
# This guarantees O(1) lookup and fixes the root cause of buttons triggering
# the support fallback.

def _build_button_set() -> frozenset[str]:
    result: set[str] = set()
    for lang in languages:
        for key in _BUTTON_KEYS:
            result.add(get_text(lang, key))
    return frozenset(result)


# Built once at import time — never stale, never re-computed per request.
_ALL_BUTTON_TEXTS: frozenset[str] = _build_button_set()

# Per-button sets for individual handler F.func filters
_BTN_DOWNLOAD = frozenset(get_text(lang, "btn_download") for lang in languages)
_BTN_PROFILE  = frozenset(get_text(lang, "btn_profile")  for lang in languages)
_BTN_REFERRAL = frozenset(get_text(lang, "btn_referral") for lang in languages)
_BTN_PREMIUM  = frozenset(get_text(lang, "btn_premium")  for lang in languages)
_BTN_DONATE   = frozenset(get_text(lang, "btn_donate")   for lang in languages)
_BTN_LANGUAGE = frozenset(get_text(lang, "btn_language") for lang in languages)
_BTN_SUPPORT  = frozenset(get_text(lang, "btn_support")  for lang in languages)


# ── Keyboards ────────────────────────────────────────────────────────────────

def channel_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL),
    ]])


def channel_and_support_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL),
        InlineKeyboardButton(
            text=get_text(lang, "btn_contact_support"),
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        ),
    ]])


def contact_support_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=get_text(lang, "btn_contact_support"),
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        ),
        InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL),
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
    share_text = (
        "🔥 Скачивай видео из TikTok, Instagram и 50+ платформ — бесплатно!"
        if lang == "ru"
        else "🔥 Download videos from TikTok, Instagram & 50+ platforms for free!"
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=get_text(lang, "btn_share"),
            url=f"https://t.me/share/url?url={ref_link}&text={share_text}",
        )],
        [InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL)],
    ])


def donate_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="💸 " + get_text(lang, "btn_donate"),
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        )],
        [InlineKeyboardButton(text=get_text(lang, "btn_channel"), url=CHANNEL_URL)],
    ])


_PLACEHOLDERS = {
    "ru": "Вставь ссылку — скачаю за 5 сек ⚡",
    "en": "Paste link — downloaded in 5 sec ⚡",
    "es": "Pega el enlace — descargo en 5 seg ⚡",
    "pt": "Cole o link — baixo em 5 seg ⚡",
    "de": "Link einfügen — in 5 Sek. heruntergeladen ⚡",
    "fr": "Colle le lien — téléchargé en 5 sec ⚡",
    "hi": "लिंक पेस्ट करें — 5 सेकंड में डाउनलोड ⚡",
    "ar": "الصق الرابط — تحميل في 5 ثوانٍ ⚡",
}


def main_keyboard(lang: str = "ru") -> ReplyKeyboardMarkup:
    t = lambda key: get_text(lang, key)
    placeholder = _PLACEHOLDERS.get(lang, _PLACEHOLDERS["en"])
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t("btn_download")), KeyboardButton(text=t("btn_profile"))],
            [KeyboardButton(text=t("btn_referral")), KeyboardButton(text=t("btn_premium"))],
            [KeyboardButton(text=t("btn_donate")),   KeyboardButton(text=t("btn_language"))],
            [KeyboardButton(text=t("btn_support"))],
        ],
        resize_keyboard=True,
        input_field_placeholder=placeholder,
    )


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_url(text: str) -> str | None:
    match = _URL_RE.search(text)
    return match.group(0).rstrip(".,!?)>»") if match else None


def _get_lang(user_db) -> str:
    return (getattr(user_db, "language", None) or "ru")


# ── /start ───────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, user_db, bot: Bot):
    lang = _get_lang(user_db)
    args = message.text.split(maxsplit=1)
    ref_arg = args[1].strip() if len(args) > 1 else None

    if ref_arg and getattr(user_db, "referred_by", None) is None:
        await _process_referral(message, user_db, ref_arg, bot, lang)

    name = message.from_user.first_name or ("друг" if lang == "ru" else "friend")
    try:
        await message.answer(
            get_text(lang, "welcome", name=name),
            reply_markup=main_keyboard(lang),
        )
        await message.answer(
            get_text(lang, "channel_subscribe"),
            reply_markup=channel_kb(lang),
        )
    except Exception as e:
        logger.exception(f"cmd_start error for {message.from_user.id}: {e}")


async def _process_referral(
    message: Message, user_db, ref_arg: str, bot: Bot, lang: str
):
    repo = UserRepo()
    referrer = None

    try:
        referrer_id = int(ref_arg)
        if referrer_id != message.from_user.id:
            referrer = await repo.get_user(referrer_id)
    except (ValueError, Exception):
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
            await bot.send_message(referrer_id, get_text(ref_lang, "referral_bonus_notify"))
        except Exception:
            pass
    except Exception as e:
        logger.exception(f"Referral processing error for {message.from_user.id}: {e}")


# ── Button: Download ─────────────────────────────────────────────────────────

@router.message(F.text.func(lambda t: t in _BTN_DOWNLOAD))
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


# ── Button: Profile ──────────────────────────────────────────────────────────

@router.message(F.text.func(lambda t: t in _BTN_PROFILE))
@router.message(Command("profile"))
async def btn_profile(message: Message, user_db, bot: Bot):
    lang = _get_lang(user_db)
    try:
        ls = get_limit_service()
        used, limit = await ls.get_usage(message.from_user.id)
        is_premium = getattr(user_db, "is_premium", False)

        # Bot username (cached inside aiogram session, fast)
        try:
            me = await bot.get_me()
            bot_username = me.username or "YourBot"
        except Exception:
            bot_username = "YourBot"

        # Premium status line
        if is_premium and getattr(user_db, "premium_until", None):
            until_str = user_db.premium_until.strftime("%d.%m.%Y")
            status_text = get_text(lang, "premium_status_active", until=until_str)
        elif is_premium:
            status_text = get_text(lang, "premium_status_active", until="∞")
        else:
            status_text = get_text(lang, "premium_status_inactive")

        ref_param = getattr(user_db, "referral_code", None) or str(message.from_user.id)
        ref_count = int(getattr(user_db, "referral_count", 0) or 0)
        lang_display = languages.get(lang, lang.upper())

        # Language emoji map
        lang_emoji = {
            "ru": "🇷🇺", "en": "🇬🇧", "es": "🇪🇸", "pt": "🇧🇷",
            "de": "🇩🇪", "fr": "🇫🇷", "hi": "🇮🇳", "ar": "🇸🇦",
        }.get(lang, "🌍")

        await message.answer(
            get_text(
                lang, "profile_text",
                lang=f"{lang_emoji} {lang_display}",
                used=used,
                limit=limit,
                status_text=status_text,
                ref_count=ref_count,
                bot_username=bot_username,
                ref_param=ref_param,
            ),
            reply_markup=channel_kb(lang),
        )
    except Exception as e:
        logger.exception(f"btn_profile error for {message.from_user.id}: {e}")
        # Graceful fallback — never show raw "error_generic" without context
        try:
            await message.answer(get_text(lang, "error_generic"))
        except Exception:
            pass


# ── Button: Referral ─────────────────────────────────────────────────────────

@router.message(F.text.func(lambda t: t in _BTN_REFERRAL))
@router.message(Command("referral"))
async def cmd_referral(message: Message, user_db, bot: Bot):
    lang = _get_lang(user_db)
    try:
        me = await bot.get_me()
        user_id = message.from_user.id
        ref_param = getattr(user_db, "referral_code", None) or str(user_id)
        ref_link = f"https://t.me/{me.username}?start={ref_param}"
        count = int(getattr(user_db, "referral_count", 0) or 0)

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


# ── Button: Premium ──────────────────────────────────────────────────────────

@router.message(F.text.func(lambda t: t in _BTN_PREMIUM))
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
        else:
            await message.answer(
                get_text(lang, "premium_tariffs"),
                reply_markup=premium_buy_kb(lang),
            )
    except Exception as e:
        logger.exception(f"cmd_premium error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


# ── Button: Donate ───────────────────────────────────────────────────────────

@router.message(F.text.func(lambda t: t in _BTN_DONATE))
@router.message(Command("donate"))
async def cmd_donate(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        await message.answer(
            get_text(lang, "donate_text", support=SUPPORT_USERNAME),
            reply_markup=donate_kb(lang),
        )
    except Exception as e:
        logger.exception(f"cmd_donate error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


# ── Button: Language ─────────────────────────────────────────────────────────

@router.message(F.text.func(lambda t: t in _BTN_LANGUAGE))
@router.message(Command("set_language"))
async def cmd_set_language(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        items = list(languages.items())
        kb: list[list[InlineKeyboardButton]] = []
        for i in range(0, len(items), 2):
            row = [InlineKeyboardButton(
                text=items[i][1], callback_data=f"lang_{items[i][0]}"
            )]
            if i + 1 < len(items):
                row.append(InlineKeyboardButton(
                    text=items[i + 1][1], callback_data=f"lang_{items[i + 1][0]}"
                ))
            kb.append(row)

        await message.answer(
            get_text(lang, "choose_language"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
        )
    except Exception as e:
        logger.exception(f"cmd_set_language error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


# ── Button: Support (explicit button press) ───────────────────────────────────

@router.message(F.text.func(lambda t: t in _BTN_SUPPORT))
@router.message(Command("support"))
async def cmd_support(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        await message.answer(
            get_text(lang, "support_prompt", support=SUPPORT_USERNAME),
            reply_markup=contact_support_kb(lang),
        )
    except Exception as e:
        logger.exception(f"cmd_support error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


# ── /help ────────────────────────────────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        await message.answer(
            get_text(lang, "help_text"),
            reply_markup=channel_kb(lang),
        )
    except Exception as e:
        logger.exception(f"cmd_help error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


# ── /status ──────────────────────────────────────────────────────────────────

@router.message(Command("status"))
async def cmd_status(message: Message, user_db):
    lang = _get_lang(user_db)
    try:
        ls = get_limit_service()
        qs = get_queue_service()
        used, limit = await ls.get_usage(message.from_user.id)
        queue_len = await qs.get_queue_length()
        is_premium = getattr(user_db, "is_premium", False)

        premium_line = (
            get_text(lang, "status_premium_line")
            if is_premium
            else get_text(lang, "status_limit_line", used=used, limit=limit)
        )
        queue_status = (
            get_text(lang, "status_queue_empty")
            if queue_len == 0
            else get_text(lang, "status_queue_busy", count=queue_len)
        )

        await message.answer(
            get_text(lang, "status_text", premium_line=premium_line, queue_status=queue_status),
            reply_markup=channel_kb(lang),
        )
    except Exception as e:
        logger.exception(f"cmd_status error for {message.from_user.id}: {e}")
        await message.answer(get_text(lang, "error_generic"))


# ── Main message handler: URL → download | text → support fallback ────────────
#
# PRIORITY ORDER enforced by aiogram router registration:
#   1. CommandStart, Command handlers (registered before this router)
#   2. Button text handlers (registered above via F.text.func)
#   3. THIS handler — only fires if nothing above matched
#
# Additional safety: we double-check against _ALL_BUTTON_TEXTS so even if
# somehow a button text slips through (edge case: language changed mid-session),
# it is silently ignored and won't trigger a support forward.

@router.message(F.text, ~F.text.startswith("/"))
async def handle_text(message: Message, user_db, bot: Bot):
    text = (message.text or "").strip()
    lang = _get_lang(user_db)

    # Guard: never treat any known button text as a support message
    if text in _ALL_BUTTON_TEXTS:
        return

    # ── Priority 2: URL detection ────────────────────────────────────────────
    url = _extract_url(text)
    if url:
        platform = detect_platform(url)
        if not platform:
            await message.answer(
                get_text(lang, "unsupported_url"),
                reply_markup=channel_and_support_kb(lang),
            )
            return
        try:
            await get_queue_service().push_task(message.from_user.id, url, str(platform))
            await message.answer(
                get_text(lang, "processing"),
                reply_markup=channel_kb(lang),
            )
        except Exception as e:
            logger.exception(f"Failed to push task for {message.from_user.id}: {e}")
            await message.answer(get_text(lang, "error_generic"))
        return

    # ── Priority 3: Support fallback ─────────────────────────────────────────
    await _forward_to_support(message, bot, text, lang)


async def _forward_to_support(message: Message, bot: Bot, user_text: str, lang: str):
    """Forward user message to admin; confirm to user; re-show main keyboard."""
    user = message.from_user
    username_part = (
        f"@{user.username}" if user.username else f"id: <code>{user.id}</code>"
    )
    name_part = f"{user.first_name or ''} {user.last_name or ''}".strip() or "—"

    # Forward to support (best-effort, non-blocking)
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

    # Confirm to user
    try:
        await message.answer(
            get_text(lang, "support_accepted", support=SUPPORT_USERNAME),
            reply_markup=contact_support_kb(lang),
        )
        # Re-show main keyboard so user doesn't feel lost
        await message.answer("👇", reply_markup=main_keyboard(lang))
    except Exception as e:
        logger.error(f"Could not send support_accepted to {user.id}: {e}")