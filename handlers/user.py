"""
handlers/user.py

Изменения:
- Полностью переписан копирайтинг на вирусный/энергичный русский
- Добавлен /support — форвардинг сообщений в @YourBotPayments с меткой отправителя
- Inline-кнопка «Наш канал» (https://t.me/downloaddq) прикреплена к стартовому
  сообщению и к ключевым ответам
- handle_url теперь ведёт в support, если пользователь пишет текст (не ссылку)
  вместо молчаливого «не понимаю»
- Обновлены все строки (кнопки меню, подсказки, ошибки)
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

# Куда форвардить support-сообщения
SUPPORT_USERNAME = "@YourBotPayments"

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
#  Inline-кнопки (канал + поддержка)                                          #
# --------------------------------------------------------------------------- #

def channel_kb() -> InlineKeyboardMarkup:
    """Inline-клавиатура с кнопкой канала — крепится к сообщениям."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📣 Наш канал", url="https://t.me/downloaddq"),
    ]])


def channel_and_support_kb() -> InlineKeyboardMarkup:
    """Канал + поддержка в одной строке."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📣 Наш канал", url="https://t.me/downloaddq"),
        InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}"),
    ]])


# --------------------------------------------------------------------------- #
#  Клавиатура                                                                  #
# --------------------------------------------------------------------------- #

def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📥 Скачать видео"), KeyboardButton(text="👤 Мой профиль")],
            [KeyboardButton(text="🔗 Реферальная ссылка"), KeyboardButton(text="💎 Premium")],
            [KeyboardButton(text="❤️ Донат"), KeyboardButton(text="🌍 Язык")],
            [KeyboardButton(text="🆘 Поддержка")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Вставь ссылку — скачаю за 5 сек ⚡",
    )


_KEYBOARD_BUTTONS = frozenset({
    "📥 Скачать видео",
    "👤 Мой профиль",
    "🔗 Реферальная ссылка",
    "💎 Premium",
    "❤️ Донат",
    "🌍 Язык",
    "🆘 Поддержка",
})


# --------------------------------------------------------------------------- #
#  Утилиты                                                                     #
# --------------------------------------------------------------------------- #

def _is_valid_url(text: str) -> bool:
    try:
        result = urlparse(text.strip())
        return result.scheme in ("http", "https") and bool(result.netloc)
    except Exception:
        return False


def _extract_url(text: str) -> str | None:
    match = _URL_RE.search(text)
    return match.group(0).rstrip(".,!?)>»") if match else None


# --------------------------------------------------------------------------- #
#  /start                                                                      #
# --------------------------------------------------------------------------- #

@router.message(CommandStart())
async def cmd_start(message: Message, gettext, user_db, bot: Bot):
    args = message.text.split(maxsplit=1)
    ref_arg = args[1].strip() if len(args) > 1 else None

    if ref_arg and getattr(user_db, "referred_by", None) is None:
        await _process_referral(message, user_db, ref_arg, bot)

    name = message.from_user.first_name or "друг"
    await message.answer(
        f"👋 Привет, <b>{name}</b>!\n\n"
        f"Отправь ссылку на видео — скачаю за секунды ⚡️\n\n"
        f"<b>Поддерживаю:</b>\n"
        f"🎵 TikTok · 📸 Instagram Reels · 🐦 Twitter/X\n"
        f"🤖 Reddit · 👤 Facebook · 🎬 Vimeo\n"
        f"🎮 Twitch · 📌 Pinterest · 👻 Snapchat\n"
        f"❤️ Likee · 🎤 Triller · 💼 Microsoft Stream\n"
        f"▶️ YouTube Shorts · и ещё 50+ платформ!\n\n"
        f"📲 Поделись с друзьями → /referral\n"
        f"<i>(+5 загрузок за каждого приглашённого)</i>",
        reply_markup=main_keyboard(),
    )

    # Inline-кнопка канала отдельным сообщением, чтобы не перекрывать reply-клавиатуру
    await message.answer(
        "📣 Подпишись на наш канал — там обновления, фишки и розыгрыши:",
        reply_markup=channel_kb(),
    )


async def _process_referral(message: Message, user_db, ref_arg: str, bot: Bot):
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

        try:
            await bot.send_message(
                referrer_id,
                f"🔥 <b>Твоя реферальная ссылка сработала!</b>\n\n"
                f"Новый пользователь только что зарегистрировался по твоей ссылке.\n"
                f"На счёт начислено <b>+5 бонусных загрузок</b> 🚀\n\n"
                f"Чем больше друзей — тем больше халявных видео. Делись!",
            )
        except Exception:
            pass

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
            "💎 <b>Безлимит</b> — Premium активен, качай сколько хочешь!" if is_premium
            else f"📊 Осталось сегодня: <b>{max(0, limit - used)}/{limit}</b> загрузок"
        )

        await message.answer(
            f"🔗 <b>Скидывай ссылку — скачаю мгновенно!</b>\n\n"
            f"Работаю с:\n"
            f"🎵 TikTok · 📸 Instagram Reels · 🐦 Twitter/X\n"
            f"🤖 Reddit · 👤 Facebook · 🎬 Vimeo\n"
            f"🎮 Twitch · 📌 Pinterest · 👻 Snapchat · и ещё 50+\n\n"
            f"{limit_line}\n\n"
            f"<i>Просто вставь ссылку прямо сюда 👇</i>"
        )
    except Exception as e:
        logger.exception(f"btn_download error for {message.from_user.id}: {e}")
        await message.answer("🔗 <b>Вставь ссылку на видео</b> — скачаю за секунды ⚡")


# --------------------------------------------------------------------------- #
#  Профиль                                                                     #
# --------------------------------------------------------------------------- #

@router.message(F.text == "👤 Мой профиль")
@router.message(Command("profile"))
async def btn_profile(message: Message, user_db):
    try:
        ls = get_limit_service()
        used, limit = await ls.get_usage(message.from_user.id)
        is_premium = getattr(user_db, "is_premium", False)

        premium_status = "💎 Активен" if is_premium else "❌ Нет"
        premium_line = ""
        if is_premium and getattr(user_db, "premium_until", None):
            premium_line = f" (до {user_db.premium_until.strftime('%d.%m.%Y')})"

        ref_code = getattr(user_db, "referral_code", None) or str(message.from_user.id)

        await message.answer(
            f"👤 <b>Твой профиль</b>\n\n"
            f"🌍 Язык: <b>{getattr(user_db, 'language', 'ru').upper()}</b>\n"
            f"📊 Загружено сегодня: <b>{used}/{limit}</b>\n"
            f"💎 Премиум: {premium_status}{premium_line}\n"
            f"👥 Приглашено друзей: <b>{getattr(user_db, 'referral_count', 0) or 0}</b>\n"
            f"🔗 Твой реферальный код: <code>{ref_code}</code>"
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
                text="📤 Поделиться с другом",
                url=(
                    f"https://t.me/share/url?url={ref_link}"
                    f"&text=🔥 Скачивай видео из TikTok, Instagram и 50+ платформ бесплатно!"
                ),
            )
        ]])

        await message.answer(
            f"🔗 <b>Твоя реферальная ссылка:</b>\n"
            f"<code>{ref_link}</code>\n\n"
            f"👥 Приглашено друзей: <b>{count}</b>\n"
            f"🎁 Бонусных загрузок накоплено: <b>+{bonus}</b>\n\n"
            f"💡 <b>За каждого друга — +5 загрузок навсегда!</b>\n"
            f"Чем больше поделишься — тем больше качаешь бесплатно 🚀",
            reply_markup=share_kb,
        )
    except Exception as e:
        logger.exception(f"cmd_referral error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось загрузить реферальную ссылку.")


# --------------------------------------------------------------------------- #
#  Премиум                                                                     #
# --------------------------------------------------------------------------- #

@router.message(F.text == "💎 Premium")
@router.message(Command("premium"))
async def cmd_premium(message: Message, gettext, user_db):
    try:
        if getattr(user_db, "is_premium", False):
            until = (
                user_db.premium_until.strftime("%d.%m.%Y")
                if getattr(user_db, "premium_until", None)
                else "∞"
            )
            await message.answer(
                f"💎 <b>Premium активен до {until}</b>\n\n"
                f"✅ Безлимитные загрузки\n"
                f"✅ Без рекламы\n"
                f"✅ Приоритетная обработка\n\n"
                f"Наслаждайся! 🚀",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="📣 Наш канал", url="https://t.me/downloaddq"),
                ]]),
            )
            return

        tariffs_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="💳 Оформить подписку",
                url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
            )],
            [InlineKeyboardButton(text="📣 Наш канал", url="https://t.me/downloaddq")],
        ])

        await message.answer(
            "💎 <b>Premium — качай без ограничений</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🗓 <b>5 дней</b>  →  <b>49 ₽</b>\n"
            "🗓 <b>15 дней</b> →  <b>99 ₽</b>\n"
            "🗓 <b>30 дней</b> →  <b>149 ₽</b>\n"
            "🗓 <b>60 дней</b> →  <b>249 ₽</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "✅ Безлимитные загрузки каждый день\n"
            "✅ Ноль рекламы\n"
            "✅ Твои задачи в приоритете очереди\n\n"
            f"👉 Для оплаты обратитесь к администратору {SUPPORT_USERNAME}",
            reply_markup=tariffs_kb,
        )
    except Exception as e:
        logger.exception(f"cmd_premium error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось загрузить тарифы. Попробуй позже.")


# --------------------------------------------------------------------------- #
#  Донат                                                                       #
# --------------------------------------------------------------------------- #

@router.message(F.text == "❤️ Донат")
@router.message(Command("donate"))
async def cmd_donate(message: Message):
    donate_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="💸 Поддержать проект",
            url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
        )],
        [InlineKeyboardButton(text="📣 Наш канал", url="https://t.me/downloaddq")],
    ])

    await message.answer(
        "❤️ <b>Поддержать проект</b>\n\n"
        "Бот работает бесплатно — и это благодаря тем, кто поддерживает его развитие.\n\n"
        "Если он тебе полезен — любой донат мотивирует делать его лучше и быстрее 🙏\n\n"
        f"👉 Реквизиты и способы оплаты — у администратора {SUPPORT_USERNAME}",
        reply_markup=donate_kb,
    )


# --------------------------------------------------------------------------- #
#  Статус задачи                                                               #
# --------------------------------------------------------------------------- #

@router.message(Command("status"))
async def cmd_status(message: Message, user_db):
    try:
        ls = get_limit_service()
        qs = get_queue_service()
        used, limit = await ls.get_usage(message.from_user.id)
        queue_len = await qs.get_queue_length()
        is_premium = getattr(user_db, "is_premium", False)

        premium_line = (
            "💎 Premium: ✅ Безлимит активен" if is_premium
            else f"📊 Лимит: <b>{used}/{limit}</b> загрузок сегодня"
        )
        queue_status = (
            "✅ Очередь пуста — отвечу мгновенно!" if queue_len == 0
            else f"⏳ Сейчас в очереди: <b>{queue_len}</b> задач"
        )

        await message.answer(
            f"📋 <b>Статус сервиса</b>\n\n"
            f"{premium_line}\n"
            f"{queue_status}\n\n"
            f"<i>Видео долго не приходит? Попробуй отправить ссылку ещё раз.</i>"
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
            "🌍 <b>Выбери язык интерфейса:</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
        )
    except Exception as e:
        logger.exception(f"cmd_set_language error for {message.from_user.id}: {e}")
        await message.answer("❌ Не удалось открыть выбор языка.")


# --------------------------------------------------------------------------- #
#  Поддержка                                                                   #
# --------------------------------------------------------------------------- #

@router.message(F.text == "🆘 Поддержка")
@router.message(Command("support"))
async def cmd_support(message: Message):
    """Показывает инструкцию по поддержке и предлагает написать напрямую."""
    await message.answer(
        f"🆘 <b>Служба поддержки</b>\n\n"
        f"Есть вопрос, проблема или предложение?\n\n"
        f"<b>Напиши следующим сообщением</b> — опиши ситуацию, "
        f"и мы передадим её команде. Обычно отвечаем в течение нескольких часов.\n\n"
        f"Или напиши напрямую: {SUPPORT_USERNAME}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✍️ Написать в поддержку",
                url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
            )
        ]]),
    )


# --------------------------------------------------------------------------- #
#  Помощь                                                                      #
# --------------------------------------------------------------------------- #

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ <b>Как пользоваться ботом?</b>\n\n"
        "<b>Всё просто:</b>\n"
        "1️⃣ Скопируй ссылку на видео из приложения\n"
        "2️⃣ Вставь её в этот чат\n"
        "3️⃣ Получи видео без водяных знаков за секунды!\n\n"
        "<b>Поддерживаемые платформы:</b>\n"
        "🎵 TikTok · 📸 Instagram · 🐦 Twitter/X\n"
        "🤖 Reddit · 👤 Facebook · 🎬 Vimeo\n"
        "🎮 Twitch · 📌 Pinterest · 👻 Snapchat · и 50+ других\n\n"
        "<b>Лимиты:</b>\n"
        "• Бесплатно: <b>5 загрузок в день</b>\n"
        "• 💎 Premium: <b>безлимитно</b>\n"
        "• 👥 Реферал: <b>+5 загрузок</b> за каждого друга\n\n"
        "<b>Команды:</b>\n"
        "/start — главное меню\n"
        "/status — статус очереди и лимитов\n"
        "/referral — твоя реферальная ссылка\n"
        "/premium — тарифы и подписка\n"
        "/donate — поддержать проект\n"
        "/support — написать в поддержку\n"
        "/set_language — сменить язык\n"
        "/help — эта справка",
        reply_markup=channel_kb(),
    )


# --------------------------------------------------------------------------- #
#  Обработчик URL (основной)                                                   #
# --------------------------------------------------------------------------- #

@router.message(F.text, ~F.text.startswith("/"))
async def handle_url(message: Message, gettext, user_db, bot: Bot):
    text = message.text.strip()

    # Фильтр кнопок клавиатуры
    if text in _KEYBOARD_BUTTONS:
        return

    # Пробуем извлечь URL
    url = _extract_url(text)

    if not url:
        # Пользователь написал текст — предлагаем поддержку и перенаправляем
        await _forward_to_support(message, bot, text)
        return

    platform = detect_platform(url)
    if not platform:
        await message.answer(
            "🤔 <b>Платформа не поддерживается</b>\n\n"
            "Работаю с TikTok, Instagram, Twitter/X, Reddit, Facebook, Vimeo, Twitch и 50+ другими.\n\n"
            "Убедись, что ссылка ведёт на публичное видео и попробуй ещё раз.",
            reply_markup=channel_kb(),
        )
        return

    try:
        await get_queue_service().push_task(message.from_user.id, url, str(platform))
        await message.answer(
            "⚡️ <b>Принял! Скачиваю...</b>\n\n"
            "Видео будет здесь через несколько секунд. Не уходи далеко 😉"
        )
    except Exception as e:
        logger.exception(f"Failed to push task for {message.from_user.id}: {e}")
        await message.answer(
            "⚠️ <b>Сервис временно перегружен</b>\n\n"
            "Попробуй через несколько секунд — обычно это быстро проходит."
        )


# --------------------------------------------------------------------------- #
#  Форвардинг в поддержку                                                     #
# --------------------------------------------------------------------------- #

async def _forward_to_support(message: Message, bot: Bot, user_text: str):
    """
    Пересылает сообщение пользователя в SUPPORT_USERNAME с меткой отправителя.
    Пользователь получает подтверждение.
    """
    user = message.from_user
    username_part = f"@{user.username}" if user.username else f"id: <code>{user.id}</code>"
    name_part = f"{user.first_name or ''} {user.last_name or ''}".strip() or "—"

    try:
        # Пытаемся найти chat_id поддержки через username
        support_chat = await bot.get_chat(SUPPORT_USERNAME)
        await bot.send_message(
            support_chat.id,
            f"📩 <b>Новое обращение в поддержку</b>\n\n"
            f"👤 Имя: <b>{name_part}</b>\n"
            f"🔗 Username: {username_part}\n"
            f"🆔 ID: <code>{user.id}</code>\n\n"
            f"<b>Сообщение:</b>\n{user_text}",
        )
        logger.info(f"Support message forwarded from {user.id} to {SUPPORT_USERNAME}")
    except Exception as e:
        logger.warning(f"Could not forward support message to {SUPPORT_USERNAME}: {e}")
        # Даже если форвард не удался — пользователь получает ответ с прямой ссылкой

    await message.answer(
        f"✅ <b>Сообщение принято!</b>\n\n"
        f"Команда поддержки скоро ответит тебе.\n\n"
        f"Или напиши напрямую: {SUPPORT_USERNAME}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✍️ Написать в поддержку",
                url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}",
            )
        ]]),
    )