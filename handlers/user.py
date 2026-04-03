from aiogram import Router, F, Bot
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command, CommandStart

from database.database import async_session_maker, UserRepo
from services.services import QueueService, LimitService
from utils.platform_detector import detect_platform
from utils.i18n import languages

router = Router()

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


def main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📥 Скачать видео"), KeyboardButton(text="👤 Мой профиль")],
            [KeyboardButton(text="🔗 Реферальная ссылка"), KeyboardButton(text="💎 Premium")],
            [KeyboardButton(text="🌍 Язык"), KeyboardButton(text="📊 Статистика")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Вставь ссылку на видео...",
    )


@router.message(CommandStart())
async def cmd_start(message: Message, gettext, user_db, bot: Bot):
    args = message.text.split(maxsplit=1)
    ref_arg = args[1].strip() if len(args) > 1 else None

    # Реферальная система по user_id (ссылка вида ?start=123456789)
    if ref_arg and user_db.referred_by is None:
        try:
            referrer_id = int(ref_arg)
            if referrer_id != message.from_user.id:
                async with async_session_maker() as session:
                    repo = UserRepo(session)
                    referrer = await repo.get_user(referrer_id)
                    if referrer:
                        await repo.set_referred_by(message.from_user.id, referrer_id)
                        await repo.increment_referral_count(referrer_id)
                        await session.commit()
                        await get_limit_service().add_referral_bonus(referrer_id, amount=5)
                        try:
                            await bot.send_message(
                                referrer_id,
                                f"🎉 <b>По твоей ссылке зашёл новый пользователь!</b>\n\n"
                                f"Тебе начислено <b>+5 бонусных загрузок</b> 🚀\n\n"
                                f"Продолжай делиться — каждый друг = ещё +5!",
                            )
                        except Exception:
                            pass
        except (ValueError, TypeError):
            pass

    await message.answer(
        gettext("welcome", name=message.from_user.first_name),
        reply_markup=main_keyboard(),
    )


@router.message(F.text == "📥 Скачать видео")
async def btn_download(message: Message, gettext):
    await message.answer(
        "🔗 <b>Отправь ссылку на видео</b>\n\n"
        "Просто вставь ссылку из TikTok, Instagram, Twitter, Reddit и других платформ — скачаю мгновенно ⚡"
    )


@router.message(F.text == "👤 Мой профиль")
async def btn_profile(message: Message, user_db):
    ls = get_limit_service()
    used, limit = await ls.get_usage(message.from_user.id)
    premium_status = "✅ Активен" if user_db.is_premium else "❌ Нет"
    until = ""
    if user_db.is_premium and user_db.premium_until:
        until = f"\n📅 Действует до: <b>{user_db.premium_until.strftime('%d.%m.%Y')}</b>"

    await message.answer(
        f"👤 <b>Твой профиль</b>\n\n"
        f"🆔 ID: <code>{message.from_user.id}</code>\n"
        f"⬇️ Загрузок сегодня: <b>{used}/{limit}</b>\n"
        f"💎 Premium: {premium_status}{until}\n"
        f"👥 Приглашено друзей: <b>{user_db.referral_count or 0}</b>"
    )


@router.message(F.text == "🔗 Реферальная ссылка")
@router.message(Command("referral"))
async def cmd_referral(message: Message, user_db, bot: Bot):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={message.from_user.id}"
    count = user_db.referral_count or 0

    bonus = 0
    try:
        ls = get_limit_service()
        raw = await ls.redis.get(f"referral_bonus:{message.from_user.id}")
        bonus = int(raw or 0)
    except Exception:
        pass

    share_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📤 Поделиться ссылкой",
            url=f"https://t.me/share/url?url={ref_link}&text=Скачивай видео с TikTok, Instagram и других платформ за секунды!"
        )
    ]])

    await message.answer(
        f"🔗 <b>Твоя реферальная ссылка:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"👥 Приглашено друзей: <b>{count}</b>\n"
        f"🎁 Бонусных загрузок накоплено: <b>+{bonus}</b>\n\n"
        f"За каждого приглашённого — <b>+5 загрузок</b> навсегда!\n"
        f"Чем больше друзей — тем больше видео 🚀",
        reply_markup=share_kb,
    )


@router.message(F.text == "💎 Premium")
@router.message(Command("premium"))
async def cmd_premium(message: Message, gettext, user_db):
    if user_db.is_premium:
        until = user_db.premium_until.strftime("%d.%m.%Y") if user_db.premium_until else "∞"
        await message.answer(gettext("premium_active", until=until))
    else:
        await message.answer(gettext("premium_info"))


@router.message(F.text == "🌍 Язык")
@router.message(Command("set_language"))
async def cmd_set_language(message: Message, gettext):
    kb = []
    items = list(languages.items())
    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(text=items[i][1], callback_data=f"lang_{items[i][0]}")]
        if i + 1 < len(items):
            row.append(InlineKeyboardButton(text=items[i+1][1], callback_data=f"lang_{items[i+1][0]}"))
        kb.append(row)
    await message.answer(
        gettext("choose_language"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
    )


@router.message(F.text == "📊 Статистика")
@router.message(Command("stats"))
async def cmd_stats(message: Message, user_db):
    ls = get_limit_service()
    used, limit = await ls.get_usage(message.from_user.id)
    premium_status = "✅ Активен" if user_db.is_premium else "❌ Нет"
    until = ""
    if user_db.is_premium and user_db.premium_until:
        until = f" (до {user_db.premium_until.strftime('%d.%m.%Y')})"

    await message.answer(
        f"📊 <b>Твоя статистика</b>\n\n"
        f"⬇️ Загрузок сегодня: <b>{used}/{limit}</b>\n"
        f"💎 Premium: {premium_status}{until}\n"
        f"👥 Рефералов: <b>{user_db.referral_count or 0}</b>"
    )


@router.message(F.text, ~F.text.startswith("/"))
async def handle_url(message: Message, gettext, user_db):
    # Игнорируем кнопки клавиатуры
    keyboard_buttons = {
        "📥 Скачать видео", "👤 Мой профиль", "🔗 Реферальная ссылка",
        "💎 Premium", "🌍 Язык", "📊 Статистика",
    }
    if message.text.strip() in keyboard_buttons:
        return

    url = message.text.strip()
    platform = detect_platform(url)

    if not platform:
        await message.answer(gettext("unsupported_url"))
        return

    if user_db.is_banned:
        await message.answer(gettext("banned"))
        return

    await get_queue_service().push_task(message.from_user.id, url, str(platform))
    await message.answer(gettext("processing"))