from aiogram import Router, F, Bot
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command, CommandStart

from database.database import async_session_maker, UserRepo
from services.services import QueueService, LimitService, generate_referral_code
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


@router.message(CommandStart())
async def cmd_start(message: Message, gettext, user_db, bot: Bot):
    """
    /start — обычный старт
    /start REF_CODE — старт с реферальным кодом
    """
    args = message.text.split(maxsplit=1)
    ref_code = args[1].strip() if len(args) > 1 else None

    # Обрабатываем реферала, если пользователь новый (registered_at почти совпадает с now)
    if ref_code and user_db.referred_by is None and ref_code != user_db.referral_code:
        async with async_session_maker() as session:
            repo = UserRepo(session)
            referrer = await repo.get_user_by_referral_code(ref_code)

            if referrer and referrer.id != message.from_user.id:
                # Записываем, кто пригласил
                await session.execute(
                    __import__("sqlalchemy", fromlist=["update"]).update(
                        __import__("database.database", fromlist=["User"]).User
                    ).where(
                        __import__("database.database", fromlist=["User"]).User.id == message.from_user.id
                    ).values(referred_by=referrer.id)
                )
                await repo.increment_referral_count(referrer.id)
                await session.commit()

                # Бонус рефереру: +5 загрузок
                await get_limit_service().add_referral_bonus(referrer.id, amount=5)

                try:
                    await bot.send_message(
                        referrer.id,
                        f"🎉 По твоей ссылке зашёл новый пользователь!\n"
                        f"Тебе начислено <b>+5 бонусных загрузок</b> 🚀"
                    )
                except Exception:
                    pass

    # Отправляем приветствие
    await message.answer(gettext("welcome", name=message.from_user.first_name))

    # Клавиатура выбора языка
    kb = [
        [InlineKeyboardButton(text=name, callback_data=f"lang_{code}")]
        for code, name in languages.items()
    ]
    await message.answer(
        gettext("choose_language"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
    )


@router.message(Command("set_language"))
async def cmd_set_language(message: Message, gettext):
    kb = [
        [InlineKeyboardButton(text=name, callback_data=f"lang_{code}")]
        for code, name in languages.items()
    ]
    await message.answer(
        gettext("choose_language"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
    )


@router.message(Command("premium"))
async def cmd_premium(message: Message, gettext, user_db):
    if user_db.is_premium:
        until = user_db.premium_until.strftime("%d.%m.%Y") if user_db.premium_until else "∞"
        await message.answer(gettext("premium_active", until=until))
    else:
        await message.answer(gettext("premium_info"))


@router.message(Command("referral"))
async def cmd_referral(message: Message, user_db, bot: Bot):
    """Показывает реферальную ссылку и статистику."""
    me = await bot.get_me()
    code = user_db.referral_code or "—"
    ref_link = f"https://t.me/{me.username}?start={code}"
    count = user_db.referral_count or 0

    # Узнаём бонус из Redis
    bonus = 0
    try:
        ls = get_limit_service()
        bonus_key = f"referral_bonus:{message.from_user.id}"
        raw = await ls.redis.get(bonus_key)
        bonus = int(raw or 0)
    except Exception:
        pass

    text = (
        f"🔗 <b>Твоя реферальная ссылка:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"👥 Приглашено друзей: <b>{count}</b>\n"
        f"🎁 Бонусных загрузок: <b>+{bonus}</b>\n\n"
        f"За каждого приглашённого друга — <b>+5 загрузок</b> навсегда!\n"
        f"Чем больше друзей — тем больше видео 🚀"
    )
    await message.answer(text)


@router.message(Command("stats"))
async def cmd_stats(message: Message, user_db):
    """Личная статистика пользователя."""
    ls = get_limit_service()
    used, limit = await ls.get_usage(message.from_user.id)
    premium_status = "✅ Активен" if user_db.is_premium else "❌ Нет"
    until = ""
    if user_db.is_premium and user_db.premium_until:
        until = f" (до {user_db.premium_until.strftime('%d.%m.%Y')})"

    text = (
        f"📊 <b>Твоя статистика</b>\n\n"
        f"⬇️ Загрузок сегодня: <b>{used}/{limit}</b>\n"
        f"💎 Премиум: {premium_status}{until}\n"
        f"👥 Рефералов: <b>{user_db.referral_count or 0}</b>"
    )
    await message.answer(text)


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