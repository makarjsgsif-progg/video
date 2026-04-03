import asyncio
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

from config.config import settings
from database.database import async_session_maker, UserRepo, DownloadRepo, AdRepo

router = Router()
logger = logging.getLogger(__name__)


def is_admin(user_id: int) -> bool:
    return user_id in settings.ADMIN_IDS


def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stat"),
            InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"),
        ],
        [
            InlineKeyboardButton(text="📢 Объявления", callback_data="admin_ads"),
            InlineKeyboardButton(text="📨 Рассылка", callback_data="admin_broadcast_prompt"),
        ],
        [
            InlineKeyboardButton(text="💎 Выдать Premium", callback_data="admin_premium_prompt"),
            InlineKeyboardButton(text="🚫 Бан/Разбан", callback_data="admin_ban_prompt"),
        ],
    ])


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        return

    await message.answer(
        "🛠 <b>Панель администратора</b>\n\n"
        "Выбери действие:",
        reply_markup=admin_keyboard(),
    )


# ── Статистика ────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_stat")
async def admin_stat_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer()

    async with async_session_maker() as session:
        user_repo = UserRepo(session)
        download_repo = DownloadRepo(session)
        ad_repo = AdRepo(session)

        users_count = await user_repo.get_all_users_count()
        active_today = await user_repo.get_active_users_today()
        total_downloads = await download_repo.get_total_downloads()
        ads_count = len(await ad_repo.get_active_ads())

    text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: <b>{users_count}</b>\n"
        f"🔥 Активных сегодня: <b>{active_today}</b>\n"
        f"⬇️ Всего загрузок: <b>{total_downloads}</b>\n"
        f"📢 Активных объявлений: <b>{ads_count}</b>"
    )
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
    ]))
    await callback.answer()


# ── Пользователи ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_users")
async def admin_users_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer()

    async with async_session_maker() as session:
        count = await UserRepo(session).get_all_users_count()

    await callback.message.edit_text(
        f"👥 <b>Пользователи</b>\n\n"
        f"Всего: <b>{count}</b>\n\n"
        f"Управление пользователями:\n"
        f"• /admin_ban &lt;user_id&gt; — заблокировать\n"
        f"• /admin_unban &lt;user_id&gt; — разблокировать\n"
        f"• /admin_premium &lt;user_id&gt; &lt;days&gt; — выдать premium\n"
        f"• /admin_unpremium &lt;user_id&gt; — снять premium\n"
        f"• /admin_user_info &lt;user_id&gt; — инфо о пользователе",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()


# ── Объявления ────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_ads")
async def admin_ads_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer()

    async with async_session_maker() as session:
        ads = await AdRepo(session).get_all_ads()

    if not ads:
        text = "📢 <b>Объявления</b>\n\nОбъявлений пока нет."
    else:
        lines = [f"📢 <b>Объявления ({len(ads)})</b>\n"]
        for ad in ads:
            status = "✅" if ad.is_active else "❌"
            lines.append(f"{status} ID <b>{ad.id}</b>: {ad.message_text[:60]}...")
        text = "\n".join(lines)

    text += (
        "\n\n<b>Команды:</b>\n"
        "• /admin_add_ad &lt;текст&gt; — добавить\n"
        "• /admin_remove_ad &lt;id&gt; — удалить\n"
        "• /admin_toggle_ad &lt;id&gt; — вкл/выкл"
    )

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
    ]))
    await callback.answer()


# ── Подсказки для рассылки и premium ─────────────────────────────────────────

@router.callback_query(F.data == "admin_broadcast_prompt")
async def admin_broadcast_prompt_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer()
    await callback.message.edit_text(
        "📨 <b>Рассылка</b>\n\n"
        "Используй команду:\n"
        "<code>/admin_broadcast Текст сообщения</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()


@router.callback_query(F.data == "admin_premium_prompt")
async def admin_premium_prompt_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer()
    await callback.message.edit_text(
        "💎 <b>Выдать Premium</b>\n\n"
        "Используй команду:\n"
        "<code>/admin_premium &lt;user_id&gt; &lt;days&gt;</code>\n\n"
        "Пример: <code>/admin_premium 123456789 30</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()


@router.callback_query(F.data == "admin_ban_prompt")
async def admin_ban_prompt_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer()
    await callback.message.edit_text(
        "🚫 <b>Бан / Разбан</b>\n\n"
        "<code>/admin_ban &lt;user_id&gt;</code> — заблокировать\n"
        "<code>/admin_unban &lt;user_id&gt;</code> — разблокировать",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()


@router.callback_query(F.data == "admin_back")
async def admin_back_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer()
    await callback.message.edit_text(
        "🛠 <b>Панель администратора</b>\n\nВыбери действие:",
        reply_markup=admin_keyboard(),
    )
    await callback.answer()


# ── Текстовые команды ─────────────────────────────────────────────────────────

@router.message(Command("admin_stat"))
async def admin_stat(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with async_session_maker() as session:
        user_repo = UserRepo(session)
        download_repo = DownloadRepo(session)
        ad_repo = AdRepo(session)
        users_count = await user_repo.get_all_users_count()
        active_today = await user_repo.get_active_users_today()
        total_downloads = await download_repo.get_total_downloads()
        ads_count = len(await ad_repo.get_active_ads())

    await message.answer(
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: <b>{users_count}</b>\n"
        f"🔥 Активных сегодня: <b>{active_today}</b>\n"
        f"⬇️ Всего загрузок: <b>{total_downloads}</b>\n"
        f"📢 Активных объявлений: <b>{ads_count}</b>"
    )


@router.message(Command("admin_user_info"))
async def admin_user_info(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_user_info <user_id>")
        return
    user_id = int(parts[1])
    async with async_session_maker() as session:
        user = await UserRepo(session).get_user(user_id)
    if not user:
        await message.answer(f"❌ Пользователь {user_id} не найден")
        return
    premium_status = "✅" if user.is_premium else "❌"
    ban_status = "🚫 Да" if user.is_banned else "✅ Нет"
    until = user.premium_until.strftime("%d.%m.%Y") if user.premium_until else "—"
    await message.answer(
        f"👤 <b>Пользователь {user_id}</b>\n\n"
        f"🌍 Язык: <b>{user.language}</b>\n"
        f"💎 Premium: {premium_status} (до {until})\n"
        f"🚫 Забанен: {ban_status}\n"
        f"👥 Рефералов: <b>{user.referral_count or 0}</b>\n"
        f"📅 Зарегистрирован: <b>{user.registered_at.strftime('%d.%m.%Y') if user.registered_at else '—'}</b>"
    )


@router.message(Command("admin_premium"))
async def admin_premium(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("Использование: /admin_premium <user_id> <days>")
        return
    user_id, days = int(parts[1]), int(parts[2])
    async with async_session_maker() as session:
        await UserRepo(session).set_premium(user_id, days)
    await message.answer(f"✅ Premium выдан пользователю <code>{user_id}</code> на <b>{days} дней</b>")


@router.message(Command("admin_unpremium"))
async def admin_unpremium(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_unpremium <user_id>")
        return
    user_id = int(parts[1])
    async with async_session_maker() as session:
        await UserRepo(session).remove_premium(user_id)
    await message.answer(f"✅ Premium снят с пользователя <code>{user_id}</code>")


@router.message(Command("admin_ban"))
async def admin_ban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_ban <user_id>")
        return
    user_id = int(parts[1])
    async with async_session_maker() as session:
        await UserRepo(session).ban_user(user_id)
    await message.answer(f"🚫 Пользователь <code>{user_id}</code> заблокирован")


@router.message(Command("admin_unban"))
async def admin_unban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_unban <user_id>")
        return
    user_id = int(parts[1])
    async with async_session_maker() as session:
        await UserRepo(session).unban_user(user_id)
    await message.answer(f"✅ Пользователь <code>{user_id}</code> разблокирован")


@router.message(Command("admin_add_ad"))
async def admin_add_ad(message: Message):
    if not is_admin(message.from_user.id):
        return
    text = message.text.replace("/admin_add_ad", "", 1).strip()
    if not text:
        await message.answer("Использование: /admin_add_ad <текст объявления>")
        return
    async with async_session_maker() as session:
        await AdRepo(session).add_ad(text)
    await message.answer("✅ Объявление добавлено")


@router.message(Command("admin_remove_ad"))
async def admin_remove_ad(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_remove_ad <ad_id>")
        return
    ad_id = int(parts[1])
    async with async_session_maker() as session:
        await AdRepo(session).remove_ad(ad_id)
    await message.answer("✅ Объявление удалено")


@router.message(Command("admin_toggle_ad"))
async def admin_toggle_ad(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_toggle_ad <ad_id>")
        return
    ad_id = int(parts[1])
    async with async_session_maker() as session:
        repo = AdRepo(session)
        ads = await repo.get_all_ads()
        ad = next((a for a in ads if a.id == ad_id), None)
        if not ad:
            await message.answer(f"❌ Объявление {ad_id} не найдено")
            return
        await repo.toggle_ad(ad_id, not ad.is_active)
    status = "выключено" if ad.is_active else "включено"
    await message.answer(f"✅ Объявление {ad_id} {status}")


@router.message(Command("admin_ads_list"))
async def admin_ads_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with async_session_maker() as session:
        ads = await AdRepo(session).get_all_ads()
    if not ads:
        await message.answer("Объявлений нет")
        return
    lines = []
    for ad in ads:
        status = "✅" if ad.is_active else "❌"
        lines.append(f"{status} ID <b>{ad.id}</b>: {ad.message_text[:80]}")
    await message.answer("\n\n".join(lines))


@router.message(Command("admin_broadcast"))
async def admin_broadcast(message: Message):
    if not is_admin(message.from_user.id):
        return
    broadcast_text = message.text.replace("/admin_broadcast", "", 1).strip()
    if not broadcast_text:
        await message.answer("Использование: /admin_broadcast <сообщение>")
        return
    async with async_session_maker() as session:
        user_ids = await UserRepo(session).get_all_user_ids()

    await message.answer(f"📨 Начинаю рассылку для <b>{len(user_ids)}</b> пользователей...")
    sent, failed = 0, 0
    for uid in user_ids:
        try:
            await message.bot.send_message(uid, broadcast_text)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    await message.answer(f"✅ Рассылка завершена\n\n📨 Отправлено: <b>{sent}</b>\n❌ Не доставлено: <b>{failed}</b>")