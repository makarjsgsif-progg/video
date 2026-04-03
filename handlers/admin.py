import asyncio
import logging
from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command

from config.config import settings
from database.database import async_session_maker, UserRepo, DownloadRepo, AdRepo

router = Router()
logger = logging.getLogger(__name__)


def is_admin(user_id: int) -> bool:
    return user_id in settings.ADMIN_IDS


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

    text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: <b>{users_count}</b>\n"
        f"🔥 Активных сегодня: <b>{active_today}</b>\n"
        f"⬇️ Всего загрузок: <b>{total_downloads}</b>\n"
        f"📢 Активных объявлений: <b>{ads_count}</b>"
    )
    await message.answer(text)


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

    await message.answer(f"✅ Премиум выдан пользователю {user_id} на {days} дней")


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

    await message.answer(f"✅ Премиум снят с пользователя {user_id}")


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

    await message.answer(f"🚫 Пользователь {user_id} заблокирован")


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

    await message.answer(f"✅ Пользователь {user_id} разблокирован")


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


@router.message(Command("admin_ads_list"))
async def admin_ads_list(message: Message):
    if not is_admin(message.from_user.id):
        return

    async with async_session_maker() as session:
        ads = await AdRepo(session).get_all_ads()

    if not ads:
        await message.answer("Объявлений нет")
        return

    lines = [f"ID {ad.id}: {ad.message_text[:50]}... (активно: {ad.is_active})" for ad in ads]
    await message.answer("\n".join(lines))


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

    sent, failed = 0, 0
    for uid in user_ids:
        try:
            await message.bot.send_message(uid, broadcast_text)
            sent += 1
            await asyncio.sleep(0.05)  # ~20 msg/s — в рамках лимитов Telegram
        except Exception:
            failed += 1

    await message.answer(f"📨 Отправлено: {sent}, не доставлено: {failed}")