import asyncio
from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command
from sqlalchemy import select
from config.config import settings
from database.database import async_session_maker, UserRepo, DownloadRepo, AdRepo, User

router = Router()

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
    text = f"📊 <b>Statistics</b>\n👥 Users: {users_count}\n🔥 Active today: {active_today}\n⬇️ Downloads total: {total_downloads}\n📢 Active ads: {ads_count}"
    await message.answer(text)

@router.message(Command("admin_premium"))
async def admin_premium(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("Usage: /admin_premium <user_id> <days>")
        return
    user_id = int(parts[1])
    days = int(parts[2])
    async with async_session_maker() as session:
        repo = UserRepo(session)
        await repo.set_premium(user_id, days)
    await message.answer(f"✅ Premium granted to {user_id} for {days} days")

@router.message(Command("admin_unpremium"))
async def admin_unpremium(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Usage: /admin_unpremium <user_id>")
        return
    user_id = int(parts[1])
    async with async_session_maker() as session:
        repo = UserRepo(session)
        await repo.remove_premium(user_id)
    await message.answer(f"✅ Premium removed from {user_id}")

@router.message(Command("admin_ban"))
async def admin_ban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Usage: /admin_ban <user_id>")
        return
    user_id = int(parts[1])
    async with async_session_maker() as session:
        repo = UserRepo(session)
        await repo.ban_user(user_id)
    await message.answer(f"🚫 User {user_id} banned")

@router.message(Command("admin_unban"))
async def admin_unban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Usage: /admin_unban <user_id>")
        return
    user_id = int(parts[1])
    async with async_session_maker() as session:
        repo = UserRepo(session)
        await repo.unban_user(user_id)
    await message.answer(f"✅ User {user_id} unbanned")

@router.message(Command("admin_add_ad"))
async def admin_add_ad(message: Message):
    if not is_admin(message.from_user.id):
        return
    text = message.text.replace("/admin_add_ad", "").strip()
    if not text:
        await message.answer("Usage: /admin_add_ad <ad text>")
        return
    async with async_session_maker() as session:
        repo = AdRepo(session)
        await repo.add_ad(text)
    await message.answer("✅ Ad added")

@router.message(Command("admin_remove_ad"))
async def admin_remove_ad(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Usage: /admin_remove_ad <ad_id>")
        return
    ad_id = int(parts[1])
    async with async_session_maker() as session:
        repo = AdRepo(session)
        await repo.remove_ad(ad_id)
    await message.answer("✅ Ad removed")

@router.message(Command("admin_ads_list"))
async def admin_ads_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with async_session_maker() as session:
        repo = AdRepo(session)
        ads = await repo.get_all_ads()
    if not ads:
        await message.answer("No ads found")
        return
    text = "\n".join([f"ID {ad.id}: {ad.message_text[:50]}... (active: {ad.is_active})" for ad in ads])
    await message.answer(text)

@router.message(Command("admin_broadcast"))
async def admin_broadcast(message: Message):
    if not is_admin(message.from_user.id):
        return
    broadcast_text = message.text.replace("/admin_broadcast", "").strip()
    if not broadcast_text:
        await message.answer("Usage: /admin_broadcast <message>")
        return
    async with async_session_maker() as session:
        result = await session.execute(select(User.id))
        users = result.scalars().all()
    count = 0
    for uid in users:
        try:
            await message.bot.send_message(uid, broadcast_text)
            count += 1
            await asyncio.sleep(0.05)
        except:
            pass
    await message.answer(f"📨 Broadcast sent to {count} users")