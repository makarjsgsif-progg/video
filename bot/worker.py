import asyncio
import logging
import random
from typing import Optional

from aiogram.types import BufferedInputFile
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter, TelegramBadRequest

from services.services import QueueService, Downloader, AdService, LimitService
from database.database import async_session_maker, UserRepo, DownloadRepo
from bot.loader import bot

logger = logging.getLogger(__name__)

# Эмодзи для платформ — добавляет персональность в сообщения
PLATFORM_EMOJI = {
    "tiktok": "🎵",
    "instagram": "📸",
    "youtube": "▶️",
    "twitter": "🐦",
    "reddit": "🤖",
    "facebook": "👤",
    "vimeo": "🎬",
    "twitch": "🎮",
    "pinterest": "📌",
    "snapchat": "👻",
    "likee": "❤️",
    "triller": "🎤",
    "microsoftstream": "💼",
}


class Worker:
    def __init__(self, max_concurrent_tasks: int = 5):
        self.queue_service = QueueService()
        self.downloader = Downloader()
        self.ad_service = AdService()
        self.limit_service = LimitService()
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def process_task(self, task: dict):
        async with self.semaphore:
            try:
                await self._execute_download_logic(task)
            except Exception as e:
                logger.exception(f"Unhandled error in process_task: {e}")

    async def _execute_download_logic(self, task: dict):
        user_id = task["user_id"]
        url = task["url"]
        platform = task.get("platform", "unknown")
        emoji = PLATFORM_EMOJI.get(platform, "📥")

        async with async_session_maker() as session:
            user_repo = UserRepo(session)
            download_repo = DownloadRepo(session)
            user = await user_repo.get_user(user_id)

            if not user or user.is_banned:
                return

            # 1. Проверка лимита
            if not await self.limit_service.check_and_increment(user_id, user.is_premium):
                used, limit = await self.limit_service.get_usage(user_id)
                await self._safe_send(
                    user_id,
                    f"⏳ <b>Дневной лимит исчерпан</b>\n\n"
                    f"Ты сегодня уже скачал <b>{used}/{limit}</b> видео.\n\n"
                    f"🔑 Купи <b>Premium</b> — безлимитные загрузки без ограничений.\n"
                    f"👥 Или пригласи друзей по /referral — каждый друг = <b>+5 загрузок</b>!"
                )
                return

            # 2. Скачивание
            video_bytes, error = await self._download_with_retries(url)

            if not video_bytes:
                await self._handle_download_error(user_id, platform, emoji, error)
                return

            # 3. Отправка видео
            try:
                video_file = BufferedInputFile(video_bytes.read(), filename="video.mp4")
                caption = (
                    f"{emoji} <b>Готово!</b> Твоё видео скачано 🎉\n\n"
                    f"📲 Поделись ботом с друзьями — /referral"
                )
                await bot.send_video(user_id, video=video_file, caption=caption)

                # Фиксируем загрузку
                await download_repo.add_download(user_id, platform)
                await session.commit()

                # 4. Реклама для не-премиумов (через одну загрузку, не каждый раз)
                if not user.is_premium:
                    await self._send_ad_if_available(user_id)

            except TelegramForbiddenError:
                logger.info(f"User {user_id} blocked the bot.")
            except TelegramRetryAfter as e:
                logger.warning(f"Flood control: retry after {e.retry_after}s")
                await asyncio.sleep(e.retry_after)
            except TelegramBadRequest as e:
                if "file is too big" in str(e).lower():
                    await self._safe_send(
                        user_id,
                        f"😔 <b>Файл слишком большой</b>\n\n"
                        f"Telegram не принимает видео тяжелее 50 МБ.\n"
                        f"Попробуй видео покороче или другое качество."
                    )
                else:
                    logger.error(f"TelegramBadRequest for {user_id}: {e}")
            except Exception as e:
                logger.error(f"Error sending video to {user_id}: {e}")
                await self._safe_send(user_id, "⚠️ Видео скачано, но не удалось отправить. Попробуй позже.")

    async def _handle_download_error(self, user_id: int, platform: str, emoji: str, error: Optional[str]):
        """Красивые, понятные сообщения об ошибках вместо технического мусора."""
        if error == "auth_required":
            if platform == "youtube":
                msg = (
                    f"🔒 <b>YouTube требует авторизацию</b>\n\n"
                    f"Это видео защищено от скачивания.\n"
                    f"Попробуй другое видео или обычную ссылку без <code>?is=...</code> в конце."
                )
            else:
                msg = (
                    f"🔒 <b>Требуется авторизация</b>\n\n"
                    f"Этот контент закрыт от скачивания.\n"
                    f"Проверь, что ссылка ведёт на публичное видео."
                )
        elif error and "Private" in error:
            msg = (
                f"🔒 <b>Приватное видео</b>\n\n"
                f"Этот пост закрыт — скачать не получится.\n"
                f"Попробуй другую ссылку."
            )
        elif error and ("unavailable" in error.lower() or "removed" in error.lower()):
            msg = (
                f"🗑 <b>Видео удалено или недоступно</b>\n\n"
                f"Контент больше не существует.\n"
                f"Возможно, автор удалил его."
            )
        elif error and "format" in error.lower():
            msg = (
                f"{emoji} <b>Не удалось скачать</b>\n\n"
                f"Платформа изменила формат — попробуй скинуть ссылку ещё раз.\n"
                f"Если не помогает — попробуй другое видео."
            )
        else:
            msg = (
                f"😕 <b>Не удалось скачать</b>\n\n"
                f"Что-то пошло не так. Проверь ссылку и попробуй снова.\n"
                f"Работают только публичные видео."
            )

        await self._safe_send(user_id, msg)

    async def _download_with_retries(self, url: str) -> tuple[Optional[any], Optional[str]]:
        """3 попытки с нарастающей задержкой."""
        last_error = None
        for attempt in range(1, 4):
            video_bytes, error = await self.downloader.download(url)
            if video_bytes:
                return video_bytes, None

            last_error = error
            logger.warning(f"Attempt {attempt}/3 failed for {url}: {error}")

            # Не ретраим если проблема с авторизацией — бесполезно
            if error in ("auth_required",):
                break
            if attempt < 3:
                await asyncio.sleep(attempt * 2)

        return None, last_error

    async def _send_ad_if_available(self, user_id: int):
        try:
            ads = await self.ad_service.get_active_ads()
            if ads:
                ad = random.choice(ads)
                await self._safe_send(user_id, f"📢 {ad.message_text}")
        except Exception:
            pass

    async def _safe_send(self, user_id: int, text: str):
        try:
            await bot.send_message(user_id, text)
        except Exception as e:
            logger.debug(f"Could not send message to {user_id}: {e}")

    async def run(self):
        logger.info("🚀 Worker started")
        while True:
            try:
                task = await self.queue_service.pop_task()
                if task:
                    asyncio.create_task(self.process_task(task))
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.exception(f"Critical worker loop error: {e}")
                await asyncio.sleep(5)