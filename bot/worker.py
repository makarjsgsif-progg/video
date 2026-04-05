"""
bot/worker.py

Изменения:
- Все пользовательские сообщения переписаны на вирусный/энергичный русский
- Логика без изменений (pop_task BRPOP, rollback, retry, etc.)
"""

import asyncio
import logging
import random
from typing import Optional

from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter, TelegramBadRequest

from aiogram.types import BufferedInputFile

from bot.loader import bot
from database.database import UserRepo, DownloadRepo
from services.services import QueueService, Downloader, AdService, LimitService
from config.config import settings

logger = logging.getLogger(__name__)

# Artificial delay (seconds) shown to free users before download starts.
# Creates perceived value gap vs Premium ("instant").
FREE_USER_DELAY = 15  # seconds

PLATFORM_EMOJI = {
    "tiktok": "🎵",
    "instagram": "📸",
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
        self.max_concurrent_tasks = max_concurrent_tasks
        self._active_tasks: set[asyncio.Task] = set()

    # ---------------------------------------------------------------------- #
    #  Точка входа                                                            #
    # ---------------------------------------------------------------------- #

    async def run(self):
        logger.info("🚀 Worker started")
        while True:
            try:
                if len(self._active_tasks) >= self.max_concurrent_tasks:
                    await asyncio.wait(
                        self._active_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    continue

                task = await self.queue_service.pop_task(timeout=2)
                if task:
                    t = asyncio.create_task(self.process_task(task))
                    self._active_tasks.add(t)
                    t.add_done_callback(self._task_done_callback)

            except asyncio.CancelledError:
                logger.info("Worker cancelled, waiting for active tasks…")
                if self._active_tasks:
                    await asyncio.gather(*self._active_tasks, return_exceptions=True)
                break
            except Exception as e:
                logger.exception(f"Critical worker loop error: {e}")
                await asyncio.sleep(5)

    def _task_done_callback(self, task: asyncio.Task):
        self._active_tasks.discard(task)
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error(f"Task raised an exception: {exc}")

    # ---------------------------------------------------------------------- #
    #  Обработка задачи                                                       #
    # ---------------------------------------------------------------------- #

    async def process_task(self, task: dict):
        try:
            await self._execute_download_logic(task)
        except Exception as e:
            logger.exception(f"Unhandled error in process_task: {e}")

    async def _execute_download_logic(self, task: dict):
        user_id: int = task["user_id"]
        url: str = task["url"]
        platform: str = task.get("platform", "unknown")
        platform_key = platform.split(".")[-1].lower() if "." in platform else platform.lower()
        emoji = PLATFORM_EMOJI.get(platform_key, "📥")

        # ── 1. Проверяем пользователя ─────────────────────────────────────
        user_repo = UserRepo()
        user = await user_repo.get_user(user_id)
        if not user or user.is_banned:
            logger.info(f"Skipping task for banned/unknown user {user_id}")
            return
        is_premium: bool = bool(user.is_premium)

        # ── 2. Проверяем лимит ────────────────────────────────────────────
        if not await self.limit_service.check_and_increment(user_id, is_premium):
            used, limit = await self.limit_service.get_usage(user_id)
            await self._safe_send(
                user_id,
                f"⏳ <b>Лимит на сегодня исчерпан!</b>\n\n"
                f"Ты уже скачал <b>{used}/{limit}</b> видео сегодня — это максимум для бесплатного плана.\n\n"
                f"💎 <b>Premium</b> — безлимитные загрузки каждый день без ограничений.\n"
                f"👥 Или пригласи друга → /referral и получи <b>+5 загрузок</b> прямо сейчас!",
            )
            return

        # ── 3. Скачивание ─────────────────────────────────────────────────
        try:
            video_bytes, error = await asyncio.wait_for(
                self._download_with_retries(url),
                timeout=settings.DOWNLOAD_TIMEOUT + 10,
            )
        except asyncio.TimeoutError:
            await self.limit_service.rollback(user_id)
            await self._safe_send(
                user_id,
                f"{emoji} <b>Слишком долго!</b>\n\n"
                "Скачивание зависло — скорее всего, сервер платформы лагает.\n"
                "Попробуй ещё раз или выбери другое видео.",
            )
            return

        if not video_bytes:
            await self.limit_service.rollback(user_id)
            await self._handle_download_error(user_id, platform_key, emoji, error)
            return

        # ── 4. Отправка видео ─────────────────────────────────────────────
        caption = (
            f"{emoji} <b>Готово! Лови своё видео 🎉</b>\n\n"
            f"📲 Понравился бот? Поделись с друзьями → /referral"
        )
        sent = await self._send_video(user_id, video_bytes, caption)

        if not sent:
            await self.limit_service.rollback(user_id)
            return

        # ── 5. Запись в БД ────────────────────────────────────────────────
        try:
            dl_repo = DownloadRepo()
            await dl_repo.add_download(user_id, platform_key)
        except Exception as e:
            logger.error(f"Failed to record download for {user_id}: {e}")

        # ── 6. Реклама для не-премиумов ───────────────────────────────────
        if not is_premium:
            await self._send_ad_if_available(user_id)

    # ---------------------------------------------------------------------- #
    #  Отправка видео                                                         #
    # ---------------------------------------------------------------------- #

    async def _send_video(self, user_id: int, video_bytes, caption: str) -> bool:
        async def _do_send(buf):
            buf.seek(0)
            file = BufferedInputFile(buf.read(), filename="video.mp4")
            await bot.send_video(user_id, video=file, caption=caption)

        try:
            await _do_send(video_bytes)
            return True

        except TelegramForbiddenError:
            logger.info(f"User {user_id} blocked the bot.")
            return False

        except TelegramRetryAfter as e:
            logger.warning(f"Flood control: retry after {e.retry_after}s for {user_id}")
            await asyncio.sleep(e.retry_after)
            try:
                await _do_send(video_bytes)
                return True
            except Exception as retry_err:
                logger.error(f"Retry send failed for {user_id}: {retry_err}")
                return False

        except TelegramBadRequest as e:
            err_lower = str(e).lower()
            if "file is too big" in err_lower:
                await self._safe_send(
                    user_id,
                    "😔 <b>Файл слишком тяжёлый</b>\n\n"
                    "Telegram не принимает видео тяжелее 50 МБ — это его ограничение, не моё.\n"
                    "Попробуй видео покороче или выбери другое качество.",
                )
            elif "wrong file identifier" in err_lower:
                await self._safe_send(
                    user_id,
                    "⚠️ <b>Что-то пошло не так с файлом</b>\n\nПопробуй ещё раз — обычно помогает!"
                )
            else:
                logger.error(f"TelegramBadRequest for {user_id}: {e}")
                await self._safe_send(
                    user_id,
                    "⚠️ <b>Не удалось отправить видео</b>\n\nПопробуй позже — мы уже разбираемся."
                )
            return False

        except Exception as e:
            logger.error(f"Error sending video to {user_id}: {e}")
            await self._safe_send(
                user_id,
                "⚠️ <b>Видео скачано, но не отправилось</b>\n\n"
                "Попробуй отправить ссылку ещё раз — это должно сработать.",
            )
            return False

    # ---------------------------------------------------------------------- #
    #  Загрузка с повторами                                                   #
    # ---------------------------------------------------------------------- #

    async def _download_with_retries(
        self, url: str
    ) -> tuple[Optional[object], Optional[str]]:
        last_error = None
        for attempt in range(1, settings.MAX_RETRIES + 1):
            logger.info(f"Download attempt {attempt}/{settings.MAX_RETRIES} for {url}")
            video_bytes, error = await self.downloader.download(url)
            if video_bytes:
                return video_bytes, None

            last_error = error
            logger.warning(f"Attempt {attempt}/{settings.MAX_RETRIES} failed: {error}")

            if error == "auth_required" or (error and "Private" in error):
                break
            if attempt < settings.MAX_RETRIES:
                await asyncio.sleep(attempt * 2)

        return None, last_error

    # ---------------------------------------------------------------------- #
    #  Обработка ошибки скачивания                                           #
    # ---------------------------------------------------------------------- #

    async def _handle_download_error(
        self, user_id: int, platform: str, emoji: str, error: Optional[str]
    ):
        logger.error(
            f"Download failed | user={user_id} platform={platform} error={error!r}"
        )

        if error == "auth_required":
            msg = (
                "🔒 <b>Нужна авторизация</b>\n\n"
                "Этот контент закрыт — я не могу его скачать.\n"
                "Убедись, что ссылка ведёт на <b>публичное</b> видео и попробуй снова."
            )
        elif error and ("Private" in error or "private" in error):
            msg = (
                "🔒 <b>Приватное видео</b>\n\n"
                "Этот пост скрыт от публики — скачать невозможно.\n"
                "Попробуй другую ссылку."
            )
        elif error and ("unavailable" in error.lower() or "removed" in error.lower()):
            msg = (
                "🗑 <b>Видео удалено или недоступно</b>\n\n"
                "Похоже, автор удалил его или оно стало недоступным.\n"
                "Ничего не могу поделать — попробуй другое видео."
            )
        elif error and "format" in error.lower():
            msg = (
                f"{emoji} <b>Платформа обновила защиту</b>\n\n"
                "Мы уже работаем над исправлением — попробуй отправить ссылку ещё раз.\n"
                "Если не помогает — попробуй другое видео."
            )
        else:
            msg = (
                f"{emoji} <b>Не получилось скачать</b>\n\n"
                "Что-то пошло не так на стороне платформы.\n"
                "Проверь ссылку и попробуй ещё раз — работают только публичные видео."
            )

        await self._safe_send(user_id, msg)

    # ---------------------------------------------------------------------- #
    #  Вспомогательные методы                                                 #
    # ---------------------------------------------------------------------- #

    async def _send_ad_if_available(self, user_id: int, position: str = "after_download"):
        try:
            ads = await self.ad_service.get_active_ads()
            # Filter by position (default "after_download" for ads without position field)
            filtered = [
                a for a in ads
                if getattr(a, "position", "after_download") == position
            ]
            if filtered:
                ad = random.choice(filtered)
                await self._safe_send(user_id, f"📢 {ad.message_text}")
        except Exception as e:
            logger.debug(f"Ad send failed for {user_id}: {e}")

    async def _safe_send(self, user_id: int, text: str, kb=None):
        try:
            await bot.send_message(user_id, text, reply_markup=kb)
        except TelegramForbiddenError:
            logger.debug(f"User {user_id} blocked the bot (safe_send).")
        except Exception as e:
            logger.debug(f"safe_send failed for {user_id}: {e}")