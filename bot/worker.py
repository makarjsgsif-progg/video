"""
bot/worker.py

Changes vs previous version:
- Full i18n: every user-facing message uses get_text(lang, key) based on user's language
- Progress bar: all users see an animated progress bar while their video downloads
- Queue position: non-premium users see their position in queue at the start
- Artificial delay: non-premium users wait FREE_USER_DELAY seconds before download begins
  (progress bar fills slowly; premium users see a fast bar)
- _safe_send now returns the sent Message object so we can track the progress message ID
- _delete_progress: cleanly deletes the progress message before sending the video
- Bug fix: push_task now retries on first-connection Redis failures (see services.py)
"""

import asyncio
import logging
import random
from typing import Optional

from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter, TelegramBadRequest
from aiogram.types import BufferedInputFile, Message

from bot.loader import bot
from database.database import UserRepo, DownloadRepo
from services.services import QueueService, Downloader, AdService, LimitService
from config.config import settings
from utils.i18n import get_text

logger = logging.getLogger(__name__)

# Artificial delay for non-premium users (seconds) — makes the queue feel real
FREE_USER_DELAY = 12

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


def _make_bar(step: int, total: int = 10) -> str:
    """Build a simple block progress bar: █████░░░░░"""
    return "█" * step + "░" * (total - step)


class Worker:
    def __init__(self, max_concurrent_tasks: int = 5):
        self.queue_service = QueueService()
        self.downloader = Downloader()
        self.ad_service = AdService()
        self.limit_service = LimitService()
        self.max_concurrent_tasks = max_concurrent_tasks
        self._active_tasks: set[asyncio.Task] = set()

    # ---------------------------------------------------------------------- #
    #  Entry point                                                            #
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
    #  Task processing                                                        #
    # ---------------------------------------------------------------------- #

    async def process_task(self, task: dict):
        try:
            await self._execute_download_logic(task)
        except Exception as e:
            logger.exception(f"Unhandled error in process_task: {e}")

    # ---------------------------------------------------------------------- #
    #  Progress bar animation                                                 #
    # ---------------------------------------------------------------------- #

    async def _animate_progress(
        self,
        user_id: int,
        message_id: int,
        lang: str,
        is_premium: bool,
        queue_pos: Optional[int],
        stop_event: asyncio.Event,
    ):
        """
        Edits the progress message to animate a bar from 0 → 100 %.
        Premium  : 10 steps × 1.0 s  = ~10 s  (fast bar)
        Non-prem : 10 steps × 2.2 s  = ~22 s  (slow, matches FREE_USER_DELAY)

        Stops immediately when stop_event is set.
        """
        step_delay = 1.0 if is_premium else 2.2

        for step in range(1, 11):
            if stop_event.is_set():
                return

            pct = step * 10
            bar = _make_bar(step)

            # Show queue position for the first 3 steps of non-premium
            if queue_pos and not is_premium and step <= 3:
                prefix = get_text(lang, "progress_queue_pos", pos=queue_pos) + "\n\n"
            else:
                prefix = ""

            text = (
                f"{prefix}"
                f"⏳ {get_text(lang, 'progress_downloading')}\n\n"
                f"[{bar}] {pct}%"
            )

            try:
                await bot.edit_message_text(
                    text,
                    chat_id=user_id,
                    message_id=message_id,
                )
            except Exception:
                pass  # message deleted / flood — ignore

            # Wait step_delay, but abort early if stop_event fires
            try:
                await asyncio.wait_for(
                    asyncio.shield(stop_event.wait()),
                    timeout=step_delay,
                )
                return  # stop_event was set during wait
            except asyncio.TimeoutError:
                pass  # normal: continue to next step

    # ---------------------------------------------------------------------- #
    #  Main download logic                                                    #
    # ---------------------------------------------------------------------- #

    async def _execute_download_logic(self, task: dict):
        user_id: int = task["user_id"]
        url: str = task["url"]
        platform: str = task.get("platform", "unknown")
        platform_key = platform.split(".")[-1].lower() if "." in platform else platform.lower()
        emoji = PLATFORM_EMOJI.get(platform_key, "📥")

        # ── 1. Verify user ────────────────────────────────────────────────
        user_repo = UserRepo()
        user = await user_repo.get_user(user_id)
        if not user or user.is_banned:
            logger.info(f"Skipping task for banned/unknown user {user_id}")
            return

        is_premium: bool = bool(user.is_premium)
        lang: str = getattr(user, "language", "ru") or "ru"

        # ── 2. Check daily limit (with Turbo-Demo fallback) ───────────────
        is_turbo = False
        limit_ok = await self.limit_service.check_and_increment(user_id, is_premium)

        if not limit_ok:
            turbo_granted = await user_repo.use_turbo(user_id)
            if turbo_granted:
                await self.limit_service.rollback(user_id)
                is_turbo = True
                logger.info(f"Turbo-Download granted to user {user_id}")
            else:
                used, limit = await self.limit_service.get_usage(user_id)
                await self._safe_send(
                    user_id,
                    get_text(lang, "download_limit_reached", used=used, limit=limit),
                )
                return

        # ── 3. Queue position (non-premium only) ──────────────────────────
        queue_pos: Optional[int] = None
        if not is_premium:
            try:
                q_len = await self.queue_service.get_queue_length()
                if q_len > 0:
                    queue_pos = q_len
            except Exception:
                pass

        # ── 4. Send initial progress message ─────────────────────────────
        progress_msg: Optional[Message] = None
        stop_event = asyncio.Event()
        anim_task: Optional[asyncio.Task] = None

        initial_text = get_text(lang, "progress_starting")
        if queue_pos:
            initial_text = (
                get_text(lang, "progress_queue_pos", pos=queue_pos) + "\n\n" + initial_text
            )

        progress_msg = await self._safe_send(user_id, initial_text)

        if progress_msg:
            anim_task = asyncio.create_task(
                self._animate_progress(
                    user_id, progress_msg.message_id,
                    lang, is_premium, queue_pos, stop_event,
                )
            )

        # ── 5. Non-premium artificial delay ──────────────────────────────
        if not is_premium and not is_turbo:
            await asyncio.sleep(FREE_USER_DELAY)

        # ── 6. Download ───────────────────────────────────────────────────
        try:
            video_bytes, error = await asyncio.wait_for(
                self._download_with_retries(url),
                timeout=settings.DOWNLOAD_TIMEOUT + 10,
            )
        except asyncio.TimeoutError:
            await self.limit_service.rollback(user_id)
            stop_event.set()
            await self._cancel_anim(anim_task)
            await self._delete_progress(user_id, progress_msg)
            await self._safe_send(
                user_id,
                get_text(lang, "download_timeout", emoji=emoji),
            )
            return
        finally:
            # Always stop animation when download finishes (success or error)
            stop_event.set()
            await self._cancel_anim(anim_task)

        await self._delete_progress(user_id, progress_msg)

        if not video_bytes:
            await self.limit_service.rollback(user_id)
            await self._handle_download_error(user_id, platform_key, emoji, error, lang)
            return

        # ── 7. Build success caption ──────────────────────────────────────
        if is_turbo:
            caption = get_text(lang, "download_done_turbo", emoji=emoji)
        else:
            caption = get_text(lang, "download_done", emoji=emoji)

        # ── 8. Send video ─────────────────────────────────────────────────
        sent = await self._send_video(user_id, video_bytes, caption, lang)

        if not sent:
            await self.limit_service.rollback(user_id)
            return

        # ── 9. Log download ───────────────────────────────────────────────
        try:
            dl_repo = DownloadRepo()
            await dl_repo.add_download(user_id, platform_key)
        except Exception as e:
            logger.error(f"Failed to record download for {user_id}: {e}")

        # ── 10. Post-send messaging ───────────────────────────────────────
        if is_turbo:
            await self._safe_send(user_id, get_text(lang, "turbo_download_used"))
        elif not is_premium:
            await self._send_ad_if_available(user_id)

    # ---------------------------------------------------------------------- #
    #  Send video                                                             #
    # ---------------------------------------------------------------------- #

    async def _send_video(
        self, user_id: int, video_bytes, caption: str, lang: str
    ) -> bool:
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
                await self._safe_send(user_id, get_text(lang, "download_file_too_big"))
            elif "wrong file identifier" in err_lower:
                await self._safe_send(user_id, get_text(lang, "download_file_error"))
            else:
                logger.error(f"TelegramBadRequest for {user_id}: {e}")
                await self._safe_send(user_id, get_text(lang, "download_send_error"))
            return False

        except Exception as e:
            logger.error(f"Error sending video to {user_id}: {e}")
            await self._safe_send(user_id, get_text(lang, "download_send_error_retry"))
            return False

    # ---------------------------------------------------------------------- #
    #  Download with retries                                                  #
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
    #  Download error handler                                                 #
    # ---------------------------------------------------------------------- #

    async def _handle_download_error(
        self,
        user_id: int,
        platform: str,
        emoji: str,
        error: Optional[str],
        lang: str,
    ):
        logger.error(
            f"Download failed | user={user_id} platform={platform} error={error!r}"
        )

        if error == "auth_required":
            msg = get_text(lang, "download_error_auth")
        elif error and ("Private" in error or "private" in error):
            msg = get_text(lang, "download_error_private")
        elif error and ("unavailable" in error.lower() or "removed" in error.lower()):
            msg = get_text(lang, "download_error_unavailable")
        elif error and "format" in error.lower():
            msg = get_text(lang, "download_error_format", emoji=emoji)
        else:
            msg = get_text(lang, "download_error_generic", emoji=emoji)

        await self._safe_send(user_id, msg)

    # ---------------------------------------------------------------------- #
    #  Helpers                                                                #
    # ---------------------------------------------------------------------- #

    async def _cancel_anim(self, anim_task: Optional[asyncio.Task]):
        if anim_task and not anim_task.done():
            anim_task.cancel()
            try:
                await anim_task
            except (asyncio.CancelledError, Exception):
                pass

    async def _delete_progress(self, user_id: int, msg: Optional[Message]):
        if msg is None:
            return
        try:
            await bot.delete_message(user_id, msg.message_id)
        except Exception:
            pass

    async def _send_ad_if_available(self, user_id: int, position: str = "after_download"):
        try:
            ads = await self.ad_service.get_active_ads()
            filtered = [
                a for a in ads
                if getattr(a, "position", "after_download") == position
            ]
            if filtered:
                ad = random.choice(filtered)
                await self._safe_send(user_id, f"📢 {ad.message_text}")
        except Exception as e:
            logger.debug(f"Ad send failed for {user_id}: {e}")

    async def _safe_send(
        self, user_id: int, text: str, kb=None
    ) -> Optional[Message]:
        """Send a message and return the Message object (or None on failure)."""
        try:
            return await bot.send_message(user_id, text, reply_markup=kb)
        except TelegramForbiddenError:
            logger.debug(f"User {user_id} blocked the bot (safe_send).")
        except Exception as e:
            logger.debug(f"safe_send failed for {user_id}: {e}")
        return None