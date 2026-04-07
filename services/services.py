"""
services/services.py

Улучшения:
- LimitService: атомарный Lua-скрипт для check_and_increment — нет race condition
  между INCR и проверкой лимита в конкурентной среде
- LimitService.get_usage: один MGET вместо двух отдельных GET
- Downloader: YouTube поддержка через cookies + SponsorBlock-aware опции убраны
  (не нужны для TG-бота), добавлен таймаут из settings
- QueueService: BRPOP с таймаутом вместо busy-loop (pop_task теперь блокирующий,
  worker.run вызывает его напрямую без sleep)
- AdService: кэширует активные объявления в Redis на 5 минут
- Полное логирование с уровнями
"""

import asyncio
import copy
import json
import logging
import os
import random
import shutil
import string
import tempfile
from io import BytesIO
from typing import Optional, Tuple

import redis.asyncio as aioredis
import yt_dlp
from yt_dlp.utils import DownloadError

from config.config import settings
from database.database import AdRepo

logger = logging.getLogger(__name__)

_AD_CACHE_TTL = 300  # 5 минут


# --------------------------------------------------------------------------- #
#  Утилиты                                                                     #
# --------------------------------------------------------------------------- #

def generate_referral_code(length: int = 8) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


# --------------------------------------------------------------------------- #
#  AdService                                                                   #
# --------------------------------------------------------------------------- #

class AdService:
    """Сервис рекламных объявлений с Redis-кэшированием."""

    def __init__(self):
        self.redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        self._cache_key = "ads:active_cache"

    async def get_active_ads(self):
        try:
            cached = await self.redis.get(self._cache_key)
            if cached:
                rows = json.loads(cached)
                return [type("Ad", (), row)() for row in rows]
        except Exception as e:
            logger.debug(f"AdService cache get failed: {e}")

        repo = AdRepo()
        ads = await repo.get_active_ads()

        try:
            rows = [
                {
                    "id": a.id,
                    "message_text": a.message_text,
                    "is_active": a.is_active,
                }
                for a in ads
            ]
            await self.redis.setex(self._cache_key, _AD_CACHE_TTL, json.dumps(rows))
        except Exception as e:
            logger.debug(f"AdService cache set failed: {e}")

        return ads

    async def invalidate_cache(self):
        """Вызывается после изменения объявлений через /admin_*."""
        try:
            await self.redis.delete(self._cache_key)
        except Exception:
            pass


# --------------------------------------------------------------------------- #
#  Downloader                                                                  #
# --------------------------------------------------------------------------- #

class Downloader:
    """
    Загрузчик видео через yt-dlp.

    - Deep copy опций между вызовами — нет мутации shared state
    - Временная директория через tempfile.mkdtemp, чистится в finally
    - Поиск файла с учётом изменения расширения постпроцессором
    - Cookies из settings.COOKIES_FILE (опционально)
    - Таймаут из settings.DOWNLOAD_TIMEOUT
    """

    _UA_DESKTOP = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    _BASE_OPTS: dict = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 30,
        "retries": 3,
        "fragment_retries": 3,
        "ignoreerrors": False,
        "http_headers": {
            "User-Agent": _UA_DESKTOP,
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    FORMAT_CHAIN = [
        "best[ext=mp4][height<=720]/best[ext=mp4]/best[height<=720]/best",
        "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
        "worst",
    ]

    _VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".m4v", ".mov", ".avi", ".flv")

    # Ключевые слова ошибок, при которых не надо повторять попытки
    _FATAL_ERRORS = (
        "Sign in",
        "bot detection",
        "login required",
        "confirm your age",
        "This video is only available",
        "This content isn't available",
    )

    def __init__(self):
        self.cookies_available = os.path.exists(settings.COOKIES_FILE)
        if self.cookies_available:
            logger.info(f"Cookies file found: {settings.COOKIES_FILE}")
        else:
            logger.warning(
                f"Cookies file not found: {settings.COOKIES_FILE} — "
                "YouTube and some platforms may block downloads"
            )

    async def download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        return await asyncio.to_thread(self._sync_download, url)

    def _build_opts(self, fmt: str, out_dir: str) -> dict:
        opts = copy.deepcopy(self._BASE_OPTS)
        opts["format"] = fmt
        opts["outtmpl"] = os.path.join(out_dir, "%(id)s.%(ext)s")
        opts["socket_timeout"] = settings.DOWNLOAD_TIMEOUT
        if self.cookies_available:
            opts["cookiefile"] = settings.COOKIES_FILE
        return opts

    def _find_downloaded_file(self, out_dir: str, expected_path: str) -> Optional[str]:
        if os.path.exists(expected_path):
            return expected_path
        base = os.path.splitext(expected_path)[0]
        for ext in self._VIDEO_EXTS:
            candidate = base + ext
            if os.path.exists(candidate):
                return candidate
        try:
            for fname in os.listdir(out_dir):
                if any(fname.endswith(ext) for ext in self._VIDEO_EXTS):
                    return os.path.join(out_dir, fname)
        except OSError:
            pass
        return None

    def _sync_download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        last_error: Optional[str] = None
        out_dir = tempfile.mkdtemp(prefix="ytdlp_")

        try:
            for fmt in self.FORMAT_CHAIN:
                try:
                    opts = self._build_opts(fmt, out_dir)
                    logger.info(f"Trying format '{fmt}' for: {url}")

                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(url, download=True)

                        if info and "entries" in info:
                            entries = list(info["entries"])
                            if not entries:
                                last_error = "Empty playlist"
                                continue
                            info = entries[0]

                        if not info:
                            last_error = "No media info returned"
                            logger.warning(f"No info for: {url}")
                            continue

                        expected_path = ydl.prepare_filename(info)
                        file_path = self._find_downloaded_file(out_dir, expected_path)

                        if not file_path:
                            last_error = "Downloaded file not found on disk"
                            logger.warning(
                                f"File not found. Expected: {expected_path}, "
                                f"dir: {os.listdir(out_dir)}"
                            )
                            continue

                        size = os.path.getsize(file_path)
                        logger.info(f"Downloaded: {file_path} ({size:,} bytes)")

                        with open(file_path, "rb") as f:
                            buf = BytesIO(f.read())
                            buf.name = "video.mp4"
                        return buf, None

                except DownloadError as de:
                    err_str = str(de)
                    last_error = err_str
                    logger.warning(f"DownloadError (fmt='{fmt}'): {de}")

                    if any(kw in err_str for kw in self._FATAL_ERRORS):
                        return None, "auth_required"
                    if "Private" in err_str or "private" in err_str:
                        return None, err_str
                    continue

                except Exception as e:
                    last_error = str(e)
                    logger.exception(f"Unexpected error downloading {url}: {e}")
                    continue

        finally:
            try:
                shutil.rmtree(out_dir, ignore_errors=True)
            except Exception:
                pass

        logger.error(f"All format attempts failed for {url}. Last error: {last_error}")
        return None, last_error


# --------------------------------------------------------------------------- #
#  LimitService                                                                #
# --------------------------------------------------------------------------- #

# Lua-скрипт: атомарный INCR + EXPIRE + проверка лимита.
# Возвращает [current_value, effective_limit].
_LIMIT_LUA = """
local key     = KEYS[1]
local bkey    = KEYS[2]
local ttl     = tonumber(ARGV[1])
local base    = tonumber(ARGV[2])

local current = redis.call('INCR', key)
if current == 1 then
    redis.call('EXPIRE', key, ttl)
end
local bonus = tonumber(redis.call('GET', bkey) or 0)
local limit = base + bonus
return {current, limit}
"""


class LimitService:
    """
    Сервис ежедневных лимитов загрузок.

    Использует Lua-скрипт для атомарной операции INCR + проверка,
    чтобы исключить race condition в конкурентной среде.
    """

    def __init__(self):
        self.redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        self.TTL = 86400  # 24 часа
        self._script = self.redis.register_script(_LIMIT_LUA)

    async def check_and_increment(self, user_id: int, is_premium: bool) -> bool:
        """True — лимит не исчерпан (загрузку можно выполнять)."""
        if is_premium:
            return True
        try:
            current, limit = await self._script(
                keys=[f"daily_limit:{user_id}", f"referral_bonus:{user_id}"],
                args=[self.TTL, settings.DEFAULT_DAILY_LIMIT],
            )
            return int(current) <= int(limit)
        except Exception as e:
            logger.error(f"LimitService.check_and_increment error for {user_id}: {e}")
            return True  # Fail-open: не блокируем пользователя при сбое Redis

    async def get_usage(self, user_id: int) -> tuple[int, int]:
        """Возвращает (использовано, лимит)."""
        try:
            values = await self.redis.mget(
                f"daily_limit:{user_id}",
                f"referral_bonus:{user_id}",
            )
            current = int(values[0] or 0)
            bonus = int(values[1] or 0)
            limit = settings.DEFAULT_DAILY_LIMIT + bonus
            return current, limit
        except Exception as e:
            logger.error(f"LimitService.get_usage error for {user_id}: {e}")
            return 0, settings.DEFAULT_DAILY_LIMIT

    async def add_referral_bonus(self, user_id: int, amount: int = 5):
        try:
            await self.redis.incrby(f"referral_bonus:{user_id}", amount)
        except Exception as e:
            logger.error(f"LimitService.add_referral_bonus error for {user_id}: {e}")

    async def rollback(self, user_id: int):
        """Откатывает счётчик загрузок на 1 (вызывается при ошибке скачивания)."""
        try:
            key = f"daily_limit:{user_id}"
            current = int(await self.redis.get(key) or 0)
            if current > 0:
                await self.redis.decr(key)
        except Exception as e:
            logger.debug(f"LimitService.rollback error for {user_id}: {e}")


# --------------------------------------------------------------------------- #
#  QueueService                                                                #
# --------------------------------------------------------------------------- #

class QueueService:
    """
    Очередь задач на скачивание через Redis LIST.

    push_task: LPUSH (добавляет в голову)
    pop_task:  BRPOP с таймаутом 2 сек — блокирующий pop, worker не спинит
    """

    def __init__(self):
        # Отдельный клиент для блокирующего BRPOP (нельзя смешивать с pubsub/pipeline)
        self.redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        self._blocking_redis = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=None,       # Без таймаута для блокирующего чтения
            socket_connect_timeout=10,
        )
        self.queue_key = "download_queue"

    async def push_task(self, user_id: int, url: str, platform: str):
        """
        Push a download task to the Redis queue.

        Retries up to 3 times with a short backoff to handle the transient
        "first-connection" failure that can occur when the Redis connection
        pool has not yet been used (e.g. first request after a cold start).
        Without this retry the very first URL a user sends would always
        return an error while the second one would succeed.
        """
        task = {"user_id": user_id, "url": url, "platform": platform, "attempt": 1}
        payload = json.dumps(task)
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                await self.redis.lpush(self.queue_key, payload)
                logger.debug(f"Task pushed: user={user_id} platform={platform}")
                return
            except Exception as e:
                last_exc = e
                logger.warning(
                    f"push_task attempt {attempt + 1}/3 failed for user={user_id}: {e}"
                )
                await asyncio.sleep(0.4 * (attempt + 1))
        # All retries exhausted — propagate so handle_text shows error_generic
        raise last_exc

    async def pop_task(self, timeout: int = 2) -> Optional[dict]:
        """
        Блокирующий pop с таймаутом timeout секунд.
        Возвращает dict задачи или None при таймауте.
        """
        try:
            result = await self._blocking_redis.brpop(self.queue_key, timeout=timeout)
            if result:
                _, raw = result
                return json.loads(raw)
        except aioredis.exceptions.ConnectionError as e:
            logger.warning(f"QueueService.pop_task connection error: {e}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.exception(f"QueueService.pop_task error: {e}")
        return None

    async def get_queue_length(self) -> int:
        try:
            return await self.redis.llen(self.queue_key)
        except Exception as e:
            logger.error(f"QueueService.get_queue_length error: {e}")
            return -1