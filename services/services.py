import asyncio
import json
import os
import logging
import string
import random
from io import BytesIO
from typing import Optional, Tuple

import redis.asyncio as redis
import yt_dlp
from yt_dlp.utils import DownloadError

from config.config import settings
from database.database import async_session_maker, AdRepo

logger = logging.getLogger(__name__)


def generate_referral_code(length: int = 8) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


class AdService:
    async def get_active_ads(self):
        async with async_session_maker() as session:
            repo = AdRepo(session)
            return await repo.get_active_ads()


class Downloader:
    """
    Загрузчик видео через yt-dlp с расширенными настройками для TikTok.
    Исправляет ошибку «status code 0» на Render.com.
    """

    _UA_MOBILE = (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Mobile Safari/537.36"
    )
    _UA_DESKTOP = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    BASE_OPTS: dict = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "merge_output_format": "mp4",
        "outtmpl": "downloads/%(id)s.%(ext)s",
        "extractor_args": {
            "youtube": {
                "player_client": ["ios", "web"],
                "player_skip": ["webpage", "configs"],
            },
            "tiktok": {
                "webpage_download": True,          # скачивать через эмуляцию браузера
                "api_hostname": "www.tiktok.com",   # использовать основной домен
            },
        },
        "http_headers": {
            "User-Agent": _UA_MOBILE,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.tiktok.com/",
            "Origin": "https://www.tiktok.com",
        },
        "socket_timeout": 30,
        "retries": 5,
        "fragment_retries": 5,
        "continuedl": True,
        "ignoreerrors": False,
        "postprocessors": [
            {
                "key": "FFmpegVideoConvertor",
                "preferedformat": "mp4",
            }
        ],
    }

    FORMAT_CHAIN = [
        "best[height<=720]/best",
        "bestvideo+bestaudio/best",
        "best",
    ]

    def __init__(self):
        os.makedirs("downloads", exist_ok=True)
        self.cookies_available = os.path.exists(settings.COOKIES_FILE)

    async def download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        return await asyncio.to_thread(self._sync_download, url)

    def _build_opts(self, fmt: str) -> dict:
        opts = {**self.BASE_OPTS, "format": fmt}
        if self.cookies_available:
            opts["cookiefile"] = settings.COOKIES_FILE
        return opts

    def _sync_download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        last_error: Optional[str] = None

        for fmt in self.FORMAT_CHAIN:
            temp_path: Optional[str] = None
            try:
                opts = self._build_opts(fmt)
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)

                    if info and "entries" in info:
                        info = info["entries"][0]

                    if not info:
                        last_error = "No media info returned"
                        continue

                    temp_path = ydl.prepare_filename(info)
                    if not os.path.exists(temp_path):
                        base = os.path.splitext(temp_path)[0]
                        for ext in (".mp4", ".mkv", ".webm", ".m4v", ".mov"):
                            candidate = base + ext
                            if os.path.exists(candidate):
                                temp_path = candidate
                                break

                    if not os.path.exists(temp_path):
                        last_error = "Downloaded file not found on disk"
                        logger.warning(f"File not found after download: {temp_path}")
                        continue

                    with open(temp_path, "rb") as f:
                        buf = BytesIO(f.read())
                        buf.name = "video.mp4"
                    return buf, None

            except DownloadError as de:
                err_str = str(de)
                last_error = err_str
                logger.warning(f"yt-dlp DownloadError (fmt='{fmt}'): {de}")

                if any(kw in err_str for kw in ("Sign in", "bot detection", "login required")):
                    return None, "auth_required"
                if "Private" in err_str or "private" in err_str:
                    return None, err_str
                continue

            except Exception as e:
                last_error = str(e)
                logger.exception(f"Unexpected error downloading {url}: {e}")
                continue

            finally:
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError as oe:
                        logger.debug(f"Could not remove temp file {temp_path}: {oe}")

        logger.error(f"All format attempts failed for {url}. Last error: {last_error}")
        return None, last_error


class LimitService:
    def __init__(self):
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.TTL = 86400

    async def check_and_increment(self, user_id: int, is_premium: bool) -> bool:
        if is_premium:
            return True
        key = f"daily_limit:{user_id}"
        bonus_key = f"referral_bonus:{user_id}"
        current = await self.redis.incr(key)
        if current == 1:
            await self.redis.expire(key, self.TTL)
        bonus = int(await self.redis.get(bonus_key) or 0)
        effective_limit = settings.DEFAULT_DAILY_LIMIT + bonus
        return current <= effective_limit

    async def get_usage(self, user_id: int) -> tuple[int, int]:
        key = f"daily_limit:{user_id}"
        bonus_key = f"referral_bonus:{user_id}"
        current = int(await self.redis.get(key) or 0)
        bonus = int(await self.redis.get(bonus_key) or 0)
        limit = settings.DEFAULT_DAILY_LIMIT + bonus
        return current, limit

    async def add_referral_bonus(self, user_id: int, amount: int = 5):
        bonus_key = f"referral_bonus:{user_id}"
        await self.redis.incrby(bonus_key, amount)


class QueueService:
    def __init__(self):
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.queue_key = "download_queue"

    async def push_task(self, user_id: int, url: str, platform: str):
        task = {"user_id": user_id, "url": url, "platform": platform, "attempt": 1}
        await self.redis.lpush(self.queue_key, json.dumps(task))

    async def pop_task(self) -> Optional[dict]:
        raw = await self.redis.rpop(self.queue_key)
        return json.loads(raw) if raw else None

    async def get_queue_length(self) -> int:
        return await self.redis.llen(self.queue_key)