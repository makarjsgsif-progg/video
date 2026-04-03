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

# Генерация уникального реферального кода
def generate_referral_code(length: int = 8) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


class AdService:
    async def get_active_ads(self):
        async with async_session_maker() as session:
            repo = AdRepo(session)
            return await repo.get_active_ads()


class Downloader:
    """Загрузчик видео с поддержкой TikTok, YouTube, Instagram и других платформ."""

    # Базовые опции — без жёстких фильтров по ext, чтобы не падать на TikTok
    BASE_OPTS = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "merge_output_format": "mp4",
        "outtmpl": "downloads/%(id)s.%(ext)s",
        # iOS-клиент обходит bot-detection на YouTube
        "extractor_args": {
            "youtube": {"player_client": ["ios", "android"]},
        },
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
            ),
        },
        "socket_timeout": 20,
        "retries": 5,
        "continuedl": True,
    }

    # Цепочка форматов: сначала пробуем хорошее качество, потом best
    FORMAT_CHAIN = [
        "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=720]+bestaudio/best[height<=720]/best",
        "bestvideo+bestaudio/best",
        "best",
    ]

    def __init__(self):
        os.makedirs("downloads", exist_ok=True)
        # Если есть cookies-файл — используем его
        self.cookies_available = os.path.exists(settings.COOKIES_FILE)

    async def download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        return await asyncio.to_thread(self._sync_download, url)

    def _build_opts(self, fmt: str) -> dict:
        opts = {**self.BASE_OPTS, "format": fmt}
        if self.cookies_available:
            opts["cookiefile"] = settings.COOKIES_FILE
        return opts

    def _sync_download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        last_error = None

        for fmt in self.FORMAT_CHAIN:
            try:
                opts = self._build_opts(fmt)
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)

                    # yt-dlp иногда возвращает плейлист даже с noplaylist=True
                    if "entries" in info:
                        info = info["entries"][0]

                    temp_path = ydl.prepare_filename(info)

                    # Иногда расширение меняется после merge
                    if not os.path.exists(temp_path):
                        base = os.path.splitext(temp_path)[0]
                        for ext in (".mp4", ".mkv", ".webm", ".m4v"):
                            candidate = base + ext
                            if os.path.exists(candidate):
                                temp_path = candidate
                                break

                    if not os.path.exists(temp_path):
                        last_error = "Файл не найден после скачивания"
                        continue

                    with open(temp_path, "rb") as f:
                        buffer = BytesIO(f.read())
                        buffer.name = "video.mp4"

                    return buffer, None

            except DownloadError as de:
                last_error = str(de)
                logger.warning(f"Format '{fmt}' failed: {de}")
                # Если ошибка про cookies/авторизацию — не пробуем другие форматы
                if "Sign in" in str(de) or "bot" in str(de).lower():
                    return None, "auth_required"
                continue
            except Exception as e:
                last_error = str(e)
                logger.exception(f"Critical error downloading {url}: {e}")
                continue
            finally:
                if "temp_path" in locals() and temp_path and os.path.exists(temp_path):
                    try:
                        os.close(os.open(temp_path, os.O_RDONLY))  # Снимаем блокировку если зависла
                        os.remove(temp_path)
                    except:
                        pass

        logger.error(f"All format attempts failed for {url}. Last error: {last_error}")
        return None, last_error


class LimitService:
    """Контроль дневных лимитов через Redis."""

    def __init__(self):
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.TTL = 86400  # 24 часа

    async def check_and_increment(self, user_id: int, is_premium: bool) -> bool:
        if is_premium:
            return True

        key = f"daily_limit:{user_id}"
        bonus_key = f"referral_bonus:{user_id}"

        current = await self.redis.incr(key)
        if current == 1:
            await self.redis.expire(key, self.TTL)

        # Считаем бонус от рефералов
        bonus = int(await self.redis.get(bonus_key) or 0)
        effective_limit = settings.DEFAULT_DAILY_LIMIT + bonus

        return current <= effective_limit

    async def get_usage(self, user_id: int) -> tuple[int, int]:
        """Возвращает (использовано, лимит)."""
        key = f"daily_limit:{user_id}"
        bonus_key = f"referral_bonus:{user_id}"
        current = int(await self.redis.get(key) or 0)
        bonus = int(await self.redis.get(bonus_key) or 0)
        limit = settings.DEFAULT_DAILY_LIMIT + bonus
        return current, limit

    async def add_referral_bonus(self, user_id: int, amount: int = 5):
        """Добавляет бонусные загрузки рефереру (+5 за каждого приглашённого)."""
        bonus_key = f"referral_bonus:{user_id}"
        await self.redis.incrby(bonus_key, amount)
        # Бонус не сбрасывается — это накопленный лимит


class QueueService:
    """Очередь задач на Redis."""

    def __init__(self):
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.queue_key = "download_queue"

    async def push_task(self, user_id: int, url: str, platform: str):
        task = {
            "user_id": user_id,
            "url": url,
            "platform": platform,
            "attempt": 1,
        }
        await self.redis.lpush(self.queue_key, json.dumps(task))

    async def pop_task(self) -> Optional[dict]:
        task = await self.redis.rpop(self.queue_key)
        return json.loads(task) if task else None

    async def get_queue_length(self) -> int:
        return await self.redis.llen(self.queue_key)