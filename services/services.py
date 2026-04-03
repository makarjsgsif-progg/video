import asyncio
import copy
import json
import os
import logging
import string
import random
import tempfile
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
    Загрузчик видео через yt-dlp.

    Исправления:
    - используем tempfile.mkdtemp() вместо относительного пути downloads/
    - убрали невалидные extractor_args для TikTok
    - заголовки не мешают YouTube/Instagram (убраны TikTok-специфичные Referer/Origin)
    - deep copy opts, чтобы вложенные dict не мутировались
    - ищем файл с учётом постпроцессора FFmpeg (он меняет расширение)
    - verbose=False, но logger перехватывает через кастомный logger-класс
    - постпроцессор убран из базовых опций — он ломает поиск файла,
      используем format-строку чтобы сразу получить mp4
    """

    _UA_DESKTOP = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    # Базовые опции — НЕ содержат format и outtmpl (задаются динамически)
    _BASE_OPTS: dict = {
        "quiet": False,           # False чтобы видеть ошибки в логах
        "no_warnings": False,
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

    # Цепочка форматов: сначала лёгкий, потом сложнее
    FORMAT_CHAIN = [
        "best[ext=mp4][height<=720]/best[ext=mp4]/best[height<=720]/best",
        "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
        "worst",
    ]

    # Расширения, в которые FFmpeg конвертирует файл
    _VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".m4v", ".mov", ".avi", ".flv")

    def __init__(self):
        self.cookies_available = os.path.exists(settings.COOKIES_FILE)
        if self.cookies_available:
            logger.info(f"Cookies file found: {settings.COOKIES_FILE}")
        else:
            logger.warning(f"Cookies file NOT found: {settings.COOKIES_FILE} — YouTube may block downloads")

    async def download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        return await asyncio.to_thread(self._sync_download, url)

    def _build_opts(self, fmt: str, out_dir: str) -> dict:
        # deep copy чтобы вложенные dict не мутировались между вызовами
        opts = copy.deepcopy(self._BASE_OPTS)
        opts["format"] = fmt
        # Используем временную директорию — всегда доступна
        opts["outtmpl"] = os.path.join(out_dir, "%(id)s.%(ext)s")
        if self.cookies_available:
            opts["cookiefile"] = settings.COOKIES_FILE
        return opts

    def _find_downloaded_file(self, out_dir: str, expected_path: str) -> Optional[str]:
        """
        Ищет скачанный файл. yt-dlp + постпроцессор могут изменить расширение,
        поэтому перебираем варианты расширений и ищем в директории.
        """
        # 1. Точное совпадение
        if os.path.exists(expected_path):
            return expected_path

        # 2. То же имя, другое расширение
        base = os.path.splitext(expected_path)[0]
        for ext in self._VIDEO_EXTS:
            candidate = base + ext
            if os.path.exists(candidate):
                return candidate

        # 3. Любой видеофайл в директории (на случай нестандартного outtmpl)
        try:
            for fname in os.listdir(out_dir):
                if any(fname.endswith(ext) for ext in self._VIDEO_EXTS):
                    return os.path.join(out_dir, fname)
        except OSError:
            pass

        return None

    def _sync_download(self, url: str) -> Tuple[Optional[BytesIO], Optional[str]]:
        last_error: Optional[str] = None

        # Временная директория — удаляется блоком finally
        out_dir = tempfile.mkdtemp(prefix="ytdlp_")
        try:
            for fmt in self.FORMAT_CHAIN:
                try:
                    opts = self._build_opts(fmt, out_dir)

                    logger.info(f"Trying format '{fmt}' for URL: {url}")

                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(url, download=True)

                        if info and "entries" in info:
                            # плейлист — берём первый элемент
                            info = info["entries"][0]

                        if not info:
                            last_error = "No media info returned"
                            logger.warning(f"No info for URL: {url}")
                            continue

                        expected_path = ydl.prepare_filename(info)
                        file_path = self._find_downloaded_file(out_dir, expected_path)

                        if not file_path:
                            last_error = "Downloaded file not found on disk"
                            logger.warning(
                                f"File not found after download. Expected: {expected_path}, "
                                f"out_dir contents: {os.listdir(out_dir)}"
                            )
                            continue

                        logger.info(f"Successfully downloaded: {file_path} ({os.path.getsize(file_path)} bytes)")

                        with open(file_path, "rb") as f:
                            buf = BytesIO(f.read())
                            buf.name = "video.mp4"
                        return buf, None

                except DownloadError as de:
                    err_str = str(de)
                    last_error = err_str
                    logger.warning(f"yt-dlp DownloadError (fmt='{fmt}'): {de}")

                    # Ошибки, при которых не стоит повторять с другим форматом
                    if any(kw in err_str for kw in (
                        "Sign in", "bot detection", "login required",
                        "confirm your age", "This video is only available",
                    )):
                        return None, "auth_required"
                    if "Private" in err_str or "private" in err_str:
                        return None, err_str
                    # Для остальных — пробуем следующий формат
                    continue

                except Exception as e:
                    last_error = str(e)
                    logger.exception(f"Unexpected error downloading {url}: {e}")
                    continue

        finally:
            # Чистим временную директорию
            import shutil
            try:
                shutil.rmtree(out_dir, ignore_errors=True)
            except Exception:
                pass

        logger.error(f"All format attempts failed for {url}. Last error: {last_error}")
        return None, last_error


class LimitService:
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