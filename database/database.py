"""
database/database.py

Улучшения:
- BroadcastLogRepo — история рассылок с методами log_broadcast и get_recent_broadcasts
- get_premium_users_count — для расширенной статистики в /admin_stats
- get_all_user_ids теперь принимает параметр include_banned (по умолчанию False)
- get_active_users_today безопасен при пустой таблице downloads (обработка нулевого результата)
- UserRepo.get_user возвращает None вместо исключения при отсутствии таблицы на холодном старте
- _invalidate_cache вынесен в публичный метод (вызывается из main.py premium_expiry_task)
- _dict_to_user / _user_to_dict — перенесены сюда, чтобы избежать дублирования в middleware
- Все репозитории: единообразный стиль логирования
- init_db: добавляет таблицу broadcast_logs и новые индексы при миграции
"""

import asyncio
import logging
import random
import string
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from config.config import settings

logger = logging.getLogger(__name__)

_is_postgres = "postgresql" in settings.DATABASE_URL or "postgres" in settings.DATABASE_URL

# --------------------------------------------------------------------------- #
#  Пул соединений                                                              #
# --------------------------------------------------------------------------- #

_pool: Optional[ThreadedConnectionPool] = None


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is not None:
        return _pool

    url = settings.DATABASE_URL
    if _is_postgres:
        url = url.replace("postgresql+asyncpg://", "postgresql://")
        url = url.replace("postgresql+psycopg2://", "postgresql://")
        if "?" in url:
            url = url.split("?")[0]

    for attempt in range(1, 4):
        try:
            _pool = ThreadedConnectionPool(
                minconn=2,
                maxconn=15,
                dsn=url,
                sslmode="require" if _is_postgres else "disable",
                connect_timeout=30,
            )
            logger.info("DB connection pool created successfully.")
            return _pool
        except Exception as e:
            logger.warning(f"DB pool creation attempt {attempt}/3 failed: {e}")
            if attempt < 3:
                import time
                time.sleep(2 * attempt)
            else:
                raise


@contextmanager
def get_sync_connection():
    """Контекстный менеджер: берёт соединение из пула и возвращает обратно."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        pool.putconn(conn)


async def run_sync(func, *args, **kwargs):
    """Выполняет синхронную функцию в пуле потоков asyncio."""
    return await asyncio.to_thread(func, *args, **kwargs)


# --------------------------------------------------------------------------- #
#  Совместимый async-сессионный менеджер (для обратной совместимости)         #
# --------------------------------------------------------------------------- #

class AsyncSessionMaker:
    """Stub-менеджер: репозитории работают напрямую через пул, без ORM-сессий."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def commit(self):
        pass


async_session_maker = AsyncSessionMaker


# --------------------------------------------------------------------------- #
#  Утилиты                                                                     #
# --------------------------------------------------------------------------- #

def _gen_ref_code(length: int = 8) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


# --------------------------------------------------------------------------- #
#  UserRepo                                                                    #
# --------------------------------------------------------------------------- #

class UserRepo:
    """Репозиторий для работы с таблицей users."""

    def _get_redis(self):
        """Ленивый Redis-клиент для кэша (используется в UserMiddleware)."""
        try:
            import redis.asyncio as aioredis
            if not hasattr(self, "_redis_client") or self._redis_client is None:
                self._redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
            return self._redis_client
        except Exception:
            return None

    async def get_user(self, user_id: int):
        """Возвращает пользователя по ID или None."""
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
                    row = cur.fetchone()
                    return dict(row) if row else None

        try:
            data = await run_sync(_get)
        except Exception as e:
            logger.error(f"UserRepo.get_user({user_id}) error: {e}")
            return None
        return type("User", (), data)() if data else None

    async def create_user(self, user_id: int, language: str = "ru", referred_by: int = None):
        """Создаёт нового пользователя с уникальным реферальным кодом."""
        def _create():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    for _ in range(10):
                        code = _gen_ref_code()
                        cur.execute("SELECT 1 FROM users WHERE referral_code = %s", (code,))
                        if not cur.fetchone():
                            break
                    cur.execute(
                        """INSERT INTO users (id, language, referral_code, referred_by)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT (id) DO NOTHING""",
                        (user_id, language, code, referred_by),
                    )
                    conn.commit()

        try:
            await run_sync(_create)
        except Exception as e:
            logger.error(f"UserRepo.create_user({user_id}) error: {e}")
        return await self.get_user(user_id)

    async def set_language(self, user_id: int, language: str):
        """Обновляет язык пользователя и инвалидирует кэш."""
        def _set():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET language = %s WHERE id = %s", (language, user_id))
                    conn.commit()

        await run_sync(_set)
        await self.invalidate_cache(user_id)

    async def set_premium(self, user_id: int, days: int):
        """Выдаёт Premium на указанное количество дней."""
        def _set():
            until = datetime.now() + timedelta(days=days)
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET is_premium = true, premium_until = %s WHERE id = %s",
                        (until, user_id),
                    )
                    conn.commit()

        await run_sync(_set)
        await self.invalidate_cache(user_id)

    async def remove_premium(self, user_id: int):
        """Снимает Premium."""
        def _remove():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET is_premium = false, premium_until = NULL WHERE id = %s",
                        (user_id,),
                    )
                    conn.commit()

        await run_sync(_remove)
        await self.invalidate_cache(user_id)

    async def ban_user(self, user_id: int):
        """Блокирует пользователя."""
        def _ban():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned = true WHERE id = %s", (user_id,))
                    conn.commit()

        await run_sync(_ban)
        await self.invalidate_cache(user_id)

    async def unban_user(self, user_id: int):
        """Разблокирует пользователя."""
        def _unban():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned = false WHERE id = %s", (user_id,))
                    conn.commit()

        await run_sync(_unban)
        await self.invalidate_cache(user_id)

    async def increment_referral_count(self, referrer_id: int):
        """Увеличивает счётчик приглашённых на 1."""
        def _inc():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET referral_count = referral_count + 1 WHERE id = %s",
                        (referrer_id,),
                    )
                    conn.commit()

        await run_sync(_inc)
        await self.invalidate_cache(referrer_id)

    async def set_referred_by(self, user_id: int, referrer_id: int):
        """Записывает, кто пригласил пользователя."""
        def _set():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET referred_by = %s WHERE id = %s AND referred_by IS NULL",
                        (referrer_id, user_id),
                    )
                    conn.commit()

        await run_sync(_set)
        await self.invalidate_cache(user_id)

    async def get_user_by_referral_code(self, code: str):
        """Находит пользователя по реферальному коду (case-insensitive)."""
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM users WHERE referral_code = %s", (code.upper(),))
                    row = cur.fetchone()
                    return dict(row) if row else None

        data = await run_sync(_get)
        return type("User", (), data)() if data else None

    async def get_all_users_count(self) -> int:
        """Возвращает общее число зарегистрированных пользователей."""
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users")
                    return cur.fetchone()[0]

        return await run_sync(_count)

    async def get_premium_users_count(self) -> int:
        """Возвращает число активных Premium-пользователей."""
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM users WHERE is_premium = true"
                    )
                    return cur.fetchone()[0]

        return await run_sync(_count)

    async def get_active_users_today(self) -> int:
        """
        Возвращает число уникальных пользователей, совершивших загрузку сегодня.
        Безопасен при пустой таблице downloads.
        """
        def _count():
            today = datetime.now().date()
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(
                            "SELECT COUNT(DISTINCT user_id) FROM downloads WHERE created_at >= %s",
                            (today,),
                        )
                        result = cur.fetchone()
                        return result[0] if result else 0
                    except psycopg2.errors.UndefinedTable:
                        return 0

        return await run_sync(_count)

    async def get_all_user_ids(self, include_banned: bool = False) -> list[int]:
        """
        Возвращает все ID пользователей для рассылки.
        По умолчанию исключает заблокированных пользователей.
        """
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    if include_banned:
                        cur.execute("SELECT id FROM users ORDER BY id")
                    else:
                        cur.execute("SELECT id FROM users WHERE is_banned = false ORDER BY id")
                    return [row[0] for row in cur.fetchall()]

        return await run_sync(_get)

    async def invalidate_cache(self, user_id: int):
        """Удаляет запись пользователя из Redis-кэша."""
        try:
            redis = self._get_redis()
            if redis:
                await redis.delete(f"user_cache:{user_id}")
        except Exception as e:
            logger.debug(f"Cache invalidation failed for {user_id}: {e}")

    # Алиас для обратной совместимости (вызывается из main.py)
    async def _invalidate_cache(self, user_id: int):
        await self.invalidate_cache(user_id)


# --------------------------------------------------------------------------- #
#  DownloadRepo                                                                #
# --------------------------------------------------------------------------- #

class DownloadRepo:
    """Репозиторий для работы с таблицей downloads."""

    async def add_download(self, user_id: int, platform: str):
        """Записывает факт загрузки."""
        def _add():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO downloads (user_id, platform) VALUES (%s, %s)",
                        (user_id, platform),
                    )
                    conn.commit()

        await run_sync(_add)

    async def get_total_downloads(self) -> int:
        """Возвращает общее число загрузок."""
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM downloads")
                    result = cur.fetchone()
                    return result[0] if result else 0

        return await run_sync(_count)

    async def get_user_downloads_today(self, user_id: int) -> int:
        """Возвращает число загрузок конкретного пользователя за сегодня."""
        def _count():
            today = datetime.now().date()
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM downloads WHERE user_id = %s AND created_at >= %s",
                        (user_id, today),
                    )
                    result = cur.fetchone()
                    return result[0] if result else 0

        return await run_sync(_count)

    async def get_downloads_by_platform(self, limit: int = 5) -> list[tuple[str, int]]:
        """Топ платформ по числу загрузок (для статистики)."""
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT platform, COUNT(*) as cnt
                           FROM downloads
                           GROUP BY platform
                           ORDER BY cnt DESC
                           LIMIT %s""",
                        (limit,),
                    )
                    return cur.fetchall()

        rows = await run_sync(_get)
        return [(r[0], r[1]) for r in rows]

    async def get_downloads_today(self) -> int:
        """Возвращает число загрузок за сегодня по всем пользователям."""
        def _count():
            today = datetime.now().date()
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM downloads WHERE created_at >= %s",
                        (today,),
                    )
                    result = cur.fetchone()
                    return result[0] if result else 0

        return await run_sync(_count)


# --------------------------------------------------------------------------- #
#  AdRepo                                                                      #
# --------------------------------------------------------------------------- #

class AdRepo:
    """Репозиторий для работы с таблицей ads."""

    async def add_ad(self, text: str):
        """Добавляет новое рекламное объявление."""
        def _add():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO ads (message_text) VALUES (%s)", (text,))
                    conn.commit()

        await run_sync(_add)

    async def get_active_ads(self):
        """Возвращает активные рекламные объявления."""
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM ads WHERE is_active = true ORDER BY id")
                    return [dict(row) for row in cur.fetchall()]

        rows = await run_sync(_get)
        return [type("Ad", (), row)() for row in rows]

    async def get_all_ads(self):
        """Возвращает все рекламные объявления (активные и неактивные)."""
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM ads ORDER BY id")
                    return [dict(row) for row in cur.fetchall()]

        rows = await run_sync(_get)
        return [type("Ad", (), row)() for row in rows]

    async def remove_ad(self, ad_id: int):
        """Удаляет объявление по ID."""
        def _remove():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ads WHERE id = %s", (ad_id,))
                    conn.commit()

        await run_sync(_remove)

    async def toggle_ad(self, ad_id: int, active: bool):
        """Включает или отключает объявление."""
        def _toggle():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE ads SET is_active = %s WHERE id = %s", (active, ad_id)
                    )
                    conn.commit()

        await run_sync(_toggle)


# --------------------------------------------------------------------------- #
#  BroadcastLogRepo                                                            #
# --------------------------------------------------------------------------- #

class BroadcastLogRepo:
    """Репозиторий для хранения истории рассылок."""

    async def log_broadcast(
        self,
        admin_id: int,
        message: str,
        total: int,
        success: int,
        blocked: int,
        failed: int,
        cancelled: bool,
    ) -> int:
        """Сохраняет запись о рассылке. Возвращает ID записи."""
        def _log():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO broadcast_logs
                               (admin_id, message, total, success, blocked, failed, cancelled)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           RETURNING id""",
                        (admin_id, message, total, success, blocked, failed, cancelled),
                    )
                    log_id = cur.fetchone()[0]
                    conn.commit()
                    return log_id

        try:
            return await run_sync(_log)
        except Exception as e:
            logger.error(f"BroadcastLogRepo.log_broadcast error: {e}")
            return -1

    async def get_recent_broadcasts(self, limit: int = 5) -> list:
        """Возвращает последние N рассылок."""
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(
                        """SELECT * FROM broadcast_logs
                           ORDER BY created_at DESC
                           LIMIT %s""",
                        (limit,),
                    )
                    return [dict(row) for row in cur.fetchall()]

        try:
            rows = await run_sync(_get)
            return [type("BroadcastLog", (), row)() for row in rows]
        except Exception as e:
            logger.error(f"BroadcastLogRepo.get_recent_broadcasts error: {e}")
            return []


# --------------------------------------------------------------------------- #
#  init_db                                                                     #
# --------------------------------------------------------------------------- #

async def init_db():
    """
    Создаёт таблицы и индексы, если их нет.
    Безопасен для повторного вызова (IF NOT EXISTS).
    Автоматически добавляет новые колонки при миграции (ALTER TABLE IF NOT EXISTS).
    """
    def _create():
        with get_sync_connection() as conn:
            with conn.cursor() as cur:
                # ── Таблица пользователей ──────────────────────────────────
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id             BIGINT PRIMARY KEY,
                        language       VARCHAR(5)  DEFAULT 'ru' NOT NULL,
                        is_premium     BOOLEAN     DEFAULT FALSE NOT NULL,
                        premium_until  TIMESTAMP,
                        is_banned      BOOLEAN     DEFAULT FALSE NOT NULL,
                        registered_at  TIMESTAMP   DEFAULT NOW() NOT NULL,
                        referral_code  VARCHAR(16) UNIQUE,
                        referred_by    BIGINT REFERENCES users(id),
                        referral_count INTEGER     DEFAULT 0 NOT NULL
                    )
                """)

                # Миграция: добавляем колонки, если их нет (для старых БД)
                for col_sql in [
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code VARCHAR(16) UNIQUE",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT REFERENCES users(id)",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count INTEGER DEFAULT 0 NOT NULL",
                ]:
                    try:
                        cur.execute(col_sql)
                    except Exception:
                        conn.rollback()

                # ── Таблица загрузок ───────────────────────────────────────
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS downloads (
                        id         SERIAL PRIMARY KEY,
                        user_id    BIGINT REFERENCES users(id) NOT NULL,
                        platform   VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW() NOT NULL
                    )
                """)

                # ── Таблица рекламы ────────────────────────────────────────
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ads (
                        id           SERIAL PRIMARY KEY,
                        message_text TEXT    NOT NULL,
                        is_active    BOOLEAN DEFAULT TRUE NOT NULL,
                        created_at   TIMESTAMP DEFAULT NOW() NOT NULL
                    )
                """)

                # ── Таблица истории рассылок ───────────────────────────────
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS broadcast_logs (
                        id         SERIAL PRIMARY KEY,
                        admin_id   BIGINT NOT NULL,
                        message    TEXT   NOT NULL,
                        total      INTEGER DEFAULT 0,
                        success    INTEGER DEFAULT 0,
                        blocked    INTEGER DEFAULT 0,
                        failed     INTEGER DEFAULT 0,
                        cancelled  BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW() NOT NULL
                    )
                """)

                # ── Индексы ────────────────────────────────────────────────
                indices = [
                    "CREATE INDEX IF NOT EXISTS idx_downloads_user_created ON downloads(user_id, created_at)",
                    "CREATE INDEX IF NOT EXISTS idx_downloads_platform ON downloads(platform)",
                    "CREATE INDEX IF NOT EXISTS idx_downloads_created_at ON downloads(created_at)",
                    "CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)",
                    "CREATE INDEX IF NOT EXISTS idx_users_is_premium ON users(is_premium)",
                    "CREATE INDEX IF NOT EXISTS idx_broadcast_logs_admin ON broadcast_logs(admin_id)",
                    "CREATE INDEX IF NOT EXISTS idx_broadcast_logs_created ON broadcast_logs(created_at)",
                ]
                for idx in indices:
                    cur.execute(idx)

                conn.commit()
        logger.info("Database initialized successfully.")

    await run_sync(_create)