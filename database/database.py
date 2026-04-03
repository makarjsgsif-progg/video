import ssl
import string
import random
from datetime import datetime, timedelta
import asyncio
from functools import partial

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager

from config.config import settings

# ---------------------------------------------------------------------------
_is_postgres = "postgresql" in settings.DATABASE_URL or "postgres" in settings.DATABASE_URL

# Создаём синхронный пул соединений (работает в потоках)
_pool = None

def _get_pool():
    global _pool
    if _pool is None:
        # Преобразуем asyncpg URL в psycopg2
        url = settings.DATABASE_URL
        if _is_postgres:
            # Заменяем asyncpg на psycopg2
            url = url.replace("postgresql+asyncpg://", "postgresql://")
            url = url.replace("postgresql+psycopg2://", "postgresql://")
            # Убираем параметры, которые не поддерживает psycopg2
            if "?" in url:
                url = url.split("?")[0]
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=url,
            sslmode="require" if _is_postgres else "disable",
            connect_timeout=30,
        )
    return _pool

@contextmanager
def get_sync_connection():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)

def run_sync(func, *args, **kwargs):
    """Выполняет синхронную функцию в потоке и возвращает результат"""
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(None, partial(func, *args, **kwargs))

# ---------------------------------------------------------------------------
# Асинхронные репозитории (обёртки над синхронным кодом)
# ---------------------------------------------------------------------------

class UserRepo:
    def __init__(self, session=None):  # session игнорируется
        pass

    async def get_user(self, user_id: int):
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
                    row = cur.fetchone()
                    if row:
                        return dict(row)
                    return None
        data = await run_sync(_get)
        if data:
            # Возвращаем объект, совместимый с ожидаемой моделью
            return type('User', (), data)()
        return None

    async def create_user(self, user_id: int, language: str = "en", referred_by: int = None):
        def _create():
            code = _gen_ref_code()
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    # Убедимся, что код уникален
                    while True:
                        cur.execute("SELECT 1 FROM users WHERE referral_code = %s", (code,))
                        if not cur.fetchone():
                            break
                        code = _gen_ref_code()
                    cur.execute(
                        "INSERT INTO users (id, language, referral_code, referred_by) VALUES (%s, %s, %s, %s) RETURNING id",
                        (user_id, language, code, referred_by)
                    )
                    conn.commit()
                    return user_id
        await run_sync(_create)
        return await self.get_user(user_id)

    async def set_language(self, user_id: int, language: str):
        def _set():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET language = %s WHERE id = %s", (language, user_id))
                    conn.commit()
        await run_sync(_set)

    async def set_premium(self, user_id: int, days: int):
        def _set():
            until = datetime.now() + timedelta(days=days)
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_premium = true, premium_until = %s WHERE id = %s", (until, user_id))
                    conn.commit()
        await run_sync(_set)

    async def remove_premium(self, user_id: int):
        def _remove():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_premium = false, premium_until = NULL WHERE id = %s", (user_id,))
                    conn.commit()
        await run_sync(_remove)

    async def ban_user(self, user_id: int):
        def _ban():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned = true WHERE id = %s", (user_id,))
                    conn.commit()
        await run_sync(_ban)

    async def unban_user(self, user_id: int):
        def _unban():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned = false WHERE id = %s", (user_id,))
                    conn.commit()
        await run_sync(_unban)

    async def increment_referral_count(self, referrer_id: int):
        def _inc():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET referral_count = referral_count + 1 WHERE id = %s", (referrer_id,))
                    conn.commit()
        await run_sync(_inc)

    async def set_referred_by(self, user_id: int, referrer_id: int):
        def _set():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET referred_by = %s WHERE id = %s", (referrer_id, user_id))
                    conn.commit()
        await run_sync(_set)

    async def get_user_by_referral_code(self, code: str):
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM users WHERE referral_code = %s", (code.upper(),))
                    row = cur.fetchone()
                    if row:
                        return dict(row)
                    return None
        data = await run_sync(_get)
        if data:
            return type('User', (), data)()
        return None

    async def get_all_users_count(self) -> int:
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users")
                    return cur.fetchone()[0]
        return await run_sync(_count)

    async def get_active_users_today(self) -> int:
        def _count():
            today = datetime.now().date()
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(DISTINCT user_id) FROM downloads WHERE created_at >= %s", (today,))
                    return cur.fetchone()[0]
        return await run_sync(_count)

    async def get_all_user_ids(self) -> list[int]:
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM users")
                    return [row[0] for row in cur.fetchall()]
        return await run_sync(_get)


class DownloadRepo:
    def __init__(self, session=None):
        pass

    async def add_download(self, user_id: int, platform: str):
        def _add():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO downloads (user_id, platform) VALUES (%s, %s)", (user_id, platform))
                    conn.commit()
        await run_sync(_add)

    async def get_total_downloads(self) -> int:
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM downloads")
                    return cur.fetchone()[0]
        return await run_sync(_count)

    async def get_user_downloads_today(self, user_id: int) -> int:
        def _count():
            today = datetime.now().date()
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM downloads WHERE user_id = %s AND created_at >= %s", (user_id, today))
                    return cur.fetchone()[0]
        return await run_sync(_count)


class AdRepo:
    def __init__(self, session=None):
        pass

    async def add_ad(self, text: str):
        def _add():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO ads (message_text) VALUES (%s) RETURNING id", (text,))
                    conn.commit()
        await run_sync(_add)

    async def get_active_ads(self):
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM ads WHERE is_active = true")
                    return [dict(row) for row in cur.fetchall()]
        rows = await run_sync(_get)
        return [type('Ad', (), row)() for row in rows]

    async def get_all_ads(self):
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM ads ORDER BY id")
                    return [dict(row) for row in cur.fetchall()]
        rows = await run_sync(_get)
        return [type('Ad', (), row)() for row in rows]

    async def remove_ad(self, ad_id: int):
        def _remove():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ads WHERE id = %s", (ad_id,))
                    conn.commit()
        await run_sync(_remove)

    async def toggle_ad(self, ad_id: int, active: bool):
        def _toggle():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE ads SET is_active = %s WHERE id = %s", (active, ad_id))
                    conn.commit()
        await run_sync(_toggle)


# ---------------------------------------------------------------------------
# init_db — создание таблиц
# ---------------------------------------------------------------------------
async def init_db():
    def _create():
        with get_sync_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGINT PRIMARY KEY,
                        language VARCHAR(5) DEFAULT 'en',
                        is_premium BOOLEAN DEFAULT FALSE,
                        premium_until TIMESTAMP,
                        is_banned BOOLEAN DEFAULT FALSE,
                        registered_at TIMESTAMP DEFAULT NOW(),
                        referral_code VARCHAR(16) UNIQUE,
                        referred_by BIGINT REFERENCES users(id),
                        referral_count INTEGER DEFAULT 0
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS downloads (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(id),
                        platform VARCHAR(50),
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ads (
                        id SERIAL PRIMARY KEY,
                        message_text TEXT NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                conn.commit()
    await run_sync(_create)


def _gen_ref_code() -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=8))


# Для совместимости со старым кодом, где ожидается async_session_maker
class DummySession:
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass
    async def commit(self):
        pass

async_session_maker = None  # не используется