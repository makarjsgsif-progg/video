"""
database/database.py

New vs previous version:
- UserRepo.use_turbo(user_id) → bool
    Atomically checks whether the free-user weekly Turbo-Download is available
    (last_turbo_used IS NULL or older than 7 days) and, if so, sets it to NOW().
    Uses SELECT … FOR UPDATE to prevent double-spending in concurrent requests.
    Returns True when turbo was available and has been consumed.

- UserRepo.get_fresh_referral_count(user_id) → int
    Reads referral_count directly from the DB (bypasses Redis cache), used
    right after increment_referral_count to detect milestone thresholds.

- UserRepo.grant_milestone_premium(user_id, hours=24)
    Grants N hours of Premium.  If the user already has Premium that hasn't
    expired, the new duration is ADDED on top (fair stacking).

- init_db: ALTER TABLE migration adds last_turbo_used to existing databases.
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
#  Connection pool                                                             #
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
    return await asyncio.to_thread(func, *args, **kwargs)


# --------------------------------------------------------------------------- #
#  Async session stub (backward compat)                                       #
# --------------------------------------------------------------------------- #

class AsyncSessionMaker:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def commit(self):
        pass


async_session_maker = AsyncSessionMaker


# --------------------------------------------------------------------------- #
#  Utilities                                                                   #
# --------------------------------------------------------------------------- #

def _gen_ref_code(length: int = 8) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=length))


# --------------------------------------------------------------------------- #
#  UserRepo                                                                    #
# --------------------------------------------------------------------------- #

class UserRepo:

    def _get_redis(self):
        try:
            import redis.asyncio as aioredis
            if not hasattr(self, "_redis_client") or self._redis_client is None:
                self._redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
            return self._redis_client
        except Exception:
            return None

    async def get_user(self, user_id: int):
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
        def _set():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET language = %s WHERE id = %s", (language, user_id))
                    conn.commit()

        await run_sync(_set)
        await self.invalidate_cache(user_id)

    async def set_premium(self, user_id: int, days: int):
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
        def _ban():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned = true WHERE id = %s", (user_id,))
                    conn.commit()

        await run_sync(_ban)
        await self.invalidate_cache(user_id)

    async def unban_user(self, user_id: int):
        def _unban():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET is_banned = false WHERE id = %s", (user_id,))
                    conn.commit()

        await run_sync(_unban)
        await self.invalidate_cache(user_id)

    async def increment_referral_count(self, referrer_id: int):
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

    async def get_fresh_referral_count(self, user_id: int) -> int:
        """
        Returns current referral_count straight from the DB (bypasses Redis
        cache).  Call this right after increment_referral_count so milestone
        detection always sees the up-to-date value.
        """
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT referral_count FROM users WHERE id = %s", (user_id,))
                    row = cur.fetchone()
                    return int(row[0]) if row else 0

        try:
            return await run_sync(_get)
        except Exception as e:
            logger.error(f"UserRepo.get_fresh_referral_count({user_id}) error: {e}")
            return 0

    async def grant_milestone_premium(self, user_id: int, hours: int = 24):
        """
        Grants `hours` of Premium as a referral milestone reward.

        Fair-stacking rule: if the user already has active Premium (premium_until
        > NOW()), the reward is added on top of the existing expiry so the user
        never loses their existing time.
        """
        def _grant():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT is_premium, premium_until FROM users WHERE id = %s",
                        (user_id,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return
                    is_premium, current_until = row
                    now = datetime.now()
                    # If premium is active and not yet expired, stack on top
                    if is_premium and current_until and current_until > now:
                        new_until = current_until + timedelta(hours=hours)
                    else:
                        new_until = now + timedelta(hours=hours)
                    cur.execute(
                        "UPDATE users SET is_premium = true, premium_until = %s WHERE id = %s",
                        (new_until, user_id),
                    )
                    conn.commit()

        try:
            await run_sync(_grant)
            await self.invalidate_cache(user_id)
            logger.info(f"Granted {hours}h milestone premium to user {user_id}")
        except Exception as e:
            logger.error(f"UserRepo.grant_milestone_premium({user_id}) error: {e}")

    async def set_referred_by(self, user_id: int, referrer_id: int):
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
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM users WHERE referral_code = %s", (code.upper(),))
                    row = cur.fetchone()
                    return dict(row) if row else None

        data = await run_sync(_get)
        return type("User", (), data)() if data else None

    async def use_turbo(self, user_id: int) -> bool:
        """
        Atomically checks whether the free-user weekly Turbo-Download is
        available and, if so, marks it as used.

        Availability: last_turbo_used IS NULL  OR  NOW() - last_turbo_used >= 7 days.

        Uses SELECT … FOR UPDATE so two concurrent requests for the same user
        cannot both consume the turbo.  Returns True when turbo was available
        and has been consumed; False otherwise.
        """
        def _check_and_set():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT last_turbo_used FROM users WHERE id = %s FOR UPDATE",
                        (user_id,)
                    )
                    row = cur.fetchone()
                    if not row:
                        conn.rollback()
                        return False
                    last_turbo = row[0]
                    now = datetime.now()
                    cooldown_seconds = 7 * 24 * 3600
                    if last_turbo is None or (now - last_turbo).total_seconds() >= cooldown_seconds:
                        cur.execute(
                            "UPDATE users SET last_turbo_used = %s WHERE id = %s",
                            (now, user_id)
                        )
                        conn.commit()
                        return True
                    conn.rollback()
                    return False

        try:
            return await run_sync(_check_and_set)
        except Exception as e:
            logger.error(f"UserRepo.use_turbo({user_id}) error: {e}")
            return False  # fail-closed: don't grant turbo on DB errors

    async def get_all_users_count(self) -> int:
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users")
                    return cur.fetchone()[0]

        return await run_sync(_count)

    async def get_premium_users_count(self) -> int:
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM users WHERE is_premium = true"
                    )
                    return cur.fetchone()[0]

        return await run_sync(_count)

    async def get_active_users_today(self) -> int:
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
        try:
            redis = self._get_redis()
            if redis:
                await redis.delete(f"user_cache:{user_id}")
        except Exception as e:
            logger.debug(f"Cache invalidation failed for {user_id}: {e}")

    async def _invalidate_cache(self, user_id: int):
        await self.invalidate_cache(user_id)


# --------------------------------------------------------------------------- #
#  DownloadRepo                                                                #
# --------------------------------------------------------------------------- #

class DownloadRepo:

    async def add_download(self, user_id: int, platform: str):
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
        def _count():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM downloads")
                    result = cur.fetchone()
                    return result[0] if result else 0

        return await run_sync(_count)

    async def get_user_downloads_today(self, user_id: int) -> int:
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

    async def add_ad(self, text: str, position: str = "after_download"):
        def _add():
            with get_sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO ads (message_text, position) VALUES (%s, %s)",
                        (text, position)
                    )
                    conn.commit()

        await run_sync(_add)

    async def get_active_ads(self):
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM ads WHERE is_active = true ORDER BY id")
                    return [dict(row) for row in cur.fetchall()]

        rows = await run_sync(_get)
        return [type("Ad", (), row)() for row in rows]

    async def get_all_ads(self):
        def _get():
            with get_sync_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT * FROM ads ORDER BY id")
                    return [dict(row) for row in cur.fetchall()]

        rows = await run_sync(_get)
        return [type("Ad", (), row)() for row in rows]

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
                    cur.execute(
                        "UPDATE ads SET is_active = %s WHERE id = %s", (active, ad_id)
                    )
                    conn.commit()

        await run_sync(_toggle)


# --------------------------------------------------------------------------- #
#  BroadcastLogRepo                                                            #
# --------------------------------------------------------------------------- #

class BroadcastLogRepo:

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
    Creates tables and indices if they don't exist.
    Safe for repeated calls (IF NOT EXISTS / ALTER … IF NOT EXISTS).
    """
    def _create():
        with get_sync_connection() as conn:
            with conn.cursor() as cur:

                # ── Users ──────────────────────────────────────────────────
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id              BIGINT PRIMARY KEY,
                        language        VARCHAR(5)  DEFAULT 'ru' NOT NULL,
                        is_premium      BOOLEAN     DEFAULT FALSE NOT NULL,
                        premium_until   TIMESTAMP,
                        is_banned       BOOLEAN     DEFAULT FALSE NOT NULL,
                        registered_at   TIMESTAMP   DEFAULT NOW() NOT NULL,
                        referral_code   VARCHAR(16) UNIQUE,
                        referred_by     BIGINT REFERENCES users(id),
                        referral_count  INTEGER     DEFAULT 0 NOT NULL,
                        last_turbo_used TIMESTAMP
                    )
                """)

                # Migrations for existing databases
                for col_sql in [
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code VARCHAR(16) UNIQUE",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT REFERENCES users(id)",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count INTEGER DEFAULT 0 NOT NULL",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_turbo_used TIMESTAMP",
                ]:
                    try:
                        cur.execute(col_sql)
                    except Exception:
                        conn.rollback()

                # ── Downloads ─────────────────────────────────────────────
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS downloads (
                        id         SERIAL PRIMARY KEY,
                        user_id    BIGINT REFERENCES users(id) NOT NULL,
                        platform   VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW() NOT NULL
                    )
                """)

                # ── Ads ───────────────────────────────────────────────────
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ads (
                        id           SERIAL PRIMARY KEY,
                        message_text TEXT    NOT NULL,
                        is_active    BOOLEAN DEFAULT TRUE NOT NULL,
                        position     VARCHAR(30) DEFAULT 'after_download' NOT NULL,
                        created_at   TIMESTAMP DEFAULT NOW() NOT NULL
                    )
                """)
                try:
                    cur.execute(
                        "ALTER TABLE ads ADD COLUMN IF NOT EXISTS "
                        "position VARCHAR(30) DEFAULT 'after_download' NOT NULL"
                    )
                except Exception:
                    conn.rollback()

                # ── Broadcast logs ────────────────────────────────────────
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

                # ── Indices ───────────────────────────────────────────────
                for idx in [
                    "CREATE INDEX IF NOT EXISTS idx_downloads_user_created ON downloads(user_id, created_at)",
                    "CREATE INDEX IF NOT EXISTS idx_downloads_platform ON downloads(platform)",
                    "CREATE INDEX IF NOT EXISTS idx_downloads_created_at ON downloads(created_at)",
                    "CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)",
                    "CREATE INDEX IF NOT EXISTS idx_users_is_premium ON users(is_premium)",
                    "CREATE INDEX IF NOT EXISTS idx_users_last_turbo ON users(last_turbo_used)",
                    "CREATE INDEX IF NOT EXISTS idx_broadcast_logs_admin ON broadcast_logs(admin_id)",
                    "CREATE INDEX IF NOT EXISTS idx_broadcast_logs_created ON broadcast_logs(created_at)",
                ]:
                    cur.execute(idx)

                conn.commit()
        logger.info("Database initialized successfully.")

    await run_sync(_create)