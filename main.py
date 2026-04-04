"""
main.py

Улучшения:
- Graceful shutdown: polling и worker останавливаются корректно по SIGINT/SIGTERM
- Health-check эндпоинт /health возвращает JSON со статусом очереди
- Structured logging с JSON-форматтером для production (Render/Railway)
- Задержка перед polling вынесена в константу
- Premium expiry task: фоновая задача каждый час снимает истёкший Premium
- Общий обработчик необработанных исключений asyncio
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime

from aiohttp import web
from aiogram.exceptions import TelegramConflictError

from bot.dispatcher import setup_all
from bot.loader import dp, bot
from bot.worker import Worker
from database.database import init_db, UserRepo
from services.services import QueueService

logger = logging.getLogger(__name__)

PORT = int(os.environ.get("PORT", 10000))
POLLING_START_DELAY = int(os.environ.get("POLLING_START_DELAY", 5))


# --------------------------------------------------------------------------- #
#  Logging                                                                     #
# --------------------------------------------------------------------------- #

def setup_logging():
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    use_json = os.environ.get("LOG_JSON", "false").lower() == "true"

    if use_json:
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                return json.dumps({
                    "ts": datetime.utcnow().isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "msg": record.getMessage(),
                    **({"exc": self.formatException(record.exc_info)} if record.exc_info else {}),
                })
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

    logging.root.setLevel(log_level)
    logging.root.handlers = [handler]

    # Убираем спам от библиотек
    for noisy in ("aiohttp.access", "aiogram.event", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# --------------------------------------------------------------------------- #
#  Health check                                                                #
# --------------------------------------------------------------------------- #

async def health_check(request: web.Request) -> web.Response:
    try:
        qs = QueueService()
        queue_len = await qs.get_queue_length()
        payload = {"status": "ok", "queue": queue_len}
    except Exception as e:
        payload = {"status": "degraded", "error": str(e)}
    return web.Response(
        text=json.dumps(payload),
        content_type="application/json",
    )


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health check server started on :{PORT}")
    return runner


# --------------------------------------------------------------------------- #
#  Polling с retry                                                             #
# --------------------------------------------------------------------------- #

async def start_polling_with_retry():
    max_retries = 10
    delay = 3.0
    for attempt in range(1, max_retries + 1):
        try:
            await dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types(),
                drop_pending_updates=True,
            )
            return
        except TelegramConflictError as e:
            logger.warning(f"Telegram conflict (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 30.0)
            else:
                raise


# --------------------------------------------------------------------------- #
#  Фоновая задача: снятие истёкшего Premium                                   #
# --------------------------------------------------------------------------- #

async def premium_expiry_task():
    """Каждый час проверяет пользователей с истёкшим Premium и снимает его."""
    import psycopg2
    from database.database import get_sync_connection, run_sync

    logger.info("Premium expiry task started.")
    while True:
        try:
            await asyncio.sleep(3600)  # раз в час

            def _expire():
                with get_sync_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE users
                            SET is_premium = false, premium_until = NULL
                            WHERE is_premium = true
                              AND premium_until IS NOT NULL
                              AND premium_until < NOW()
                            RETURNING id
                        """)
                        rows = cur.fetchall()
                        conn.commit()
                        return [r[0] for r in rows]

            expired_ids = await run_sync(_expire)
            if expired_ids:
                logger.info(f"Premium expired for {len(expired_ids)} users: {expired_ids}")

                # Инвалидируем Redis-кэш
                repo = UserRepo()
                for uid in expired_ids:
                    await repo._invalidate_cache(uid)

        except asyncio.CancelledError:
            logger.info("Premium expiry task cancelled.")
            break
        except Exception as e:
            logger.exception(f"Premium expiry task error: {e}")


# --------------------------------------------------------------------------- #
#  Main                                                                        #
# --------------------------------------------------------------------------- #

async def main():
    setup_logging()
    logger.info("Bot starting…")

    # Обработчик необработанных asyncio-исключений
    def _handle_exception(loop, context):
        msg = context.get("exception", context["message"])
        logger.error(f"Unhandled asyncio exception: {msg}")

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_handle_exception)

    await init_db()
    setup_all()

    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Received stop signal, shutting down…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Запускаем фоновые задачи
    worker = Worker()
    worker_task = asyncio.create_task(worker.run(), name="worker")
    expiry_task = asyncio.create_task(premium_expiry_task(), name="premium_expiry")
    health_runner = await start_health_server()

    # Задержка перед polling (старый экземпляр на Render должен завершиться)
    logger.info(f"Waiting {POLLING_START_DELAY}s before polling…")
    await asyncio.sleep(POLLING_START_DELAY)

    polling_task = asyncio.create_task(start_polling_with_retry(), name="polling")

    try:
        await stop_event.wait()
    except Exception as e:
        logger.exception(f"Fatal error in main: {e}")
    finally:
        logger.info("Initiating graceful shutdown…")

        for task in (polling_task, worker_task, expiry_task):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        await health_runner.cleanup()

        try:
            await bot.session.close()
        except Exception:
            pass

        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())