import asyncio
import logging
import os
import signal
from aiohttp import web

from aiogram.exceptions import TelegramConflictError

from bot.dispatcher import setup_all
from bot.loader import dp, bot
from bot.worker import Worker
from database.database import init_db

logger = logging.getLogger(__name__)

PORT = int(os.environ.get("PORT", 10000))


async def health_check(request):
    return web.Response(text="OK")


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health check server started on port {PORT}")
    return runner


async def start_polling_with_retry():
    max_retries = 10
    retry_delay = 3
    for attempt in range(max_retries):
        try:
            await dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types(),
                drop_pending_updates=True,
            )
            return
        except TelegramConflictError as e:
            logger.warning(f"Conflict error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 30)
            else:
                raise


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    await init_db()
    setup_all()

    worker = Worker()
    worker_task = asyncio.create_task(worker.run())

    health_runner = await start_health_server()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Received stop signal, shutting down...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Задержка перед запуском polling, чтобы старый процесс на Render успел завершиться
    logger.info("Waiting 5 seconds before starting polling to avoid conflict...")
    await asyncio.sleep(5)

    try:
        polling_task = asyncio.create_task(start_polling_with_retry())
        await stop_event.wait()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
    finally:
        logger.info("Stopping polling...")
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass

        logger.info("Cancelling worker...")
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        logger.info("Stopping health server...")
        await health_runner.cleanup()

        logger.info("Closing bot session...")
        await bot.session.close()

        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())