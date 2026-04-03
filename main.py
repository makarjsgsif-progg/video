import asyncio
import logging
import signal

from aiogram.exceptions import TelegramConflictError

from bot.dispatcher import setup_all
from bot.loader import dp, bot
from bot.worker import Worker
from database.database import init_db

logger = logging.getLogger(__name__)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    await init_db()
    setup_all()

    worker = Worker()
    worker_task = asyncio.create_task(worker.run())

    # Graceful shutdown — важно на Render.com при деплое
    # Render посылает SIGTERM перед остановкой контейнера
    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Received stop signal, shutting down...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        # drop_pending_updates=True — отбрасываем накопившиеся апдейты при рестарте,
        # это устраняет TelegramConflictError при rolling-деплое на Render
        polling_task = asyncio.create_task(
            dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types(),
                drop_pending_updates=True,
            )
        )

        # Ждём сигнала остановки
        await stop_event.wait()

    except TelegramConflictError:
        logger.error(
            "TelegramConflictError: another bot instance is running. "
            "Waiting 5 seconds and retrying..."
        )
        await asyncio.sleep(5)
        raise

    finally:
        logger.info("Stopping polling...")
        await dp.stop_polling()

        logger.info("Cancelling worker...")
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        logger.info("Closing bot session...")
        await bot.session.close()

        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())