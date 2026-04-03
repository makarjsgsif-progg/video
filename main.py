import asyncio
import logging
from bot.dispatcher import setup_all
from bot.loader import dp, bot
from bot.worker import Worker
from database.database import init_db


async def main():
    logging.basicConfig(level=logging.INFO)
    await init_db()
    setup_all()

    worker = Worker()
    asyncio.create_task(worker.run())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())