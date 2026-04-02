from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from config.config import settings

bot = Bot(token=settings.BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()