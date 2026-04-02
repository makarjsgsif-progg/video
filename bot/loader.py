from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties  # Добавили этот импорт
from aiogram.enums import ParseMode
from config.config import settings

# Инициализируем бота по новому стандарту
bot = Bot(
    token=settings.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()