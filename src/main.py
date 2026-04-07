from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
import datetime
import os

API_TOKEN = 'YOUR_BOT_API_TOKEN'

dp = Dispatcher()

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.reply("Привет! Я ваш трекер сна. Используйте /sleep и /wake для отмечания времени сна и пробуждения.", parse_mode=ParseMode.HTML)

@dp.message_handler(commands=['sleep'])
async def sleep_handler(message: types.Message):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("sleep_times.txt", "a") as file:
        file.write(f"Сон: {current_time}\n")
    await message.reply(f"Отмечено время сна: {current_time}", parse_mode=ParseMode.HTML)

@dp.message_handler(commands=['wake'])
async def wake_handler(message: types.Message):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("sleep_times.txt", "a") as file:
        file.write(f"Пробуждение: {current_time}\n")
    await message.reply(f"Отмечено время пробуждения: {current_time}", parse_mode=ParseMode.HTML)

@dp.message_handler(commands=['report'])
async def report_handler(message: types.Message):
    if os.path.exists("sleep_times.txt"):
        with open("sleep_times.txt", "r") as file:
            content = file.read()
        await message.reply(f"Отчет о сне:\n\n{content}", parse_mode=ParseMode.HTML)
    else:
        await message.reply("У вас еще нет отмеченных данных.", parse_mode=ParseMode.HTML)

if __name__ == '__main__':
    bot = Bot(token=API_TOKEN, parse_mode=ParseMode.HTML)
    dp.run_polling(bot)