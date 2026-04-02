import os
from typing import List
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS: List[int] = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id]

    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./bot.db")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    DEFAULT_DAILY_LIMIT: int = int(os.getenv("DEFAULT_DAILY_LIMIT", "5"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    DOWNLOAD_TIMEOUT: int = int(os.getenv("DOWNLOAD_TIMEOUT", "60"))


settings = Settings()