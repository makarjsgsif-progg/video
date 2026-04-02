import os
from typing import List, Union
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # Указываем Pydantic, что нужно брать данные из окружения
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    BOT_TOKEN: str
    # Используем Union, чтобы он не ругался при первичной загрузке
    ADMIN_IDS: List[int]

    DATABASE_URL: str = "sqlite+aiosqlite:///./bot.db"
    REDIS_URL: str = "redis://localhost:6379/0"

    DEFAULT_DAILY_LIMIT: int = 5
    MAX_RETRIES: int = 3
    DOWNLOAD_TIMEOUT: int = 60

    @field_validator("ADMIN_IDS", mode="before")
    @classmethod
    def parse_admin_ids(cls, v: Union[str, List[int], int]) -> List[int]:
        if isinstance(v, str):
            # Убираем лишние пробелы и скобки, если они вдруг прилетят
            v = v.replace("[", "").replace("]", "").strip()
            return [int(i.strip()) for i in v.split(",") if i.strip()]
        if isinstance(v, int):
            return [v]
        return v

settings = Settings()