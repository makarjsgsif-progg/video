import json
import os

languages = {
    "en": "English",
    "ru": "Русский",
    "es": "Español",
    "pt": "Português",
    "de": "Deutsch",
    "fr": "Français",
    "hi": "हिंदी",
    "ar": "العربية"
}

translations = {}

def load_translations():
    path = os.path.join("locales", "all.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for lang, content in data.items():
                translations[lang] = content
    else:
        # fallback – русский по умолчанию
        translations["ru"] = {
            "welcome": "Привет {name}! Отправь ссылку на видео.",
            "choose_language": "Выбери язык:",
            "language_set": "Язык обновлён!",
            "unsupported_url": "Неподдерживаемая ссылка.",
            "processing": "Скачиваю...",
            "banned": "Ты забанен.",
            "daily_limit_reached": "Дневной лимит исчерпан.",
            "premium_active": "Премиум активен до {until}.",
            "premium_info": "Купи премиум."
        }

load_translations()

def get_text(lang: str, key: str, **kwargs) -> str:
    # Сначала пробуем запрошенный язык, потом русский, потом английский
    text = translations.get(lang, {}).get(key)
    if not text:
        text = translations.get("ru", {}).get(key)
    if not text:
        text = translations.get("en", {}).get(key, key)
    if kwargs:
        return text.format(**kwargs)
    return text