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
        # fallback – только английский
        translations["en"] = {
            "welcome": "Hello {name}! Send me a link.",
            "choose_language": "Choose language:",
            "language_set": "Language updated!",
            "unsupported_url": "Unsupported URL.",
            "processing": "Downloading...",
            "banned": "You are banned.",
            "daily_limit_reached": "Daily limit reached.",
            "premium_active": "Premium active until {until}.",
            "premium_info": "Upgrade to premium."
        }

load_translations()

def get_text(lang: str, key: str, **kwargs) -> str:
    text = translations.get(lang, {}).get(key)
    if not text:
        text = translations.get("en", {}).get(key, key)
    if kwargs:
        return text.format(**kwargs)
    return text