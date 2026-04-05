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


def get_text(__lang: str, __key: str, **kwargs) -> str:
    """
    Fetch a translated string and format it with kwargs.

    Parameters are intentionally named with double-underscore prefix so they
    can NEVER collide with translation placeholder names like {lang}, {key},
    {limit}, {used}, {name}, etc. that callers pass via **kwargs.

    Before this fix, calling:
        get_text(lang, "profile_text", lang="🇷🇺 Русский", ...)
    crashed with "TypeError: got multiple values for argument 'lang'"
    because 'lang' was both a positional param name AND a kwarg key.
    """
    text = translations.get(__lang, {}).get(__key)
    if not text:
        text = translations.get("ru", {}).get(__key)
    if not text:
        text = translations.get("en", {}).get(__key, __key)
    if kwargs:
        return text.format(**kwargs)
    return text