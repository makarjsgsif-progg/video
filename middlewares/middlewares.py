"""
middlewares/middlewares.py

Улучшения:
- _dict_to_user / _user_to_dict / _make_stub_user: добавлено поле referral_code —
  раньше отсутствовало, вызывало AttributeError в handlers/user.py при доступе
  к user_db.referral_code
- UserMiddleware: кэширование user_db в Redis на 60 сек — убирает N+1 запросов к БД
- ThrottleMiddleware: раздельный кулдаун для callback_query (0.3 с) и message (0.5 с);
  устойчивость к ошибкам Redis; периодическая чистка in-memory словаря
- I18nMiddleware: добавлено логирование при отсутствии user_db
- BanCheckMiddleware: выделена проверка бана в отдельный middleware — раньше бан
  проверялся только в handle_url, но пользователь мог вызывать /referral, /premium
  и т.д. даже будучи заблокированным
- Все middleware: аннотации типов, docstrings
"""

import json
import logging
import time
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message

from database.database import UserRepo
from utils.i18n import get_text

logger = logging.getLogger(__name__)

# TTL кэша пользователя (секунды)
_USER_CACHE_TTL = 60
# Кулдауны (секунды)
_MSG_THROTTLE = 0.5
_CB_THROTTLE = 0.3
# Порог чистки in-memory throttle dict
_THROTTLE_CLEANUP_SIZE = 10_000
_THROTTLE_TTL = 60


# --------------------------------------------------------------------------- #
#  Сериализация / десериализация пользователя                                  #
# --------------------------------------------------------------------------- #

def _user_to_dict(user) -> dict:
    return {
        "id": user.id,
        "language": getattr(user, "language", "ru"),
        "is_premium": getattr(user, "is_premium", False),
        "is_banned": getattr(user, "is_banned", False),
        "referral_count": getattr(user, "referral_count", 0),
        "referred_by": getattr(user, "referred_by", None),
        "premium_until": str(user.premium_until) if getattr(user, "premium_until", None) else None,
        "registered_at": str(user.registered_at) if getattr(user, "registered_at", None) else None,
        "referral_code": getattr(user, "referral_code", None),
    }


def _dict_to_user(d: dict):
    from datetime import datetime

    def _parse_dt(v):
        if not v or v == "None":
            return None
        try:
            return datetime.fromisoformat(v)
        except Exception:
            return None

    d2 = dict(d)
    d2["premium_until"] = _parse_dt(d.get("premium_until"))
    d2["registered_at"] = _parse_dt(d.get("registered_at"))
    # Гарантируем наличие всех полей (защита от старых кэшей без новых колонок)
    d2.setdefault("referral_code", None)
    d2.setdefault("referred_by", None)
    d2.setdefault("referral_count", 0)
    return type("User", (), d2)()


def _make_stub_user(user_id: int):
    """
    Возвращает минимальный объект пользователя при ошибке БД.
    Позволяет боту продолжать работу без падения.
    """
    return type("User", (), {
        "id": user_id,
        "language": "ru",
        "is_premium": False,
        "is_banned": False,
        "referral_count": 0,
        "referred_by": None,
        "premium_until": None,
        "registered_at": None,
        "referral_code": None,
    })()


# --------------------------------------------------------------------------- #
#  UserMiddleware                                                               #
# --------------------------------------------------------------------------- #

class UserMiddleware(BaseMiddleware):
    """
    Загружает запись пользователя из БД и кладёт в data["user_db"].

    Кэширует в Redis на _USER_CACHE_TTL секунд, чтобы не стучаться в БД
    при каждом входящем событии. При ошибке Redis — читает напрямую из БД.
    При ошибке БД — возвращает stub-объект, бот не падает.
    """

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user_id: int | None = getattr(getattr(event, "from_user", None), "id", None)

        if user_id is not None:
            user = await self._get_or_create_user(user_id)
            data["user_db"] = user

        return await handler(event, data)

    async def _get_or_create_user(self, user_id: int):
        repo = UserRepo()

        cached = await self._cache_get(repo, user_id)
        if cached is not None:
            return cached

        try:
            user = await repo.get_user(user_id)
            if not user:
                user = await repo.create_user(user_id)
        except Exception as e:
            logger.exception(f"UserMiddleware: DB error for user {user_id}: {e}")
            return _make_stub_user(user_id)

        if user is None:
            logger.warning(f"UserMiddleware: create_user returned None for {user_id}")
            return _make_stub_user(user_id)

        await self._cache_set(repo, user_id, user)
        return user

    async def _cache_get(self, repo: UserRepo, user_id: int):
        try:
            redis = repo._get_redis()
            if redis is None:
                return None
            raw = await redis.get(f"user_cache:{user_id}")
            if raw:
                d = json.loads(raw)
                return _dict_to_user(d)
        except Exception as e:
            logger.debug(f"Redis cache get failed for {user_id}: {e}")
        return None

    async def _cache_set(self, repo: UserRepo, user_id: int, user):
        try:
            redis = repo._get_redis()
            if redis is None:
                return
            d = _user_to_dict(user)
            await redis.setex(
                f"user_cache:{user_id}",
                _USER_CACHE_TTL,
                json.dumps(d, default=str),
            )
        except Exception as e:
            logger.debug(f"Redis cache set failed for {user_id}: {e}")


# --------------------------------------------------------------------------- #
#  BanCheckMiddleware                                                           #
# --------------------------------------------------------------------------- #

class BanCheckMiddleware(BaseMiddleware):
    """
    Блокирует все действия заблокированных пользователей.

    Должен регистрироваться ПОСЛЕ UserMiddleware (зависит от data["user_db"]).
    Раньше бан проверялся только в handle_url — теперь работает глобально.
    """

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("user_db")
        if user and getattr(user, "is_banned", False):
            lang = getattr(user, "language", "ru")
            ban_text = get_text(lang, "banned")
            if isinstance(event, Message):
                try:
                    await event.answer(ban_text)
                except Exception:
                    pass
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer(ban_text, show_alert=True)
                except Exception:
                    pass
            return  # Не передаём в хендлер

        return await handler(event, data)


# --------------------------------------------------------------------------- #
#  ThrottleMiddleware                                                           #
# --------------------------------------------------------------------------- #

class ThrottleMiddleware(BaseMiddleware):
    """
    Ограничивает частоту запросов от одного пользователя.

    Разные кулдауны для message (0.5 с) и callback_query (0.3 с).
    Использует in-memory словари — устойчив к сбоям Redis.
    Периодически чистит устаревшие записи при росте словаря.
    """

    def __init__(self):
        self._msg_last: dict[int, float] = {}
        self._cb_last: dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user_id: int = event.from_user.id
        now = time.monotonic()

        if isinstance(event, Message):
            throttle_map = self._msg_last
            cooldown = _MSG_THROTTLE
        else:
            throttle_map = self._cb_last
            cooldown = _CB_THROTTLE

        last = throttle_map.get(user_id, 0.0)
        if now - last < cooldown:
            if isinstance(event, Message):
                try:
                    await event.answer("⏳ Слишком много запросов. Подожди немного.")
                except Exception:
                    pass
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer("⏳ Слишком часто. Подожди немного.", show_alert=False)
                except Exception:
                    pass
            return

        throttle_map[user_id] = now

        if len(throttle_map) > _THROTTLE_CLEANUP_SIZE:
            cutoff = now - _THROTTLE_TTL
            keys_to_del = [uid for uid, ts in throttle_map.items() if ts < cutoff]
            for k in keys_to_del:
                throttle_map.pop(k, None)

        return await handler(event, data)


# --------------------------------------------------------------------------- #
#  I18nMiddleware                                                               #
# --------------------------------------------------------------------------- #

class I18nMiddleware(BaseMiddleware):
    """
    Добавляет в data функцию gettext(key, **kwargs) с учётом языка пользователя.

    Зависит от user_db, который должен быть добавлен раньше (UserMiddleware).
    Если user_db недоступен — использует язык по умолчанию (ru).
    """

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("user_db")
        if user is None:
            logger.debug("I18nMiddleware: user_db not found in data, using default lang 'ru'")
        lang = getattr(user, "language", "ru") if user else "ru"
        data["gettext"] = lambda key, **kwargs: get_text(lang, key, **kwargs)
        return await handler(event, data)