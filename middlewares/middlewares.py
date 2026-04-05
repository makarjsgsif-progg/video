"""
middlewares/middlewares.py

Mega Upgrade:
- ThrottleMiddleware: throttle-сообщения переведены по языку user_db
- BanCheckMiddleware: бан-сообщение переведено по языку пользователя
- UserMiddleware: кэш Redis, stub-пользователь при ошибке БД
- I18nMiddleware: передаёт gettext и lang в data
- Все поля ORM включая referral_code, referred_by
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

_USER_CACHE_TTL = 60
_MSG_THROTTLE = 0.5
_CB_THROTTLE = 0.3
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
    d2.setdefault("referral_code", None)
    d2.setdefault("referred_by", None)
    d2.setdefault("referral_count", 0)
    return type("User", (), d2)()


def _make_stub_user(user_id: int):
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
    Кэширует в Redis на _USER_CACHE_TTL секунд.
    """

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user_id: int | None = getattr(getattr(event, "from_user", None), "id", None)
        if user_id is not None:
            data["user_db"] = await self._get_or_create_user(user_id)
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
                return _dict_to_user(json.loads(raw))
        except Exception as e:
            logger.debug(f"Redis cache get failed for {user_id}: {e}")
        return None

    async def _cache_set(self, repo: UserRepo, user_id: int, user):
        try:
            redis = repo._get_redis()
            if redis is None:
                return
            await redis.setex(
                f"user_cache:{user_id}",
                _USER_CACHE_TTL,
                json.dumps(_user_to_dict(user), default=str),
            )
        except Exception as e:
            logger.debug(f"Redis cache set failed for {user_id}: {e}")


# --------------------------------------------------------------------------- #
#  BanCheckMiddleware                                                           #
# --------------------------------------------------------------------------- #

class BanCheckMiddleware(BaseMiddleware):
    """
    Блокирует все действия забаненных пользователей.
    Сообщение о бане переводится по языку пользователя.
    Должен регистрироваться ПОСЛЕ UserMiddleware.
    """

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("user_db")
        if user and getattr(user, "is_banned", False):
            lang = getattr(user, "language", "ru") or "ru"
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
            return
        return await handler(event, data)


# --------------------------------------------------------------------------- #
#  ThrottleMiddleware                                                           #
# --------------------------------------------------------------------------- #

class ThrottleMiddleware(BaseMiddleware):
    """
    Ограничивает частоту запросов.
    Throttle-сообщения переведены по языку пользователя.
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
            user_db = data.get("user_db")
            lang = getattr(user_db, "language", "ru") if user_db else "ru"
            if isinstance(event, Message):
                try:
                    await event.answer(get_text(lang, "throttle_message"))
                except Exception:
                    pass
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer(get_text(lang, "throttle_callback"), show_alert=False)
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
    Добавляет в data:
    - gettext(key, **kwargs): локализованный текст
    - lang: строка кода языка пользователя
    """

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("user_db")
        if user is None:
            logger.debug("I18nMiddleware: user_db not found, using default lang 'ru'")
        lang = (getattr(user, "language", "ru") if user else "ru") or "ru"
        data["gettext"] = lambda key, **kwargs: get_text(lang, key, **kwargs)
        data["lang"] = lang
        return await handler(event, data)