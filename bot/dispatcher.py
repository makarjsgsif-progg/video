"""
bot/dispatcher.py

Улучшения:
- Регистрация BanCheckMiddleware — блокирует доступ забаненных пользователей
  ко всем хендлерам, а не только к handle_url (как было раньше)
- Порядок middleware строго определён и задокументирован:
  outer: User → BanCheck → Throttle
  inner:  I18n
- Явный комментарий о порядке роутеров (admin первым — критично!)
"""

from aiogram import Router
from bot.loader import dp
from handlers import user, admin, callback
from middlewares.middlewares import (
    UserMiddleware,
    BanCheckMiddleware,
    ThrottleMiddleware,
    I18nMiddleware,
)


def setup_handlers():
    router = Router()

    # ВАЖНО: порядок роутеров критичен.
    # admin.router — первым, чтобы /admin_* команды не перехватывались
    # универсальным handle_url из user.router.
    router.include_router(admin.router)
    router.include_router(callback.router)
    router.include_router(user.router)

    dp.include_router(router)


def setup_middlewares():
    # Единственный экземпляр ThrottleMiddleware — общее состояние между
    # message и callback_query (иначе у каждого свой словарь last_calls).
    throttle = ThrottleMiddleware()

    # ── outer middleware (выполняются СНАРУЖИ, в порядке регистрации) ──────
    # 1. UserMiddleware: загружает / создаёт пользователя, кладёт в data["user_db"]
    dp.message.outer_middleware(UserMiddleware())
    dp.callback_query.outer_middleware(UserMiddleware())

    # 2. BanCheckMiddleware: останавливает обработку для забаненных
    #    (зависит от user_db, поэтому после UserMiddleware)
    ban_check = BanCheckMiddleware()
    dp.message.outer_middleware(ban_check)
    dp.callback_query.outer_middleware(ban_check)

    # 3. ThrottleMiddleware: защита от flood
    dp.message.outer_middleware(throttle)
    dp.callback_query.outer_middleware(throttle)

    # ── inner middleware (выполняются ВНУТРИ, после outer) ──────────────────
    # I18nMiddleware: зависит от user_db → регистрируем как inner
    dp.message.middleware(I18nMiddleware())
    dp.callback_query.middleware(I18nMiddleware())


def setup_all():
    setup_handlers()
    setup_middlewares()