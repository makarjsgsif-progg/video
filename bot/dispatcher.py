from aiogram import Router
from bot.loader import dp
from handlers import user, admin, callback
from middlewares.middlewares import UserMiddleware, ThrottleMiddleware, I18nMiddleware


def setup_handlers():
    router = Router()

    # ВАЖНО: admin.router должен быть первым —
    # иначе хендлер handle_url в user.router перехватит /admin_* команды,
    # если фильтр ~F.text.startswith("/") вдруг не сработает.
    router.include_router(admin.router)
    router.include_router(callback.router)
    router.include_router(user.router)

    dp.include_router(router)


def setup_middlewares():
    # Один общий экземпляр ThrottleMiddleware для message и callback_query —
    # иначе у каждого свой last_calls и throttling не работает на callback_query.
    throttle = ThrottleMiddleware()

    # outer_middleware — выполняются ДО хендлера, обёрткой снаружи
    dp.message.outer_middleware(UserMiddleware())
    dp.callback_query.outer_middleware(UserMiddleware())

    dp.message.outer_middleware(throttle)
    dp.callback_query.outer_middleware(throttle)

    # inner middleware (middleware) — выполняются после outer, но до хендлера
    # I18n зависит от user_db, который кладёт UserMiddleware — порядок важен
    dp.message.middleware(I18nMiddleware())
    dp.callback_query.middleware(I18nMiddleware())


def setup_all():
    setup_handlers()
    setup_middlewares()
