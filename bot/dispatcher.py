from aiogram import Router
from bot.loader import dp
from handlers import user, admin, callback
from middlewares.middlewares import UserMiddleware, ThrottleMiddleware, I18nMiddleware

def setup_handlers():
    router = Router()
    # Админский роутер должен быть ВЫШЕ пользовательского
    router.include_router(admin.router)
    router.include_router(user.router)
    router.include_router(callback.router)
    dp.include_router(router)

def setup_middlewares():
    # FIX: один общий экземпляр ThrottleMiddleware для message и callback_query,
    # иначе у каждого свой last_calls и троттлинг не работает на callback
    throttle = ThrottleMiddleware()

    dp.message.outer_middleware(UserMiddleware())
    dp.callback_query.outer_middleware(UserMiddleware())
    dp.message.outer_middleware(throttle)
    dp.callback_query.outer_middleware(throttle)
    dp.message.middleware(I18nMiddleware())
    dp.callback_query.middleware(I18nMiddleware())

def setup_all():
    setup_handlers()
    setup_middlewares()