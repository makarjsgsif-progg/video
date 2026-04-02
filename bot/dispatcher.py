from aiogram import Router
from bot.loader import dp
from handlers import user, admin, callback
from middlewares.user_middleware import UserMiddleware
from middlewares.throttle_middleware import ThrottleMiddleware
from middlewares.i18n_middleware import I18nMiddleware

def setup_handlers():
    router = Router()
    router.include_router(user.router)
    router.include_router(admin.router)
    router.include_router(callback.router)
    dp.include_router(router)

def setup_middlewares():
    dp.message.outer_middleware(UserMiddleware())
    dp.callback_query.outer_middleware(UserMiddleware())
    dp.message.outer_middleware(ThrottleMiddleware())
    dp.callback_query.outer_middleware(ThrottleMiddleware())
    dp.message.middleware(I18nMiddleware())
    dp.callback_query.middleware(I18nMiddleware())

def setup_all():
    setup_handlers()
    setup_middlewares()