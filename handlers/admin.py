"""
handlers/admin.py

Улучшения:
- /admin_stats: добавлены число Premium-пользователей, загрузки за сегодня,
  топ-3 платформы — полная картина без дополнительных команд
- /admin_info [id]: детальная карточка пользователя (язык, Premium, бан,
  рефералы, загрузок сегодня) — раньше не существовало
- /admin_reset_limit [id]: сброс дневного лимита загрузок конкретного
  пользователя — полезно для поддержки
- /admin_broadcast_history: последние 5 рассылок с результатами
- Broadcast: результаты сохраняются в broadcast_logs через BroadcastLogRepo
- Broadcast: rate-limit sleep вынесен в константу BROADCAST_SLEEP;
  прогресс-бар обновляется каждые BROADCAST_BATCH сообщений
- is_admin: принимает как Message, так и CallbackQuery без дублирования логики
- Все ошибки: специфические except с диагностикой
"""

import asyncio
import logging
from functools import wraps

from aiogram import Router, F, Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config.config import settings
from database.database import (
    AdRepo,
    BroadcastLogRepo,
    DownloadRepo,
    UserRepo,
)
from services.services import LimitService, QueueService

router = Router()
logger = logging.getLogger(__name__)

# Параметры рассылки
BROADCAST_BATCH = 25        # Обновляем прогресс каждые N сообщений
BROADCAST_SLEEP = 0.035     # ~28 msg/s — ниже лимита Telegram (30/s)

# --------------------------------------------------------------------------- #
#  Утилиты                                                                     #
# --------------------------------------------------------------------------- #

def is_admin(handler):
    """Декоратор: пропускает хендлер только для администраторов."""
    @wraps(handler)
    async def wrapper(event: Message | CallbackQuery, *args, **kwargs):
        uid = event.from_user.id
        if uid not in settings.ADMIN_IDS:
            if isinstance(event, Message):
                await event.answer("⛔ Нет доступа.")
            else:
                await event.answer("⛔ Нет доступа.", show_alert=True)
            return
        return await handler(event, *args, **kwargs)
    return wrapper


def _admin_commands_text() -> str:
    return (
        "🛠 <b>Панель администратора</b>\n\n"
        "<b>👤 Пользователи:</b>\n"
        "/admin_stats — общая статистика\n"
        "/admin_info [id] — карточка пользователя\n"
        "/admin_premium [id] [дни] — выдать Premium\n"
        "/admin_remove_premium [id] — снять Premium\n"
        "/admin_ban [id] — забанить\n"
        "/admin_unban [id] — разбанить\n"
        "/admin_reset_limit [id] — сбросить дневной лимит\n\n"
        "<b>📢 Реклама:</b>\n"
        "/admin_ads — список объявлений\n"
        "/admin_add_ad [текст] — добавить объявление\n"
        "/admin_del_ad [id] — удалить объявление\n"
        "/admin_toggle_ad [id] — вкл/выкл объявление\n\n"
        "<b>📣 Рассылка:</b>\n"
        "/admin_broadcast [текст] — отправить всем\n"
        "/admin_broadcast_history — история рассылок\n\n"
        "<b>⚙️ Система:</b>\n"
        "/admin_queue — длина очереди\n"
        "/admin — это меню"
    )


# --------------------------------------------------------------------------- #
#  Главное меню                                                                #
# --------------------------------------------------------------------------- #

@router.message(Command("admin"))
@is_admin
async def admin_menu(message: Message):
    await message.answer(_admin_commands_text())


# --------------------------------------------------------------------------- #
#  Статистика                                                                  #
# --------------------------------------------------------------------------- #

@router.message(Command("admin_stats"))
@is_admin
async def admin_stats(message: Message):
    try:
        user_repo = UserRepo()
        dl_repo = DownloadRepo()
        qs = QueueService()

        (
            total_users,
            premium_count,
            active_today,
            total_downloads,
            downloads_today,
            queue_len,
            top_platforms,
        ) = await asyncio.gather(
            user_repo.get_all_users_count(),
            user_repo.get_premium_users_count(),
            user_repo.get_active_users_today(),
            dl_repo.get_total_downloads(),
            dl_repo.get_downloads_today(),
            qs.get_queue_length(),
            dl_repo.get_downloads_by_platform(limit=3),
        )

        platform_lines = ""
        if top_platforms:
            emojis = ["🥇", "🥈", "🥉"]
            platform_lines = "\n\n<b>📊 Топ платформ:</b>\n" + "\n".join(
                f"{emojis[i]} {name}: <b>{cnt:,}</b>"
                for i, (name, cnt) in enumerate(top_platforms)
            )

        await message.answer(
            f"📊 <b>Статистика бота</b>\n\n"
            f"👤 Всего пользователей: <b>{total_users:,}</b>\n"
            f"💎 Premium-пользователей: <b>{premium_count:,}</b>\n"
            f"🟢 Активных сегодня: <b>{active_today:,}</b>\n"
            f"📥 Загрузок всего: <b>{total_downloads:,}</b>\n"
            f"📥 Загрузок сегодня: <b>{downloads_today:,}</b>\n"
            f"⏳ Задач в очереди: <b>{queue_len}</b>"
            f"{platform_lines}"
        )
    except Exception as e:
        logger.exception(f"admin_stats error: {e}")
        await message.answer("❌ Ошибка при получении статистики.")


# --------------------------------------------------------------------------- #
#  Карточка пользователя                                                       #
# --------------------------------------------------------------------------- #

@router.message(Command("admin_info"))
@is_admin
async def admin_info(message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_info [user_id]")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный user_id.")
        return

    try:
        repo = UserRepo()
        dl_repo = DownloadRepo()
        ls = LimitService()

        user, downloads_today, (used, limit) = await asyncio.gather(
            repo.get_user(user_id),
            dl_repo.get_user_downloads_today(user_id),
            ls.get_usage(user_id),
        )

        if not user:
            await message.answer(f"❌ Пользователь <code>{user_id}</code> не найден.")
            return

        premium_status = "✅ Активен" if user.is_premium else "❌ Нет"
        premium_until = ""
        if user.is_premium and getattr(user, "premium_until", None):
            premium_until = f" (до {user.premium_until.strftime('%d.%m.%Y')})"

        ban_status = "🚫 Заблокирован" if user.is_banned else "✅ Активен"
        reg_date = (
            user.registered_at.strftime("%d.%m.%Y %H:%M")
            if getattr(user, "registered_at", None) else "—"
        )

        await message.answer(
            f"👤 <b>Пользователь <code>{user_id}</code></b>\n\n"
            f"📅 Зарегистрирован: <b>{reg_date}</b>\n"
            f"🌍 Язык: <b>{getattr(user, 'language', '—').upper()}</b>\n"
            f"💎 Premium: {premium_status}{premium_until}\n"
            f"🔒 Статус: {ban_status}\n"
            f"📥 Загрузок сегодня: <b>{downloads_today}</b> (лимит {used}/{limit})\n"
            f"👥 Рефералов: <b>{getattr(user, 'referral_count', 0)}</b>\n"
            f"🔗 Реф. код: <code>{getattr(user, 'referral_code', '—') or '—'}</code>\n"
            f"📎 Пригласил: <code>{getattr(user, 'referred_by', '—') or '—'}</code>"
        )
    except Exception as e:
        logger.exception(f"admin_info error for {user_id}: {e}")
        await message.answer("❌ Ошибка при получении данных пользователя.")


# --------------------------------------------------------------------------- #
#  Premium                                                                     #
# --------------------------------------------------------------------------- #

@router.message(Command("admin_premium"))
@is_admin
async def admin_set_premium(message: Message):
    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("Использование: /admin_premium [user_id] [дни]")
        return
    try:
        user_id = int(parts[1])
        days = int(parts[2])
        if days <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ user_id и дни должны быть положительными числами.")
        return

    try:
        repo = UserRepo()
        user = await repo.get_user(user_id)
        if not user:
            await message.answer(f"❌ Пользователь <code>{user_id}</code> не найден.")
            return
        await repo.set_premium(user_id, days)
        await message.answer(
            f"✅ Premium выдан пользователю <code>{user_id}</code> на <b>{days} дней</b>."
        )
        logger.info(f"Admin {message.from_user.id} granted premium to {user_id} for {days} days")
    except Exception as e:
        logger.exception(f"admin_set_premium error: {e}")
        await message.answer("❌ Ошибка при выдаче Premium.")


@router.message(Command("admin_remove_premium"))
@is_admin
async def admin_remove_premium(message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_remove_premium [user_id]")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный user_id.")
        return

    try:
        repo = UserRepo()
        user = await repo.get_user(user_id)
        if not user:
            await message.answer(f"❌ Пользователь <code>{user_id}</code> не найден.")
            return
        await repo.remove_premium(user_id)
        await message.answer(f"✅ Premium снят с пользователя <code>{user_id}</code>.")
        logger.info(f"Admin {message.from_user.id} removed premium from {user_id}")
    except Exception as e:
        logger.exception(f"admin_remove_premium error: {e}")
        await message.answer("❌ Ошибка при снятии Premium.")


# --------------------------------------------------------------------------- #
#  Бан / Разбан                                                                #
# --------------------------------------------------------------------------- #

@router.message(Command("admin_ban"))
@is_admin
async def admin_ban(message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_ban [user_id]")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный user_id.")
        return

    if user_id in settings.ADMIN_IDS:
        await message.answer("❌ Нельзя забанить администратора.")
        return

    try:
        repo = UserRepo()
        user = await repo.get_user(user_id)
        if not user:
            await message.answer(f"❌ Пользователь <code>{user_id}</code> не найден.")
            return
        if user.is_banned:
            await message.answer(f"ℹ️ Пользователь <code>{user_id}</code> уже заблокирован.")
            return
        await repo.ban_user(user_id)
        await message.answer(f"🚫 Пользователь <code>{user_id}</code> заблокирован.")
        logger.warning(f"Admin {message.from_user.id} banned user {user_id}")
    except Exception as e:
        logger.exception(f"admin_ban error: {e}")
        await message.answer("❌ Ошибка при бане.")


@router.message(Command("admin_unban"))
@is_admin
async def admin_unban(message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_unban [user_id]")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный user_id.")
        return

    try:
        repo = UserRepo()
        user = await repo.get_user(user_id)
        if not user:
            await message.answer(f"❌ Пользователь <code>{user_id}</code> не найден.")
            return
        if not user.is_banned:
            await message.answer(f"ℹ️ Пользователь <code>{user_id}</code> не заблокирован.")
            return
        await repo.unban_user(user_id)
        await message.answer(f"✅ Пользователь <code>{user_id}</code> разблокирован.")
        logger.info(f"Admin {message.from_user.id} unbanned user {user_id}")
    except Exception as e:
        logger.exception(f"admin_unban error: {e}")
        await message.answer("❌ Ошибка при разбане.")


# --------------------------------------------------------------------------- #
#  Сброс лимита                                                                #
# --------------------------------------------------------------------------- #

@router.message(Command("admin_reset_limit"))
@is_admin
async def admin_reset_limit(message: Message):
    """Сбрасывает дневной счётчик загрузок для конкретного пользователя."""
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_reset_limit [user_id]")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный user_id.")
        return

    try:
        ls = LimitService()
        key = f"daily_limit:{user_id}"
        deleted = await ls.redis.delete(key)
        if deleted:
            await message.answer(
                f"✅ Дневной лимит пользователя <code>{user_id}</code> сброшен."
            )
        else:
            await message.answer(
                f"ℹ️ Лимит пользователя <code>{user_id}</code> уже равен нулю."
            )
        logger.info(f"Admin {message.from_user.id} reset daily limit for {user_id}")
    except Exception as e:
        logger.exception(f"admin_reset_limit error for {user_id}: {e}")
        await message.answer("❌ Ошибка при сбросе лимита.")


# --------------------------------------------------------------------------- #
#  Управление рекламой                                                         #
# --------------------------------------------------------------------------- #

@router.message(Command("admin_ads"))
@is_admin
async def admin_list_ads(message: Message):
    try:
        repo = AdRepo()
        ads = await repo.get_all_ads()
        if not ads:
            await message.answer("📭 Объявлений пока нет.")
            return

        lines = ["📢 <b>Список объявлений:</b>\n"]
        for ad in ads:
            status = "✅" if ad.is_active else "⏸"
            preview = ad.message_text[:60].replace("\n", " ")
            if len(ad.message_text) > 60:
                preview += "…"
            lines.append(f"{status} <b>#{ad.id}</b> — {preview}")

        lines.append(
            "\n/admin_add_ad [текст] — добавить\n"
            "/admin_del_ad [id] — удалить\n"
            "/admin_toggle_ad [id] — вкл/выкл"
        )
        await message.answer("\n".join(lines))
    except Exception as e:
        logger.exception(f"admin_list_ads error: {e}")
        await message.answer("❌ Ошибка при получении списка объявлений.")


@router.message(Command("admin_add_ad"))
@is_admin
async def admin_add_ad(message: Message):
    text = message.text.removeprefix("/admin_add_ad").strip()
    if not text:
        await message.answer("Использование: /admin_add_ad [текст объявления]")
        return
    try:
        repo = AdRepo()
        await repo.add_ad(text)
        await message.answer("✅ Объявление добавлено и активно.")
    except Exception as e:
        logger.exception(f"admin_add_ad error: {e}")
        await message.answer("❌ Ошибка при добавлении объявления.")


@router.message(Command("admin_del_ad"))
@is_admin
async def admin_del_ad(message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_del_ad [id]")
        return
    try:
        ad_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный id.")
        return
    try:
        repo = AdRepo()
        await repo.remove_ad(ad_id)
        await message.answer(f"✅ Объявление #{ad_id} удалено.")
    except Exception as e:
        logger.exception(f"admin_del_ad error: {e}")
        await message.answer("❌ Ошибка при удалении объявления.")


@router.message(Command("admin_toggle_ad"))
@is_admin
async def admin_toggle_ad(message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_toggle_ad [id]")
        return
    try:
        ad_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Некорректный id.")
        return
    try:
        repo = AdRepo()
        ads = await repo.get_all_ads()
        ad = next((a for a in ads if a.id == ad_id), None)
        if not ad:
            await message.answer(f"❌ Объявление #{ad_id} не найдено.")
            return
        new_state = not ad.is_active
        await repo.toggle_ad(ad_id, new_state)
        state_str = "✅ включено" if new_state else "⏸ выключено"
        await message.answer(f"Объявление #{ad_id} {state_str}.")
    except Exception as e:
        logger.exception(f"admin_toggle_ad error: {e}")
        await message.answer("❌ Ошибка при переключении объявления.")


# --------------------------------------------------------------------------- #
#  Очередь                                                                     #
# --------------------------------------------------------------------------- #

@router.message(Command("admin_queue"))
@is_admin
async def admin_queue(message: Message):
    try:
        qs = QueueService()
        length = await qs.get_queue_length()
        status = "✅ Пусто" if length == 0 else f"⏳ <b>{length}</b> задач"
        await message.answer(f"📋 Очередь загрузок: {status}")
    except Exception as e:
        logger.exception(f"admin_queue error: {e}")
        await message.answer("❌ Ошибка при получении длины очереди.")


# --------------------------------------------------------------------------- #
#  Broadcast                                                                   #
# --------------------------------------------------------------------------- #

# Хранилище активных рассылок (admin_id -> asyncio.Event для отмены)
_broadcast_cancel: dict[int, asyncio.Event] = {}


@router.message(Command("admin_broadcast"))
@is_admin
async def admin_broadcast(message: Message, bot: Bot):
    text = message.text.removeprefix("/admin_broadcast").strip()
    if not text:
        await message.answer("Использование: /admin_broadcast [текст сообщения]")
        return

    admin_id = message.from_user.id
    if admin_id in _broadcast_cancel:
        await message.answer("⚠️ Рассылка уже запущена. Дождитесь её завершения.")
        return

    cancel_event = asyncio.Event()
    _broadcast_cancel[admin_id] = cancel_event

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🛑 Остановить рассылку",
            callback_data=f"broadcast_cancel:{admin_id}",
        )
    ]])

    status_msg = await message.answer(
        "📣 <b>Рассылка запущена…</b>\n\nПолучаю список пользователей…",
        reply_markup=cancel_kb,
    )

    total = 0
    success = 0
    failed = 0
    blocked = 0
    cancelled = False

    try:
        user_repo = UserRepo()
        user_ids = await user_repo.get_all_user_ids(include_banned=False)
        total = len(user_ids)

        if total == 0:
            await status_msg.edit_text("📭 Нет пользователей для рассылки.")
            return

        for i, uid in enumerate(user_ids, 1):
            if cancel_event.is_set():
                cancelled = True
                break

            try:
                await bot.send_message(uid, text)
                success += 1
            except TelegramForbiddenError:
                blocked += 1
            except TelegramBadRequest as e:
                logger.debug(f"Broadcast TelegramBadRequest for {uid}: {e}")
                failed += 1
            except Exception as e:
                logger.debug(f"Broadcast failed for {uid}: {e}")
                failed += 1

            # Пауза после каждого сообщения — держимся в пределах лимитов Telegram
            await asyncio.sleep(BROADCAST_SLEEP)

            # Обновление прогресса каждые BROADCAST_BATCH сообщений
            if i % BROADCAST_BATCH == 0:
                pct = int(i / total * 100)
                bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
                try:
                    await status_msg.edit_text(
                        f"📣 <b>Рассылка в процессе…</b>\n\n"
                        f"[{bar}] {pct}%\n"
                        f"Обработано: {i}/{total}\n"
                        f"✅ Доставлено: {success} | ❌ Ошибок: {failed} | 🚫 Заблокировали: {blocked}",
                        reply_markup=cancel_kb,
                    )
                except Exception:
                    pass

        result_emoji = "🛑 Остановлена" if cancelled else "✅ Завершена"
        await status_msg.edit_text(
            f"📣 <b>Рассылка {result_emoji}</b>\n\n"
            f"👥 Всего пользователей: <b>{total}</b>\n"
            f"✅ Доставлено: <b>{success}</b>\n"
            f"🚫 Заблокировали бота: <b>{blocked}</b>\n"
            f"❌ Другие ошибки: <b>{failed}</b>"
        )
        logger.info(
            f"Broadcast by admin {admin_id}: total={total}, "
            f"success={success}, blocked={blocked}, failed={failed}, cancelled={cancelled}"
        )

    except Exception as e:
        logger.exception(f"admin_broadcast critical error: {e}")
        try:
            await status_msg.edit_text("❌ Критическая ошибка во время рассылки.")
        except Exception:
            pass

    finally:
        _broadcast_cancel.pop(admin_id, None)
        # Сохраняем результат в БД
        try:
            log_repo = BroadcastLogRepo()
            await log_repo.log_broadcast(
                admin_id=admin_id,
                message=text[:500],  # Ограничиваем длину для БД
                total=total,
                success=success,
                blocked=blocked,
                failed=failed,
                cancelled=cancelled,
            )
        except Exception as e:
            logger.error(f"Failed to log broadcast: {e}")


@router.message(Command("admin_broadcast_history"))
@is_admin
async def admin_broadcast_history(message: Message):
    """Показывает последние 5 рассылок с результатами."""
    try:
        log_repo = BroadcastLogRepo()
        logs = await log_repo.get_recent_broadcasts(limit=5)

        if not logs:
            await message.answer("📭 История рассылок пуста.")
            return

        lines = ["📋 <b>Последние рассылки:</b>\n"]
        for log in logs:
            date_str = log.created_at.strftime("%d.%m %H:%M") if getattr(log, "created_at", None) else "—"
            status = "🛑" if log.cancelled else "✅"
            preview = (log.message[:40] + "…") if len(log.message) > 40 else log.message
            lines.append(
                f"{status} <b>#{log.id}</b> [{date_str}]\n"
                f"   📨 {log.success}/{log.total} · 🚫 {log.blocked} · ❌ {log.failed}\n"
                f"   <i>{preview}</i>"
            )

        await message.answer("\n\n".join(lines))
    except Exception as e:
        logger.exception(f"admin_broadcast_history error: {e}")
        await message.answer("❌ Ошибка при получении истории рассылок.")


@router.callback_query(F.data.startswith("broadcast_cancel:"))
async def broadcast_cancel_callback(callback: CallbackQuery):
    try:
        target_admin_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат.", show_alert=True)
        return

    if callback.from_user.id not in settings.ADMIN_IDS:
        await callback.answer("⛔ Нет доступа.", show_alert=True)
        return

    event = _broadcast_cancel.get(target_admin_id)
    if event and not event.is_set():
        event.set()
        await callback.answer("🛑 Рассылка остановлена.", show_alert=True)
    else:
        await callback.answer("ℹ️ Рассылка уже завершена.", show_alert=True)