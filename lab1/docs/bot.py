"""
Бот сбора анонимной обратной связи команды.
python-telegram-bot 20.x (async-хендлеры), SQLite.

Запуск: asyncio.run() — без application.run_polling(), т.к. на Python 3.14 нет неявного
event loop в главном потоке и run_polling падает с RuntimeError.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Шаги диалога /new_question (только админ)
STATE_QUESTION, STATE_PERIOD = range(2)

DB_PATH = Path(__file__).resolve().parent / "feedback.db"


def get_admin_id() -> Optional[int]:
    if not ADMIN_ID_RAW:
        return None
    try:
        return int(ADMIN_ID_RAW)
    except ValueError:
        logger.error("ADMIN_ID должен быть целым числом")
        return None


def is_admin(user_id: int) -> bool:
    aid = get_admin_id()
    return aid is not None and user_id == aid


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db_sync() -> None:
    conn = get_connection()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                name TEXT,
                role TEXT NOT NULL DEFAULT 'user'
            );

            CREATE TABLE IF NOT EXISTS surveys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_text TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                survey_id INTEGER NOT NULL,
                rating INTEGER,
                feedback_text TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, survey_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (survey_id) REFERENCES surveys(id)
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


async def db_init() -> None:
    await asyncio.to_thread(init_db_sync)


def _upsert_user_sync(
    user_id: int, username: Optional[str], name: str, role: str
) -> None:
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO users (user_id, username, name, role)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                name = excluded.name,
                role = excluded.role
            """,
            (user_id, username or "", name, role),
        )
        conn.commit()
    finally:
        conn.close()


async def upsert_user(
    user_id: int, username: Optional[str], name: str, role: str
) -> None:
    await asyncio.to_thread(_upsert_user_sync, user_id, username, name, role)


def _get_active_survey_sync() -> Optional[sqlite3.Row]:
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT * FROM surveys WHERE active = 1 ORDER BY id DESC LIMIT 1"
        )
        return cur.fetchone()
    finally:
        conn.close()


async def get_active_survey() -> Optional[sqlite3.Row]:
    return await asyncio.to_thread(_get_active_survey_sync)


def _create_survey_sync(question_text: str, start: date, end: date) -> int:
    conn = get_connection()
    try:
        conn.execute("UPDATE surveys SET active = 0")
        cur = conn.execute(
            """
            INSERT INTO surveys (question_text, start_date, end_date, active)
            VALUES (?, ?, ?, 1)
            """,
            (question_text, start.isoformat(), end.isoformat()),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


async def create_survey(question_text: str, start: date, end: date) -> int:
    return await asyncio.to_thread(_create_survey_sync, question_text, start, end)


def _set_rating_sync(user_id: int, survey_id: int, rating: int) -> float:
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO responses (user_id, survey_id, rating, feedback_text, created_at)
            VALUES (?, ?, ?, NULL, ?)
            ON CONFLICT(user_id, survey_id) DO UPDATE SET
                rating = excluded.rating,
                created_at = excluded.created_at
            """,
            (user_id, survey_id, rating, now),
        )
        conn.commit()
        cur = conn.execute(
            """
            SELECT AVG(rating) FROM responses
            WHERE survey_id = ? AND rating IS NOT NULL
            """,
            (survey_id,),
        )
        row = cur.fetchone()
        avg = row[0]
        return float(avg) if avg is not None else float(rating)
    finally:
        conn.close()


async def set_rating(user_id: int, survey_id: int, rating: int) -> float:
    return await asyncio.to_thread(_set_rating_sync, user_id, survey_id, rating)


def _set_feedback_sync(user_id: int, survey_id: int, text: str) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO responses (user_id, survey_id, rating, feedback_text, created_at)
            VALUES (?, ?, NULL, ?, ?)
            ON CONFLICT(user_id, survey_id) DO UPDATE SET
                feedback_text = excluded.feedback_text,
                created_at = excluded.created_at
            """,
            (user_id, survey_id, text.strip(), now),
        )
        conn.commit()
    finally:
        conn.close()


async def set_feedback(user_id: int, survey_id: int, text: str) -> None:
    await asyncio.to_thread(_set_feedback_sync, user_id, survey_id, text)


def _survey_stats_sync(survey_id: int) -> dict:
    conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM surveys WHERE id = ?", (survey_id,))
        survey = cur.fetchone()
        if not survey:
            return {}

        cur = conn.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0] or 0

        cur = conn.execute(
            """
            SELECT COUNT(*) FROM responses
            WHERE survey_id = ?
              AND (rating IS NOT NULL OR (feedback_text IS NOT NULL AND TRIM(feedback_text) != ''))
            """,
            (survey_id,),
        )
        answered = cur.fetchone()[0] or 0

        cur = conn.execute(
            """
            SELECT AVG(rating) FROM responses
            WHERE survey_id = ? AND rating IS NOT NULL
            """,
            (survey_id,),
        )
        avg_row = cur.fetchone()
        avg_rating = avg_row[0]

        cur = conn.execute(
            """
            SELECT feedback_text FROM responses
            WHERE survey_id = ? AND feedback_text IS NOT NULL AND TRIM(feedback_text) != ''
            """,
            (survey_id,),
        )
        feedback_rows = [r[0].strip() for r in cur.fetchall()]

        return {
            "survey": survey,
            "total_users": total_users,
            "answered": answered,
            "avg_rating": float(avg_rating) if avg_rating is not None else None,
            "feedback_texts": feedback_rows,
        }
    finally:
        conn.close()


async def survey_stats(survey_id: int) -> dict:
    return await asyncio.to_thread(_survey_stats_sync, survey_id)


def _all_surveys_stats_sync() -> list:
    conn = get_connection()
    try:
        cur = conn.execute("SELECT id, question_text, active FROM surveys ORDER BY id")
        surveys = cur.fetchall()
        result = []
        for s in surveys:
            sid = s["id"]
            cur = conn.execute(
                """
                SELECT COUNT(*) FROM responses
                WHERE survey_id = ?
                  AND (rating IS NOT NULL OR (feedback_text IS NOT NULL AND TRIM(feedback_text) != ''))
                """,
                (sid,),
            )
            resp_count = cur.fetchone()[0]
            cur = conn.execute(
                """
                SELECT AVG(rating) FROM responses
                WHERE survey_id = ? AND rating IS NOT NULL
                """,
                (sid,),
            )
            avg = cur.fetchone()[0]
            result.append(
                {
                    "id": sid,
                    "question": s["question_text"],
                    "active": bool(s["active"]),
                    "responses": resp_count,
                    "avg": float(avg) if avg is not None else None,
                }
            )
        return result
    finally:
        conn.close()


async def all_surveys_stats() -> list:
    return await asyncio.to_thread(_all_surveys_stats_sync)


def _users_without_response_sync(survey_id: int) -> list:
    conn = get_connection()
    try:
        cur = conn.execute(
            """
            SELECT u.user_id FROM users u
            WHERE NOT EXISTS (
                SELECT 1 FROM responses r
                WHERE r.user_id = u.user_id AND r.survey_id = ?
                  AND (
                    r.rating IS NOT NULL
                    OR (r.feedback_text IS NOT NULL AND TRIM(r.feedback_text) != '')
                  )
            )
            """,
            (survey_id,),
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


async def users_without_response(survey_id: int) -> list:
    return await asyncio.to_thread(_users_without_response_sync, survey_id)


def format_period(start_iso: str, end_iso: str) -> str:
    d1 = date.fromisoformat(start_iso[:10])
    d2 = date.fromisoformat(end_iso[:10])
    return f"{d1.strftime('%d.%m')} - {d2.strftime('%d.%m')}"


def top_feedback_lines(texts: list[str], limit: int = 5) -> list[str]:
    """Топ формулировок по числу повторений (регистр не важен)."""
    if not texts:
        return []
    cnt = Counter(t.lower().strip() for t in texts if t.strip())
    out = []
    for key, _ in cnt.most_common(limit):
        for t in texts:
            if t.lower().strip() == key:
                out.append(t.strip())
                break
    return out


# --- Обработчики команд ---


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    u = update.effective_user
    try:
        role = "admin" if is_admin(u.id) else "user"
        name = u.full_name or (u.username or str(u.id))
        await upsert_user(u.id, u.username, name, role)
        await update.message.reply_text(
            "Привет! Я бот для сбора обратной связи. "
            "Оценки и отзывы по текущему вопросу видит только руководитель в отчёте; "
            "в сводке имена не показываются.\n\n"
            "/rate 1-5 — оценка\n"
            "/feedback текст — отзыв\n"
            "Для руководителя: /new_question, /report, /stats, /remind"
        )
    except Exception as e:
        logger.exception("Ошибка в /start: %s", e)
        await update.message.reply_text(
            "Не удалось зарегистрироваться. Попробуйте позже."
        )


async def cmd_rate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    try:
        survey = await get_active_survey()
        if not survey:
            await update.message.reply_text("Сейчас нет активного опроса.")
            return

        args = context.args or []
        if len(args) != 1 or not args[0].isdigit():
            await update.message.reply_text("Использование: /rate 1 (число от 1 до 5)")
            return
        rating = int(args[0])
        if rating < 1 or rating > 5:
            await update.message.reply_text("Оценка должна быть от 1 до 5.")
            return

        avg = await set_rating(update.effective_user.id, survey["id"], rating)
        await update.message.reply_text(
            f"Спасибо! Твой голос учтён. Текущая средняя оценка: {avg:.1f}"
        )
    except Exception as e:
        logger.exception("Ошибка в /rate: %s", e)
        await update.message.reply_text("Ошибка при сохранении оценки.")


async def cmd_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    try:
        survey = await get_active_survey()
        if not survey:
            await update.message.reply_text("Сейчас нет активного опроса.")
            return

        text = " ".join(context.args or []).strip()
        if not text:
            await update.message.reply_text(
                "Использование: /feedback Ваш текст отзыва"
            )
            return

        await set_feedback(update.effective_user.id, survey["id"], text)
        await update.message.reply_text("Спасибо за отзыв! Он учтён.")
    except Exception as e:
        logger.exception("Ошибка в /feedback: %s", e)
        await update.message.reply_text("Ошибка при сохранении отзыва.")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда доступна только руководителю.")
        return
    try:
        survey = await get_active_survey()
        if not survey:
            await update.message.reply_text("Нет активного опроса.")
            return

        stats = await survey_stats(survey["id"])
        total = stats["total_users"]
        answered = stats["answered"]
        pct = (100.0 * answered / total) if total else 0.0
        avg = stats["avg_rating"]
        avg_str = f"{avg:.1f}" if avg is not None else "—"
        period = format_period(survey["start_date"], survey["end_date"])
        tops = top_feedback_lines(stats["feedback_texts"], limit=5)

        lines = [
            f'📊 Опрос: "{survey["question_text"]}"',
            f"📅 Период: {period}",
            f"✅ Ответов: {answered}/{total} ({pct:.0f}%)",
            f"⭐ Средняя оценка: {avg_str}",
            "📝 Топ проблем (из feedback):",
        ]
        if tops:
            for i, t in enumerate(tops, 1):
                lines.append(f"    {i}. {t}")
        else:
            lines.append("    (пока нет текстовых отзывов)")

        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.exception("Ошибка в /report: %s", e)
        await update.message.reply_text("Ошибка при формировании отчёта.")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда доступна только руководителю.")
        return
    try:
        rows = await all_surveys_stats()
        if not rows:
            await update.message.reply_text("Опросов ещё не было.")
            return

        out = ["📈 Статистика по опросам:\n"]
        for r in rows:
            mark = "🟢" if r["active"] else "⚪"
            avg = f"{r['avg']:.1f}" if r["avg"] is not None else "—"
            q = r["question"][:60] + ("…" if len(r["question"]) > 60 else "")
            out.append(
                f"{mark} #{r['id']}: {q}\n"
                f"   Ответов: {r['responses']}, средняя: {avg}\n"
            )
        await update.message.reply_text("\n".join(out))
    except Exception as e:
        logger.exception("Ошибка в /stats: %s", e)
        await update.message.reply_text("Ошибка при получении статистики.")


async def cmd_new_question_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.effective_user or not update.message:
        return ConversationHandler.END
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда доступна только руководителю.")
        return ConversationHandler.END
    await update.message.reply_text(
        "Напиши текст нового вопроса одним сообщением.\nОтмена: /cancel"
    )
    return STATE_QUESTION


async def new_question_receive_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        await update.message.reply_text("Нужен текстовый вопрос.")
        return STATE_QUESTION
    text = update.message.text.strip()
    if len(text) < 3:
        await update.message.reply_text("Слишком короткий вопрос, напиши подробнее.")
        return STATE_QUESTION
    context.user_data["new_q_text"] = text
    await update.message.reply_text(
        "Укажи период опроса двумя датами: ДД.ММ.ГГГГ ДД.ММ.ГГГГ\n"
        "Например: 01.04.2026 07.04.2026\n"
        "Или отправь /skip — тогда период: сегодня + 6 дней."
    )
    return STATE_PERIOD


def parse_two_dates(s: str) -> Optional[tuple[date, date]]:
    parts = s.split()
    if len(parts) != 2:
        return None
    try:
        d1 = datetime.strptime(parts[0], "%d.%m.%Y").date()
        d2 = datetime.strptime(parts[1], "%d.%m.%Y").date()
        if d2 < d1:
            d1, d2 = d2, d1
        return d1, d2
    except ValueError:
        return None


async def new_question_skip_period(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message:
        return ConversationHandler.END
    qtext = context.user_data.get("new_q_text")
    if not qtext:
        await update.message.reply_text("Сессия сброшена. Начни с /new_question")
        return ConversationHandler.END
    try:
        start = date.today()
        end = start + timedelta(days=6)
        await create_survey(qtext, start, end)
        context.user_data.pop("new_q_text", None)
        await update.message.reply_text(
            f'Новый опрос создан: "{qtext}"\n'
            f"Период: {format_period(start.isoformat(), end.isoformat())}"
        )
    except Exception as e:
        logger.exception("Ошибка при создании опроса: %s", e)
        await update.message.reply_text("Ошибка при создании опроса.")
    return ConversationHandler.END


async def new_question_receive_period(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message:
        return ConversationHandler.END
    qtext = context.user_data.get("new_q_text")
    if not qtext:
        await update.message.reply_text("Сессия сброшена. Начни с /new_question")
        return ConversationHandler.END
    try:
        raw = (update.message.text or "").strip()
        parsed = parse_two_dates(raw)
        if not parsed:
            await update.message.reply_text(
                "Не понял формат. Пример: 01.04.2026 07.04.2026 или /skip"
            )
            return STATE_PERIOD
        start, end = parsed
        await create_survey(qtext, start, end)
        context.user_data.pop("new_q_text", None)
        await update.message.reply_text(
            f'Новый опрос создан: "{qtext}"\n'
            f"Период: {format_period(start.isoformat(), end.isoformat())}"
        )
    except Exception as e:
        logger.exception("Ошибка при создании опроса: %s", e)
        await update.message.reply_text("Ошибка при создании опроса.")
    return ConversationHandler.END


async def new_question_cancel(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    context.user_data.pop("new_q_text", None)
    if update.message:
        await update.message.reply_text("Создание опроса отменено.")
    return ConversationHandler.END


async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда доступна только руководителю.")
        return
    try:
        survey = await get_active_survey()
        if not survey:
            await update.message.reply_text("Нет активного опроса.")
            return

        aid = get_admin_id()
        pending = [
            uid
            for uid in await users_without_response(survey["id"])
            if aid is None or uid != aid
        ]
        if not pending:
            await update.message.reply_text(
                "Все зарегистрированные пользователи уже ответили."
            )
            return

        q_short = survey["question_text"][:200]
        sent = 0
        fail = 0
        for uid in pending:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=(
                        "Напоминание: пожалуйста, оставь обратную связь по текущему опросу.\n"
                        f'Вопрос: "{q_short}"\n\n'
                        "/rate 1-5 и/или /feedback ваш комментарий"
                    ),
                )
                sent += 1
            except Exception as e:
                logger.warning("Не удалось отправить напоминание %s: %s", uid, e)
                fail += 1

        await update.message.reply_text(
            f"Напоминания отправлены: {sent}. Не доставлено: {fail}."
        )
    except Exception as e:
        logger.exception("Ошибка в /remind: %s", e)
        await update.message.reply_text("Ошибка при рассылке напоминаний.")


async def post_init(application: Application) -> None:
    """Инициализация БД при старте (вызывается вручную после initialize)."""
    await db_init()


def register_handlers(application: Application) -> None:
    """Регистрация всех команд и диалога /new_question."""
    conv = ConversationHandler(
        entry_points=[CommandHandler("new_question", cmd_new_question_entry)],
        states={
            STATE_QUESTION: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    new_question_receive_text,
                )
            ],
            STATE_PERIOD: [
                CommandHandler("skip", new_question_skip_period),
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    new_question_receive_period,
                ),
            ],
        },
        fallbacks=[CommandHandler("cancel", new_question_cancel)],
    )
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("rate", cmd_rate))
    application.add_handler(CommandHandler("feedback", cmd_feedback))
    application.add_handler(CommandHandler("report", cmd_report))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("remind", cmd_remind))
    application.add_handler(conv)


async def run_bot_async(application: Application) -> None:
    """
    Полный цикл: initialize → post_init → polling → start → ожидание → корректный shutdown.
    Порядок как внутри Application.run_polling / __run, но без get_event_loop().
    """
    await application.initialize()
    if application.post_init:
        await application.post_init(application)

    def polling_error_callback(exc: TelegramError) -> None:
        application.create_task(application.process_error(error=exc, update=None))

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def on_sigint(_signum: int, _frame: object) -> None:
        if loop.is_running():
            loop.call_soon_threadsafe(stop_event.set)

    old_sigint = None
    try:
        old_sigint = signal.signal(signal.SIGINT, on_sigint)
    except ValueError:
        # не главный поток — редко
        pass

    try:
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            error_callback=polling_error_callback,
        )
        await application.start()
        logger.info("Бот запущен. Остановка: Ctrl+C")
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Задача бота отменена")
        raise
    finally:
        if old_sigint is not None:
            try:
                signal.signal(signal.SIGINT, old_sigint)
            except ValueError:
                pass

        try:
            if application.updater.running:
                await application.updater.stop()
        except Exception as e:
            logger.warning("Ошибка при updater.stop: %s", e)

        try:
            if application.running:
                await application.stop()
        except Exception as e:
            logger.warning("Ошибка при application.stop: %s", e)

        if application.post_stop:
            try:
                await application.post_stop(application)
            except Exception as e:
                logger.warning("Ошибка в post_stop: %s", e)

        try:
            await application.shutdown()
        except Exception as e:
            logger.warning("Ошибка при shutdown: %s", e)

        if application.post_shutdown:
            try:
                await application.post_shutdown(application)
            except Exception as e:
                logger.warning("Ошибка в post_shutdown: %s", e)


def main() -> None:
    if not BOT_TOKEN:
        logger.error("Укажите BOT_TOKEN в файле .env")
        return
    if get_admin_id() is None:
        logger.error("Укажите корректный ADMIN_ID в файле .env")
        return

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )
    register_handlers(application)

    try:
        asyncio.run(run_bot_async(application))
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")


if __name__ == "__main__":
    main()
