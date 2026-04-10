"""
Бот сбора анонимной обратной связи команды.
python-telegram-bot 20.x (async-хендлеры), SQLite.

Запуск: в конце файла только webhook (ngrok URL обязателен).
"""
from __future__ import annotations

import os
import logging
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Раньше было:
# BOT_TOKEN = "123456789:ABCdefGHIjklMNO"

# Теперь:
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в .env файле")

import asyncio
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

# Шаги диалога /new_question (только админ)
STATE_QUESTION, STATE_PERIOD = range(2)

DB_PATH = Path(__file__).resolve().parent / "feedback.db"
EMPLOYEES_CSV_PATH = Path(__file__).resolve().parent / "data.csv"

# Поддерживаемые города и их координаты для Open-Meteo
SUPPORTED_CITIES = {
    "москва": {"name": "Москва", "lat": 55.7558, "lon": 37.6173},
    "санкт-петербург": {"name": "Санкт-Петербург", "lat": 59.9311, "lon": 30.3609},
    "казань": {"name": "Казань", "lat": 55.7887, "lon": 49.1221},
    "новосибирск": {"name": "Новосибирск", "lat": 55.0084, "lon": 82.9357},
    "екатеринбург": {"name": "Екатеринбург", "lat": 56.8389, "lon": 60.6057},
}


def weather_code_to_text(code: int) -> str:
    """Преобразует код погоды Open-Meteo в человекочитаемый текст."""
    mapping = {
        0: "Ясно",
        1: "Преимущественно ясно",
        2: "Переменная облачность",
        3: "Пасмурно",
        45: "Туман",
        48: "Туман с инеем",
        51: "Лёгкая морось",
        53: "Морось",
        55: "Сильная морось",
        56: "Лёгкая ледяная морось",
        57: "Сильная ледяная морось",
        61: "Небольшой дождь",
        63: "Дождь",
        65: "Сильный дождь",
        66: "Лёгкий ледяной дождь",
        67: "Сильный ледяной дождь",
        71: "Небольшой снег",
        73: "Снег",
        75: "Сильный снег",
        77: "Снежные зёрна",
        80: "Кратковременный дождь",
        81: "Ливень",
        82: "Сильный ливень",
        85: "Слабый снегопад",
        86: "Сильный снегопад",
        95: "Гроза",
        96: "Гроза с небольшим градом",
        99: "Гроза с сильным градом",
    }
    return mapping.get(code, "Погодные условия уточняются")


def available_cities_text() -> str:
    cities = [city["name"] for city in SUPPORTED_CITIES.values()]
    return ", ".join(cities)


def _load_employees_sync() -> Optional[pd.DataFrame]:
    """Читает CSV сотрудников с проверкой наличия обязательных колонок."""
    if not EMPLOYEES_CSV_PATH.exists():
        return None
    df = pd.read_csv(EMPLOYEES_CSV_PATH, encoding="utf-8")
    required_columns = {"name", "department", "role", "email", "city"}
    if not required_columns.issubset(set(df.columns)):
        missing = required_columns.difference(set(df.columns))
        raise ValueError(f"В data.csv отсутствуют колонки: {', '.join(sorted(missing))}")
    return df


async def load_employees() -> Optional[pd.DataFrame]:
    return await asyncio.to_thread(_load_employees_sync)


def _fetch_weather_sync(lat: float, lon: float) -> dict:
    """Запрашивает текущую погоду из Open-Meteo."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": "auto",
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    payload = response.json()
    current = payload.get("current")
    if not current:
        raise ValueError("Ответ API не содержит текущие данные")
    return current


async def fetch_weather(lat: float, lon: float) -> dict:
    return await asyncio.to_thread(_fetch_weather_sync, lat, lon)


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


def _delete_rating_sync(user_id: int, survey_id: int) -> bool:
    """Удаляет только оценку пользователя по опросу (feedback не трогает)."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """
            SELECT id, rating, feedback_text FROM responses
            WHERE user_id = ? AND survey_id = ?
            """,
            (user_id, survey_id),
        )
        row = cur.fetchone()
        if not row or row["rating"] is None:
            return False

        # Если в строке нет текстового отзыва, удаляем запись целиком.
        if not row["feedback_text"] or not str(row["feedback_text"]).strip():
            conn.execute("DELETE FROM responses WHERE id = ?", (row["id"],))
        else:
            conn.execute(
                "UPDATE responses SET rating = NULL WHERE id = ?",
                (row["id"],),
            )
        conn.commit()
        return True
    finally:
        conn.close()


async def delete_rating(user_id: int, survey_id: int) -> bool:
    return await asyncio.to_thread(_delete_rating_sync, user_id, survey_id)


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
            "👋 Привет! Я бот для сбора обратной связи. "
            "Оценки и отзывы по текущему вопросу видит только руководитель в отчёте; "
            "в сводке имена не показываются.\n\n"
            "⭐ /rate 1-5 — оценка\n"
            "🗑️ /delete_feedback — удалить свою оценку\n"
            "💬 /feedback текст — отзыв\n"
            "🧾 /employees — список сотрудников\n"
            "🔎 /search [имя] — поиск сотрудника\n"
            "🏢 /department [отдел] — сотрудники отдела\n"
            "📊 /stats — статистика по отделам\n"
            "🌍 /weather [город] — погода\n"
            "ℹ️ /help — все команды\n"
            "Для руководителя дополнительно: /new_question, /report, /remind"
        )
    except Exception as e:
        logger.exception("Ошибка в /start: %s", e)
        await update.message.reply_text(
            "❌ Не удалось зарегистрироваться. Попробуйте позже."
        )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lines = [
        "📘 Доступные команды:",
        "👋 /start — приветствие и регистрация",
        "⭐ /rate 1-5 — оставить оценку",
        "🗑️ /delete_feedback — удалить свою оценку",
        "💬 /feedback [текст] — оставить отзыв",
        "🧾 /employees — показать всех сотрудников",
        "🔎 /search [имя] — поиск сотрудника по имени",
        "🏢 /department [отдел] — сотрудники из отдела (IT, HR, Sales, Marketing)",
        "📊 /stats — статистика сотрудников по отделам",
        "🌍 /weather [город] — текущая погода",
        "",
        "👑 Команды руководителя:",
        "🆕 /new_question — создать новый опрос",
        "🧮 /report — отчёт по активному опросу",
        "🔔 /remind — напомнить тем, кто не ответил",
    ]
    if update.effective_user and is_admin(update.effective_user.id):
        lines.append("📈 /survey_stats — статистика по всем опросам")
    await update.message.reply_text("\n".join(lines))


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


async def cmd_delete_feedback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Удаляет оценку пользователя по активному опросу."""
    if not update.effective_user or not update.message:
        return
    try:
        survey = await get_active_survey()
        if not survey:
            await update.message.reply_text("Сейчас нет активного опроса.")
            return

        deleted = await delete_rating(update.effective_user.id, survey["id"])
        if deleted:
            await update.message.reply_text("🗑️ Ваша оценка удалена.")
        else:
            await update.message.reply_text("ℹ️ У вас нет сохранённой оценки для удаления.")
    except Exception as e:
        logger.exception("Ошибка в /delete_feedback: %s", e)
        await update.message.reply_text("❌ Ошибка при удалении оценки.")


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
    """Статистика по сотрудникам: количество по отделам."""
    if not update.message:
        return
    try:
        df = await load_employees()
        if df is None:
            await update.message.reply_text(
                "⚠️ Файл `data.csv` не найден.\n"
                "Пожалуйста, добавьте файл в корень проекта."
            )
            return
        if df.empty:
            await update.message.reply_text("📭 Файл сотрудников пуст.")
            return

        dep_stats = df["department"].astype(str).str.strip().value_counts()
        lines = ["📊 Статистика по отделам:"]
        for dep, count in dep_stats.items():
            lines.append(f"🏢 {dep}: {int(count)}")
        lines.append(f"\n👥 Всего сотрудников: {len(df)}")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.exception("Ошибка в /stats (employees): %s", e)
        await update.message.reply_text(
            "❌ Не удалось получить статистику по сотрудникам."
        )


async def cmd_survey_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Старая админская статистика по всем опросам."""
    if not update.effective_user or not update.message:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("🚫 Команда доступна только руководителю.")
        return
    try:
        rows = await all_surveys_stats()
        if not rows:
            await update.message.reply_text("📭 Опросов ещё не было.")
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
        logger.exception("Ошибка в /survey_stats: %s", e)
        await update.message.reply_text("❌ Ошибка при получении статистики по опросам.")


async def cmd_employees(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает список всех сотрудников из CSV."""
    if not update.message:
        return
    try:
        df = await load_employees()
        if df is None:
            await update.message.reply_text(
                "⚠️ Файл `data.csv` не найден.\n"
                "Пожалуйста, добавьте файл в корень проекта."
            )
            return
        if df.empty:
            await update.message.reply_text("📭 Список сотрудников пуст.")
            return

        lines = ["🧾 Список сотрудников:"]
        for _, row in df.iterrows():
            lines.append(
                f"👤 {row['name']}\n"
                f"   🏢 {row['department']} | 💼 {row['role']}\n"
                f"   ✉️ {row['email']} | 📍 {row['city']}"
            )
        await update.message.reply_text("\n\n".join(lines))
    except Exception as e:
        logger.exception("Ошибка в /employees: %s", e)
        await update.message.reply_text("❌ Не удалось прочитать список сотрудников.")


async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ищет сотрудника по частичному совпадению имени."""
    if not update.message:
        return
    query = " ".join(context.args or []).strip()
    if not query:
        await update.message.reply_text("ℹ️ Использование: /search [имя]")
        return
    try:
        df = await load_employees()
        if df is None:
            await update.message.reply_text(
                "⚠️ Файл `data.csv` не найден.\n"
                "Пожалуйста, добавьте файл в корень проекта."
            )
            return
        if df.empty:
            await update.message.reply_text("📭 Список сотрудников пуст.")
            return

        mask = (
            df["name"]
            .astype(str)
            .str.contains(query, case=False, na=False)
        )
        matched = df[mask]
        if matched.empty:
            await update.message.reply_text(
                f"🔍 По запросу «{query}» сотрудников не найдено."
            )
            return

        lines = [f"🔍 Найдено сотрудников: {len(matched)}"]
        for _, row in matched.iterrows():
            lines.append(
                f"👤 {row['name']}\n"
                f"   🏢 {row['department']} | 💼 {row['role']}\n"
                f"   ✉️ {row['email']} | 📍 {row['city']}"
            )
        await update.message.reply_text("\n\n".join(lines))
    except Exception as e:
        logger.exception("Ошибка в /search: %s", e)
        await update.message.reply_text("❌ Ошибка при поиске сотрудника.")


async def cmd_department(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает сотрудников выбранного отдела."""
    if not update.message:
        return
    department = " ".join(context.args or []).strip()
    if not department:
        await update.message.reply_text(
            "ℹ️ Использование: /department [отдел]\n"
            "Доступно: IT, HR, Sales, Marketing"
        )
        return
    try:
        df = await load_employees()
        if df is None:
            await update.message.reply_text(
                "⚠️ Файл `data.csv` не найден.\n"
                "Пожалуйста, добавьте файл в корень проекта."
            )
            return
        if df.empty:
            await update.message.reply_text("📭 Список сотрудников пуст.")
            return

        allowed_departments = {"it", "hr", "sales", "marketing"}
        normalized_dep = department.strip().lower()
        if normalized_dep not in allowed_departments:
            await update.message.reply_text(
                "🚫 Неизвестный отдел.\n"
                "Доступные отделы: IT, HR, Sales, Marketing"
            )
            return

        dep_mask = (
            df["department"]
            .astype(str)
            .str.strip()
            .str.lower()
            == normalized_dep
        )
        matched = df[dep_mask]
        if matched.empty:
            await update.message.reply_text(
                f"📭 В отделе {department} пока нет сотрудников."
            )
            return

        lines = [f"🏢 Сотрудники отдела {department.upper()} ({len(matched)}):"]
        for _, row in matched.iterrows():
            lines.append(
                f"👤 {row['name']} — 💼 {row['role']}\n"
                f"   ✉️ {row['email']} | 📍 {row['city']}"
            )
        await update.message.reply_text("\n\n".join(lines))
    except Exception as e:
        logger.exception("Ошибка в /department: %s", e)
        await update.message.reply_text("❌ Ошибка при фильтрации по отделу.")


async def cmd_weather(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает текущую погоду по поддерживаемому городу."""
    if not update.message:
        return
    city_raw = " ".join(context.args or []).strip()
    if not city_raw:
        await update.message.reply_text(
            "ℹ️ Использование: /weather [город]\n"
            f"🌆 Доступные города: {available_cities_text()}"
        )
        return

    city_key = city_raw.lower()
    city = SUPPORTED_CITIES.get(city_key)
    if not city:
        await update.message.reply_text(
            "🚫 Город не найден.\n"
            f"🌆 Доступные города: {available_cities_text()}"
        )
        return

    try:
        current = await fetch_weather(city["lat"], city["lon"])
        temp = current.get("temperature_2m")
        humidity = current.get("relative_humidity_2m")
        wind = current.get("wind_speed_10m")
        code = int(current.get("weather_code", -1))
        weather_text = weather_code_to_text(code)

        await update.message.reply_text(
            f"🌍 Погода в {city['name']}\n"
            f"🌡️ Температура: {temp}°C\n"
            f"💧 Влажность: {humidity}%\n"
            f"💨 Ветер: {wind} км/ч\n"
            f"☁️ {weather_text}"
        )
    except requests.RequestException as e:
        logger.exception("Ошибка запроса Open-Meteo в /weather: %s", e)
        await update.message.reply_text(
            "❌ Не удалось получить данные о погоде. Попробуйте позже."
        )
    except Exception as e:
        logger.exception("Ошибка обработки данных погоды в /weather: %s", e)
        await update.message.reply_text("❌ Ошибка при обработке данных о погоде.")


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
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("rate", cmd_rate))
    application.add_handler(CommandHandler("delete_feedback", cmd_delete_feedback))
    application.add_handler(CommandHandler("feedback", cmd_feedback))
    application.add_handler(CommandHandler("employees", cmd_employees))
    application.add_handler(CommandHandler("search", cmd_search))
    application.add_handler(CommandHandler("department", cmd_department))
    application.add_handler(CommandHandler("weather", cmd_weather))
    application.add_handler(CommandHandler("report", cmd_report))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("survey_stats", cmd_survey_stats))
    application.add_handler(CommandHandler("remind", cmd_remind))
    application.add_handler(conv)


if get_admin_id() is None:
    raise ValueError("Укажите корректный ADMIN_ID в файле .env")

application = (
    Application.builder()
    .token(BOT_TOKEN)
    .post_init(post_init)
    .build()
)
register_handlers(application)

if __name__ == '__main__':
    application.run_polling()
