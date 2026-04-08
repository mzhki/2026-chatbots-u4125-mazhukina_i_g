# Бот обратной связи (python-telegram-bot 20.x / async)

Сбор оценок (1–5) и текстовых отзывов по текущему опросу. Сводки и напоминания — у руководителя. Данные в SQLite.

Запросы к Telegram идут на официальный [Bot API](https://api.telegram.org) по HTTPS. При системном VPN и доступном `api.telegram.org` дополнительный прокси в коде не нужен.

## Требования

- Python 3.10+
- Бот от [@BotFather](https://t.me/BotFather)

## Установка и запуск

```bash
cd telegram-feedback-bot
python -m venv .venv
```

**Windows (PowerShell):**

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Отредактируйте `.env`: `BOT_TOKEN`, `ADMIN_ID`.

```bash
python bot.py
```

Остановка: `Ctrl+C`. При первом запуске создаётся `feedback.db`.

Запуск в `bot.py` сделан через **`asyncio.run()`**: на **Python 3.14** у потока нет неявного event loop, поэтому `application.run_polling()` из библиотеки может выдать `RuntimeError: There is no current event loop`. В коде используется ручная последовательность `initialize` → `post_init` → `updater.start_polling` → `start` и корректный shutdown.

## Команды

| Команда | Кто | Описание |
|--------|-----|----------|
| `/start` | все | Регистрация, приветствие |
| `/rate 1-5` | все | Оценка по активному опросу |
| `/feedback текст` | все | Текстовый отзыв |
| `/new_question` | руководитель | Новый опрос (вопрос → даты или `/skip`) |
| `/report` | руководитель | Отчёт по активному опросу |
| `/stats` | руководитель | Статистика по всем опросам |
| `/remind` | руководитель | Напоминание тем, кто не ответил |

Напоминания получают только пользователи, которые хотя бы раз написали `/start`.

## Версия библиотеки

В задании указана **20.7**: на Python **до 3.14** `requirements.txt` ставит именно её. На **Python 3.14+** официальный пакет **20.7 падает** при создании `Application` (известная проблема с `__slots__` в `Updater`); для таких версий pip автоматически установит **21.x** — синтаксис async-хендлеров тот же. Альтернатива: Python 3.12–3.13 и строго `20.7`. Документация: [python-telegram-bot](https://docs.python-telegram-bot.org/).

## Таблицы SQLite

- `users` — `user_id`, `username`, `name`, `role`
- `surveys` — опросы, поле `active` у текущего
- `responses` — одна строка на пару пользователь + опрос
