# UltradoxBot

Telegram-бот для мониторинга новинок и обновлений на сайте [ultadox.space](https://010.ultadox.space/).

Бот каждые 5 минут парсит таблицу с фильмами и сериалами, определяет новые записи и изменения в существующих, и рассылает уведомления всем подписчикам.

## Возможности

- Автоматический мониторинг сайта каждые 5 минут
- Уведомления о новинках и обновлениях (изменение размера, рейтинга и т.д.)
- Поддержка нескольких зеркал с автоматическим переключением
- Хранение данных в SQLite
- Авто-удаление заблокировавших бота из базы подписчиков
- Еженедельная очистка устаревших записей

## Команды

| Команда | Описание |
|---------|----------|
| `/start` | Подписаться на уведомления |
| `/stop` | Отписаться от уведомлений |
| `/help` | Помощь и контакты |
| `/ping` | Проверить работу бота |

## Запуск через Docker

### 1. Клонировать репозиторий

```bash
git clone https://github.com/cardi101/UltradoxBot.git
cd UltradoxBot
```

### 2. Создать `.env` файл

```bash
cp .env.example .env
```

Заполнить `.env`:

```env
BOT_TOKEN=your_token_here
ADMIN_CHAT_ID=123456789
ADMIN_USERNAME=your_username
```

### 3. Запустить

```bash
docker compose up -d
```

База данных хранится в папке `data/` — создаётся автоматически.

### Логи

```bash
docker compose logs -f
```

## Структура проекта

```
├── main.py          # Точка входа
├── config.py        # Конфигурация и константы
├── db.py            # Работа с базой данных (SQLite)
├── parser.py        # Парсинг сайта (requests + BeautifulSoup)
├── broadcaster.py   # Рассылка уведомлений
├── handlers.py      # Обработчики команд Telegram
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Стек

- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) 13.15
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/) — парсинг HTML
- [APScheduler](https://apscheduler.readthedocs.io/) — планировщик задач
- SQLite — хранение данных
- Docker — деплой
