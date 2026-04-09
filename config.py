#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import threading

import pytz
import requests

# =======================
# Конфигурация
# =======================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_token() -> str:
    t = os.environ.get('BOT_TOKEN', '').strip()
    if not t:
        token_file = os.path.join(BASE_DIR, 'bot_token.txt')
        if os.path.isfile(token_file):
            with open(token_file, 'r', encoding='utf-8') as f:
                t = f.read().strip()
    if not t:
        raise RuntimeError('BOT_TOKEN не задан. Установите переменную окружения BOT_TOKEN '
                           'или положите файл bot_token.txt с токеном рядом со скриптом.')
    return t


BOT_TOKEN = _load_token()
DB_PATH = os.environ.get('DB_PATH', os.path.join(BASE_DIR, 'bot_data.db'))

# периодичность
CHECK_INTERVAL_SECONDS = 300

# зеркало
SITE_URLS = [
    'https://010.ultadox.space/',
]

# лимиты
MAX_ROWS_SCAN = 300
MAX_SEND_PER_CYCLE = 20
FIRST_RUN_SEND_LIMIT = 0

# алерты и очистка
ALERT_FAIL_THRESHOLD = 3   # сколько циклов подряд без ответа → алерт админу
EXPIRE_DAYS = 180           # записи старше N дней удаляются
CLEANUP_INTERVAL_SECONDS = 7 * 24 * 3600  # раз в неделю

# таймауты HTTP
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 10

SITE_TZ = pytz.timezone('Europe/Moscow')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (TelegramBotParser)'
}

# 🔒 Админ
ADMIN_CHAT_ID = int(os.environ.get('ADMIN_CHAT_ID', '443539115'))
ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'Cardinal_GriseX')

# Поля
FIELD_LABELS = {
    'date': 'Дата',
    'type': 'Тип',
    'title': 'Название',
    'orig_title': 'Оригинальное название',
    'size': 'Размер',
    'kp': 'Кинопоиск',
    'imdb': 'IMDb',
    'rel_link': 'Относительная ссылка',
    'country': 'Страна',
}
SNAPSHOT_FIELDS = ['date', 'type', 'title', 'size', 'kp', 'imdb', 'rel_link', 'country']
LEGACY_SNAPSHOT_FIELDS = ['date', 'type', 'title', 'size', 'kp', 'imdb', 'rel_link']

# =============== HTTP session (connection pool) ===============
_http = requests.Session()
_http.headers.update(HEADERS)

# =============== Анти-перекрытие сканера ===============
SCANNER_LOCK = threading.Lock()
