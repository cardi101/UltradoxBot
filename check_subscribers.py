#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Проверяет всех подписчиков из БД:
- Отправляет sendChatAction(typing) — не видно пользователю
- Если ошибка 403/400 (blocked / deactivated / not found) — удаляет из БД
- Выводит итоговую статистику и список удалённых
"""

import logging
import time
from typing import List, Optional, Tuple

import requests

from config import BOT_TOKEN
from db import get_subscribers, remove_subscriber


def _get_all_subscribers_with_usernames() -> List[Tuple[int, Optional[str]]]:
    """Возвращает (chat_id, username) для всех подписчиков."""
    import sqlite3
    from config import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=15)
    try:
        cur = conn.cursor()
        cur.execute('SELECT chat_id, username FROM subscribers ORDER BY chat_id')
        return cur.fetchall()
    finally:
        conn.close()


def check_subscriber(chat_id: int) -> Tuple[Optional[bool], str]:
    """
    Отправляет sendChatAction(typing).
    Возвращает (жив, причина): True — OK, False — мёртвый, None — сетевая ошибка.
    """
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendChatAction'
    try:
        r = requests.post(url, json={'chat_id': chat_id, 'action': 'typing'}, timeout=10)
        data = r.json()
        if data.get('ok'):
            return True, 'ok'
        err = data.get('description', '')
        code = data.get('error_code', 0)
        return False, f'{code}: {err}'
    except requests.RequestException as e:
        return None, f'network error: {e}'


_DEAD_KEYWORDS = ('blocked', 'deactivated', 'not found', 'chat not found', 'user not found', '403', '400')


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    subs = _get_all_subscribers_with_usernames()
    total = len(subs)
    logging.info(f'Всего подписчиков в БД: {total}')

    removed = []
    errors = []
    ok_count = 0

    for i, (chat_id, username) in enumerate(subs, 1):
        alive, reason = check_subscriber(chat_id)

        if alive is True:
            ok_count += 1
            status = 'OK'
        elif alive is False:
            if any(kw in reason.lower() for kw in _DEAD_KEYWORDS):
                remove_subscriber(chat_id)
                removed.append((chat_id, username, reason))
                status = f'УДАЛЁН ({reason})'
            else:
                errors.append((chat_id, username, reason))
                status = f'ОШИБКА ({reason})'
        else:
            errors.append((chat_id, username, reason))
            status = f'СЕТЬ ({reason})'

        uname = f'@{username}' if username else str(chat_id)
        print(f'[{i}/{total}] {uname}: {status}')
        time.sleep(0.05)  # не словить flood-limit (30 req/s у TG API)

    print('\n' + '=' * 50)
    print(f'Итого проверено : {total}')
    print(f'Активных        : {ok_count}')
    print(f'Удалено         : {len(removed)}')
    print(f'Сетевых ошибок  : {len(errors)}')

    if removed:
        print('\nУдалённые подписчики:')
        for chat_id, username, reason in removed:
            uname = f'@{username}' if username else str(chat_id)
            print(f'  {uname} ({chat_id}) — {reason}')

    if errors:
        print('\nОшибки (не удалены, требуют проверки):')
        for chat_id, username, reason in errors:
            uname = f'@{username}' if username else str(chat_id)
            print(f'  {uname} ({chat_id}) — {reason}')


if __name__ == '__main__':
    main()
