#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import functools
import json
import logging
import time
from typing import List
from urllib.parse import urljoin

from telegram import Bot, Update
from telegram.ext import CallbackContext

from config import ADMIN_CHAT_ID, ADMIN_USERNAME, SITE_URLS, SITE_TZ, SNAPSHOT_FIELDS
from db import (
    _connect,
    add_subscriber,
    get_kv,
    get_subscribers_missing_username,
    is_bootstrapped,
    remove_subscriber,
    set_kv,
    update_username,
)
from broadcaster import (
    broadcast_cycle,
    compute_row_hash,
    _alert_admin,
)
from parser import fetch_page, fetch_entry_details, fill_entry_details, parse_entries


# =======================
# Админ-гард + error handler
# =======================

def _is_admin(update: Update) -> bool:
    u = update.effective_user
    if not u:
        return False
    if u.id == ADMIN_CHAT_ID:
        return True
    uname = (u.username or '').strip()
    if uname and uname.lower() == ADMIN_USERNAME.lower():
        return True
    return False


def _reply_safe(update: Update, text: str):
    msg = getattr(update, 'effective_message', None)
    if msg:
        try:
            msg.reply_text(text, parse_mode='HTML', protect_content=False)
        except Exception as e:
            logging.warning(f'_reply_safe: не удалось отправить ответ: {e}')


def admin_required(func):
    @functools.wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        try:
            if _is_admin(update):
                return func(update, context, *args, **kwargs)
            _reply_safe(update, '⛔ Команда недоступна.')
        except Exception:
            logging.exception('❌ Ошибка в admin_required')
        return None
    return wrapper


def error_handler(update: object, context: CallbackContext):
    logging.exception('Unhandled exception in handler', exc_info=context.error)
    try:
        if isinstance(update, Update):
            _reply_safe(update, '⚠️ Внутренняя ошибка обработчика. Смотрю логи.')
    except Exception:
        pass


# =======================
# Команды
# =======================

def start_cmd(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    uname = (update.effective_user.username or '') if update.effective_user else ''
    add_subscriber(chat_id, uname)
    _reply_safe(update, '✅ Вы подписаны на новые записи и обновления.')


def stop_cmd(update: Update, context: CallbackContext):
    remove_subscriber(update.effective_chat.id)
    _reply_safe(update, '👋 Вы отписались от уведомлений.')


def ping_cmd(update: Update, context: CallbackContext):
    _reply_safe(update, '🏓 Pong')


def help_cmd(update: Update, context: CallbackContext):
    _reply_safe(update, '📬 По вопросам и предложениям: striker12411@gmail.com')


@admin_required
def debug_cmd(update: Update, context: CallbackContext):
    base_url, html = fetch_page()
    if not html or not base_url:
        _reply_safe(update, '❌ Не удалось получить страницу ни с одного зеркала.')
        return
    try:
        entries = parse_entries(html, base_url)
        fill_entry_details(entries)
        if not entries:
            _reply_safe(update, 'Похоже, таблица пуста.')
            return
        msgs = []
        for e in entries[:5]:
            country = e.get('country', '') or '—'
            orig = e.get('orig_title', '') or '—'
            msgs.append(f"{e['raw_date']} | {e['type']} | {e['title']} ({orig}) | КП:{e['kp']} IMDb:{e['imdb']} | {country}")
        _reply_safe(update, 'Top 5 записей:\n' + '\n'.join(msgs))
    except Exception as e:
        _reply_safe(update, f'❌ Ошибка парсинга: {e}')


@admin_required
def stats_cmd(update: Update, context: CallbackContext):
    from config import MAX_ROWS_SCAN, MAX_SEND_PER_CYCLE
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM seen_entries')
        row = cur.fetchone()
        seen_count = row[0] if row else 0
        cur.execute('SELECT COUNT(*) FROM subscribers')
        row = cur.fetchone()
        subs_count = row[0] if row else 0
        cur.execute("SELECT COUNT(*) FROM subscribers WHERE username IS NOT NULL AND username <> ''")
        row = cur.fetchone()
        subs_with_username = row[0] if row else 0
    finally:
        conn.close()
    from datetime import datetime
    bs = 'да' if is_bootstrapped() else 'нет'
    last_scan_ts = get_kv('last_scan_ts')
    fail_count = get_kv('scan_fail_count') or '0'
    active_mirror = get_kv('active_base_url') or SITE_URLS[0]
    if last_scan_ts:
        last_scan_str = datetime.fromtimestamp(int(last_scan_ts), SITE_TZ).strftime('%Y-%m-%d %H:%M:%S')
    else:
        last_scan_str = 'никогда'
    _reply_safe(update,
        f"📊 <b>Статистика</b>\n"
        f"— Виденных записей: {seen_count}\n"
        f"— Подписчиков: {subs_count} (с username: {subs_with_username})\n"
        f"— Bootstrap: {bs}\n"
        f"— Последний скан: {last_scan_str}\n"
        f"— Сбоев подряд: {fail_count}\n"
        f"— Зеркало: {active_mirror}\n"
        f"— Лимит за цикл: {MAX_SEND_PER_CYCLE}, строк: до {MAX_ROWS_SCAN}"
    )


@admin_required
def backfill_cmd(update: Update, context: CallbackContext):
    cnt = backfill_usernames(context.bot)
    _reply_safe(update, f'🔄 Бэкофилл username завершён. Обновлено записей: {cnt}.')


@admin_required
def scan_cmd(update: Update, context: CallbackContext):
    try:
        broadcast_cycle(context.bot)
        _reply_safe(update, '🧪 Принудительный скан выполнен.')
    except Exception as e:
        logging.exception('Ошибка в принудительном скане')
        _reply_safe(update, f'❌ Ошибка в скане: {e}')


@admin_required
def reset_bootstrap_cmd(update: Update, context: CallbackContext):
    set_kv('bootstrapped', '0')
    _reply_safe(update, '🔁 Флаг bootstrap сброшен.')


@admin_required
def mirror_cmd(update: Update, context: CallbackContext):
    """Показать или установить активное зеркало вручную. /mirror [url]"""
    args = context.args
    if args:
        new_url = args[0].strip()
        if not new_url.endswith('/'):
            new_url += '/'
        set_kv('active_base_url', new_url)
        _reply_safe(update, f'🔗 Зеркало установлено: <code>{new_url}</code>')
    else:
        active = get_kv('active_base_url') or '(не определено)'
        configured = '\n'.join(f'  • {u}' for u in SITE_URLS)
        _reply_safe(update,
            f'🔗 <b>Активное зеркало:</b> <code>{active}</code>\n'
            f'📋 <b>Настроенные в коде:</b>\n{configured}'
        )


@admin_required
def country_backfill_cmd(update: Update, context: CallbackContext):
    """
    Однократная дозаправка country для уже известных записей.
    Без уведомлений, просто обновляем snapshot/last_hash, если нашли страну.
    """
    from db import _active_base_url

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT link, last_hash, snapshot FROM seen_entries')
        rows = cur.fetchall()
    except Exception as e:
        conn.close()
        _reply_safe(update, f'❌ Ошибка чтения БД: {e}')
        return

    COMMIT_EVERY = 50
    checked = 0
    updated = 0
    for link, last_hash, snap_txt in rows:
        checked += 1
        try:
            snap = json.loads(snap_txt) if snap_txt else {}
        except (json.JSONDecodeError, ValueError):
            snap = {}

        curr_country = (snap or {}).get('country', '').strip()
        curr_orig = (snap or {}).get('orig_title', '').strip()
        if curr_country and curr_orig:
            continue
        if not link or link.startswith('nolink://'):
            continue

        # Ключ может быть rel-путём после миграции — строим abs URL
        abs_link = link if link.startswith('http') else urljoin(_active_base_url(), link)
        details = fetch_entry_details(abs_link)
        changed = False
        if not curr_country and details['country']:
            snap['country'] = details['country']
            changed = True
        if not curr_orig and details['orig_title']:
            snap['orig_title'] = details['orig_title']
            changed = True
        if not changed:
            continue
        entry_like = {k: snap.get(k, '') for k in SNAPSHOT_FIELDS}
        new_hash = compute_row_hash(entry_like)
        now_ts = int(time.time())
        snap_txt_new = json.dumps(snap, ensure_ascii=False, separators=(',', ':'))

        try:
            cur.execute(
                'UPDATE seen_entries SET last_hash = ?, last_seen_ts = ?, snapshot = ? WHERE link = ?',
                (new_hash, now_ts, snap_txt_new, link)
            )
            updated += 1
        except Exception as e:
            logging.warning(f'country_backfill: ошибка обновления {link}: {e}')

        if updated % COMMIT_EVERY == 0:
            conn.commit()

        time.sleep(0.02)

    conn.commit()
    conn.close()
    _reply_safe(update, f'🧩 Country backfill: обработано {checked}, обновлено {updated} записей (без уведомлений).')


# =======================
# Бэкофилл username (вспом.)
# =======================

def backfill_usernames(bot: Bot) -> int:
    missing = get_subscribers_missing_username()
    if not missing:
        logging.info('Бэкофилл username: нет пустых записей.')
        return 0

    updated = 0
    for chat_id in missing:
        try:
            chat = bot.get_chat(chat_id=chat_id)
            uname = getattr(chat, 'username', None)
            if uname:
                update_username(chat_id, uname)
                updated += 1
                logging.info(f'Бэкофилл: chat_id={chat_id} -> @{uname}')
            else:
                logging.info(f'Бэкофилл: chat_id={chat_id} без username — пропуск')
        except Exception as e:
            logging.warning(f'Бэкофилл: не удалось получить chat {chat_id}: {e}')
        time.sleep(0.02)
    logging.info(f'Бэкофилл завершён, обновлено: {updated}')
    return updated
