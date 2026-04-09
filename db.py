#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import sqlite3
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from config import DB_PATH, EXPIRE_DAYS


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None, check_same_thread=False)
    conn.execute('PRAGMA journal_mode = WAL;')
    conn.execute('PRAGMA synchronous = NORMAL;')
    conn.execute('PRAGMA busy_timeout = 5000;')
    conn.execute('PRAGMA wal_autocheckpoint = 500;')
    return conn


def init_db():
    conn = _connect()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS subscribers(
            chat_id INTEGER PRIMARY KEY,
            username TEXT
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS seen_entries(
            link TEXT PRIMARY KEY,
            last_hash TEXT NOT NULL,
            first_seen_ts INTEGER NOT NULL,
            last_seen_ts INTEGER NOT NULL,
            snapshot TEXT DEFAULT ""
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS kv(
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );
    ''')
    conn.commit()
    try:
        cur.execute('PRAGMA wal_checkpoint(TRUNCATE);')
        conn.commit()
    except Exception:
        pass
    conn.close()
    migrate_db()


def migrate_db():
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute('PRAGMA table_info(seen_entries)')
        cols_seen = [r[1] for r in cur.fetchall()]
        if 'snapshot' not in cols_seen:
            cur.execute('ALTER TABLE seen_entries ADD COLUMN snapshot TEXT DEFAULT ""')
            conn.commit()
        cur.execute('PRAGMA table_info(subscribers)')
        cols_subs = [r[1] for r in cur.fetchall()]
        if 'username' not in cols_subs:
            cur.execute('ALTER TABLE subscribers ADD COLUMN username TEXT')
            conn.commit()

        # Нормализуем ключи: абсолютные URL → относительные пути.
        # Это устраняет повторную рассылку при смене зеркала.
        cur.execute("SELECT link FROM seen_entries WHERE link LIKE 'http%'")
        abs_rows = cur.fetchall()
        for (abs_link,) in abs_rows:
            path = urlparse(abs_link).path
            if not path:
                continue
            cur.execute("SELECT 1 FROM seen_entries WHERE link = ?", (path,))
            if cur.fetchone():
                # rel-ключ уже есть — удаляем дубликат с abs-ключом
                cur.execute("DELETE FROM seen_entries WHERE link = ?", (abs_link,))
            else:
                cur.execute("UPDATE seen_entries SET link = ? WHERE link = ?", (path, abs_link))
        if abs_rows:
            conn.commit()
            logging.info(f'migrate_db: нормализовано {len(abs_rows)} абсолютных ссылок → относительные пути')
    finally:
        conn.close()


def get_kv(key: str) -> Optional[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT v FROM kv WHERE k = ?', (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_kv(key: str, value: str):
    conn = _connect()
    cur = conn.cursor()
    cur.execute('INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v', (key, value))
    conn.commit()
    conn.close()


def get_subscribers() -> List[int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT chat_id FROM subscribers')
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_subscribers_missing_username() -> List[int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM subscribers WHERE username IS NULL OR username = ''")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def add_subscriber(chat_id: int, username: str):
    conn = _connect()
    try:
        conn.execute('''
            INSERT INTO subscribers(chat_id, username)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET username = excluded.username
        ''', (chat_id, username))
        conn.commit()
    finally:
        conn.close()


def update_username(chat_id: int, username: str):
    conn = _connect()
    try:
        conn.execute('UPDATE subscribers SET username = ? WHERE chat_id = ?', (username, chat_id))
        conn.commit()
    finally:
        conn.close()


def remove_subscriber(chat_id: int):
    conn = _connect()
    try:
        conn.execute('DELETE FROM subscribers WHERE chat_id = ?', (chat_id,))
        conn.commit()
    finally:
        conn.close()


def seen_get(link: str) -> Optional[Tuple[str, int, int, Dict[str, str]]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT last_hash, first_seen_ts, last_seen_ts, snapshot FROM seen_entries WHERE link = ?', (link,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    last_hash, first_ts, last_ts, snap_txt = row
    snap = {}
    if snap_txt:
        try:
            snap = json.loads(snap_txt)
        except (json.JSONDecodeError, ValueError):
            snap = {}
    return last_hash, first_ts, last_ts, snap


def seen_upsert(link: str, row_hash: str, ts: int, snapshot: Dict[str, str]):
    snap_txt = json.dumps(snapshot, ensure_ascii=False, separators=(',', ':'))
    conn = _connect()
    try:
        conn.execute('''
            INSERT INTO seen_entries(link, last_hash, first_seen_ts, last_seen_ts, snapshot)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(link) DO UPDATE SET last_hash=excluded.last_hash,
                                            last_seen_ts=excluded.last_seen_ts,
                                            snapshot=excluded.snapshot
        ''', (link, row_hash, ts, ts, snap_txt))
        conn.commit()
    finally:
        conn.close()


def seen_touch(link: str, ts: int):
    conn = _connect()
    try:
        conn.execute('UPDATE seen_entries SET last_seen_ts = ? WHERE link = ?', (ts, link))
        conn.commit()
    finally:
        conn.close()


def is_bootstrapped() -> bool:
    return get_kv('bootstrapped') == '1'


def mark_bootstrapped():
    set_kv('bootstrapped', '1')


def _active_base_url() -> str:
    """Возвращает текущее рабочее зеркало из БД или первый из списка."""
    from config import SITE_URLS
    return get_kv('active_base_url') or SITE_URLS[0]


def cleanup_old_entries():
    """Удаляет записи, не виденные дольше EXPIRE_DAYS дней."""
    cutoff = int(time.time()) - EXPIRE_DAYS * 86400
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute('DELETE FROM seen_entries WHERE last_seen_ts < ?', (cutoff,))
        deleted = cur.rowcount
        conn.commit()
        if deleted:
            logging.info(f'Cleanup: удалено {deleted} устаревших записей (старше {EXPIRE_DAYS} дней)')
    finally:
        conn.close()
