#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import logging
import time
from typing import Dict, List, Optional, Tuple

from telegram import Bot

from config import (
    ADMIN_CHAT_ID,
    ALERT_FAIL_THRESHOLD,
    FIELD_LABELS,
    FIRST_RUN_SEND_LIMIT,
    LEGACY_SNAPSHOT_FIELDS,
    MAX_SEND_PER_CYCLE,
    SCANNER_LOCK,
    SNAPSHOT_FIELDS,
)
from db import (
    entry_key,
    get_kv,
    get_subscribers,
    is_bootstrapped,
    mark_bootstrapped,
    remove_subscriber,
    seen_get,
    seen_touch,
    seen_upsert,
    set_kv,
    wal_checkpoint,
)
from parser import fetch_page, fill_entry_details, parse_entries


# =======================
# Вспомогательные функции
# =======================

_DEAD_SUBSCRIBER_ERRORS = ('bot was blocked', 'user is deactivated', 'chat not found')


def _is_dead_subscriber(error_text: str) -> bool:
    return any(kw in error_text for kw in _DEAD_SUBSCRIBER_ERRORS)


def compute_row_hash(entry: Dict[str, str]) -> str:
    h = hashlib.sha1()
    parts = [entry.get(k, '') for k in SNAPSHOT_FIELDS]
    h.update('||'.join(parts).encode('utf-8', errors='ignore'))
    return h.hexdigest()


def compute_row_hash_legacy(entry: Dict[str, str]) -> str:
    h = hashlib.sha1()
    parts = [entry.get(k, '') for k in LEGACY_SNAPSHOT_FIELDS]
    h.update('||'.join(parts).encode('utf-8', errors='ignore'))
    return h.hexdigest()


def build_snapshot(entry: Dict[str, str]) -> Dict[str, str]:
    snap = {k: entry.get(k, '') for k in SNAPSHOT_FIELDS}
    snap['orig_title'] = entry.get('orig_title', '')
    return snap


def compute_diffs(old: Dict[str, str], new: Dict[str, str]) -> List[Tuple[str, str, str]]:
    diffs: List[Tuple[str, str, str]] = []
    for k in SNAPSHOT_FIELDS:
        ov = str(old.get(k, '') or '')
        nv = str(new.get(k, '') or '')
        if ov != nv:
            diffs.append((k, ov, nv))
    return diffs


# =======================
# Форматирование сообщений
# =======================

def _entry_header(e: Dict[str, str]) -> str:
    """Заголовок записи: название + оригинальное название (если есть)."""
    title = e.get('title', '').strip()
    orig = e.get('orig_title', '').strip()
    if orig and orig.lower() != title.lower():
        return f"🎬 <b>{title}</b>\n    <i>{orig}</i>"
    return f"🎬 <b>{title}</b>"


def _entry_meta(e: Dict[str, str]) -> str:
    """Строки с метаданными: тип, страна, дата, размер, рейтинги."""
    country = e.get('country', '').strip() or '—'
    kp = e.get('kp', '').strip()
    imdb = e.get('imdb', '').strip()
    size = e.get('size', '').strip()
    lines = [
        f"🎞 {e.get('type', '')}  ·  🌍 {country}",
        f"📅 {e.get('date', '')}",
    ]
    if size:
        lines[-1] += f"  ·  💾 {size}"
    ratings = []
    if kp:
        ratings.append(f"КП <code>{kp}</code>")
    if imdb:
        ratings.append(f"IMDb <code>{imdb}</code>")
    if ratings:
        lines.append("⭐ " + "  ·  ".join(ratings))
    return '\n'.join(lines)


def _russia_warning(e: Dict[str, str]) -> Optional[str]:
    country = e.get('country', '').lower()
    if 'россия' in country or 'russia' in country:
        return "🚫❌🚫❌🚫  <b>РОССИЯ</b>  🚫❌🚫❌🚫"
    return None


def format_new_message(e: Dict[str, str]) -> str:
    parts = ["📢 <b>Новинка</b>"]
    warn = _russia_warning(e)
    if warn:
        parts += ["", warn]
    parts += [
        "",
        _entry_header(e),
        "",
        _entry_meta(e),
        "",
        f"🔗 {e.get('link', '')}",
    ]
    return '\n'.join(parts)


def format_update_message(e: Dict[str, str], diffs: List[Tuple[str, str, str]]) -> str:
    parts = ["♻️ <b>Обновление</b>"]
    warn = _russia_warning(e)
    if warn:
        parts += ["", warn]
    parts += [
        "",
        _entry_header(e),
        "",
        _entry_meta(e),
    ]

    if diffs:
        parts += ["", "📝 <b>Изменения:</b>"]
        for field, ov, nv in diffs:
            label = FIELD_LABELS.get(field, field)
            ov_cut = (ov[:300] + '…') if len(ov) > 300 else ov
            nv_cut = (nv[:300] + '…') if len(nv) > 300 else nv
            parts.append(f"  • <b>{label}:</b>  {ov_cut}  →  {nv_cut}")

    parts += ["", f"🔗 {e.get('link', '')}"]
    return '\n'.join(parts)


# =======================
# Детекция изменений
# =======================

def detect_changes(entries: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Tuple[Dict[str, str], List[Tuple[str, str, str]]]]]:
    new_items: List[Dict[str, str]] = []
    upd_items: List[Tuple[Dict[str, str], List[Tuple[str, str, str]]]] = []

    ts = int(time.time())

    for e in entries:
        db_key = entry_key(e)
        # Если rel_link пустой — ставим синтетический link для отображения
        if not e.get('link'):
            e['link'] = 'nolink://' + hashlib.sha1(e.get('title', '').encode('utf-8')).hexdigest()

        snapshot_now = build_snapshot(e)
        row_hash_now = compute_row_hash(e)
        seen = seen_get(db_key)

        if seen is None:
            new_items.append(e)
            seen_upsert(db_key, row_hash_now, ts, snapshot_now)
        else:
            last_hash, first_ts, last_ts, snap_before = seen
            snap_before = snap_before or {}

            if 'country' not in snap_before:
                legacy_hash_now = compute_row_hash_legacy(e)
                if legacy_hash_now == last_hash:
                    # тихая миграция (без уведомления)
                    seen_upsert(db_key, row_hash_now, ts, snapshot_now)
                    continue

            if last_hash != row_hash_now:
                diffs_all = compute_diffs(snap_before, snapshot_now)
                seen_upsert(db_key, row_hash_now, ts, snapshot_now)
                diffs_wo_date = [d for d in diffs_all if d[0] != 'date']
                if diffs_wo_date:
                    upd_items.append((e, diffs_wo_date))
            else:
                seen_touch(db_key, ts)

    return new_items, upd_items


# =======================
# Рассылка (НЕ перекрывается)
# =======================

def _alert_admin(bot: Bot, text: str):
    try:
        bot.send_message(chat_id=ADMIN_CHAT_ID, text=text, parse_mode='HTML', protect_content=False)
    except Exception as e:
        logging.warning(f'Не удалось отправить алерт админу: {e}')


def broadcast_cycle(bot: Bot):
    if not SCANNER_LOCK.acquire(blocking=False):
        logging.warning('Сканер уже работает — пропуск цикла.')
        return
    try:
        logging.info('⏱ Старт цикла сканирования…')
        base_url, html = fetch_page()
        if not html or not base_url:
            logging.warning('Все зеркала недоступны.')
            fail_count = int(get_kv('scan_fail_count') or '0') + 1
            set_kv('scan_fail_count', str(fail_count))
            if fail_count == ALERT_FAIL_THRESHOLD:
                _alert_admin(bot, f'⚠️ <b>Сайт недоступен {fail_count} цикла подряд.</b>\nВсе зеркала не отвечают.')
            return

        try:
            entries = parse_entries(html, base_url)
        except Exception as e:
            logging.error(f'Ошибка парсинга: {e}')
            return

        if not entries:
            logging.info('Парсер: пусто.')
            return

        # Сброс счётчика сбоев и фиксация времени успешного скана
        prev_fail = int(get_kv('scan_fail_count') or '0')
        set_kv('scan_fail_count', '0')
        set_kv('last_scan_ts', str(int(time.time())))
        if prev_fail >= ALERT_FAIL_THRESHOLD:
            _alert_admin(bot, '✅ Сайт снова доступен.')

        # детали (страна, оригинальное название) — только для новых
        fill_entry_details(entries)

        # первый запуск: фиксируем всё без рассылки
        if not is_bootstrapped():
            ts = int(time.time())
            for e in entries:
                db_key = entry_key(e)
                seen_upsert(db_key, compute_row_hash(e), ts, build_snapshot(e))
            mark_bootstrapped()
            logging.info(f'Bootstrap: помечено {len(entries)} записей как виденные (без рассылки).')
            if FIRST_RUN_SEND_LIMIT > 0:
                subs = get_subscribers()
                for e in entries[:FIRST_RUN_SEND_LIMIT]:
                    text = format_new_message(e)
                    for chat_id in subs:
                        try:
                            bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML',
                                             disable_web_page_preview=True, protect_content=False)
                        except Exception as ex:
                            err = str(ex)
                            if _is_dead_subscriber(err):
                                remove_subscriber(chat_id)
                                logging.info(f'Подписчик {chat_id} удалён: {err}')
                            else:
                                logging.error(f'Не удалось отправить {chat_id}: {ex}')
            return

        new_items, upd_items = detect_changes(entries)

        queue: List[Tuple[str, Dict[str, str], Optional[List[Tuple[str, str, str]]]]] = []
        for e in new_items:
            queue.append(('new', e, None))
        for e, diffs in upd_items:
            queue.append(('upd', e, diffs))

        if not queue:
            logging.info('Изменений нет.')
            return

        queue = queue[:MAX_SEND_PER_CYCLE]
        subscribers = get_subscribers()
        if not subscribers:
            logging.info('Нет подписчиков, отправка пропущена.')
            return

        sent = 0
        for typ, e, diffs in queue:
            text = format_new_message(e) if typ == 'new' else format_update_message(e, diffs or [])
            for chat_id in subscribers:
                try:
                    bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML',
                                     disable_web_page_preview=True, protect_content=False)
                    time.sleep(0.03)
                    sent += 1
                except Exception as ex:
                    err = str(ex)
                    if _is_dead_subscriber(err):
                        remove_subscriber(chat_id)
                        logging.info(f'Подписчик {chat_id} удалён: {err}')
                    else:
                        logging.error(f'Не удалось отправить {chat_id}: {ex}')
        logging.info(f'Цикл завершён. Отправлено сообщений: {sent}')
    finally:
        wal_checkpoint()
        SCANNER_LOCK.release()
