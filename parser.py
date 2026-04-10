#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config import (
    CONNECT_TIMEOUT,
    MAX_ROWS_SCAN,
    READ_TIMEOUT,
    SITE_TZ,
    SITE_URLS,
    _http,
)
from db import entry_key, get_kv, seen_get, set_kv


def fetch_page() -> Tuple[Optional[str], Optional[str]]:
    # Сначала пробуем последнее известное рабочее зеркало, затем остальные из списка
    stored = get_kv('active_base_url')
    urls_to_try: List[str] = []
    if stored:
        urls_to_try.append(stored)
    for u in SITE_URLS:
        if u not in urls_to_try:
            urls_to_try.append(u)

    for url in urls_to_try:
        try:
            r = _http.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
            if r.status_code == 200 and r.text:
                # Берём финальный URL после всех редиректов как новый base
                parsed = urlparse(r.url)
                base = f"{parsed.scheme}://{parsed.netloc}/"
                if base != stored:
                    set_kv('active_base_url', base)
                    logging.info(f'Активное зеркало обновлено: {stored!r} → {base!r}')
                    stored = base
                return base, r.text
        except requests.RequestException as e:
            logging.warning(f'Зеркало недоступно {url}: {e}')
            continue
    return None, None


def normalize_date(text: str) -> str:
    t = text.strip().lower()
    now = datetime.now(SITE_TZ)
    if t.startswith('сегодня'):
        d = now.date()
    elif t.startswith('вчера'):
        d = (now - timedelta(days=1)).date()
    else:
        d = None
        for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d-%m-%Y'):
            try:
                d = datetime.strptime(text.strip(), fmt).date()
                break
            except ValueError:
                pass
        if d is None:
            return text.strip()
    return d.isoformat()


def parse_entries(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if not table:
        raise ValueError('Таблица не найдена на странице')

    rows = table.find_all('tr')
    if not rows or len(rows) < 2:
        return []

    entries: List[Dict[str, str]] = []
    for idx, row in enumerate(rows[1:MAX_ROWS_SCAN+1], start=1):
        cols = row.find_all('td')
        if not cols:
            continue

        texts = [td.get_text(strip=True) for td in cols]
        link_tag = row.find('a', href=True)
        rel = link_tag['href'].strip() if link_tag else ''
        abs_link = urljoin(base_url, rel) if rel else ''

        date_text = texts[0] if len(texts) >= 1 else ''
        type_ = texts[1] if len(texts) >= 2 else ''
        title = texts[2] if len(texts) >= 3 else ''
        # Последние 3 колонки — size/kp/imdb — должны не перекрываться с первыми тремя
        size  = texts[-3] if len(texts) >= 6 else ''
        kp    = texts[-2] if len(texts) >= 5 else ''
        imdb  = texts[-1] if len(texts) >= 4 else ''

        norm_date = normalize_date(date_text)

        entry = {
            'date': norm_date,
            'raw_date': date_text,
            'type': type_,
            'title': title,
            'size': size,
            'kp': kp,
            'imdb': imdb,
            'link': abs_link,
            'rel_link': rel,
            'row_index': str(idx),
            'country': ''  # заполняется для новых
        }

        entries.append(entry)

    return entries


def fetch_entry_details(page_url: str) -> Dict[str, str]:
    """За один HTTP-запрос тянет страну и оригинальное название."""
    result: Dict[str, str] = {'country': '', 'orig_title': ''}
    if not page_url:
        return result
    try:
        r = _http.get(page_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
        if r.status_code != 200 or not r.text:
            return result
        soup = BeautifulSoup(r.text, 'html.parser')

        # Оригинальное название — div.orig_name
        orig_tag = soup.find(class_='orig_name')
        if orig_tag:
            result['orig_title'] = orig_tag.get_text(strip=True)

        # Страна — ищем в li элементах вида "Страна: ..."
        for li in soup.find_all('li'):
            txt = li.get_text(' ', strip=True)
            if txt.lower().startswith('страна:'):
                result['country'] = txt.split(':', 1)[1].strip()
                break

        # Fallback: li[itemprop="contributor"] со span «страна»
        if not result['country']:
            for li in soup.select('li[itemprop="contributor"]'):
                lbl = li.find('span')
                if lbl and 'страна' in lbl.get_text(strip=True).lower():
                    links = [a.get_text(strip=True) for a in li.find_all('a')]
                    if links:
                        result['country'] = ', '.join(t for t in links if t)
                    else:
                        lbl.extract()
                        result['country'] = ', '.join(t.strip() for t in li.get_text(' ', strip=True).split(',') if t.strip())
                    break

        # Fallback: label-тег со словом «страна»
        if not result['country']:
            label = soup.find(lambda tag: tag.name in ('span', 'b', 'strong')
                              and 'страна' in tag.get_text(strip=True).lower())
            if label and label.parent:
                parent = label.parent
                links = [a.get_text(strip=True) for a in parent.find_all('a')]
                if links:
                    result['country'] = ', '.join(t for t in links if t)
                else:
                    raw = ' '.join(parent.stripped_strings)
                    raw = raw.replace(label.get_text(strip=True), '').replace(':', '').strip()
                    if raw:
                        result['country'] = raw

    except requests.RequestException:
        pass
    return result


def fill_entry_details(entries: List[Dict[str, str]]):
    """Тянет страну и оригинальное название.
    Для новых записей — HTTP-запрос. Для уже известных — из снапшота,
    но если поле отсутствует (старый снапшот без orig_title) — тоже идёт на страницу."""
    _cache: Dict[str, Dict[str, str]] = {}  # дедуп HTTP на один вызов
    for e in entries:
        db_key = entry_key(e)
        seen = seen_get(db_key)
        link = (e.get('link') or '').strip()

        if seen is None:
            # Новая запись — тянем всё
            if link and link not in _cache:
                _cache[link] = fetch_entry_details(link)
            details = _cache.get(link, {'country': '', 'orig_title': ''})
            e['country'] = details['country']
            e['orig_title'] = details['orig_title']
        else:
            _, _, _, snap_before = seen
            snap_before = snap_before or {}
            e['country'] = snap_before.get('country', '') or ''
            e['orig_title'] = snap_before.get('orig_title', '') or ''

            # Если orig_title отсутствует в старом снапшоте — дозаполняем
            if not e['orig_title'] and link and link.startswith('http'):
                if link not in _cache:
                    _cache[link] = fetch_entry_details(link)
                e['orig_title'] = _cache[link].get('orig_title', '')
                # Если и страна пустая — заодно обновим
                if not e['country']:
                    e['country'] = _cache[link].get('country', '')
