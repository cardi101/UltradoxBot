#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import signal

from telegram import BotCommand
from telegram.ext import CommandHandler, Updater

from config import BOT_TOKEN, CHECK_INTERVAL_SECONDS, CLEANUP_INTERVAL_SECONDS, CONNECT_TIMEOUT, READ_TIMEOUT
from db import cleanup_old_entries, init_db
from broadcaster import broadcast_cycle
from handlers import (
    backfill_cmd,
    backfill_usernames,
    country_backfill_cmd,
    debug_cmd,
    error_handler,
    help_cmd,
    mirror_cmd,
    ping_cmd,
    reset_bootstrap_cmd,
    scan_cmd,
    start_cmd,
    stats_cmd,
    stop_cmd,
)


def main():
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

    init_db()

    updater = Updater(token=BOT_TOKEN, use_context=True,
                      request_kwargs={'read_timeout': READ_TIMEOUT, 'connect_timeout': CONNECT_TIMEOUT})
    dp = updater.dispatcher

    # Регистрация команд в меню Telegram
    updater.bot.set_my_commands([
        BotCommand('start', 'Подписаться на уведомления'),
        BotCommand('stop', 'Отписаться от уведомлений'),
        BotCommand('help', 'Помощь и контакты'),
        BotCommand('ping', 'Проверить работу бота'),
    ])

    # Публичные
    dp.add_handler(CommandHandler('start', start_cmd))
    dp.add_handler(CommandHandler('stop', stop_cmd))
    dp.add_handler(CommandHandler('ping', ping_cmd))
    dp.add_handler(CommandHandler('help', help_cmd))

    # 🔒 Админ
    dp.add_handler(CommandHandler('debug', debug_cmd))
    dp.add_handler(CommandHandler('stats', stats_cmd))
    dp.add_handler(CommandHandler('backfill', backfill_cmd))
    dp.add_handler(CommandHandler('scan', scan_cmd))
    dp.add_handler(CommandHandler('reset_bootstrap', reset_bootstrap_cmd))
    dp.add_handler(CommandHandler('country_backfill', country_backfill_cmd))
    dp.add_handler(CommandHandler('mirror', mirror_cmd))

    # Глобальный обработчик ошибок
    dp.add_error_handler(error_handler)

    # Graceful shutdown по SIGTERM / SIGINT
    def _shutdown(signum, frame):
        logging.info(f'Получен сигнал {signum}, останавливаем бот…')
        updater.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Разовый авто-бэкофилл username
    try:
        backfill_usernames(updater.bot)
    except Exception as e:
        logging.warning(f'Авто-бэкофилл на старте не удался: {e}')

    job_queue = updater.job_queue

    # Периодический скан (не перекрывается из-за SCANNER_LOCK)
    job_queue.run_repeating(lambda ctx: broadcast_cycle(ctx.bot),
                            interval=CHECK_INTERVAL_SECONDS,
                            first=0,
                            name='broadcast_cycle')

    # Еженедельная очистка старых записей
    job_queue.run_repeating(lambda ctx: cleanup_old_entries(),
                            interval=CLEANUP_INTERVAL_SECONDS,
                            first=CLEANUP_INTERVAL_SECONDS,
                            name='cleanup_old_entries')

    updater.start_polling()
    logging.info('Бот запущен и готов.')
    updater.idle()


if __name__ == '__main__':
    main()
