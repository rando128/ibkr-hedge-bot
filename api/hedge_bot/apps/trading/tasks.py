"""
Procrastinate tasks for the simplified trading architecture (Option B).

These tasks are intentionally lightweight and only mutate DB state / create
events. The always-on TWS agent performs all IB/TWS side effects.
"""

from __future__ import annotations

import logging

from django.utils import timezone
from procrastinate.contrib.django import app

from .models import Bot, Event

logger = logging.getLogger(__name__)


@app.task(queue="monitor")
def request_start_bot(bot_id: int):
    bot = Bot.objects.get(pk=bot_id)
    if bot.status in ("RUNNING", "STOPPING"):
        return
    bot.status = "RUNNING"
    bot.started_at = timezone.now()
    bot.stopped_at = None
    bot.panic_requested = False
    bot.last_error = ""
    bot.save(update_fields=["status", "started_at", "stopped_at", "panic_requested", "last_error"])
    Event.objects.create(bot=bot, level="INFO", event_type="BOT_START", message="Bot start requested (task)")


@app.task(queue="monitor")
def request_stop_bot(bot_id: int):
    bot = Bot.objects.get(pk=bot_id)
    if bot.status != "RUNNING":
        return
    bot.status = "STOPPING"
    bot.stopped_at = timezone.now()
    bot.save(update_fields=["status", "stopped_at"])
    Event.objects.create(bot=bot, level="INFO", event_type="BOT_STOP", message="Bot stop requested (task)")


@app.task(queue="monitor")
def request_panic_bot(bot_id: int):
    bot = Bot.objects.get(pk=bot_id)
    bot.panic_requested = True
    bot.save(update_fields=["panic_requested"])
    Event.objects.create(bot=bot, level="CRITICAL", event_type="PANIC_REQUESTED", message="PANIC requested (task)")


@app.periodic(cron="*/1 * * * *")
@app.task(queue="monitor")
def reconcile_running_bots(timestamp: int):
    """
    Periodic tick to help operators see the system is alive.

    The TWS agent performs reconciliation itself; this task just emits a log
    event when there are running bots, and can be used later to enqueue commands
    if we introduce a DB-backed command queue.
    """
    running = Bot.objects.filter(status__in=("RUNNING", "STOPPING")).count()
    if running:
        logger.debug("reconcile_running_bots tick: %s running/stopping bots", running)

