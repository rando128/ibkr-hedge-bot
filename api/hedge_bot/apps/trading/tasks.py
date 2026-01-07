"""
Procrastinate tasks for trading bot management

These tasks monitor bot status and launch bot runners in the background.
"""

import asyncio
import logging
import traceback
from procrastinate.contrib.django import app
from django.db import models
from django.utils import timezone

from .models import Bot, Event

logger = logging.getLogger(__name__)


@app.periodic(cron="* * * * *")  # Check every minute
@app.task
def monitor_bots(timestamp: int):
    """
    Monitor all bots and ensure RUNNING bots have active processes.

    This task runs every minute and:
    1. Finds all bots with status='RUNNING'
    2. Checks if they have an active worker task (using DB field)
    3. Launches workers for bots that need them
    4. Cleans up stale worker tracking

    Uses database-level worker_task_id for multi-worker coordination.
    """
    from django.db import transaction
    from datetime import timedelta

    def launch_worker(bot):
        """Atomically claim and launch a worker for the given bot."""
        task_id = f"bot_{bot.id}_{int(timezone.now().timestamp())}"
        with transaction.atomic():
            bot_locked = Bot.objects.select_for_update().get(pk=bot.id)
            if bot_locked.worker_task_id or bot_locked.status != 'RUNNING':
                logger.debug(f"launch_worker: bot {bot.id} already owned or not RUNNING (status={bot_locked.status}, worker_task_id={bot_locked.worker_task_id})")
                return None
            now = timezone.now()
            bot_locked.worker_task_id = task_id
            bot_locked.worker_started_at = now
            bot_locked.worker_last_heartbeat = now  # Initial heartbeat
            bot_locked.save()
        logger.info(f"launch_worker: scheduled worker for bot {bot.id} ({bot.symbol}), task_id={task_id}")
        run_bot_worker.defer(bot_id=bot.id, task_id=task_id)
        Event.objects.create(
            bot=bot_locked,
            event_type='WORKER_RESTART',
            level='INFO',
            message=f"Bot worker launched for {bot_locked.symbol}"
        )
        return task_id

    # Find all bots that should be running
    running_bots = Bot.objects.filter(status='RUNNING')

    for bot in running_bots:
        has_active_cycle = bot.cycles.filter(
            status__in=['INITIALIZING', 'ENTERING', 'ACTIVE', 'TRANSITIONING', 'ERROR']
        ).exists()

        # Check if bot has a worker assigned
        if not bot.worker_task_id:
            # Extra guard: if we recently saw a heartbeat, assume a worker is still alive even if the id was cleared
            if not has_active_cycle and bot.worker_last_heartbeat and timezone.now() - bot.worker_last_heartbeat < timedelta(minutes=2):
                logger.warning(f"Bot {bot.id} has no worker_task_id but heartbeat is fresh; skipping duplicate start (no active cycle)")
                continue

            # No worker - launch one
            logger.info(f"Launching worker for bot {bot.id} ({bot.symbol})")

            # Generate unique task identifier
            task_id = f"bot_{bot.id}_{int(timezone.now().timestamp())}"

            # Atomically claim this bot (prevent race conditions)
            with transaction.atomic():
                # Re-fetch with lock
                bot_locked = Bot.objects.select_for_update().get(pk=bot.id)

                # Double-check no worker was assigned in the meantime
                if not bot_locked.worker_task_id and bot_locked.status == 'RUNNING':
                    now = timezone.now()
                    bot_locked.worker_task_id = task_id
                    bot_locked.worker_started_at = now
                    bot_locked.worker_last_heartbeat = now  # Initial heartbeat
                    bot_locked.save()

                    # Schedule the bot worker task
                    run_bot_worker.defer(bot_id=bot.id, task_id=task_id)

                    Event.objects.create(
                        bot=bot_locked,
                        event_type='BOT_START',
                        level='INFO',
                        message=f"Bot worker launched for {bot_locked.symbol}"
                    )
                    logger.info(f"monitor_bots: worker launched for bot {bot.id} ({bot.symbol}) task_id={task_id}")
        else:
            # Has worker - check if it's stale (no heartbeat for more than 2 minutes)
            if bot.worker_last_heartbeat:
                age = timezone.now() - bot.worker_last_heartbeat
                recent_start = bot.worker_started_at and (timezone.now() - bot.worker_started_at) < timedelta(minutes=3)

                # If we have an active cycle, be stricter: restart quickly when heartbeat > 90s
                fast_stale = age > timedelta(seconds=45) if has_active_cycle else False

                if fast_stale or (not recent_start and age > timedelta(minutes=4)):
                    logger.warning(f"monitor_bots: worker stale for bot {bot.id} (age={age}, active_cycle={has_active_cycle}), restarting")
                    Event.objects.create(
                        bot=bot,
                        event_type='WORKER_STALE',
                        level='WARNING',
                        message=f"Worker stale (last heartbeat {age} ago); restarting"
                    )
                    bot.worker_task_id = None
                    bot.worker_started_at = None
                    bot.worker_last_heartbeat = None
                    bot.save()
                    # Relaunch immediately after clearing
                    launch_worker(bot)
                else:
                    logger.debug(f"monitor_bots: worker healthy for bot {bot.id} (age={age}, active_cycle={has_active_cycle}, recent_start={recent_start})")
            else:
                if has_active_cycle:
                    logger.warning(f"monitor_bots: bot {bot.id} has worker_task_id but no heartbeat; relaunching (active cycle present)")
                    bot.worker_task_id = None
                    bot.worker_started_at = None
                    bot.worker_last_heartbeat = None
                    bot.save()
                    launch_worker(bot)
                else:
                    logger.debug(f"monitor_bots: bot {bot.id} has worker_task_id but no heartbeat yet; waiting")

    # Clean up bots that are no longer RUNNING
    Bot.objects.exclude(status='RUNNING').update(
        worker_task_id=None,
        worker_started_at=None,
        worker_last_heartbeat=None
    )

    # Mark clearly stuck bots (RUNNING with no worker and old/missing heartbeat) as ERROR for visibility
    stuck_cutoff = timezone.now() - timedelta(minutes=10)
    stuck_bots = Bot.objects.filter(
        status='RUNNING',
        worker_task_id__isnull=True
    ).filter(
        models.Q(worker_last_heartbeat__lt=stuck_cutoff) | models.Q(worker_last_heartbeat__isnull=True)
    )

    for bot in stuck_bots:
        logger.error(f"Bot {bot.id} appears stuck (no worker, stale heartbeat); marking ERROR")
        bot.status = 'ERROR'
        bot.stopped_at = timezone.now()
        bot.save(update_fields=['status', 'stopped_at'])
        Event.objects.create(
            bot=bot,
            event_type='WORKER_STALE',
            level='ERROR',
            message="Bot marked ERROR: no worker and stale/missing heartbeat"
        )

    logger.debug(f"Bot monitor: {running_bots.count()} running bots checked")


@app.task
async def run_bot_worker(bot_id: int, task_id: str = None):
    """
    Execute the bot trading logic for a specific bot.

    This is a long-running async task that:
    1. Validates task ownership (prevents duplicate workers)
    2. Loads the bot configuration
    3. Connects to IBKR
    4. Runs the hedge cycle loop
    5. Monitors bot status for stop signals
    6. Cleans up on exit

    Parameters
    ----------
    bot_id : int
        The ID of the Bot to run
    task_id : str, optional
        Unique task identifier for this worker instance (for multi-worker coordination)
    """
    from asgiref.sync import sync_to_async

    logger.info(f"Bot worker started for bot {bot_id} (task_id={task_id})")

    # Verify we own this bot (prevent race conditions) - only if task_id provided
    if task_id:
        @sync_to_async
        def verify_ownership():
            bot = Bot.objects.get(pk=bot_id)
            return bot.worker_task_id == task_id

        if not await verify_ownership():
            logger.warning(f"Bot {bot_id} worker task {task_id} aborted - another worker owns this bot")
            return
    else:
        # Legacy task without task_id - claim it now
        @sync_to_async
        def claim_bot():
            bot = Bot.objects.get(pk=bot_id)
            if not bot.worker_task_id:
                now = timezone.now()
                task_id_generated = f"bot_{bot.id}_{int(now.timestamp())}"
                bot.worker_task_id = task_id_generated
                bot.worker_started_at = now
                bot.worker_last_heartbeat = now  # Initial heartbeat
                bot.save()
                return task_id_generated
            return None

        task_id = await claim_bot()
        if not task_id:
            logger.warning(f"Bot {bot_id} already has a worker, aborting legacy task")
            return
        logger.info(f"Legacy task claimed bot {bot_id} with generated task_id={task_id}")

    try:
        # Import here to avoid circular imports
        from .bot_runner import BotRunner

        # Create and run the bot
        runner = BotRunner(bot_id=bot_id)
        await runner.run()

    except Exception as e:
        tb_str = traceback.format_exc()
        logger.error(f"Bot worker {bot_id} crashed: {e}", exc_info=True)

        # Update bot status to ERROR (using sync_to_async)
        try:
            @sync_to_async
            def update_bot_error():
                bot = Bot.objects.get(pk=bot_id)
                bot.status = 'ERROR'
                bot.stopped_at = timezone.now()
                bot.worker_task_id = None
                bot.worker_started_at = None
                bot.worker_last_heartbeat = None
                bot.save()

                Event.objects.create(
                    bot=bot,
                    event_type='WORKER_CRASH',
                    level='CRITICAL',
                    message=f"Bot worker crashed: {str(e)}",
                    data={'traceback': tb_str}
                )

            await update_bot_error()
        except Exception as db_error:
            logger.error(f"Failed to update bot status: {db_error}")

    finally:
        # Clean up worker tracking only if the bot is no longer running
        @sync_to_async
        def cleanup_worker():
            try:
                bot = Bot.objects.get(pk=bot_id)
                # Preserve worker_task_id while RUNNING to avoid duplicate starts mid-cycle
                if bot.status != 'RUNNING' and bot.worker_task_id == task_id:
                    bot.worker_task_id = None
                    bot.worker_started_at = None
                    bot.worker_last_heartbeat = None
                    bot.save()
            except Bot.DoesNotExist:
                pass

        await cleanup_worker()
        logger.info(f"Bot worker {bot_id} (task_id={task_id}) terminated")


@app.task
def stop_bot(bot_id: int):
    """
    Gracefully stop a running bot.

    This task sets the bot status to STOPPED, which will be picked up
    by the bot runner's status check loop and cause it to exit gracefully.

    Parameters
    ----------
    bot_id : int
        The ID of the Bot to stop
    """
    logger.info(f"[STOP_BOT_TASK] Task started for bot {bot_id}")
    print(f"[STOP_BOT_TASK] Task started for bot {bot_id}")

    try:
        bot = Bot.objects.get(pk=bot_id)
        logger.info(f"[STOP_BOT_TASK] Bot {bot_id} found, current status: {bot.status}")
        print(f"[STOP_BOT_TASK] Bot {bot_id} found, current status: {bot.status}")

        if bot.status not in ('RUNNING', 'ERROR'):
            logger.warning(f"[STOP_BOT_TASK] Bot {bot_id} is not running (status: {bot.status})")
            print(f"[STOP_BOT_TASK] Bot {bot_id} is not running (status: {bot.status})")
            return

            logger.info(f"[STOP_BOT_TASK] Setting bot {bot_id} status to STOPPED")
            print(f"[STOP_BOT_TASK] Setting bot {bot_id} status to STOPPED")

            bot.status = 'STOPPED'
            bot.stopped_at = timezone.now()
            bot.save()

            Event.objects.create(
                bot=bot,
                event_type='BOT_STOP',
                level='INFO',
                message=f"Bot stop requested"
            )

        logger.info(f"[STOP_BOT_TASK] Stop signal sent to bot {bot_id}, status updated to STOPPED")
        print(f"[STOP_BOT_TASK] Stop signal sent to bot {bot_id}, status updated to STOPPED")

    except Bot.DoesNotExist:
        logger.error(f"[STOP_BOT_TASK] Bot {bot_id} not found")
        print(f"[STOP_BOT_TASK] Bot {bot_id} not found")
    except Exception as e:
        logger.error(f"[STOP_BOT_TASK] Failed to stop bot {bot_id}: {e}", exc_info=True)
        print(f"[STOP_BOT_TASK] Failed to stop bot {bot_id}: {e}")


@app.task
def start_bot(bot_id: int):
    """
    Start a bot by setting its status to RUNNING.

    The monitor_bots periodic task will pick this up and launch the worker.

    Parameters
    ----------
    bot_id : int
        The ID of the Bot to start
    """
    try:
        bot = Bot.objects.get(pk=bot_id)

        if bot.status == 'RUNNING':
            logger.warning(f"Bot {bot_id} is already running")
            return

        bot.status = 'RUNNING'
        bot.started_at = timezone.now()
        bot.stopped_at = None
        bot.save()

        Event.objects.create(
            bot=bot,
            event_type='WORKER_RESTART' if bot.worker_task_id else 'BOT_START',
            level='INFO',
            message="Bot start requested" if not bot.worker_task_id else "Restarting bot worker after stale/stop"
        )

        logger.info(f"Bot {bot_id} status set to RUNNING")

        # Trigger immediate monitoring check instead of waiting for cron
        monitor_bots.defer(timestamp=int(timezone.now().timestamp()))

    except Bot.DoesNotExist:
        logger.error(f"Bot {bot_id} not found")
    except Exception as e:
        logger.error(f"Failed to start bot {bot_id}: {e}", exc_info=True)
