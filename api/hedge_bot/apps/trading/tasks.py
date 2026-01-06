"""
Procrastinate tasks for trading bot management

These tasks monitor bot status and launch bot runners in the background.
"""

import asyncio
import logging
from procrastinate.contrib.django import app
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

    # Find all bots that should be running
    running_bots = Bot.objects.filter(status='RUNNING')

    for bot in running_bots:
        # Check if bot has a worker assigned
        if not bot.worker_task_id:
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
                    bot_locked.worker_task_id = task_id
                    bot_locked.worker_started_at = timezone.now()
                    bot_locked.save()

                    # Schedule the bot worker task
                    run_bot_worker.defer(bot_id=bot.id, task_id=task_id)

                    Event.objects.create(
                        bot=bot_locked,
                        event_type='BOT_START',
                        level='INFO',
                        message=f"Bot worker launched for {bot_locked.symbol}"
                    )
        else:
            # Has worker - check if it's stale (running for more than 5 minutes without heartbeat)
            if bot.worker_started_at:
                age = timezone.now() - bot.worker_started_at
                if age > timedelta(minutes=5):
                    logger.warning(f"Bot {bot.id} worker appears stale (age: {age}), will restart")
                    bot.worker_task_id = None
                    bot.worker_started_at = None
                    bot.save()

    # Clean up bots that are no longer RUNNING
    Bot.objects.exclude(status='RUNNING').update(worker_task_id=None, worker_started_at=None)

    logger.debug(f"Bot monitor: {running_bots.count()} running bots checked")


@app.task
async def run_bot_worker(bot_id: int, task_id: str):
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
    task_id : str
        Unique task identifier for this worker instance
    """
    from asgiref.sync import sync_to_async

    logger.info(f"Bot worker started for bot {bot_id} (task_id={task_id})")

    # Verify we own this bot (prevent race conditions)
    @sync_to_async
    def verify_ownership():
        bot = Bot.objects.get(pk=bot_id)
        return bot.worker_task_id == task_id

    if not await verify_ownership():
        logger.warning(f"Bot {bot_id} worker task {task_id} aborted - another worker owns this bot")
        return

    try:
        # Import here to avoid circular imports
        from .bot_runner import BotRunner

        # Create and run the bot
        runner = BotRunner(bot_id=bot_id)
        await runner.run()

    except Exception as e:
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
                bot.save()

                Event.objects.create(
                    bot=bot,
                    event_type='SYSTEM_ERROR',
                    level='CRITICAL',
                    message=f"Bot worker crashed: {str(e)}"
                )

            await update_bot_error()
        except Exception as db_error:
            logger.error(f"Failed to update bot status: {db_error}")

    finally:
        # Clean up worker tracking
        @sync_to_async
        def cleanup_worker():
            try:
                bot = Bot.objects.get(pk=bot_id)
                if bot.worker_task_id == task_id:
                    bot.worker_task_id = None
                    bot.worker_started_at = None
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
            event_type='BOT_START',
            level='INFO',
            message=f"Bot start requested"
        )

        logger.info(f"Bot {bot_id} status set to RUNNING")

        # Trigger immediate monitoring check instead of waiting for cron
        monitor_bots.defer(timestamp=int(timezone.now().timestamp()))

    except Bot.DoesNotExist:
        logger.error(f"Bot {bot_id} not found")
    except Exception as e:
        logger.error(f"Failed to start bot {bot_id}: {e}", exc_info=True)
