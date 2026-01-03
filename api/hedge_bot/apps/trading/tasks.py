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

# Track running bot processes to avoid duplicates
_running_bots = {}


@app.periodic(cron="* * * * *")  # Check every minute
@app.task
def monitor_bots(timestamp: int):
    """
    Monitor all bots and ensure RUNNING bots have active processes.

    This task runs every minute and:
    1. Finds all bots with status='RUNNING'
    2. Checks if they have an active worker task
    3. Launches workers for bots that need them
    4. Cleans up tracking for STOPPED/ERROR bots
    """
    global _running_bots

    # Find all bots that should be running
    running_bots = Bot.objects.filter(status='RUNNING')
    running_bot_ids = set(running_bots.values_list('id', flat=True))

    # Launch tasks for bots that don't have one
    for bot in running_bots:
        if bot.id not in _running_bots:
            logger.info(f"Launching worker for bot {bot.id} ({bot.symbol})")

            # Schedule the bot worker task
            run_bot_worker.defer(bot_id=bot.id)

            _running_bots[bot.id] = {
                'started_at': timezone.now(),
                'bot_symbol': bot.symbol
            }

            Event.objects.create(
                bot=bot,
                event_type='BOT_START',
                level='INFO',
                message=f"Bot worker launched for {bot.symbol}"
            )

    # Clean up tracking for stopped bots
    tracked_bot_ids = set(_running_bots.keys())
    stopped_bot_ids = tracked_bot_ids - running_bot_ids

    for bot_id in stopped_bot_ids:
        logger.info(f"Removing tracking for stopped bot {bot_id}")
        del _running_bots[bot_id]

    logger.debug(f"Bot monitor: {len(running_bot_ids)} running, {len(stopped_bot_ids)} stopped")


@app.task
async def run_bot_worker(bot_id: int):
    """
    Execute the bot trading logic for a specific bot.

    This is a long-running async task that:
    1. Loads the bot configuration
    2. Connects to IBKR
    3. Runs the hedge cycle loop
    4. Monitors bot status for stop signals
    5. Cleans up on exit

    Parameters
    ----------
    bot_id : int
        The ID of the Bot to run
    """
    from asgiref.sync import sync_to_async
    global _running_bots

    logger.info(f"Bot worker started for bot {bot_id}")

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
        # Clean up tracking
        if bot_id in _running_bots:
            del _running_bots[bot_id]

        logger.info(f"Bot worker {bot_id} terminated")


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
