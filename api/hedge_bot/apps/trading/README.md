# Trading Bot System

Django-integrated automated hedge bot system for Interactive Brokers.

## Overview

This app provides a complete bot management system that:
- **Stores bot configurations** in the database
- **Monitors bot status** and automatically launches workers
- **Tracks all trading activity** (cycles, orders, executions, events)
- **Provides Django admin interface** for bot management
- **Uses Procrastinate/Celery** for background task execution

## Architecture

```
┌─────────────────┐
│  Django Admin   │  ← Create/Configure bots, Click "Start"
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│   Bot Model     │  ← status = 'RUNNING'
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ monitor_bots    │  ← Periodic task (every minute)
│  (Procrastinate)│     Checks for RUNNING bots
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ run_bot_worker  │  ← Long-running async task
│  (BotRunner)    │     Executes hedge cycles
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│   IBKR TWS/     │  ← Places orders, receives fills
│   Gateway       │
└─────────────────┘
```

## Models

### Bot
Configuration and status for a bot instance.

**Fields:**
- `name`: Optional human-readable name
- `symbol`: Stock symbol to trade
- `qty`: Quantity per leg
- `stop_pct`: Stop loss percentage
- `trailing_pct`: Trailing stop percentage
- `long_account`, `short_account`: IBKR account IDs
- `port`: IBKR connection port (7497 for TWS paper, 4002 for Gateway paper)
- `use_algo`: Use IBKR Adaptive algo
- `status`: IDLE, RUNNING, STOPPED, ERROR

### Cycle
A single hedge cycle from entry to exit.

**Fields:**
- `bot`: ForeignKey to Bot
- `cycle_number`: Sequential number
- `status`: INITIALIZING, ENTERING, ACTIVE, TRANSITIONING, EXITING, COMPLETED, ABORTED, ERROR
- `total_buys`, `total_sells`, `total_commission`, `net_pnl`: P&L tracking

### Order
An order placed with IBKR.

**Fields:**
- `cycle`: ForeignKey to Cycle
- `order_id`: IBKR order ID
- `role`: LONG_ENTRY, SHORT_ENTRY, LONG_SL, SHORT_SL, LONG_TRAIL, SHORT_TRAIL, etc.
- `status`: PendingSubmit, Submitted, Filled, Cancelled, etc.
- `filled_quantity`, `avg_fill_price`: Fill tracking

### Execution
A trade execution (fill) with commission.

**Fields:**
- `order`: ForeignKey to Order
- `exec_id`: IBKR execution ID (unique)
- `side`: BUY or SELL
- `shares`, `price`: Execution details
- `commission`: Commission charged
- `executed_at`: Exchange timestamp

### Event
Audit trail of all system events.

**Fields:**
- `event_type`: BOT_START, CYCLE_START, ORDER_FILLED, STOP_LOSS_HIT, etc.
- `level`: DEBUG, INFO, WARNING, ERROR, CRITICAL
- `message`: Human-readable description
- `data`: Optional JSON data

## Usage

### 1. Create a Bot in Django Admin

Navigate to: `http://localhost:8000/admin/trading/bot/`

Click "Add Bot" and configure:
- Symbol: `AAPL`
- Quantity: `100`
- Stop %: `1.0`
- Trailing %: `2.0`
- Long Account: `DUP073403`
- Short Account: `DUP073404`
- Port: `7497` (TWS paper trading)

**Important:** The bot is created with `status='IDLE'` - it won't start yet!

### 2. Start the Bot

**Option A: Click the "Start" button** in the bot list view

**Option B: Change status to RUNNING** in the bot detail page (not recommended - status is readonly)

**Option C: Use admin actions** - Select bot(s) and choose "Start selected bots"

### 3. What Happens Next

1. **Monitor Task** (`monitor_bots`) runs every minute
2. Detects bot with `status='RUNNING'` and no active worker
3. Launches `run_bot_worker` task with the bot ID
4. Worker connects to IBKR and starts executing hedge cycles
5. All activity is logged to Events table
6. Orders and Executions are tracked in real-time

### 4. Monitor the Bot

**View Activity:**
- Click "Events" in admin to see real-time logs
- Click "Cycles" to see completed cycles and P&L
- Click "Orders" to see order history
- Click "Executions" to see fills and commissions

**Check Status:**
- Bot list shows status badge (green = running)
- "Last P&L" column shows latest cycle result
- "Cycles" column shows total cycle count

### 5. Stop the Bot

**Click the "Stop" button** in the bot list view

This sets `status='STOPPED'` which signals the worker to:
1. Complete the current cycle gracefully
2. Flatten any open positions
3. Disconnect from IBKR
4. Exit cleanly

## Running the Procrastinate Worker

For the system to work, you need a Procrastinate worker running:

```bash
# In the api/ directory
python manage.py procrastinate_worker
```

This worker:
- Runs periodic tasks (like `monitor_bots`)
- Executes bot worker tasks
- Handles task scheduling

## Task Queue Configuration

### Queues

- **default**: General tasks, periodic monitoring
- **trading**: Bot worker tasks (long-running)

### Periodic Tasks

- `monitor_bots`: Runs every minute, checks bot status and launches workers

### Manual Task Execution

```python
from hedge_bot.apps.trading.tasks import start_bot, stop_bot

# Start a bot programmatically
start_bot.defer(bot_id=1)

# Stop a bot programmatically
stop_bot.defer(bot_id=1)
```

## Bot Lifecycle

```
CREATE BOT → status='IDLE'
    ↓
Click "Start" → status='RUNNING'
    ↓
monitor_bots detects RUNNING bot
    ↓
Launches run_bot_worker task
    ↓
Worker runs hedge cycles in loop
    ↓
Click "Stop" → status='STOPPED'
    ↓
Worker detects status change
    ↓
Completes current cycle gracefully
    ↓
Disconnects from IBKR
    ↓
Worker exits → Bot remains STOPPED
```

## Error Handling

### Bot Crashes
If a bot worker crashes:
1. Status is set to `ERROR`
2. Event logged with error details
3. Worker is removed from tracking
4. Bot will NOT auto-restart

**Recovery:** Fix the issue, then click "Start" again

### IBKR Connection Failure
If connection fails:
1. Bot status set to `ERROR`
2. Detailed error logged to Events
3. Worker exits

### Partial Fills
The system handles partial fills:
1. Tracks `filled_quantity` vs `total_quantity`
2. Executions table stores each partial fill
3. Stop losses placed based on actual filled quantity

## Advanced Features

### Concurrent Bots
Multiple bots can run simultaneously:
- Each bot gets its own worker task
- Different symbols, accounts, parameters
- Independent P&L tracking

### Cycle History
Every cycle is preserved:
- View historical P&L trends
- Analyze execution quality
- Debug strategy performance

### Audit Trail
Complete event log:
- Every order submission
- Every fill
- Every status change
- Every error
- Full traceability

## Troubleshooting

### Bot shows RUNNING but not trading

1. Check Procrastinate worker is running
2. Check Events for errors
3. Verify IBKR connection (port, clientId)
4. Check IBKR account permissions

### Worker keeps crashing

1. View Events filtered by level=ERROR
2. Check IBKR TWS/Gateway is running
3. Verify account credentials
4. Check network connectivity

### No cycles appearing

1. Ensure bot is RUNNING (not IDLE)
2. Check worker logs
3. Verify market is open (or outsideRth=True)
4. Check symbol is valid and qualified

## Next Steps

Potential enhancements:
- REST API for programmatic control
- Real-time dashboard with WebSockets
- Strategy backtesting integration
- Multi-strategy support
- Risk management rules
- Notification system (email/Slack on cycle complete)
- Performance analytics dashboard

## Development Notes

### Adding New Event Types

Edit `models.py` → `Event.EVENT_TYPE_CHOICES`

### Customizing Bot Runner

Edit `bot_runner.py` → `BotRunner` class

### Adding New Tasks

Create in `tasks.py` and decorate with `@app.task`

### Testing

```bash
# Create a test bot
python manage.py shell
from hedge_bot.apps.trading.models import Bot
bot = Bot.objects.create(
    symbol='AAPL',
    qty=10,
    stop_pct=1.0,
    trailing_pct=2.0,
    long_account='DUP073403',
    short_account='DUP073404',
    port=7497
)

# Start it programmatically
from hedge_bot.apps.trading.tasks import start_bot
start_bot.defer(bot_id=bot.id)
```

## Architecture Decisions

### Why Procrastinate?
- Already in your stack
- Supports async tasks
- Good for long-running processes
- Periodic task support

### Why Status Monitoring?
- Simple trigger mechanism
- Admin-friendly
- No complex APIs needed initially
- Easy to understand flow

### Why BotRunner Class?
- Encapsulates all bot logic
- Easy to test
- Clean separation from tasks
- Reusable from management commands

### Why Django ORM?
- Queryable history
- Relationships between entities
- Admin interface for free
- Transaction support for consistency
