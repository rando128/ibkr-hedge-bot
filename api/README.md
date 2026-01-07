# API

This handles the API and back-office admin.

All the URLs pointing to this are prefixed by `/back`.

## Components

You'll find the following apps:

-   [people](./hedge_bot/apps/people) &mdash; The user model and
    authentication.

-   [realtime](./hedge_bot/apps/realtime) &mdash; Deals with
    websockets

-   [trading](./hedge_bot/apps/trading) &mdash; Hedge bot trading system
    with Django ORM integration

## Trading System (hedge_bot)

The `trading` app provides a Django-based hedge bot system that integrates with Interactive Brokers (IBKR) via TWS/IB Gateway.

### Architecture

The system consists of:

- **Django Models**: Store bot configurations, cycles, orders, executions, and events
- **Bot Runner**: Executes hedge bot trading logic with async IBKR integration
- **Procrastinate Workers**: Background task queue for running multiple bots concurrently
- **Django Admin**: Web interface for managing bots and viewing logs

### Models

- **Bot**: Bot configuration (symbol, accounts, trading parameters, contract settings)
- **Cycle**: Tracks each hedge cycle with P&L calculation
- **Order**: IBKR orders with roles (LONG_ENTRY, SHORT_SL, etc.)
- **Execution**: Individual fills with commission tracking
- **Event**: Comprehensive audit trail of all bot activities

### Prerequisites

1. **TWS or IB Gateway** running
   - Paper Trading: port 7497
   - Live Trading: port 7496
2. **Enable API connections** in TWS/Gateway settings
3. **PostgreSQL database** configured in Django settings
4. **Procrastinate worker** running to process background tasks

### Starting the System

#### 1. Start the Django Server

```bash
cd api
poetry run python manage.py runserver 0.0.0.0:8080
```

Access the admin at: http://127.0.0.1:8080/back/admin/

#### 2. Start Procrastinate Workers (split queues)

Queues:
- `monitor` : runs `monitor_bots` (lightweight, every minute)
- `bots`    : runs `run_bot_worker` (long-running per-bot)

Run a dedicated worker for each queue:

```bash
# In repo root
cd api

# Monitor queue (small, fast jobs)
poetry run python manage.py procrastinate worker --queues monitor

# Bot queue (long jobs). Adjust concurrency to number of bots you want in parallel.
poetry run python manage.py procrastinate worker --queues bots --concurrency 5
```

Notes:
- Without a `monitor` worker, monitor tasks will queue up while bot workers are busy.
- You can scale the bots worker separately (e.g., run multiple `--queues bots` workers or increase `--concurrency`).


### Managing Bots via Django Admin

#### Creating a Bot

1. Go to http://127.0.0.1:8080/back/admin/trading/bot/
2. Click "Add Bot"
3. Configure the bot:
   - **Name**: Descriptive name (e.g., "AIR Hedge Bot")
   - **Symbol**: Stock ticker (e.g., "AIR", "AAPL")
   - **Contract Settings**:
     - `primary_exchange`: Primary exchange (e.g., "SBF" for Euronext, "NYSE", "NASDAQ"). Leave blank if not needed.
     - `exchange`: Routing exchange (default: "SMART" for automatic routing)
     - `currency`: Currency (e.g., "EUR", "USD")
   - **Trading Parameters**:
     - `qty`: Quantity to trade per side
     - `stop_pct`: Stop loss percentage (e.g., 0.02 for 2%)
     - `trailing_pct`: Trailing stop percentage (e.g., 0.01 for 1%)
   - **Accounts**:
     - `long_account`: IBKR account for long positions
     - `short_account`: IBKR account for short positions
   - **Connection**:
     - `environment`: Select "Paper Trading" or "Live Trading" (automatically uses port 7497 or 7496)
     - `use_algo`: Enable algo orders (default: False)
4. Click "Save"

#### Starting a Bot

**Option 1: From Bot List**
1. Go to http://127.0.0.1:8080/back/admin/trading/bot/
2. Click the **"Start"** button next to the bot
3. The bot status will change to **RUNNING**
4. Check the worker console for connection logs

**Option 2: From Bot Detail Page**
1. Open the bot's detail page
2. Change `Status` to **RUNNING**
3. Click "Save"
4. The periodic monitor task will launch the worker within 1 minute

**Option 3: Bulk Start**
1. Select multiple bots in the list
2. Choose "Start selected bots" from the Actions dropdown
3. Click "Go"

#### Stopping a Bot

**Option 1: From Bot List**
1. Go to http://127.0.0.1:8080/back/admin/trading/bot/
2. Click the **"Stop"** button next to the running bot
3. The bot will detect the status change within 1-2 seconds and gracefully shut down

**Option 2: From Bot Detail Page**
1. Open the bot's detail page
2. Change `Status` to **STOPPED**
3. Click "Save"
4. The bot will stop within 1-2 seconds

**Option 3: Bulk Stop**
1. Select multiple running bots in the list
2. Choose "Stop selected bots" from the Actions dropdown
3. Click "Go"

#### Viewing Bot Logs

**From Bot List:**
1. Go to http://127.0.0.1:8080/back/admin/trading/bot/
2. Click the **"Logs"** button next to any bot
3. This will show all events filtered by that bot

**From Event List:**
1. Go to http://127.0.0.1:8080/back/admin/trading/event/
2. Use the sidebar filters to filter by:
   - Bot
   - Event Type (BOT_START, CYCLE_START, ORDER_SUBMITTED, etc.)
   - Level (INFO, WARNING, ERROR, CRITICAL)
   - Date/Time

#### Viewing Cycles and P&L

1. Go to http://127.0.0.1:8080/back/admin/trading/cycle/
2. View cycle details including:
   - Cycle number
   - Status (ACTIVE, COMPLETED, FAILED)
   - Total buys, sells, commission
   - Net P&L
3. Click on a cycle to see associated orders and executions

#### Viewing Orders and Executions

**Orders:**
- http://127.0.0.1:8080/back/admin/trading/order/
- Shows all orders with roles (LONG_ENTRY, SHORT_SL, etc.)
- Filter by status, role, action, order type

**Executions:**
- http://127.0.0.1:8080/back/admin/trading/execution/
- Shows individual fills with prices and commission
- Linked to orders and cycles

### Client ID Management

Each bot automatically gets a unique TWS client ID to avoid conflicts when running multiple bots:

**Formula:** `(bot_id * 100000) + (current_timestamp_milliseconds % 100000)`

**Examples:**
- Bot 1: client IDs 180123, 195789, etc.
- Bot 2: client IDs 280456, 298234, etc.

This ensures:
- Each bot has a unique ID range
- Reconnections get fresh IDs
- No collisions between simultaneous connections

### Monitoring

#### Worker Status

Check the Procrastinate worker console for:
- Bot connections: `[CONNECTION] Connecting to TWS with clientId=...`
- Cycle execution: `[CYCLE] Executing placeholder cycle...`
- Stop detection: `[BOT] Stop detected - status changed to STOPPED`
- Errors: `[ERROR]`, `[FATAL ERROR]`

#### Django Admin Dashboard

The bot list shows:
- **Environment Badge**: Color-coded environment (Paper: blue, Live: red/orange)
- **Status Badge**: Color-coded status (IDLE, RUNNING, STOPPED, ERROR)
- **Cycles Count**: Total number of cycles with link to cycle list
- **Last P&L**: P&L from the last completed cycle (color-coded)
- **Action Buttons**: Start/Stop based on current status
- **Logs Button**: Quick access to bot event logs

### Troubleshooting

#### Bot won't start

1. Check TWS/Gateway is running on the correct port
2. Verify API connections are enabled in TWS settings
3. Check worker is running with `--concurrency` flag
4. Look for errors in worker console and Event logs

#### Client ID already in use error

This should be resolved automatically with the timestamp-based client ID system. If it persists:
1. Restart TWS/Gateway to clear stale connections
2. Restart the Procrastinate worker to get a fresh timestamp seed
3. Check if there are zombie bot processes still connected

#### Bot won't stop

1. Check the worker console - bot should detect status change within 1-2 seconds
2. Verify the worker is running and processing tasks
3. Check Event logs for stop detection messages
4. If stuck, restart the worker (bot will be force-terminated)

#### Missing logs in admin

1. Verify the bot is creating Event records (check in Django shell)
2. Check database connection in worker
3. Look for Django ORM errors in worker console
4. Ensure `sync_to_async` wrappers are working correctly

### Database Queries

Useful Django ORM queries for debugging:

```python
from hedge_bot.apps.trading.models import Bot, Cycle, Order, Execution, Event

# Get running bots
running_bots = Bot.objects.filter(status='RUNNING')

# Get latest cycle for a bot
bot = Bot.objects.get(id=1)
latest_cycle = bot.cycles.order_by('-created_at').first()

# Get all events for a bot
events = Event.objects.filter(bot_id=1).order_by('-created_at')

# Calculate total P&L for a bot
from django.db.models import Sum
total_pnl = bot.cycles.filter(status='COMPLETED').aggregate(Sum('net_pnl'))

# Get failed orders
failed_orders = Order.objects.filter(status__in=['Cancelled', 'ApiCancelled', 'Inactive'])
```

## OpenAPI

When the app is in development mode, you can access the OpenAPI documentation at
`/back/api/schema/redoc/`.

This documentation is auto-generated using
[drf-spectacular](https://drf-spectacular.readthedocs.io/en/latest/). As you
create more APIs, make sure that they render nicely in OpenAPI format.
