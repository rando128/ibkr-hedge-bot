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

- **Django Models**: Store bot configurations, cycles, and events (no order/execution persistence in Option B)
- **Always-on TWS Agent**: Management command `tws_agent` holds the IB connection, handles SL-fill → trailing flip, and reconciles state from TWS snapshots
- **Procrastinate Tasks**: Lightweight DB-only tasks (start/stop/panic flags, periodic tick on `monitor` queue)
- **Django Admin**: Web interface for managing bots, cycles, and logs

### Models

- **Bot**: Configuration (symbol, accounts, trading parameters, contract settings) and desired status (`RUNNING` / `STOPPING` / `STOPPED` / `ERROR`), plus `panic_requested`.
- **Cycle**: One hedge cycle with persisted state (`INITIALIZING`, `ENTERING`, `ACTIVE`, `TRANSITIONING`, `TRAILING`, `RECOVERING`, `PANIC`, `PNL_CALCULATION`, `COMPLETED`, `ABORTED`, `ERROR`), P&L totals, `cycle_key` (UUID), and timestamps.
- **Event**: Append-only audit trail (state transitions, admin actions, errors).

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

#### 2. Start the TWS Agent (always-on)

```bash
cd api
# Environment chooses default port (PAPER→7497, LIVE→7496); override with --port if needed
poetry run python manage.py tws_agent --environment PAPER
```

#### 3. Start the Procrastinate Worker (monitor queue only)

Queues:
- `monitor` : runs lightweight tasks (start/stop/panic flags, periodic tick)

Run worker:

```bash
cd api
poetry run python manage.py procrastinate worker --queues monitor
```


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
3. The bot status will change to **RUNNING** (the TWS agent will reconcile and start the cycle)

**Option 2: From Bot Detail Page**
1. Open the bot's detail page
2. Change `Status` to **RUNNING**
3. Click "Save"

**Option 3: Bulk Start**
1. Select multiple bots in the list
2. Choose "Start selected bots" from the Actions dropdown
3. Click "Go"

#### Stopping a Bot

**Option 1: From Bot List**
1. Go to http://127.0.0.1:8080/back/admin/trading/bot/
2. Click the **"Stop"** button next to the running bot
3. The bot status will change to **STOPPING** (finish current cycle, then stop)

**Option 2: From Bot Detail Page**
1. Open the bot's detail page
2. Change `Status` to **STOPPING**
3. Click "Save"

**Option 3: Bulk Stop**
1. Select multiple running bots in the list
2. Choose "Stop selected bots" from the Actions dropdown
3. Click "Go"

#### PANIC (emergency flatten)
- From Bot list, click the 🚨 button; sets `panic_requested=True` and logs an event. The TWS agent cancels orders and flattens both accounts for that bot.
- Bulk action: “PANIC selected bots”.

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
   - State (INITIALIZING, ENTERING, ACTIVE, TRANSITIONING, TRAILING, RECOVERING, PANIC, PNL_CALCULATION, COMPLETED, ABORTED, ERROR)
   - Total buys, sells, commission, net P&L (computed from TWS executions)

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

Check the TWS agent console for:
- IB connection logs
- SL→trailing transitions
- Reconcile ticks and panic actions

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

1. Check TWS/Gateway is running on the correct port and API connections enabled.
2. Ensure the TWS agent is running (`manage.py tws_agent`).
3. Check admin Events for BOT_ERROR.

#### Client ID already in use error

1. Restart TWS/Gateway to clear stale connections.
2. Restart the TWS agent with a different `--client-id` if needed.

#### Bot won't stop

1. Ensure the TWS agent is running.
2. Check Events for stop/panic handling and errors.

#### Missing logs in admin

1. Verify DB connectivity.
2. Check the TWS agent console for ORM/logging errors.

### Database Queries

Useful Django ORM queries for debugging:

```python
from hedge_bot.apps.trading.models import Bot, Cycle, Event

# Get running bots
running_bots = Bot.objects.filter(status='RUNNING')

# Get latest cycle for a bot
bot = Bot.objects.get(id=1)
latest_cycle = bot.cycles.order_by('-started_at').first()

# Get all events for a bot
events = Event.objects.filter(bot_id=1).order_by('-created_at')

# Calculate total P&L for a bot
from django.db.models import Sum
total_pnl = bot.cycles.filter(status='COMPLETED').aggregate(Sum('net_pnl'))
```

## OpenAPI

When the app is in development mode, you can access the OpenAPI documentation at
`/back/api/schema/redoc/`.

This documentation is auto-generated using
[drf-spectacular](https://drf-spectacular.readthedocs.io/en/latest/). As you
create more APIs, make sure that they render nicely in OpenAPI format.
