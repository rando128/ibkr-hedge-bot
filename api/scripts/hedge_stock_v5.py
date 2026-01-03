"""
Hedge Stock V5 - Django ORM Integrated Version

This version integrates with Django ORM to persist all trading activity:
- Bot configuration and status
- Cycle tracking with P&L
- Order lifecycle management
- Execution and commission tracking
- Event logging for audit trail

Usage:
    django-admin run_hedge --symbol AAPL --qty 100 --stopPct 1.0 --trailingPct 2.0 \
        --longAccount DUP073403 --shortAccount DUP073404
"""

import asyncio
import argparse
import math
import os
import sys
import django
from datetime import datetime, timezone
from decimal import Decimal

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hedge_bot.django.settings')
django.setup()

from hedge_bot.apps.trading.models import Bot, Cycle, Order, Execution, Event
from ib_insync import IB, Stock, MarketOrder, StopOrder, Order as IBOrder, TagValue, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()


# Global state for the current cycle
current_state = {
    'bot': None,
    'cycle': None,
    'ib': None,
    'contract': None,
    'active_trades': {'long': None, 'short': None},
    'order_map': {},  # Maps IBKR orderId -> Django Order instance
    'exec_ids_seen': set(),
    'transitioning': False,
    'transition_done': False,
}


def q_floor(price, tick):
    """Round price down to nearest tick"""
    return math.floor((float(price) + 1e-12) / float(tick)) * float(tick)


def q_ceil(price, tick):
    """Round price up to nearest tick"""
    return math.ceil((float(price) - 1e-12) / float(tick)) * float(tick)


def fmt_ts(exec_time):
    """Format timestamp for logging"""
    if hasattr(exec_time, 'strftime'):
        return exec_time.strftime('%H:%M:%S')
    if isinstance(exec_time, str):
        return exec_time[-8:] if len(exec_time) >= 8 else exec_time
    return datetime.now().strftime('%H:%M:%S')


def get_safe_timestamp(exec_time):
    """Convert execution time to datetime"""
    if not exec_time:
        return datetime.now(timezone.utc)
    if isinstance(exec_time, datetime):
        if exec_time.tzinfo is None:
            return exec_time.replace(tzinfo=timezone.utc)
        return exec_time
    if isinstance(exec_time, str):
        try:
            dt = datetime.strptime(exec_time, '%Y%m%d %H:%M:%S')
            return dt.replace(tzinfo=timezone.utc)
        except:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


def log_event(event_type, level, message, order=None, data=None):
    """Log an event to the database"""
    Event.objects.create(
        bot=current_state['bot'],
        cycle=current_state['cycle'],
        order=order,
        event_type=event_type,
        level=level,
        message=message,
        data=data
    )
    print(f"[{level}] {message}")


def onError(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    """Handle IBKR errors"""
    if reqId == -1:
        return
    msg = errorString if errorString else str(errorCode)
    code = errorCode if errorString else "INFO"
    log_event('IBKR_ERROR', 'WARNING', f"IBKR {code}: {msg} (reqId={reqId})", data={'reqId': reqId, 'errorCode': code})


def report_pnl(is_final=False):
    """Generate P&L report from database"""
    cycle = current_state['cycle']
    cycle.refresh_from_db()

    status = "FINAL" if is_final else "INTERIM"
    print(f"\n{'=' * 40}")
    print(f"{status} HEDGE P&L REPORT (Cycle #{cycle.cycle_number} - {cycle.symbol})")
    print(f"{'-' * 40}")

    # Get execution details per role
    orders = cycle.orders.all().prefetch_related('executions')
    leg_details = {}

    for order in orders:
        execs = order.executions.all()
        if execs:
            total_qty = sum(e.shares for e in execs)
            total_value = sum(e.shares * e.price for e in execs)
            avg_price = total_value / total_qty if total_qty else 0
            last_exec = execs.order_by('-executed_at').first()

            leg_details[order.role] = {
                'qty': total_qty,
                'avg_price': avg_price,
                'time': last_exec.executed_at
            }

    for role, details in leg_details.items():
        time_str = fmt_ts(details['time'])
        print(f"{role:20} | {details['qty']:5} @ {details['avg_price']:8.4f} | {time_str}")

    print(f"{'-' * 40}")
    print(f"Total Cash Out (Buys):  {cycle.total_buys:.2f}")
    print(f"Total Cash In (Sells):  {cycle.total_sells:.2f}")
    print(f"Total Commissions:      {cycle.total_commission:.2f}")
    print(f"{'-' * 40}")
    print(f"NET REALIZED P&L:       {cycle.net_pnl:.2f}")
    print(f"STATUS: {'CLOSED' if is_final else 'OPEN'}")
    print(f"{'=' * 40}\n")

    log_event('PNL_REPORT', 'INFO', f"{status} P&L: {cycle.net_pnl:.2f}", data={
        'total_buys': float(cycle.total_buys),
        'total_sells': float(cycle.total_sells),
        'total_commission': float(cycle.total_commission),
        'net_pnl': float(cycle.net_pnl)
    })


def onCommissionReport(trade, fill, report):
    """Handle commission reports"""
    exec_id = getattr(report, 'execId', None) or fill.execution.execId
    if exec_id in current_state['exec_ids_seen']:
        return

    try:
        execution = Execution.objects.get(exec_id=exec_id)
        execution.commission = Decimal(str(report.commission))
        execution.commission_currency = report.currency or 'USD'
        execution.save()

        # Update cycle P&L
        cycle = current_state['cycle']
        cycle.total_commission += Decimal(str(report.commission))
        cycle.net_pnl = cycle.total_sells - cycle.total_buys - cycle.total_commission
        cycle.save()

        exec_time = get_safe_timestamp(fill.execution.time)
        print(f"[{fmt_ts(exec_time)}] [COMMISSION]: {report.commission:.2f} {report.currency}")
    except Execution.DoesNotExist:
        pass


def onTrailingStopStatus(trade):
    """Handle trailing stop status updates"""
    status = trade.orderStatus
    curr_stop = getattr(status, 'stopPrice', 0)
    if curr_stop <= 0:
        curr_stop = getattr(trade.order, 'auxPrice', 0)

    if status.status == 'Filled':
        print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | EXECUTED at {status.avgFillPrice:.4f}")
        log_event('TRAILING_UPDATE', 'INFO', f"Trailing stop filled at {status.avgFillPrice:.4f}")
    else:
        price_str = f"{curr_stop:.4f}" if 0 < curr_stop < 1e10 else "Calculating..."
        print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | Current Stop: {price_str}")


async def onStopLossFill(trade, fill):
    """Handle stop loss fill and transition to trailing stop"""
    if current_state['transitioning'] or current_state['transition_done']:
        return
    current_state['transitioning'] = True
    transitioned = False

    try:
        exec_time = get_safe_timestamp(fill.execution.time)
        print(f"\n>>>> [{fmt_ts(exec_time)}] STOP LOSS TRIGGERED on {trade.order.account} <<<<")
        log_event('STOP_LOSS_HIT', 'WARNING', f"Stop loss triggered on {trade.order.account}")

        # Update cycle status
        cycle = current_state['cycle']
        cycle.status = 'TRANSITIONING'
        cycle.save()

        # Determine which leg was hit
        bot = current_state['bot']
        hit_leg = 'long' if trade.order.account == bot.long_account else 'short'
        surviving_leg = 'short' if hit_leg == 'long' else 'long'
        surviving_trade = current_state['active_trades'][surviving_leg]
        acc = bot.long_account if surviving_leg == 'long' else bot.short_account
        label = surviving_leg.upper()

        ib = current_state['ib']
        contract = current_state['contract']

        if surviving_trade and surviving_trade.orderStatus.status in ('PreSubmitted', 'Submitted'):
            print(f"Cancelling surviving Stop Loss on {acc} ({label})...")
            ib.cancelOrder(surviving_trade.order)

            # Wait for cancellation
            deadline = asyncio.get_event_loop().time() + 10
            while not surviving_trade.isDone() and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.1)
                ib.waitOnUpdate()

            if not surviving_trade.isDone():
                print(f"Cancel timeout on {acc}; flattening survivor position defensively.")
                pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account == acc]
                if pos and pos[0].position != 0:
                    qty = pos[0].position
                    action = 'SELL' if qty > 0 else 'BUY'
                    ib.placeOrder(contract, MarketOrder(action, abs(qty), account=acc))
                return

            ib.waitOnUpdate()
            await asyncio.sleep(0)
            ib.waitOnUpdate()

            # Update order status in DB
            if surviving_trade.order.orderId in current_state['order_map']:
                db_order = current_state['order_map'][surviving_trade.order.orderId]
                db_order.status = surviving_trade.orderStatus.status
                db_order.save()
                log_event('ORDER_CANCELLED', 'INFO', f"Cancelled {db_order.role} order", order=db_order)

            st = surviving_trade.orderStatus.status
            if st not in ('Cancelled', 'ApiCancelled'):
                print(f"Surviving Stop Loss was not cancelled (Status: {st}). It likely filled. Not placing trailing stop.")
                return
        else:
            if not surviving_trade:
                print(f"No surviving SL trade object found for {acc} ({label}). Checking position.")
            elif surviving_trade.isDone():
                print(f"Surviving SL on {acc} ({label}) is already {surviving_trade.orderStatus.status}. Checking position.")
            else:
                print(f"Surviving SL on {acc} ({label}) status is {surviving_trade.orderStatus.status} (not active). Checking position.")

        # Place Trailing Stop
        action = 'SELL' if label == "LONG" else 'BUY'
        positions = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account == acc]
        if not positions or positions[0].position == 0:
            print(f"No active position found on {acc}. Not placing trailing stop.")
            return

        qty = abs(positions[0].position)
        market_price = fill.execution.price
        tick = float(cycle.min_tick)

        if label == "LONG":
            trail_price = q_floor(market_price * (1 - (float(bot.trailing_pct) / 100)), tick)
        else:
            trail_price = q_ceil(market_price * (1 + (float(bot.trailing_pct) / 100)), tick)

        print(f"Switching {label} leg to {bot.trailing_pct}% Trailing Stop (Est: {trail_price:.4f})...")
        trail_order = IBOrder(
            action=action, totalQuantity=qty, orderType='TRAIL',
            trailingPercent=float(bot.trailing_pct), account=acc, tif='GTC', outsideRth=True,
            orderRef=f"C{cycle.cycle_number}_{label}_TRAIL"
        )
        t_trade = ib.placeOrder(contract, trail_order)
        t_trade.statusEvent += onTrailingStopStatus
        current_state['active_trades'][surviving_leg] = t_trade

        # Create Order in DB
        db_order = Order.objects.create(
            cycle=cycle,
            order_id=t_trade.order.orderId,
            order_ref=trail_order.orderRef,
            role=f"{label}_TRAIL",
            account=acc,
            action=action,
            order_type='TRAIL',
            total_quantity=Decimal(str(qty)),
            trailing_percent=bot.trailing_pct,
            status='PendingSubmit'
        )
        current_state['order_map'][t_trade.order.orderId] = db_order
        log_event('TRANSITION_COMPLETE', 'INFO', f"Placed {label} trailing stop", order=db_order)

        transitioned = True
        print("Trailing Stop submitted. Protection transitioned.")
    finally:
        if transitioned:
            current_state['transition_done'] = True
            cycle = current_state['cycle']
            cycle.status = 'ACTIVE'
            cycle.save()
        current_state['transitioning'] = False


def onFill(trade, fill):
    """Handle order fills"""
    exec_id = fill.execution.execId

    if exec_id in current_state['exec_ids_seen']:
        return
    current_state['exec_ids_seen'].add(exec_id)

    # Get the Order from database
    if trade.order.orderId not in current_state['order_map']:
        print(f"Warning: Fill for unknown order {trade.order.orderId}")
        return

    db_order = current_state['order_map'][trade.order.orderId]
    cycle = current_state['cycle']
    exec_obj = fill.execution

    # Create Execution record
    exec_time = get_safe_timestamp(exec_obj.time)
    execution = Execution.objects.create(
        order=db_order,
        cycle=cycle,
        exec_id=exec_id,
        side=exec_obj.side,
        shares=Decimal(str(exec_obj.shares)),
        price=Decimal(str(exec_obj.price)),
        account=trade.order.account,
        commission=Decimal('0'),  # Will be updated when commission report arrives
        executed_at=exec_time
    )

    # Update Order
    db_order.filled_quantity += Decimal(str(exec_obj.shares))
    db_order.status = trade.orderStatus.status
    if trade.orderStatus.avgFillPrice:
        db_order.avg_fill_price = Decimal(str(trade.orderStatus.avgFillPrice))
    if trade.orderStatus.status == 'Filled':
        db_order.filled_at = exec_time
    db_order.save()

    # Update Cycle P&L
    if exec_obj.side == 'BOT':  # BOT means BUY in IBKR
        cycle.total_buys += Decimal(str(exec_obj.shares)) * Decimal(str(exec_obj.price))
    else:  # SLD means SELL
        cycle.total_sells += Decimal(str(exec_obj.shares)) * Decimal(str(exec_obj.price))

    cycle.net_pnl = cycle.total_sells - cycle.total_buys - cycle.total_commission
    cycle.save()

    print(f"--- [{fmt_ts(exec_time)}] {db_order.role} FILLED on {trade.order.account}: {exec_obj.shares} @ {exec_obj.price:.4f} ---")
    log_event('ORDER_FILLED', 'INFO', f"{db_order.role} filled: {exec_obj.shares}@{exec_obj.price:.4f}", order=db_order)

    # Trigger transition if it's a stop loss fill
    if db_order.role in ("LONG_SL", "SHORT_SL") and not current_state['transitioning'] and not current_state['transition_done']:
        asyncio.create_task(onStopLossFill(trade, fill))


def reset_cycle_state():
    """Reset state for a new cycle"""
    current_state['cycle'] = None
    current_state['active_trades'] = {'long': None, 'short': None}
    current_state['order_map'].clear()
    current_state['exec_ids_seen'].clear()
    current_state['transitioning'] = False
    current_state['transition_done'] = False


async def main():
    parser = argparse.ArgumentParser(description='Hedge Stock Bot V5 - Django ORM Integrated')
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--qty', type=float, required=True)
    parser.add_argument('--stopPct', type=float, default=1.0)
    parser.add_argument('--trailingPct', type=float, default=2.0)
    parser.add_argument('--longAccount', required=True)
    parser.add_argument('--shortAccount', required=True)
    parser.add_argument('--useAlgo', action='store_true')
    parser.add_argument('--port', type=int, default=7497)
    parser.add_argument('--name', default='', help='Optional bot name')

    # Contract configuration
    parser.add_argument('--primaryExchange', default='', help='Primary exchange (e.g., SBF for AIR)')
    parser.add_argument('--exchange', default='SMART', help='Routing exchange')
    parser.add_argument('--currency', default='USD', help='Currency (USD, EUR, etc.)')

    args = parser.parse_args()

    # Validate inputs
    if args.qty <= 0:
        print(f"Error: qty must be greater than 0 (provided: {args.qty})")
        return
    if args.stopPct <= 0:
        print(f"Error: stopPct must be greater than 0 (provided: {args.stopPct})")
        return
    if args.trailingPct <= 0:
        print(f"Error: trailingPct must be greater than 0 (provided: {args.trailingPct})")
        return

    # Create or get Bot
    bot, created = Bot.objects.get_or_create(
        symbol=args.symbol,
        long_account=args.longAccount,
        short_account=args.shortAccount,
        defaults={
            'name': args.name,
            'qty': Decimal(str(args.qty)),
            'stop_pct': Decimal(str(args.stopPct)),
            'trailing_pct': Decimal(str(args.trailingPct)),
            'port': args.port,
            'use_algo': args.useAlgo,
            'primary_exchange': args.primaryExchange,
            'exchange': args.exchange,
            'currency': args.currency,
            'status': 'IDLE'
        }
    )

    if not created:
        # Update bot parameters
        bot.qty = Decimal(str(args.qty))
        bot.stop_pct = Decimal(str(args.stopPct))
        bot.trailing_pct = Decimal(str(args.trailingPct))
        bot.port = args.port
        bot.use_algo = args.useAlgo
        bot.primary_exchange = args.primaryExchange
        bot.exchange = args.exchange
        bot.currency = args.currency
        if args.name:
            bot.name = args.name
        bot.save()

    current_state['bot'] = bot
    print(f"\nBot ID: {bot.id} | {bot.symbol} | {bot.long_account}/{bot.short_account}")

    # Connect to IBKR
    ib = IB()
    current_state['ib'] = ib
    ib.errorEvent += onError
    ib.commissionReportEvent += onCommissionReport

    try:
        await ib.connectAsync('127.0.0.1', args.port, clientId=10)
        bot.status = 'RUNNING'
        bot.started_at = datetime.now(timezone.utc)
        bot.save()
        log_event('BOT_START', 'INFO', f"Bot started: {bot.symbol}")

        # Contract setup - use bot configuration
        if bot.primary_exchange:
            # 4-arg constructor: Stock(symbol, primaryExchange, exchange, currency)
            contract = Stock(
                bot.symbol.upper(),
                bot.primary_exchange,
                bot.exchange,
                bot.currency
            )
        else:
            # 3-arg constructor: Stock(symbol, exchange, currency)
            contract = Stock(
                bot.symbol.upper(),
                bot.exchange,
                bot.currency
            )

        print(f"[CONTRACT] {contract}")
        await ib.qualifyContractsAsync(contract)
        current_state['contract'] = contract

        details = await ib.reqContractDetailsAsync(contract)
        min_tick = details[0].minTick if details else 0.01

        # Main cycle loop
        cycle_number = bot.cycles.count() + 1

        while True:
            reset_cycle_state()

            # Create new Cycle
            cycle = Cycle.objects.create(
                bot=bot,
                cycle_number=cycle_number,
                symbol=contract.symbol,
                contract_id=contract.conId,
                min_tick=Decimal(str(min_tick)),
                status='INITIALIZING'
            )
            current_state['cycle'] = cycle
            print(f"\n{'='*60}")
            print(f"CYCLE {cycle_number} STARTED - {contract.symbol}")
            print(f"{'='*60}")
            log_event('CYCLE_START', 'INFO', f"Cycle {cycle_number} started")

            tick = float(min_tick)

            # 1. Concurrent Entries
            print(f"\n>>> SUBMITTING CONCURRENT ENTRIES (Qty: {args.qty})...")
            cycle.status = 'ENTERING'
            cycle.save()

            l_ord = MarketOrder('BUY', args.qty, account=args.longAccount, tif='GTC',
                               orderRef=f"C{cycle_number}_LONG_ENTRY")
            s_ord = MarketOrder('SELL', args.qty, account=args.shortAccount, tif='GTC',
                               orderRef=f"C{cycle_number}_SHORT_ENTRY")

            if args.useAlgo and contract.currency == 'USD':
                for o in [l_ord, s_ord]:
                    o.algoStrategy = 'Adaptive'
                    o.algoParams = [TagValue('priority', 'Normal')]

            l_trade = ib.placeOrder(contract, l_ord)
            s_trade = ib.placeOrder(contract, s_ord)

            # Create Order records
            l_db_order = Order.objects.create(
                cycle=cycle,
                order_id=l_trade.order.orderId,
                order_ref=l_ord.orderRef,
                role='LONG_ENTRY',
                account=args.longAccount,
                action='BUY',
                order_type='MKT',
                total_quantity=Decimal(str(args.qty)),
                status='PendingSubmit'
            )
            s_db_order = Order.objects.create(
                cycle=cycle,
                order_id=s_trade.order.orderId,
                order_ref=s_ord.orderRef,
                role='SHORT_ENTRY',
                account=args.shortAccount,
                action='SELL',
                order_type='MKT',
                total_quantity=Decimal(str(args.qty)),
                status='PendingSubmit'
            )

            current_state['order_map'][l_trade.order.orderId] = l_db_order
            current_state['order_map'][s_trade.order.orderId] = s_db_order

            l_trade.fillEvent += onFill
            s_trade.fillEvent += onFill

            log_event('ORDER_SUBMITTED', 'INFO', "Long entry order submitted", order=l_db_order)
            log_event('ORDER_SUBMITTED', 'INFO', "Short entry order submitted", order=s_db_order)

            print("Waiting for both entries to fill...")
            deadline = asyncio.get_event_loop().time() + 30
            while not (l_trade.isDone() and s_trade.isDone()) and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.5)
                ib.waitOnUpdate()

            # Verify entry completion
            l_stat, s_stat = l_trade.orderStatus.status, s_trade.orderStatus.status
            l_filled, s_filled = l_trade.orderStatus.filled, s_trade.orderStatus.filled

            if l_stat != 'Filled' or s_stat != 'Filled' or l_filled == 0 or s_filled == 0:
                print(f"\n!!! CRITICAL: ENTRY FAILURE !!!")
                print(f"Long Status: {l_stat} (Filled: {l_filled})")
                print(f"Short Status: {s_stat} (Filled: {s_filled})")
                log_event('CYCLE_ABORT', 'ERROR', "Entry failure - aborting cycle")

                cycle.status = 'ABORTED'
                cycle.completed_at = datetime.now(timezone.utc)
                cycle.save()

                # Panic flatten
                for acc in [args.longAccount, args.shortAccount]:
                    pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account == acc]
                    if pos and pos[0].position != 0:
                        qty = pos[0].position
                        action = 'SELL' if qty > 0 else 'BUY'
                        print(f"Flattening position on {acc}: {abs(qty)} shares...")
                        ib.placeOrder(contract, MarketOrder(action, abs(qty), account=acc,
                                                           orderRef=f"C{cycle_number}_PANIC_FLATTEN"))
                        log_event('PANIC_FLATTEN', 'WARNING', f"Panic flatten on {acc}: {abs(qty)} shares")

                # Cleanup orphan orders
                for t in ib.openTrades():
                    if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                        ib.cancelOrder(t.order)

                await asyncio.sleep(5)
                cycle_number += 1
                continue

            if l_filled != s_filled:
                print(f"\nWARNING: Quantity mismatch! Long: {l_filled}, Short: {s_filled}. Proceeding with asymmetric protection.")
                log_event('SYSTEM_ERROR', 'WARNING', f"Quantity mismatch: Long {l_filled}, Short {s_filled}")

            # 4. Place Stop Losses
            print("\n>>> BOTH ENTRIES FILLED. PLACING PROTECTION...")
            cycle.status = 'ACTIVE'
            cycle.save()

            l_qty, s_qty = l_filled, s_filled
            l_price, s_price = l_trade.orderStatus.avgFillPrice, s_trade.orderStatus.avgFillPrice

            # Long SL (Sell Stop)
            l_sl_p = q_floor(l_price * (1 - args.stopPct/100), tick)
            print(f"Placing LONG Stop Loss on {args.longAccount} at {l_sl_p:.4f}...")
            l_sl_o = StopOrder('SELL', l_qty, l_sl_p, account=args.longAccount, tif='GTC', outsideRth=True,
                              orderRef=f"C{cycle_number}_LONG_SL")

            # Short SL (Buy Stop)
            s_sl_p = q_ceil(s_price * (1 + args.stopPct/100), tick)
            print(f"Placing SHORT Stop Loss on {args.shortAccount} at {s_sl_p:.4f}...")
            s_sl_o = StopOrder('BUY', s_qty, s_sl_p, account=args.shortAccount, tif='GTC', outsideRth=True,
                              orderRef=f"C{cycle_number}_SHORT_SL")

            l_sl_trade = ib.placeOrder(contract, l_sl_o)
            s_sl_trade = ib.placeOrder(contract, s_sl_o)

            current_state['active_trades']['long'] = l_sl_trade
            current_state['active_trades']['short'] = s_sl_trade

            # Create Stop Loss Order records
            l_sl_db = Order.objects.create(
                cycle=cycle,
                order_id=l_sl_trade.order.orderId,
                order_ref=l_sl_o.orderRef,
                role='LONG_SL',
                account=args.longAccount,
                action='SELL',
                order_type='STP',
                total_quantity=Decimal(str(l_qty)),
                stop_price=Decimal(str(l_sl_p)),
                status='PendingSubmit'
            )
            s_sl_db = Order.objects.create(
                cycle=cycle,
                order_id=s_sl_trade.order.orderId,
                order_ref=s_sl_o.orderRef,
                role='SHORT_SL',
                account=args.shortAccount,
                action='BUY',
                order_type='STP',
                total_quantity=Decimal(str(s_qty)),
                stop_price=Decimal(str(s_sl_p)),
                status='PendingSubmit'
            )

            current_state['order_map'][l_sl_trade.order.orderId] = l_sl_db
            current_state['order_map'][s_sl_trade.order.orderId] = s_sl_db

            for t in [l_sl_trade, s_sl_trade]:
                t.fillEvent += onFill

            log_event('ORDER_SUBMITTED', 'INFO', f"Long SL at {l_sl_p:.4f}", order=l_sl_db)
            log_event('ORDER_SUBMITTED', 'INFO', f"Short SL at {s_sl_p:.4f}", order=s_sl_db)

            print("Waiting for Stop Losses to reach live state...")
            armed_statuses = {'PreSubmitted', 'Submitted'}
            while any(t.orderStatus.status not in armed_statuses for t in [l_sl_trade, s_sl_trade]):
                await asyncio.sleep(0.1)
                ib.waitOnUpdate()
                if any(t.orderStatus.status in ('Inactive', 'Rejected') for t in [l_sl_trade, s_sl_trade]):
                    break

            # Verify protection armed
            if any(t.orderStatus.status in ('Inactive', 'Rejected') for t in [l_sl_trade, s_sl_trade]):
                print("\n!!! CRITICAL: PROTECTION FAILURE !!!")
                log_event('CYCLE_ABORT', 'ERROR', "Protection failure - aborting cycle")

                cycle.status = 'ABORTED'
                cycle.completed_at = datetime.now(timezone.utc)
                cycle.save()

                # Flatten positions
                for acc in [args.longAccount, args.shortAccount]:
                    pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account == acc]
                    if pos and pos[0].position != 0:
                        qty = pos[0].position
                        action = 'SELL' if qty > 0 else 'BUY'
                        print(f"Flattening position on {acc}: {abs(qty)} shares...")
                        ib.placeOrder(contract, MarketOrder(action, abs(qty), account=acc,
                                                           orderRef=f"C{cycle_number}_PROT_FAIL_FLATTEN"))
                        log_event('PANIC_FLATTEN', 'WARNING', f"Protection failure flatten on {acc}")

                # Cleanup
                for t in ib.openTrades():
                    if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                        ib.cancelOrder(t.order)

                await asyncio.sleep(2)
                report_pnl(is_final=True)
                await asyncio.sleep(5)
                cycle_number += 1
                continue

            # 6. Monitor until positions are flat
            print("\nHedge is ACTIVE. Monitoring positions...")
            while True:
                await asyncio.sleep(2)
                ib.waitOnUpdate()
                pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account in [args.longAccount, args.shortAccount]]
                if not pos or all(p.position == 0 for p in pos):
                    break

            print("\n>>> ALL POSITIONS CLOSED.")
            log_event('POSITION_FLAT', 'INFO', "All positions closed")

            cycle.status = 'EXITING'
            cycle.save()

            # Cleanup orphan orders
            print("Cleaning up any remaining orphan orders...")
            for t in ib.openTrades():
                if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                    print(f"Cancelling orphan {t.order.orderType} order {t.order.orderId} on {t.order.account}...")
                    ib.cancelOrder(t.order)

            await asyncio.sleep(2)

            # Finalize cycle
            cycle.status = 'COMPLETED'
            cycle.completed_at = datetime.now(timezone.utc)
            cycle.save()

            report_pnl(is_final=True)
            log_event('CYCLE_COMPLETE', 'INFO', f"Cycle {cycle_number} completed with P&L: {cycle.net_pnl:.2f}")

            print("\n>>> CYCLE COMPLETE. Waiting before next cycle...")
            await asyncio.sleep(10)
            cycle_number += 1

    except KeyboardInterrupt:
        print("\n\nShutdown requested by user...")
        log_event('BOT_STOP', 'INFO', "Bot stopped by user")
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        log_event('SYSTEM_ERROR', 'CRITICAL', f"Fatal error: {str(e)}")
        raise
    finally:
        bot.status = 'STOPPED'
        bot.stopped_at = datetime.now(timezone.utc)
        bot.save()
        ib.disconnect()
        print("Disconnected from IBKR. Goodbye!")


if __name__ == '__main__':
    asyncio.run(main())
