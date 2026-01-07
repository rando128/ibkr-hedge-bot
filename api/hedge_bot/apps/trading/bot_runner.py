"""
Bot Runner - Executes hedge bot logic for a Bot instance

This module contains the core bot execution logic that can be invoked
by Celery/Procrastinate tasks or management commands.
"""

import asyncio
import math
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional

from asgiref.sync import sync_to_async
from django.db import transaction
from ib_insync import MarketOrder

from .models import Bot, Cycle, Order, Execution, Event

# Note: ib_insync import is delayed until inside async context to avoid event loop issues

import time


class BotRunner:
    """
    Manages the execution of a single bot instance.
    """

    def __init__(self, bot_id: int):
        self.bot_id = bot_id
        self.bot: Optional[Bot] = None
        self.ib = None
        self.contract = None
        self.should_stop = False

        # Current cycle state
        self.cycle: Optional[Cycle] = None
        self.active_trades = {'long': None, 'short': None}
        self.order_map = {}  # Maps IBKR orderId -> Django Order instance
        self.exec_ids_seen = set()  # Track fills to avoid duplicates
        self.commission_ids_seen = set()  # Track commission reports separately
        self.transitioning = False
        self.transition_done = False
        self.connectivity_down = False  # Track IBKR↔TWS connectivity (1100/1101 down, 1102/1103 up)

        # Heartbeat tracking (for stale worker detection)
        self.last_heartbeat = 0  # timestamp of last heartbeat
        self.last_reconnect_attempt = 0
        self.client_id = None
        self.host = '127.0.0.1'
        self.port = None

        # Note: Signal handlers don't work in worker threads
        # Instead, we poll bot.status in check_bot_status() method

    async def log_event(self, event_type, level, message, order=None, data=None):
        """Log an event to the database"""
        try:
            @sync_to_async
            def create_event():
                Event.objects.create(
                    bot=self.bot,
                    cycle=self.cycle,
                    order=order,
                    event_type=event_type,
                    level=level,
                    message=message,
                    data=data
                )

            await create_event()
            print(f"[{level}] {message}")
        except Exception as e:
            print(f"[ERROR] Failed to log event: {e}")

    @staticmethod
    def q_floor(price, tick):
        """Round price down to nearest tick"""
        return math.floor((float(price) + 1e-12) / float(tick)) * float(tick)

    @staticmethod
    def q_ceil(price, tick):
        """Round price up to nearest tick"""
        return math.ceil((float(price) - 1e-12) / float(tick)) * float(tick)

    @staticmethod
    def get_exchange_tick_size(price, primary_exchange, base_tick):
        """
        Get the correct tick size for a given price and exchange.
        Some exchanges use variable tick sizes based on price level.
        """
        # Euronext exchanges (Paris/SBF, Amsterdam, Brussels, etc.)
        if primary_exchange in ['SBF', 'AEB', 'EBR']:
            if price < 50:
                return 0.01
            elif price < 100:
                return 0.05
            elif price < 500:
                return 0.10
            else:
                return 0.50

        # For other exchanges, use the base tick from IBKR
        return base_tick

    @staticmethod
    def fmt_ts(exec_time):
        """Format timestamp for logging"""
        if hasattr(exec_time, 'strftime'):
            return exec_time.strftime('%H:%M:%S')
        if isinstance(exec_time, str):
            return exec_time[-8:] if len(exec_time) >= 8 else exec_time
        return datetime.now().strftime('%H:%M:%S')

    @staticmethod
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

    async def send_heartbeat(self):
        """Send heartbeat to update worker_last_heartbeat timestamp"""
        import time

        # Only send heartbeat every 30 seconds to reduce DB load
        now = time.time()
        if now - self.last_heartbeat < 30:
            return

        self.last_heartbeat = now

        @sync_to_async
        def update_heartbeat():
            from django.utils import timezone
            self.bot.worker_last_heartbeat = timezone.now()
            self.bot.save(update_fields=['worker_last_heartbeat'])

        await update_heartbeat()

    async def ensure_connection(self):
        """Ensure IB connection is alive; attempt reconnection and rehydration on failure."""
        import time

        if self.ib and self.ib.isConnected():
            return True

        # Simple backoff to avoid hammering reconnect attempts
        now = time.time()
        if now - self.last_reconnect_attempt < 5:
            return False

        self.last_reconnect_attempt = now
        try:
            print("[IBKR] Connection lost. Attempting reconnect...")
            await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)

            # Rehydrate positions/orders so callbacks can be reattached
            await self.ib.reqPositionsAsync()
            await self.ib.reqAllOpenOrdersAsync()
            await asyncio.sleep(0.5)
            await self.rehydrate_open_trades()
            # On reconnect, backfill any executions that happened while we were down and reconcile DB state
            await self.backfill_executions()
            await self.reconcile_cycle_state()
            await self.log_event('IBKR_ERROR', 'WARNING', "Reconnected to IBKR after disconnect")
            print("[IBKR] Reconnected and rehydrated open orders/positions.")
            return True
        except Exception as e:
            print(f"[IBKR] Reconnect failed: {e}")
            await self.log_event('IBKR_ERROR', 'ERROR', f"Reconnect failed: {e}")
            return False

    async def rehydrate_open_trades(self):
        """
        Re-attach callbacks to open trades after a reconnect so stop losses/trailing stops keep working.
        """
        if not self.cycle:
            return

        # Refresh open orders snapshot
        open_trades = [
            t for t in self.ib.openTrades()
            if t.contract.conId == self.contract.conId
            and t.order.account in [self.bot.long_account, self.bot.short_account]
        ]

        order_ids = [t.order.orderId for t in open_trades]

        @sync_to_async
        def fetch_db_orders():
            return {o.order_id: o for o in self.cycle.orders.filter(order_id__in=order_ids)}

        db_orders = await fetch_db_orders()

        for t in open_trades:
            db_order = self.order_map.get(t.order.orderId) or db_orders.get(t.order.orderId)
            if not db_order:
                continue

            self.order_map[t.order.orderId] = db_order
            t.fillEvent += self.on_fill

            # Track active protection orders
            if db_order.role in ('LONG_SL', 'LONG_TRAIL'):
                self.active_trades['long'] = t
            elif db_order.role in ('SHORT_SL', 'SHORT_TRAIL'):
                self.active_trades['short'] = t

    async def _persist_exec_common(self, order, exec_report, exec_time):
        """
        Persist an execution and update order/cycle P&L. Shared by backfill and execDetails handler.
        """
        side_map = {'BOT': 'BUY', 'SLD': 'SELL'}
        side = side_map.get(exec_report.side, exec_report.side)
        shares = Decimal(str(exec_report.shares))
        price = Decimal(str(exec_report.price))
        avg_price = Decimal(str(exec_report.avgPrice or exec_report.price or 0))

        @sync_to_async
        def persist():
            with transaction.atomic():
                order_locked = Order.objects.select_for_update().get(pk=order.pk)
                cycle_locked = Cycle.objects.select_for_update().get(pk=order_locked.cycle_id)

                prev_status = order_locked.status
                prev_filled = order_locked.filled_quantity

                Execution.objects.create(
                    order=order_locked,
                    cycle=cycle_locked,
                    exec_id=exec_report.execId,
                    side=side,
                    shares=shares,
                    price=price,
                    account=exec_report.acctNumber,
                    executed_at=exec_time
                )

                order_locked.filled_quantity += shares
                order_locked.status = 'Filled' if order_locked.filled_quantity >= order_locked.total_quantity else 'PartiallyFilled'
                order_locked.avg_fill_price = avg_price
                if getattr(exec_report, 'permId', None):
                    order_locked.perm_id = getattr(exec_report, 'permId')
                if order_locked.status == 'Filled':
                    order_locked.filled_at = exec_time
                order_locked.save()

                if side == 'BUY':
                    cycle_locked.total_buys += shares * price
                else:
                    cycle_locked.total_sells += shares * price

                cycle_locked.net_pnl = cycle_locked.total_sells - cycle_locked.total_buys - cycle_locked.total_commission
                cycle_locked.save()

                print(
                    f"[DB] Order {order_locked.order_id} ({order_locked.role}) "
                    f"{prev_status}->{order_locked.status}, filled {prev_filled}->{order_locked.filled_quantity} "
                    f"avg {order_locked.avg_fill_price}"
                )
                print(
                    f"[DB] Cycle {cycle_locked.id} totals: buys={cycle_locked.total_buys} "
                    f"sells={cycle_locked.total_sells} pnl={cycle_locked.net_pnl}"
                )
                return order_locked, cycle_locked

        return await persist()

    async def backfill_executions(self):
        """
        Pull recent executions from IBKR and reconcile any fills that happened while the worker was down.
        This helps capture stop-loss executions that were missed during downtime.
        """
        if not self.cycle or not self.contract:
            return 0

        from ib_insync import ExecutionFilter
        from django.utils import timezone as dj_timezone

        # Look slightly before the last known execution to ensure we don't miss anything
        @sync_to_async
        def get_backfill_start():
            last_exec = self.cycle.executions.order_by('-executed_at').first()
            cycle_start = self.cycle.created_at or dj_timezone.now()

            if last_exec:
                # Use the later of: last_exec (with buffer) or cycle creation
                # to avoid importing executions from prior cycles
                recent_anchor = last_exec.executed_at - timedelta(minutes=5)
                anchor = max(recent_anchor, cycle_start)
                print(f"[BACKFILL] Using anchor: {anchor.strftime('%Y-%m-%d %H:%M:%S')} (last_exec: {last_exec.executed_at.strftime('%Y-%m-%d %H:%M:%S')}, cycle_start: {cycle_start.strftime('%Y-%m-%d %H:%M:%S')})")
                return anchor.astimezone(timezone.utc)
            else:
                print(f"[BACKFILL] No executions yet, using cycle start: {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
                return cycle_start.astimezone(timezone.utc)

        start_dt = await get_backfill_start()
        # Use explicit UTC format to avoid IBKR warning about implied time zones
        start_str = start_dt.strftime('%Y%m%d-%H:%M:%S')

        accounts = [self.bot.long_account, self.bot.short_account]
        filters = [
            ExecutionFilter(acctCode=acc, symbol=self.contract.symbol, time=start_str)
            for acc in accounts
        ]

        # First, pull open orders to enrich permIds/status for this cycle
        await self.ib.reqAllOpenOrdersAsync()
        await asyncio.sleep(0.5)
        open_orders = [
            o for o in self.ib.openOrders()
            if o.account in accounts
        ]

        @sync_to_async
        def sync_perm_ids():
            updated = 0
            for o in open_orders:
                try:
                    order = self.cycle.orders.get(order_id=o.orderId, account=o.account)
                except Order.DoesNotExist:
                    continue
                changed = False
                if getattr(o, 'permId', None) and order.perm_id != o.permId:
                    order.perm_id = o.permId
                    changed = True
                order_state = getattr(o, 'orderState', None)
                state_status = getattr(order_state, 'status', None)
                if order.status in ('PendingSubmit', 'PreSubmitted') and state_status:
                    order.status = state_status
                    changed = True
                if changed:
                    order.save()
                    updated += 1
            return updated

        updated_perm = await sync_perm_ids()
        if updated_perm:
            print(f"[BACKFILL] Refreshed {updated_perm} orders from openOrders (permId/status sync)")

        new_execs = 0

        @sync_to_async
        def execution_exists(exec_id):
            return Execution.objects.filter(exec_id=exec_id).exists()

        @sync_to_async
        def find_order(perm_id, order_id, account, order_ref):
            qs = self.cycle.orders.filter(account=account)
            # Prefer permId
            if perm_id:
                order = qs.filter(perm_id=perm_id).first()
                if order:
                    return order
            # OrderRef carries cycle number (e.g., C{cycle}_LONG_SL)
            if order_ref:
                order = qs.filter(order_ref=order_ref).first()
                if order:
                    return order
            # Fallback to order_id
            return qs.filter(order_id=order_id).first()

        for flt in filters:
            try:
                reports = await self.ib.reqExecutionsAsync(flt)
            except Exception as e:
                await self.log_event('SYSTEM_ERROR', 'ERROR', f"Execution backfill failed: {e}")
                continue

            for rpt in reports:
                # reqExecutionsAsync may yield Execution or Fill; normalize fields
                exec_obj = getattr(rpt, 'execution', rpt)
                exec_id = getattr(exec_obj, 'execId', None)
                perm_id = getattr(exec_obj, 'permId', None)
                order_id = getattr(exec_obj, 'orderId', None)
                account = getattr(exec_obj, 'acctNumber', None)
                side = getattr(exec_obj, 'side', None)
                shares = getattr(exec_obj, 'shares', None)
                price = getattr(exec_obj, 'price', None)
                avg_price = getattr(exec_obj, 'avgPrice', None)
                exec_time_raw = getattr(exec_obj, 'time', None)
                order_ref = getattr(exec_obj, 'orderRef', None)

                if not exec_id:
                    await self.log_event('BACKFILL_SKIP_NO_ID', 'WARNING', f"Skipping execution without execId (orderId={order_id}, account={account})")
                    continue

                # Skip duplicates already seen/recorded
                if exec_id in self.exec_ids_seen or await execution_exists(exec_id):
                    self.exec_ids_seen.add(exec_id)
                    continue

                order = await find_order(perm_id, order_id, account, order_ref)
                if not order:
                    await self.log_event(
                        'BACKFILL_MISSING_ORDER',
                        'WARNING',
                        f"Execution {exec_id} has no matching order (permId={perm_id}, orderId={order_id}, account={account}, orderRef={order_ref})"
                    )
                    continue

                # Rebuild a lightweight exec_report-like object for persist_execution
                class _Exec:
                    pass
                exec_report = _Exec()
                exec_report.execId = exec_id
                exec_report.permId = perm_id
                exec_report.orderId = order_id
                exec_report.acctNumber = account
                exec_report.side = side
                exec_report.shares = shares
                exec_report.price = price
                exec_report.avgPrice = avg_price
                exec_report.orderRef = order_ref

                exec_time = self.get_safe_timestamp(exec_time_raw)
                try:
                    order_locked, cycle_locked = await self._persist_exec_common(order, exec_report, exec_time)
                    self.order_map[order_locked.order_id] = order_locked
                    self.cycle = cycle_locked
                    self.exec_ids_seen.add(exec_id)
                    # If this was a stop loss fill detected via backfill, kick off trailing transition
                    if order_locked.role in ('LONG_SL', 'SHORT_SL'):
                        await self.transition_from_backfill(order_locked, exec_report, exec_time)
                    new_execs += 1
                except Exception as e:
                    await self.log_event('SYSTEM_ERROR', 'ERROR', f"Failed to persist backfilled execution {exec_id}: {e}")

        if new_execs:
            await self.log_event('BACKFILL_APPLIED', 'INFO', f"Backfilled {new_execs} executions from IBKR")

        return new_execs

    async def transition_from_backfill(self, order, exec_report, exec_time):
        """
        Handle stop-loss transition when the fill was detected via backfill (worker was down).
        """
        if self.transitioning or self.transition_done:
            return
        self.transitioning = True
        transitioned = False

        try:
            await self.log_event('STOP_LOSS_HIT', 'WARNING', f"Stop loss (backfill) on {order.account}")

            @sync_to_async
            def mark_transitioning():
                self.cycle.status = 'TRANSITIONING'
                self.cycle.save()
            await mark_transitioning()

            hit_leg = 'long' if order.account == self.bot.long_account else 'short'
            surviving_leg = 'short' if hit_leg == 'long' else 'long'
            surviving_acc = self.bot.long_account if surviving_leg == 'long' else self.bot.short_account

            # Cancel surviving stop if still open
            surviving_trade = None
            for t in self.ib.openTrades():
                if (t.contract.conId == self.contract.conId and
                        t.order.account == surviving_acc and
                        t.order.orderType in ('STP', 'STP LMT') and
                        t.orderStatus.status in ('PreSubmitted', 'Submitted')):
                    surviving_trade = t
                    break

            if surviving_trade:
                self.ib.cancelOrder(surviving_trade.order)
                deadline = asyncio.get_event_loop().time() + 10
                while not surviving_trade.isDone() and asyncio.get_event_loop().time() < deadline:
                    await asyncio.sleep(0.1)
                    self.ib.waitOnUpdate()

                st = surviving_trade.orderStatus.status
                if st not in ('Cancelled', 'ApiCancelled'):
                    # Already filled or not cancellable; abort trailing placement
                    await self.log_event('ORDER_CANCELLED', 'INFO',
                                         f"Surviving stop not cancelled (status={st}), skipping trailing")
                    return

            # Place trailing on surviving position
            await self.ib.reqPositionsAsync()
            await asyncio.sleep(0.5)
            pos = [p for p in self.ib.positions()
                   if p.contract.conId == self.contract.conId and p.account == surviving_acc and p.position != 0]
            if not pos:
                await self.log_event('TRANSITION_COMPLETE', 'INFO', f"No position on {surviving_acc}, nothing to trail")
                return

            qty = abs(pos[0].position)
            market_price = Decimal(str(exec_report.price or exec_report.avgPrice or 0))
            tick = float(self.cycle.min_tick)

            label = surviving_leg.upper()
            action = 'SELL' if label == 'LONG' else 'BUY'
            if label == 'LONG':
                trail_price = self.q_floor(float(market_price) * (1 - (float(self.bot.trailing_pct) / 100)), tick)
            else:
                trail_price = self.q_ceil(float(market_price) * (1 + (float(self.bot.trailing_pct) / 100)), tick)

            from ib_insync import Order as IBOrder
            trail_order = IBOrder(
                action=action, totalQuantity=qty, orderType='TRAIL',
                trailingPercent=float(self.bot.trailing_pct), account=surviving_acc, tif='GTC', outsideRth=True,
                orderRef=f"C{self.cycle.cycle_number}_{label}_TRAIL"
            )
            t_trade = self.ib.placeOrder(self.contract, trail_order)
            t_trade.fillEvent += self.on_fill
            t_trade.statusEvent += self.on_trailing_stop_status
            self.active_trades[surviving_leg] = t_trade

            @sync_to_async
            def create_trail_order():
                return Order.objects.create(
                    cycle=self.cycle,
                    order_id=t_trade.order.orderId,
                    order_ref=trail_order.orderRef,
                    role=f"{label}_TRAIL",
                    account=surviving_acc,
                    action=action,
                    order_type='TRAIL',
                    total_quantity=Decimal(str(qty)),
                    trailing_percent=self.bot.trailing_pct,
                    status='PendingSubmit'
                )

            db_order = await create_trail_order()
            self.order_map[t_trade.order.orderId] = db_order
            await self.log_event('TRANSITION_COMPLETE', 'INFO', f"Placed {label} trailing stop (backfill)", order=db_order)

            transitioned = True
        finally:
            if transitioned:
                self.transition_done = True

                @sync_to_async
                def update_cycle_status():
                    self.cycle.status = 'ACTIVE'
                    self.cycle.save()
                await update_cycle_status()
            self.transitioning = False

    async def reconcile_cycle_state(self):
        """
        Reconcile DB orders/cycle against current IB open trades to repair stale statuses after downtime.
        """
        if not self.cycle:
            return 0

        # Use both openOrders (true live orders) and openTrades (for status/fill details)
        open_orders = [
            o for o in self.ib.openOrders()
            if o.account in [self.bot.long_account, self.bot.short_account]
        ]
        open_trades = [
            t for t in self.ib.openTrades()
            if t.contract.conId == self.contract.conId
            and t.order.account in [self.bot.long_account, self.bot.short_account]
        ]

        open_orders_map = {o.orderId: o for o in open_orders}
        open_trades_map = {t.order.orderId: t for t in open_trades}
        pending_statuses = {'PendingSubmit', 'PreSubmitted', 'Submitted', 'PartiallyFilled'}
        updates = 0

        @sync_to_async
        def load_orders():
            return list(self.cycle.orders.select_related('cycle').prefetch_related('executions'))

        orders = await load_orders()

        for order in orders:
            trade = open_trades_map.get(order.order_id)
            open_order = open_orders_map.get(order.order_id)
            if trade or open_order:
                if trade:
                    status = trade.orderStatus.status
                    filled_qty = Decimal(str(trade.orderStatus.filled or 0))
                    avg_price = Decimal(str(trade.orderStatus.avgFillPrice or order.avg_fill_price or 0))
                    perm_id = getattr(trade.order, 'permId', None)
                else:
                    # We have an open order but no trade object; treat as submitted
                    status = 'Submitted' if order.status in ('PendingSubmit', 'PreSubmitted') else order.status
                    filled_qty = order.filled_quantity
                    avg_price = order.avg_fill_price
                    perm_id = getattr(open_order, 'permId', None)

                @sync_to_async
                def update_open_order():
                    changed = False
                    if order.status != status:
                        order.status = status
                        changed = True
                    if order.filled_quantity != filled_qty:
                        order.filled_quantity = filled_qty
                        changed = True
                    if avg_price and order.avg_fill_price != avg_price:
                        order.avg_fill_price = avg_price
                        changed = True
                    if perm_id and order.perm_id != perm_id:
                        order.perm_id = perm_id
                        changed = True
                    if changed:
                        order.save()
                    return changed

                if await update_open_order():
                    updates += 1
                continue

            # No open trade for this order - align status with executions
            if order.status in pending_statuses:
                execs = list(order.executions.all())
                last_exec = max(execs, key=lambda e: e.executed_at) if execs else None
                new_status = None
                if execs:
                    exec_qty = sum(e.shares for e in execs)
                    exec_value = sum(e.shares * e.price for e in execs)
                    avg_exec_price = exec_value / exec_qty if exec_qty else order.avg_fill_price
                    if order.filled_quantity != exec_qty:
                        order.filled_quantity = exec_qty
                    if order.avg_fill_price != avg_exec_price:
                        order.avg_fill_price = avg_exec_price
                    if exec_qty >= order.total_quantity and order.total_quantity > 0:
                        new_status = 'Filled'
                    elif exec_qty > 0:
                        new_status = 'PartiallyFilled'

                if new_status:
                    @sync_to_async
                    def mark_filled():
                        order.status = new_status
                        if last_exec:
                            order.filled_at = last_exec.executed_at
                        order.save()
                    await mark_filled()
                    updates += 1

        if updates:
            # Refresh P&L to reflect any recovered fills
            @sync_to_async
            def refresh_pnl():
                self.cycle.update_pnl()
                self.cycle.refresh_from_db()
            await refresh_pnl()
            await self.log_event('CYCLE_RECONCILED', 'WARNING', f"Reconciled {updates} orders with IB snapshot")
        return updates

    async def validate_tws_db_consistency(self):
        """
        Validate TWS state matches DB state for safe cycle recovery.

        This ensures that positions and orders in TWS match what we expect from DB records,
        preventing corruption from manual interventions or other bot instances.

        Returns:
            tuple: (is_consistent: bool, discrepancies: list of strings)
        """
        if not self.cycle or not self.contract:
            return True, []

        discrepancies = []

        # Get TWS state
        tws_positions = {}  # account -> qty
        for pos in self.ib.positions():
            if (pos.contract.conId == self.contract.conId and
                pos.account in [self.bot.long_account, self.bot.short_account] and
                pos.position != 0):
                tws_positions[pos.account] = pos.position

        tws_orders = {}  # orderId -> order
        tws_orders_by_perm = {}  # permId -> order
        for trade in self.ib.openTrades():
            if (trade.contract.conId == self.contract.conId and
                trade.order.account in [self.bot.long_account, self.bot.short_account]):
                tws_orders[trade.order.orderId] = trade.order
                if hasattr(trade.order, 'permId') and trade.order.permId:
                    tws_orders_by_perm[trade.order.permId] = trade.order

        # Get DB state
        @sync_to_async
        def get_db_state():
            # Calculate net position from executions
            db_positions = {}  # account -> net qty
            for execution in self.cycle.executions.all():
                account = execution.account
                qty = float(execution.shares)
                if execution.side == 'BUY':
                    db_positions[account] = db_positions.get(account, 0) + qty
                else:  # SELL
                    db_positions[account] = db_positions.get(account, 0) - qty

            # Get DB orders that should still be active
            db_orders = {}  # orderId -> Order
            db_orders_by_perm = {}  # permId -> Order
            active_statuses = ['PendingSubmit', 'PreSubmitted', 'Submitted', 'PartiallyFilled']
            for order in self.cycle.orders.filter(status__in=active_statuses):
                if order.order_id:
                    db_orders[order.order_id] = order
                if order.perm_id:
                    db_orders_by_perm[order.perm_id] = order

            return db_positions, db_orders, db_orders_by_perm

        db_positions, db_orders, db_orders_by_perm = await get_db_state()

        print(f"\n[VALIDATION] Comparing TWS vs DB state:")
        print(f"  TWS positions: {tws_positions}")
        print(f"  DB positions: {db_positions}")
        print(f"  TWS orders: {len(tws_orders)} open")
        print(f"  DB orders: {len(db_orders)} active")

        # 1. Validate positions match
        all_accounts = set(tws_positions.keys()) | set(db_positions.keys())
        for account in all_accounts:
            tws_qty = tws_positions.get(account, 0)
            db_qty = db_positions.get(account, 0)

            # Allow small floating point differences (< 0.01 shares)
            if abs(tws_qty - db_qty) > 0.01:
                discrepancies.append(
                    f"Position mismatch on {account}: TWS has {tws_qty} shares, "
                    f"DB records {db_qty} shares from executions"
                )

        # 2. Validate orders - check for unknown orders in TWS
        for order_id, tws_order in tws_orders.items():
            perm_id = getattr(tws_order, 'permId', None)

            # Try to find this order in DB by orderId or permId
            found = False
            if order_id in db_orders:
                found = True
            elif perm_id and perm_id in db_orders_by_perm:
                found = True

            if not found:
                discrepancies.append(
                    f"Unknown order in TWS: orderId={order_id}, permId={perm_id}, "
                    f"account={tws_order.account}, {tws_order.action} {tws_order.totalQuantity} {tws_order.orderType}"
                )

        # 3. Check for orders in DB that are missing in TWS (might have filled/cancelled during downtime)
        # Note: This is informational only, not a hard error, as backfill should have caught fills
        for order_id, db_order in db_orders.items():
            perm_id = db_order.perm_id

            # Try to find this order in TWS
            found = False
            if order_id in tws_orders:
                found = True
            elif perm_id and perm_id in tws_orders_by_perm:
                found = True

            if not found:
                # Order in DB as active but not in TWS - check if it was filled
                @sync_to_async
                def check_filled():
                    return db_order.filled_quantity >= db_order.total_quantity and db_order.total_quantity > 0

                is_filled = await check_filled()
                if not is_filled:
                    # Not filled and not in TWS - possible problem
                    discrepancies.append(
                        f"DB order {order_id} ({db_order.role}) marked as '{db_order.status}' "
                        f"but not found in TWS open orders (might have been cancelled externally)"
                    )

        is_consistent = len(discrepancies) == 0

        if is_consistent:
            print(f"[VALIDATION] ✓ TWS and DB states are consistent")
            await self.log_event('VALIDATION_PASSED', 'INFO', "TWS-DB state validation passed")
        else:
            print(f"[VALIDATION] ✗ Found {len(discrepancies)} discrepancies:")
            for disc in discrepancies:
                print(f"  - {disc}")
            await self.log_event('VALIDATION_FAILED', 'ERROR',
                               f"TWS-DB validation failed with {len(discrepancies)} discrepancies",
                               data={'discrepancies': discrepancies})

        return is_consistent, discrepancies

    async def check_bot_status(self):
        """Check if bot should continue running and send heartbeat"""
        @sync_to_async
        def refresh_bot():
            self.bot.refresh_from_db()
            return self.bot.status

        status = await refresh_bot()

        # Send heartbeat while checking status
        await self.send_heartbeat()

        # Verify IB connection; attempt reconnect when needed
        if not await self.ensure_connection():
            self.should_stop = True
            await self.log_event('SYSTEM_ERROR', 'ERROR', "IBKR connection lost and reconnect failed")
            return False

        if status != 'RUNNING':
            self.should_stop = True
            print(f"\n[BOT] Stop detected - status changed to {status}")
            await self.log_event('BOT_STOP', 'INFO', f"Bot status changed to {status}")
            return False
        return True

    def reset_cycle_state(self):
        """Reset state for a new cycle"""
        self.cycle = None
        self.active_trades = {'long': None, 'short': None}
        self.order_map.clear()
        self.exec_ids_seen.clear()
        self.commission_ids_seen.clear()  # Reset commission tracking too
        self.transitioning = False
        self.transition_done = False

    def on_error(self, trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        """Handle IBKR errors"""
        if reqId == -1:
            return
        msg = errorString if errorString else str(errorCode)
        code = errorCode if errorString else "INFO"

        # Track IBKR <-> TWS connectivity health
        if errorCode in (1100, 1101):
            self.connectivity_down = True
        elif errorCode in (1102, 1103):
            self.connectivity_down = False

        # Schedule async logging (can't await in sync callback)
        asyncio.create_task(self.log_event('IBKR_ERROR', 'WARNING', f"IBKR {code}: {msg} (reqId={reqId})",
                                          data={'reqId': reqId, 'errorCode': code}))

    def on_commission_report(self, trade, fill, report):
        """Handle commission reports"""
        exec_id = getattr(report, 'execId', None) or fill.execution.execId

        # Use separate tracking set for commissions
        if exec_id in self.commission_ids_seen:
            return
        self.commission_ids_seen.add(exec_id)

        # Schedule async handling (can't do sync DB operations in callback)
        asyncio.create_task(self._handle_commission_async(exec_id, report, fill))

    async def _handle_commission_async(self, exec_id, report, fill):
        """Handle commission report asynchronously"""
        from asgiref.sync import sync_to_async

        try:
            @sync_to_async
            def update_commission():
                try:
                    execution = Execution.objects.get(exec_id=exec_id)
                    execution.commission = Decimal(str(report.commission))
                    execution.commission_currency = report.currency or 'USD'
                    execution.save()

                    # Update cycle P&L (lookup from execution, works even after cycle completes)
                    with transaction.atomic():
                        cycle = Cycle.objects.select_for_update().get(pk=execution.cycle_id)
                        cycle.total_commission += Decimal(str(report.commission))
                        cycle.net_pnl = cycle.total_sells - cycle.total_buys - cycle.total_commission
                        cycle.save()

                        # Update self.cycle only if it's still the current cycle
                        if self.cycle and self.cycle.pk == cycle.pk:
                            self.cycle = cycle

                    return True
                except Execution.DoesNotExist:
                    return False

            result = await update_commission()

            if result:
                exec_time = self.get_safe_timestamp(fill.execution.time)
                print(f"[{self.fmt_ts(exec_time)}] [COMMISSION]: {report.commission:.2f} {report.currency}")
        except Exception as e:
            print(f"[ERROR] Commission report handling failed: {e}")

    def on_exec_details(self, trade, fill):
        """
        Capture executions even when we don't have a Trade object (e.g., after restart).
        """
        asyncio.create_task(self._handle_exec_details_async(trade, fill))

    def on_order_status(self, trade):
        """Global order status handler (covers recovery when per-trade callbacks are missing)."""
        asyncio.create_task(self._handle_order_status_async(trade))

    async def _handle_exec_details_async(self, trade, fill):
        exec_obj = fill.execution
        exec_id = getattr(exec_obj, 'execId', None)
        if not exec_id:
            return
        if exec_id in self.exec_ids_seen:
            return
        self.exec_ids_seen.add(exec_id)

        order = None
        if trade and trade.order.orderId in self.order_map:
            order = self.order_map[trade.order.orderId]
        if not order:
            # Early exit if cycle not yet initialized (race condition during startup)
            if not self.cycle:
                return

            @sync_to_async
            def find_order():
                qs = self.cycle.orders.filter(account=getattr(exec_obj, 'acctNumber', None))
                perm_id = getattr(exec_obj, 'permId', None)
                order_ref = getattr(exec_obj, 'orderRef', None)
                order_id = getattr(exec_obj, 'orderId', None)
                if perm_id:
                    o = qs.filter(perm_id=perm_id).first()
                    if o:
                        return o
                if order_ref:
                    o = qs.filter(order_ref=order_ref).first()
                    if o:
                        return o
                if order_id:
                    return qs.filter(order_id=order_id).first()
                return None

            order = await find_order()

        if not order:
            await self.log_event('BACKFILL_MISSING_ORDER', 'WARNING',
                                 f"ExecDetails {exec_id} missing order (orderId={getattr(exec_obj, 'orderId', None)}, permId={getattr(exec_obj, 'permId', None)}, ref={getattr(exec_obj, 'orderRef', None)})")
            return

        # Build a lightweight report object to reuse backfill persistence logic
        class _Exec:
            pass
        exec_report = _Exec()
        exec_report.execId = exec_id
        exec_report.permId = getattr(exec_obj, 'permId', None)
        exec_report.orderId = getattr(exec_obj, 'orderId', None)
        exec_report.acctNumber = getattr(exec_obj, 'acctNumber', None)
        exec_report.side = getattr(exec_obj, 'side', None)
        exec_report.shares = getattr(exec_obj, 'shares', None)
        exec_report.price = getattr(exec_obj, 'price', None)
        exec_report.avgPrice = getattr(exec_obj, 'avgPrice', None)
        exec_report.orderRef = getattr(exec_obj, 'orderRef', None)

        exec_time = self.get_safe_timestamp(getattr(exec_obj, 'time', None))

        try:
            order_locked, cycle_locked = await self._persist_exec_common(order, exec_report, exec_time)
            self.order_map[order_locked.order_id] = order_locked
            self.cycle = cycle_locked
            if order_locked.role in ('LONG_SL', 'SHORT_SL'):
                await self.transition_from_backfill(order_locked, exec_report, exec_time)
        except Exception as e:
            await self.log_event('SYSTEM_ERROR', 'ERROR', f"ExecDetails persist failed {exec_id}: {e}")

    async def _handle_order_status_async(self, trade):
        order_status = trade.orderStatus if trade else None
        order_obj = getattr(trade, 'order', None)
        order_id = getattr(order_status, 'orderId', None) or (order_obj.orderId if order_obj else None)
        perm_id = getattr(order_status, 'permId', None) or (order_obj.permId if order_obj else None)
        avg_price = getattr(order_status, 'avgFillPrice', None)
        filled = getattr(order_status, 'filled', 0) or 0
        status = getattr(order_status, 'status', None)
        account = getattr(order_obj, 'account', None) if order_obj else None
        order_ref = getattr(order_obj, 'orderRef', None) if order_obj else None

        if not order_id and not perm_id and not order_ref:
            return

        # Early exit if cycle not yet initialized (race condition during startup)
        if not self.cycle:
            return

        @sync_to_async
        def find_order():
            qs = self.cycle.orders.all()
            if account:
                qs = qs.filter(account=account)
            if perm_id:
                o = qs.filter(perm_id=perm_id).first()
                if o:
                    return o
            if order_ref:
                o = qs.filter(order_ref=order_ref).first()
                if o:
                    return o
            if order_id is not None:
                return qs.filter(order_id=order_id).first()
            return None

        db_order = await find_order()
        if not db_order:
            await self.log_event('BACKFILL_MISSING_ORDER', 'WARNING',
                                 f"OrderStatus for unknown order (orderId={order_id}, permId={perm_id}, ref={order_ref}, acct={account})")
            return

        @sync_to_async
        def update_order():
            changed = False
            if status and db_order.status != status:
                db_order.status = status
                changed = True
            if db_order.filled_quantity != Decimal(str(filled)):
                db_order.filled_quantity = Decimal(str(filled))
                changed = True
            if avg_price and (db_order.avg_fill_price or Decimal('0')) != Decimal(str(avg_price)):
                db_order.avg_fill_price = Decimal(str(avg_price))
                changed = True
            if perm_id and db_order.perm_id != perm_id:
                db_order.perm_id = perm_id
                changed = True
            if status == 'Filled':
                db_order.filled_at = datetime.now(timezone.utc)
            if changed:
                db_order.save()
            return changed

        await update_order()
        self.order_map[db_order.order_id] = db_order

        # Track active protection orders
        if db_order.role in ('LONG_SL', 'LONG_TRAIL'):
            self.active_trades['long'] = trade or self.active_trades.get('long')
        elif db_order.role in ('SHORT_SL', 'SHORT_TRAIL'):
            self.active_trades['short'] = trade or self.active_trades.get('short')

        # If this is a stop loss filled but we missed execDetails, trigger transition
        if status == 'Filled' and db_order.role in ('LONG_SL', 'SHORT_SL') and not self.transitioning and not self.transition_done:
            class _Exec:
                pass
            exec_report = _Exec()
            exec_report.price = avg_price or 0
            exec_report.avgPrice = avg_price or 0
            exec_report.acctNumber = account
            exec_report.side = 'SLD' if db_order.action == 'SELL' else 'BOT'
            await self.transition_from_backfill(db_order, exec_report, datetime.now(timezone.utc))

    def on_trailing_stop_status(self, trade):
        """Handle trailing stop status updates"""
        status = trade.orderStatus
        curr_stop = getattr(status, 'stopPrice', 0)
        if curr_stop <= 0:
            curr_stop = getattr(trade.order, 'auxPrice', 0)

        if status.status == 'Filled':
            print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | EXECUTED at {status.avgFillPrice:.4f}")
            # Schedule async logging (can't await in sync callback)
            asyncio.create_task(self.log_event('TRAILING_UPDATE', 'INFO', f"Trailing stop filled at {status.avgFillPrice:.4f}"))
        else:
            price_str = f"{curr_stop:.4f}" if 0 < curr_stop < 1e10 else "Calculating..."
            print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | Current Stop: {price_str}")

    async def on_stop_loss_fill(self, trade, fill):
        """Handle stop loss fill and transition to trailing stop"""
        from ib_insync import Order as IBOrder, MarketOrder

        if self.transitioning or self.transition_done:
            return
        self.transitioning = True
        transitioned = False

        try:
            exec_time = self.get_safe_timestamp(fill.execution.time)
            print(f"\n>>>> [{self.fmt_ts(exec_time)}] STOP LOSS TRIGGERED on {trade.order.account} <<<<")
            await self.log_event('STOP_LOSS_HIT', 'WARNING', f"Stop loss triggered on {trade.order.account}")

            # Update cycle status
            @sync_to_async
            def update_status():
                self.cycle.status = 'TRANSITIONING'
                self.cycle.save()
            await update_status()

            # Determine which leg was hit
            hit_leg = 'long' if trade.order.account == self.bot.long_account else 'short'
            surviving_leg = 'short' if hit_leg == 'long' else 'long'
            surviving_trade = self.active_trades[surviving_leg]
            acc = self.bot.long_account if surviving_leg == 'long' else self.bot.short_account
            label = surviving_leg.upper()

            if surviving_trade and surviving_trade.orderStatus.status in ('PreSubmitted', 'Submitted'):
                print(f"Cancelling surviving Stop Loss on {acc} ({label})...")
                self.ib.cancelOrder(surviving_trade.order)

                # Wait for cancellation
                deadline = asyncio.get_event_loop().time() + 10
                while not surviving_trade.isDone() and asyncio.get_event_loop().time() < deadline:
                    await asyncio.sleep(0.1)
                    self.ib.waitOnUpdate()

                if not surviving_trade.isDone():
                    print(f"Cancel timeout on {acc}; flattening survivor position defensively.")
                    pos = [p for p in self.ib.positions() if p.contract.conId == self.contract.conId and p.account == acc]
                    if pos and pos[0].position != 0:
                        qty = pos[0].position
                        action = 'SELL' if qty > 0 else 'BUY'
                        self.ib.placeOrder(self.contract, MarketOrder(action, abs(qty), account=acc))
                    return

                self.ib.waitOnUpdate()
                await asyncio.sleep(0)
                self.ib.waitOnUpdate()

                # Update order status in DB
                @sync_to_async
                def update_cancelled_order():
                    if surviving_trade.order.orderId in self.order_map and self.order_map[surviving_trade.order.orderId]:
                        db_order = self.order_map[surviving_trade.order.orderId]
                        db_order.status = surviving_trade.orderStatus.status
                        db_order.save()
                        return db_order
                    return None

                db_order = await update_cancelled_order()
                if db_order:
                    await self.log_event('ORDER_CANCELLED', 'INFO', f"Cancelled {db_order.role} order", order=db_order)

                st = surviving_trade.orderStatus.status
                if st not in ('Cancelled', 'ApiCancelled'):
                    print(f"Surviving Stop Loss was not cancelled (Status: {st}). It likely filled. Not placing trailing stop.")
                    return

            # Place Trailing Stop
            action = 'SELL' if label == "LONG" else 'BUY'

            # Request fresh positions from IBKR
            await self.ib.reqPositionsAsync()
            await asyncio.sleep(0.5)  # Give TWS time to send positions

            positions = [p for p in self.ib.positions() if p.contract.conId == self.contract.conId and p.account == acc]
            print(f"[DEBUG] Checking positions for {acc}: found {len(positions)} positions, conId={self.contract.conId}")
            if positions:
                print(f"[DEBUG] Position: {positions[0].position} shares")

            if not positions or positions[0].position == 0:
                print(f"No active position found on {acc}. Not placing trailing stop.")
                return

            qty = abs(positions[0].position)
            market_price = fill.execution.price
            tick = float(self.cycle.min_tick)

            if label == "LONG":
                trail_price = self.q_floor(market_price * (1 - (float(self.bot.trailing_pct) / 100)), tick)
            else:
                trail_price = self.q_ceil(market_price * (1 + (float(self.bot.trailing_pct) / 100)), tick)

            print(f"Switching {label} leg to {self.bot.trailing_pct}% Trailing Stop (Est: {trail_price:.4f})...")
            trail_order = IBOrder(
                action=action, totalQuantity=qty, orderType='TRAIL',
                trailingPercent=float(self.bot.trailing_pct), account=acc, tif='GTC', outsideRth=True,
                orderRef=f"C{self.cycle.cycle_number}_{label}_TRAIL"
            )
            t_trade = self.ib.placeOrder(self.contract, trail_order)
            t_trade.fillEvent += self.on_fill
            t_trade.statusEvent += self.on_trailing_stop_status
            self.active_trades[surviving_leg] = t_trade

            # Create Order in DB
            @sync_to_async
            def create_trail_order():
                return Order.objects.create(
                    cycle=self.cycle,
                    order_id=t_trade.order.orderId,
                    order_ref=trail_order.orderRef,
                    role=f"{label}_TRAIL",
                    account=acc,
                    action=action,
                    order_type='TRAIL',
                    total_quantity=Decimal(str(qty)),
                    trailing_percent=self.bot.trailing_pct,
                    status='PendingSubmit'
                )

            db_order = await create_trail_order()
            self.order_map[t_trade.order.orderId] = db_order
            await self.log_event('TRANSITION_COMPLETE', 'INFO', f"Placed {label} trailing stop", order=db_order)

            transitioned = True
            print("Trailing Stop submitted. Protection transitioned.")
        finally:
            if transitioned:
                self.transition_done = True

                @sync_to_async
                def update_cycle_status():
                    self.cycle.status = 'ACTIVE'
                    self.cycle.save()

                await update_cycle_status()
            self.transitioning = False

    def on_fill(self, trade, fill):
        """Handle order fills"""
        exec_id = fill.execution.execId

        print(f"[on_fill] Called for order {trade.order.orderId}, exec_id={exec_id}")

        if exec_id in self.exec_ids_seen:
            print(f"[on_fill] Skipping duplicate exec_id {exec_id}")
            return
        self.exec_ids_seen.add(exec_id)

        # Get the Order from database
        if trade.order.orderId not in self.order_map:
            print(f"[ERROR] Fill for unknown order {trade.order.orderId} - order_map has: {list(self.order_map.keys())}")
            return

        db_order = self.order_map[trade.order.orderId]
        exec_obj = fill.execution

        print(f"[on_fill] Processing fill for {db_order.role} (DB id={db_order.id})")

        # Schedule async handling (can't do sync DB operations in callback)
        asyncio.create_task(self._handle_fill_async(trade, fill, exec_id, db_order))

    async def _handle_fill_async(self, trade, fill, exec_id, db_order):
        """Handle fill database operations asynchronously"""
        from asgiref.sync import sync_to_async

        exec_obj = fill.execution
        exec_time = self.get_safe_timestamp(exec_obj.time)

        # Ensure we have a live connection before touching DB/IB state
        if not await self.ensure_connection():
            await self.log_event('SYSTEM_ERROR', 'ERROR', "Fill handling skipped: IB connection lost")
            return

        @sync_to_async
        def update_fill():
            try:
                with transaction.atomic():
                    prev_status = db_order.status
                    prev_filled = db_order.filled_quantity

                    # Map IBKR side values (BOT/SLD) to our model values (BUY/SELL)
                    side_map = {'BOT': 'BUY', 'SLD': 'SELL'}
                    side = side_map.get(exec_obj.side, exec_obj.side)

                    execution = Execution.objects.create(
                        order=db_order,
                        cycle=self.cycle,
                        exec_id=exec_id,
                        side=side,
                        shares=Decimal(str(exec_obj.shares)),
                        price=Decimal(str(exec_obj.price)),
                        account=trade.order.account,
                        commission=Decimal('0'),
                        executed_at=exec_time
                    )

                    # Update Order
                    db_order.filled_quantity += Decimal(str(exec_obj.shares))
                    # Prefer IBKR status, but force Filled if we have full qty
                    status = trade.orderStatus.status
                    if db_order.filled_quantity >= db_order.total_quantity:
                        status = 'Filled'
                    db_order.status = status
                    if trade.orderStatus.avgFillPrice:
                        db_order.avg_fill_price = Decimal(str(trade.orderStatus.avgFillPrice))
                    perm_id = getattr(exec_obj, 'permId', None)
                    if perm_id:
                        db_order.perm_id = perm_id
                    if db_order.status == 'Filled':
                        db_order.filled_at = exec_time
                    db_order.save()

                    # Update Cycle P&L
                    cycle = Cycle.objects.select_for_update().get(pk=self.cycle.pk)
                    if exec_obj.side == 'BOT':  # BOT means BUY in IBKR
                        cycle.total_buys += Decimal(str(exec_obj.shares)) * Decimal(str(exec_obj.price))
                    else:  # SLD means SELL
                        cycle.total_sells += Decimal(str(exec_obj.shares)) * Decimal(str(exec_obj.price))

                    cycle.net_pnl = cycle.total_sells - cycle.total_buys - cycle.total_commission
                    cycle.save()
                    self.cycle = cycle

                print(
                    f"[DB] Order {db_order.order_id} ({db_order.role}) "
                    f"{prev_status}->{db_order.status}, filled {prev_filled}->{db_order.filled_quantity} "
                    f"avg {db_order.avg_fill_price}"
                )
                print(
                    f"[DB] Cycle {cycle.id} totals: buys={cycle.total_buys} "
                    f"sells={cycle.total_sells} pnl={cycle.net_pnl}"
                )
                print(f"--- [{self.fmt_ts(exec_time)}] {db_order.role} FILLED on {trade.order.account}: {exec_obj.shares} @ {exec_obj.price:.4f} ---")
                return True
            except Exception as e:
                print(f"[ERROR] Fill handling failed: {e}")
                raise

        try:
            await update_fill()
            await self.log_event('ORDER_FILLED', 'INFO', f"{db_order.role} filled: {exec_obj.shares}@{exec_obj.price:.4f}", order=db_order)

            # Trigger transition if it's a stop loss fill
            if db_order.role in ("LONG_SL", "SHORT_SL") and not self.transitioning and not self.transition_done:
                await self.on_stop_loss_fill(trade, fill)
        except Exception as e:
            await self.log_event('SYSTEM_ERROR', 'ERROR', f"Fill handling error: {str(e)}")

    async def report_pnl(self, is_final=False):
        """Generate P&L report"""
        @sync_to_async
        def get_pnl_data():
            self.cycle.refresh_from_db()
            orders = list(self.cycle.orders.all().prefetch_related('executions'))
            leg_details = {}

            for order in orders:
                execs = list(order.executions.all())
                if execs:
                    total_qty = sum(e.shares for e in execs)
                    total_value = sum(e.shares * e.price for e in execs)
                    avg_price = total_value / total_qty if total_qty else 0
                    last_exec = max(execs, key=lambda e: e.executed_at)

                    leg_details[order.role] = {
                        'qty': total_qty,
                        'avg_price': avg_price,
                        'time': last_exec.executed_at
                    }

            return {
                'cycle_number': self.cycle.cycle_number,
                'symbol': self.cycle.symbol,
                'leg_details': leg_details,
                'total_buys': self.cycle.total_buys,
                'total_sells': self.cycle.total_sells,
                'total_commission': self.cycle.total_commission,
                'net_pnl': self.cycle.net_pnl
            }

        data = await get_pnl_data()
        status = "FINAL" if is_final else "INTERIM"

        print(f"\n{'=' * 40}")
        print(f"{status} HEDGE P&L REPORT (Cycle #{data['cycle_number']} - {data['symbol']})")
        print(f"{'-' * 40}")

        for role, details in data['leg_details'].items():
            time_str = self.fmt_ts(details['time'])
            print(f"{role:20} | {details['qty']:5} @ {details['avg_price']:8.4f} | {time_str}")

        print(f"{'-' * 40}")
        print(f"Total Cash Out (Buys):  {data['total_buys']:.2f}")
        print(f"Total Cash In (Sells):  {data['total_sells']:.2f}")
        print(f"Total Commissions:      {data['total_commission']:.2f}")
        print(f"{'-' * 40}")
        print(f"NET REALIZED P&L:       {data['net_pnl']:.2f}")
        print(f"STATUS: {'CLOSED' if is_final else 'OPEN'}")
        print(f"{'=' * 40}\n")

        await self.log_event('PNL_REPORT', 'INFO', f"{status} P&L: {data['net_pnl']:.2f}", data={
            'total_buys': float(data['total_buys']),
            'total_sells': float(data['total_sells']),
            'total_commission': float(data['total_commission']),
            'net_pnl': float(data['net_pnl'])
        })

    async def run(self):
        """Main bot execution loop"""
        # Import ib_insync inside async context to avoid event loop issues
        from ib_insync import IB, Stock, MarketOrder, StopOrder, Order as IBOrder, TagValue, util

        # MANDATORY: Patch asyncio for ib_insync
        util.patchAsyncio()

        try:
            # Load bot
            @sync_to_async
            def load_bot():
                return Bot.objects.get(pk=self.bot_id)

            self.bot = await load_bot()
            print(f"\n[BOT] Starting Bot #{self.bot.id}: {self.bot.symbol} | {self.bot.long_account}/{self.bot.short_account}")

            # Validate bot is in RUNNING state
            if self.bot.status != 'RUNNING':
                print(f"[ERROR] Bot status is {self.bot.status}, expected RUNNING")
                return

            # Initial heartbeat so monitor sees us immediately after restart
            await self.send_heartbeat()

            # Connect to IBKR with a stable client ID per bot (persisted) to receive updates for existing orders after restart
            async def persist_client_id(cid):
                @sync_to_async
                def _persist(cid_inner):
                    Bot.objects.filter(pk=self.bot.id).update(last_client_id=cid_inner)
                await _persist(cid)

            timestamp_component = None
            if self.bot.last_client_id:
                self.client_id = self.bot.last_client_id
            else:
                timestamp_component = int(time.time() * 1000) % 100000  # milliseconds, last 5 digits
                self.client_id = (self.bot.id * 100000) + timestamp_component
                await persist_client_id(self.client_id)
            self.port = self.bot.port

            self.ib = IB()
            self.ib.errorEvent += self.on_error
            self.ib.commissionReportEvent += self.on_commission_report
            self.ib.execDetailsEvent += self.on_exec_details
            self.ib.orderStatusEvent += self.on_order_status

            async def connect_with_retry():
                nonlocal timestamp_component
                attempt = 0
                while attempt < 2:
                    duplicate_error = asyncio.Event()

                    def _temp_error_handler(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
                        if errorCode == 326 or ("client id is already in use" in (errorString or "").lower()):
                            duplicate_error.set()

                    # Temporarily hook errorEvent to catch async 326 after connect
                    self.ib.errorEvent += _temp_error_handler
                    try:
                        print(f"[CONNECTION] Connecting to TWS with clientId={self.client_id} (bot_id={self.bot.id}, timestamp={timestamp_component or 'reuse'})")
                        await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)

                        # Wait briefly to see if TWS emits 326 after login
                        try:
                            await asyncio.wait_for(duplicate_error.wait(), timeout=1.0)
                            # 326 detected via errorEvent
                            raise RuntimeError("client id is already in use (async 326)")
                        except asyncio.TimeoutError:
                            pass
                        return True
                    except Exception as e:
                        msg = str(e).lower()
                        duplicate_id = "client id is already in use" in msg or "already in use" in msg
                        timeout = isinstance(e, TimeoutError)

                        # Retry once with a fresh clientId if TWS says the ID is in use or we hit a timeout right after that error
                        if attempt == 0 and (duplicate_id or timeout):
                            if self.ib.isConnected():
                                self.ib.disconnect()
                            await asyncio.sleep(1)
                            timestamp_component = int(time.time() * 1000) % 100000
                            self.client_id = (self.bot.id * 100000) + timestamp_component
                            await persist_client_id(self.client_id)
                            print(f"[CONNECTION] Retrying with fresh clientId={self.client_id} after failure: {e}")
                            attempt += 1
                            continue
                        raise
                    finally:
                        # Remove temporary handler
                        if _temp_error_handler in self.ib.errorEvent:
                            self.ib.errorEvent -= _temp_error_handler
                return False

            connected = await connect_with_retry()
            if not connected:
                await self.log_event('SYSTEM_ERROR', 'ERROR', "Failed to connect to TWS after retrying clientId")
                return
            await self.log_event('BOT_START', 'INFO', f"Bot started: {self.bot.symbol} (clientId={self.client_id})")

            # Contract setup - use bot configuration
            # Stock constructor: Stock(symbol, exchange, currency)
            self.contract = Stock(
                self.bot.symbol.upper(),
                self.bot.exchange,
                self.bot.currency
            )

            # Set primaryExchange as attribute if specified
            if self.bot.primary_exchange:
                self.contract.primaryExchange = self.bot.primary_exchange

            print(f"[CONTRACT] {self.contract}")
            await self.ib.qualifyContractsAsync(self.contract)

            details = await self.ib.reqContractDetailsAsync(self.contract)
            min_tick = details[0].minTick if details else 0.01
            print(f"[CONTRACT] MinTick from IBKR: {min_tick}")

            # Check for price magnifiers (some exchanges use them)
            if details and hasattr(details[0], 'priceMagnifier'):
                print(f"[CONTRACT] PriceMagnifier: {details[0].priceMagnifier}")

            # Pull current positions/orders from TWS so recovery logic has data after restarts
            await self.ib.reqPositionsAsync()
            await self.ib.reqAllOpenOrdersAsync()
            await asyncio.sleep(0.5)  # Give TWS time to push snapshots

            # Main cycle loop
            while not self.should_stop and await self.check_bot_status():
                self.reset_cycle_state()

                # CHECK FOR EXISTING ACTIVE CYCLE (from previous worker run or failed start)
                @sync_to_async
                def check_active_cycle():
                    return self.bot.cycles.filter(
                        status__in=['INITIALIZING', 'ENTERING', 'ACTIVE', 'TRANSITIONING', 'ERROR']
                    ).order_by('-id').first()

                existing_cycle = await check_active_cycle()

                if existing_cycle:
                    print(f"\n[CYCLE CHECK] Found existing active cycle {existing_cycle.cycle_number} (status: {existing_cycle.status})")

                    # PREFLIGHT CHECK: See what's holding up the old cycle
                    print("[PREFLIGHT] Checking positions/orders from previous cycle...")
                    positions = [p for p in self.ib.positions()
                               if p.contract.conId == self.contract.conId
                               and p.account in [self.bot.long_account, self.bot.short_account]
                               and p.position != 0]

                    open_trades = [t for t in self.ib.openTrades()
                                   if t.contract.conId == self.contract.conId
                                   and t.order.account in [self.bot.long_account, self.bot.short_account]]

                    open_orders = [o for o in self.ib.openOrders()
                                   if o.account in [self.bot.long_account, self.bot.short_account]]
                    # Refresh open orders snapshot (all accounts) to catch pending orders immediately after restart
                    await self.ib.reqAllOpenOrdersAsync()
                    await asyncio.sleep(0.5)
                    open_orders = [o for o in self.ib.openOrders()
                                   if o.account in [self.bot.long_account, self.bot.short_account]]

                    if positions:
                        print(f"  Found {len(positions)} open positions:")
                        for p in positions:
                            print(f"    {p.account}: {p.position} shares @ {p.avgCost:.2f}")

                    if open_trades or open_orders:
                        print(f"  Found {len(open_trades) + len(open_orders)} pending orders:")
                        for t in open_trades:
                            print(f"    Trade {t.order.orderId} on {t.order.account}: "
                                  f"{t.order.action} {t.order.totalQuantity} {t.order.orderType} "
                                  f"(Status: {t.orderStatus.status})")
                        for o in open_orders:
                            print(f"    Order {o.orderId} on {o.account}: "
                                  f"{o.action} {o.totalQuantity} {o.orderType} "
                                  f"(State: {getattr(o, 'orderState', None) and o.orderState.status})")

                    if not positions and not open_trades and not open_orders:
                        print(f"  No positions or orders found - marking cycle {existing_cycle.cycle_number} as COMPLETED")

                        @sync_to_async
                        def complete_orphan_cycle():
                            existing_cycle.status = 'COMPLETED'
                            existing_cycle.completed_at = datetime.now(timezone.utc)
                            existing_cycle.save()

                        await complete_orphan_cycle()
                        await self.log_event('CYCLE_AUTO_COMPLETED', 'INFO',
                                           f"Auto-completed orphan cycle {existing_cycle.cycle_number}")
                        continue  # Now try creating a new cycle

                    # RECOVERY: Re-attach to existing cycle and restore state
                    print(f"\n[RECOVERY] Attempting to recover cycle {existing_cycle.cycle_number}...")
                    self.cycle = existing_cycle

                    # Backfill any executions that may have been missed while the worker was down
                    await self.backfill_executions()
                    await self.reconcile_cycle_state()

                    # Validate TWS state matches DB state before proceeding
                    is_consistent, discrepancies = await self.validate_tws_db_consistency()

                    if not is_consistent:
                        # Check if TWS is already clean (positions flat, no orders)
                        # This happens after PANIC - we can safely complete the cycle
                        tws_positions = [p for p in self.ib.positions()
                                       if p.contract.conId == self.contract.conId
                                       and p.account in [self.bot.long_account, self.bot.short_account]
                                       and p.position != 0]
                        tws_orders = [t for t in self.ib.openTrades()
                                    if t.contract.conId == self.contract.conId
                                    and t.order.account in [self.bot.long_account, self.bot.short_account]]

                        if not tws_positions and not tws_orders:
                            # TWS is clean - PANIC was run, just complete the stale cycle
                            print(f"\n[VALIDATION] TWS is clean (no positions/orders), but DB has stale data.")
                            print(f"[VALIDATION] Auto-completing stale cycle {existing_cycle.cycle_number}...")

                            @sync_to_async
                            def complete_stale_cycle():
                                existing_cycle.status = 'COMPLETED'
                                existing_cycle.completed_at = datetime.now(timezone.utc)
                                existing_cycle.save()

                            await complete_stale_cycle()
                            await self.log_event('CYCLE_AUTO_COMPLETED', 'WARNING',
                                               f"Auto-completed stale cycle {existing_cycle.cycle_number} after validation failure (TWS clean)")

                            print(f"[VALIDATION] Cycle {existing_cycle.cycle_number} completed, will start fresh cycle next iteration")
                            continue  # Start new cycle

                        # TWS has positions/orders but they don't match DB - real problem
                        error_msg = (
                            f"Cannot recover cycle {existing_cycle.cycle_number}: TWS state does not match DB state.\n"
                            f"Found {len(discrepancies)} discrepancies:\n"
                        )
                        for disc in discrepancies:
                            error_msg += f"  • {disc}\n"
                        error_msg += (
                            "\n⚠️  MANUAL INTERVENTION REQUIRED:\n"
                            f"  1. Review the discrepancies above\n"
                            f"  2. Use the PANIC button in admin to cancel all orders and close positions\n"
                            f"  3. Restart the bot to begin a fresh cycle\n"
                            f"\nThis safety check prevents data corruption from manual TWS interventions "
                            f"or conflicting bot instances."
                        )

                        print(f"\n{'='*70}")
                        print(f"[RECOVERY BLOCKED]")
                        print(error_msg)
                        print(f"{'='*70}\n")

                        await self.log_event('RECOVERY_BLOCKED', 'CRITICAL', error_msg,
                                           data={'discrepancies': discrepancies})

                        # Mark BOTH bot and cycle as ERROR to prevent restart loop
                        @sync_to_async
                        def mark_error_state():
                            # Mark cycle as ERROR (discrepancy details already in Event log)
                            existing_cycle.status = 'ERROR'
                            existing_cycle.completed_at = datetime.now(timezone.utc)
                            existing_cycle.save()

                            # Mark bot as ERROR
                            self.bot.status = 'ERROR'
                            self.bot.save()

                        await mark_error_state()

                        # Exit - user must use PANIC to clean up
                        self.should_stop = True
                        return

                    # Validation passed - proceed with recovery
                    print(f"[RECOVERY] ✓ Validation passed, continuing with cycle recovery...")

                    # Refresh open orders/trades after backfill (all accounts)
                    await self.ib.reqAllOpenOrdersAsync()
                    await asyncio.sleep(0.5)
                    open_trades = [t for t in self.ib.openTrades()
                                   if t.contract.conId == self.contract.conId
                                   and t.order.account in [self.bot.long_account, self.bot.short_account]]
                    open_orders = [o for o in self.ib.openOrders()
                                   if o.account in [self.bot.long_account, self.bot.short_account]]

                    # Build lookup helpers for matching even if orderIds changed or only permId/orderRef is available
                    trade_by_order_id = {t.order.orderId: t for t in open_trades}
                    trade_by_perm_id = {getattr(t.order, 'permId', None): t for t in open_trades if getattr(t.order, 'permId', None)}
                    trade_by_ref = {getattr(t.order, 'orderRef', None): t for t in open_trades if getattr(t.order, 'orderRef', None)}
                    order_by_order_id = {o.orderId: o for o in open_orders}
                    order_by_perm_id = {getattr(o, 'permId', None): o for o in open_orders if getattr(o, 'permId', None)}
                    order_by_ref = {getattr(o, 'orderRef', None): o for o in open_orders if getattr(o, 'orderRef', None)}

                    # Load existing orders from database and map to open trades
                    @sync_to_async
                    def load_cycle_orders():
                        return list(existing_cycle.orders.filter(
                            status__in=['PendingSubmit', 'PreSubmitted', 'Submitted', 'Filled']
                        ).select_related('cycle'))

                    db_orders = await load_cycle_orders()

                    # Match DB orders with IBKR open trades/orders and re-register callbacks
                    for db_order in db_orders:
                        matching_trade = (
                            trade_by_order_id.get(db_order.order_id)
                            or trade_by_perm_id.get(db_order.perm_id)
                            or trade_by_ref.get(db_order.order_ref)
                        )
                        matching_order = (
                            order_by_order_id.get(db_order.order_id)
                            or order_by_perm_id.get(db_order.perm_id)
                            or order_by_ref.get(db_order.order_ref)
                        )
                        if matching_trade:
                            print(f"[RECOVERY] Re-registering callbacks for {db_order.role} (order {db_order.order_id})")
                            self.order_map[db_order.order_id] = db_order

                            # Re-register fill callbacks
                            matching_trade.fillEvent += self.on_fill

                            # Store active stop loss trades for monitoring
                            if db_order.role == 'LONG_SL':
                                self.active_trades['long'] = matching_trade
                            elif db_order.role == 'SHORT_SL':
                                self.active_trades['short'] = matching_trade
                        elif matching_order:
                            print(f"[RECOVERY] Found open order {db_order.role} (order {db_order.order_id}) without trade; keeping for monitoring")
                            self.order_map[db_order.order_id] = db_order

                    await self.log_event('CYCLE_RECOVERED', 'INFO',
                                       f"Recovered cycle {existing_cycle.cycle_number}, monitoring {len(self.order_map)} orders")

                    # Continue monitoring this cycle
                    print(f"[RECOVERY] Monitoring recovered cycle {existing_cycle.cycle_number}...")
                    last_exec_poll = time.time()

                    # Monitor positions until flat (same as normal cycle monitoring)
                    while True:
                        if not await self.check_bot_status():
                            await self.log_event('CYCLE_ABORTED', 'WARNING', "Bot stop detected during monitoring")
                            await self._panic_flatten()
                            break

                        await asyncio.sleep(2)
                        self.ib.waitOnUpdate()

                        # Poll executions during recovered monitoring since live execEvents may not arrive for legacy orders
                        if time.time() - last_exec_poll > 5:
                            await self.backfill_executions()
                            await self.reconcile_cycle_state()
                            last_exec_poll = time.time()

                        # Keep heartbeat fresh during long monitoring to avoid stale-worker trips
                        await self.send_heartbeat()

                        # Check if all positions are flat
                        pos = [p for p in self.ib.positions()
                              if p.contract.conId == self.contract.conId
                              and p.account in [self.bot.long_account, self.bot.short_account]]

                        if not pos or all(p.position == 0 for p in pos):
                            print("\n>>> ALL POSITIONS CLOSED (recovered cycle).")
                            break

                    # Cleanup and complete cycle
                    await self._cleanup_orphan_orders()

                    @sync_to_async
                    def finalize_recovered_cycle():
                        self.cycle.status = 'COMPLETED'
                        self.cycle.completed_at = datetime.now(timezone.utc)
                        self.cycle.save()

                    await finalize_recovered_cycle()
                    await self.report_pnl(is_final=True)
                    await self.log_event('CYCLE_COMPLETE', 'INFO',
                                       f"Recovered cycle {existing_cycle.cycle_number} completed with P&L: {self.cycle.net_pnl:.2f}")

                    continue  # Start next cycle

                # PREFLIGHT CHECK: Verify no existing positions or orders before starting new cycle
                print("\n[PREFLIGHT] Checking for existing positions and orders...")
                positions = [p for p in self.ib.positions()
                           if p.contract.conId == self.contract.conId
                           and p.account in [self.bot.long_account, self.bot.short_account]
                           and p.position != 0]

                open_orders = [t for t in self.ib.openTrades()
                             if t.contract.conId == self.contract.conId
                             and t.order.account in [self.bot.long_account, self.bot.short_account]]

                if positions or open_orders:
                    print(f"\n[PREFLIGHT] BLOCKED - Found existing positions/orders:")
                    if positions:
                        print(f"  Positions: {len(positions)}")
                        for p in positions:
                            print(f"    {p.account}: {p.position} shares @ {p.avgCost:.2f}")

                    if open_orders:
                        print(f"  Open Orders: {len(open_orders)}")
                        for t in open_orders:
                            print(f"    Order {t.order.orderId} on {t.order.account}: "
                                  f"{t.order.action} {t.order.totalQuantity} {t.order.orderType} "
                                  f"(Status: {t.orderStatus.status})")

                    await self.log_event('PREFLIGHT_BLOCKED', 'ERROR',
                                       f"Cycle start blocked: {len(positions)} positions, {len(open_orders)} orders found")

                    print("\n[PREFLIGHT] Waiting 10 seconds before retrying preflight check...")
                    await asyncio.sleep(10)
                    continue  # Skip to next iteration, retry preflight check

                print("[PREFLIGHT] Check passed - no positions or orders found")

                # Get next cycle number and create cycle atomically
                @sync_to_async
                def get_or_create_next_cycle():
                    from django.db.models import Max
                    # Get the highest cycle number for this bot
                    max_cycle = self.bot.cycles.aggregate(Max('cycle_number'))['cycle_number__max']
                    next_cycle_number = (max_cycle or 0) + 1

                    # Use get_or_create to avoid duplicates
                    cycle, created = Cycle.objects.get_or_create(
                        bot=self.bot,
                        cycle_number=next_cycle_number,
                        defaults={
                            'symbol': self.contract.symbol,
                            'contract_id': self.contract.conId,
                            'min_tick': Decimal(str(min_tick)),
                            'status': 'INITIALIZING'
                        }
                    )
                    return cycle, next_cycle_number

                self.cycle, cycle_number = await get_or_create_next_cycle()

                print(f"\n{'='*60}")
                print(f"CYCLE {cycle_number} STARTED - {self.contract.symbol}")
                print(f"{'='*60}")
                await self.log_event('CYCLE_START', 'INFO', f"Cycle {cycle_number} started")

                # Execute cycle (same logic as v5 script)
                # [The rest of the cycle logic would go here - entry, protection, monitoring]
                # For brevity, I'll create a separate method

                success = await self._execute_cycle(min_tick, cycle_number)

                if not success:
                    print("[CYCLE] Cycle failed - setting bot to ERROR status")
                    # Mark cycle and bot as ERROR
                    @sync_to_async
                    def mark_failed_and_stop():
                        self.cycle.status = 'ERROR'
                        self.cycle.completed_at = datetime.now(timezone.utc)
                        self.cycle.save()

                        # Set bot to ERROR status to stop retry loop
                        self.bot.status = 'ERROR'
                        self.bot.stopped_at = datetime.now(timezone.utc)
                        self.bot.save()

                    await mark_failed_and_stop()
                    await self.log_event('CYCLE_FAILED', 'ERROR', f"Cycle {cycle_number} failed, bot stopped")

                    # Exit the loop - bot is now in ERROR status
                    break

                # Finalize cycle
                @sync_to_async
                def finalize_cycle():
                    self.cycle.status = 'COMPLETED'
                    self.cycle.completed_at = datetime.now(timezone.utc)
                    self.cycle.save()

                await finalize_cycle()
                await self.report_pnl(is_final=True)
                await self.log_event('CYCLE_COMPLETE', 'INFO', f"Cycle {cycle_number} completed with P&L: {self.cycle.net_pnl:.2f}")

                print("\n>>> CYCLE COMPLETE. Waiting before next cycle...")
                # Sleep with status checking (check every second)
                for _ in range(10):
                    if not await self.check_bot_status():
                        break
                    await asyncio.sleep(1)

        except Exception as e:
            print(f"\n[FATAL ERROR] {e}")
            import traceback
            traceback.print_exc()
            await self.log_event('SYSTEM_ERROR', 'CRITICAL', f"Fatal error: {str(e)}")
            if self.bot:
                @sync_to_async
                def set_error():
                    self.bot.refresh_from_db()
                    self.bot.status = 'ERROR'
                    self.bot.save()
                await set_error()
            # Don't raise - this will terminate the worker and cause restart loop
        finally:
            # Only set to STOPPED if we're intentionally stopping (not on error)
            if self.bot:
                @sync_to_async
                def update_final_status():
                    self.bot.refresh_from_db()
                    # Only mark as stopped if status is not RUNNING and not ERROR
                    # Preserve ERROR status to help with debugging
                    if self.bot.status not in ('RUNNING', 'ERROR'):
                        self.bot.status = 'STOPPED'
                        self.bot.stopped_at = datetime.now(timezone.utc)
                        self.bot.save()
                await update_final_status()
            if self.ib:
                self.ib.disconnect()
            print("[BOT] Disconnected. Goodbye!")

    async def _execute_cycle(self, min_tick, cycle_number):
        """
        Execute a single hedge cycle with full trading logic.

        Flow:
        1. Submit concurrent long BUY and short SELL market orders
        2. Wait for both entries to fill
        3. Place stop loss protection on both sides
        4. Monitor positions until flat (one stop hits, triggers transition to trailing)
        5. Cleanup orphan orders

        Returns True if cycle completed successfully, False otherwise
        """
        from ib_insync import MarketOrder, StopOrder, Order as IBOrder, TagValue

        try:
            # Update cycle status
            @sync_to_async
            def update_cycle_status(status):
                self.cycle.status = status
                self.cycle.save()

            # 1. CONCURRENT ENTRIES
            await update_cycle_status('ENTERING')
            print(f"\n>>> SUBMITTING CONCURRENT ENTRIES (Qty: {self.bot.qty})...")
            await self.log_event('CYCLE_ENTRY_START', 'INFO', f"Submitting concurrent entries: {self.bot.qty} shares")

            # Create market orders
            l_ord = MarketOrder('BUY', float(self.bot.qty), account=self.bot.long_account, tif='GTC',
                               orderRef=f"C{cycle_number}_LONG_ENTRY")
            s_ord = MarketOrder('SELL', float(self.bot.qty), account=self.bot.short_account, tif='GTC',
                               orderRef=f"C{cycle_number}_SHORT_ENTRY")

            # Add algo if enabled (USD only)
            if self.bot.use_algo and self.contract.currency == 'USD':
                for o in [l_ord, s_ord]:
                    o.algoStrategy = 'Adaptive'
                    o.algoParams = [TagValue('priority', 'Normal')]

            # Place orders
            l_trade = self.ib.placeOrder(self.contract, l_ord)
            s_trade = self.ib.placeOrder(self.contract, s_ord)

            # Create Order records in DB
            @sync_to_async
            def create_entry_orders():
                l_order = Order.objects.create(
                    cycle=self.cycle, order_id=l_trade.order.orderId, order_ref=l_ord.orderRef,
                    role='LONG_ENTRY', account=self.bot.long_account, action='BUY', order_type='MKT',
                    total_quantity=self.bot.qty, status='PendingSubmit'
                )
                s_order = Order.objects.create(
                    cycle=self.cycle, order_id=s_trade.order.orderId, order_ref=s_ord.orderRef,
                    role='SHORT_ENTRY', account=self.bot.short_account, action='SELL', order_type='MKT',
                    total_quantity=self.bot.qty, status='PendingSubmit'
                )
                return l_order, s_order

            l_db_order, s_db_order = await create_entry_orders()
            self.order_map[l_trade.order.orderId] = l_db_order
            self.order_map[s_trade.order.orderId] = s_db_order

            # Register fill callbacks
            l_trade.fillEvent += self.on_fill
            s_trade.fillEvent += self.on_fill

            print("Waiting for both entries to fill...")

            # Wait for fills with timeout; pause failure if IBKR connectivity is down
            deadline = asyncio.get_event_loop().time() + 30
            while not (l_trade.isDone() and s_trade.isDone()) and asyncio.get_event_loop().time() < deadline:
                if not await self.check_bot_status():
                    await self.log_event('CYCLE_ABORTED', 'WARNING', "Bot stop detected during entry")
                    await self._panic_flatten()
                    return False
                await asyncio.sleep(0.5)
                self.ib.waitOnUpdate()

            # Log status after wait
            print(f"[ENTRY] Long: {l_trade.orderStatus.status} (filled: {l_trade.orderStatus.filled})")
            print(f"[ENTRY] Short: {s_trade.orderStatus.status} (filled: {s_trade.orderStatus.filled})")

            # 2. VERIFY ENTRY COMPLETION OR SUBMISSION
            l_stat, s_stat = l_trade.orderStatus.status, s_trade.orderStatus.status
            l_filled, s_filled = l_trade.orderStatus.filled, s_trade.orderStatus.filled

            # Accept orders that are either Filled OR PreSubmitted/Submitted (waiting for market open)
            valid_statuses = {'Filled', 'PreSubmitted', 'Submitted'}

            if l_stat not in valid_statuses or s_stat not in valid_statuses:
                # If IBKR connectivity is down, keep waiting until it returns instead of panicking
                if self.connectivity_down:
                    print("\n[ENTRY] Connectivity down (1100/1101); delaying entry failure and waiting for recovery...")
                    await self.log_event('ENTRY_PENDING', 'WARNING',
                                        f"IBKR connectivity down; waiting for entry fills. Long: {l_stat}/{l_filled}, Short: {s_stat}/{s_filled}")
                    # Wait up to an additional 120 seconds for recovery
                    recovery_deadline = asyncio.get_event_loop().time() + 120
                    while asyncio.get_event_loop().time() < recovery_deadline:
                        if not await self.check_bot_status():
                            await self.log_event('CYCLE_ABORTED', 'WARNING', "Bot stop detected during entry recovery wait")
                            await self._panic_flatten()
                            return False
                        # If connectivity restored and orders filled, break
                        if not self.connectivity_down and l_trade.orderStatus.status in valid_statuses and s_trade.orderStatus.status in valid_statuses:
                            break
                        await asyncio.sleep(2)
                        self.ib.waitOnUpdate()
                    # Refresh status values after recovery wait
                    l_stat, s_stat = l_trade.orderStatus.status, s_trade.orderStatus.status
                    l_filled, s_filled = l_trade.orderStatus.filled, s_trade.orderStatus.filled

                # Re-evaluate after recovery wait
                if l_stat not in valid_statuses or s_stat not in valid_statuses:
                    print(f"\n!!! CRITICAL: ENTRY FAILURE !!!")
                    print(f"Long Status: {l_stat} (Filled: {l_filled})")
                    print(f"Short Status: {s_stat} (Filled: {s_filled})")
                    await self.log_event('ENTRY_FAILURE', 'ERROR',
                                        f"Entry failed - Long: {l_stat}/{l_filled}, Short: {s_stat}/{s_filled}")

                    # Panic flatten based on actual positions
                    await self._panic_flatten()
                    return False

            # If orders are pending (not yet filled), wait for them to fill
            if l_stat != 'Filled' or s_stat != 'Filled':
                print(f"\n[ENTRY] Orders submitted but not yet filled (market may be closed)")
                print(f"[ENTRY] Long: {l_stat}, Short: {s_stat}")
                print(f"[ENTRY] Waiting for market open and fills...")
                await self.log_event('ENTRY_PENDING', 'INFO',
                                    f"Entry orders submitted, waiting for fills - Long: {l_stat}, Short: {s_stat}")

                # Wait indefinitely for fills (with status checking)
                while (l_trade.orderStatus.status != 'Filled' or
                       s_trade.orderStatus.status != 'Filled'):
                    if not await self.check_bot_status():
                        await self.log_event('CYCLE_ABORTED', 'WARNING', "Bot stop detected while waiting for fills")
                        await self._panic_flatten()
                        return False

                    # Check if either order failed
                    if (l_trade.orderStatus.status in ('Cancelled', 'Inactive', 'ApiCancelled') or
                        s_trade.orderStatus.status in ('Cancelled', 'Inactive', 'ApiCancelled')):
                        print(f"\n!!! ORDER CANCELLED OR REJECTED !!!")
                        print(f"Long: {l_trade.orderStatus.status}, Short: {s_trade.orderStatus.status}")
                        await self.log_event('ENTRY_CANCELLED', 'ERROR',
                                           f"Entry order cancelled - Long: {l_trade.orderStatus.status}, Short: {s_trade.orderStatus.status}")
                        await self._panic_flatten()
                        return False

                    await asyncio.sleep(2)
                    self.ib.waitOnUpdate()

                # Update filled quantities after waiting
                l_filled, s_filled = l_trade.orderStatus.filled, s_trade.orderStatus.filled
                print(f"[ENTRY] Both orders filled! Long: {l_filled}, Short: {s_filled}")

            if l_filled != s_filled:
                print(f"\nWARNING: Quantity mismatch! Long: {l_filled}, Short: {s_filled}")
                await self.log_event('QTY_MISMATCH', 'WARNING',
                                    f"Quantity mismatch - Long: {l_filled}, Short: {s_filled}")

            # 3. PLACE STOP LOSS PROTECTION
            await update_cycle_status('ACTIVE')
            print("\n>>> BOTH ENTRIES FILLED. PLACING PROTECTION...")
            await self.log_event('PROTECTION_START', 'INFO', "Placing stop loss protection")

            l_qty, s_qty = l_filled, s_filled
            l_price, s_price = l_trade.orderStatus.avgFillPrice, s_trade.orderStatus.avgFillPrice

            print(f"[STOP LOSS] Entry prices - Long: {l_price}, Short: {s_price}")
            print(f"[STOP LOSS] MinTick from IBKR: {min_tick}, Stop %: {self.bot.stop_pct}")

            # Get correct tick size for this price level and exchange
            actual_tick = self.get_exchange_tick_size(l_price, self.bot.primary_exchange, min_tick)
            print(f"[STOP LOSS] Actual tick size for price level: {actual_tick}")

            # Long Stop Loss (Sell Stop below entry)
            l_sl_price_raw = l_price * (1 - float(self.bot.stop_pct) / 100)
            l_sl_price = round(l_sl_price_raw / actual_tick) * actual_tick
            print(f"[STOP LOSS] Long SL: raw={l_sl_price_raw}, rounded={l_sl_price}")
            print(f"Placing LONG Stop Loss on {self.bot.long_account} at {l_sl_price:.2f}...")
            l_sl_ord = StopOrder('SELL', l_qty, l_sl_price, account=self.bot.long_account,
                                tif='GTC', outsideRth=True, orderRef=f"C{cycle_number}_LONG_SL")

            # Short Stop Loss (Buy Stop above entry)
            s_sl_price_raw = s_price * (1 + float(self.bot.stop_pct) / 100)
            s_sl_price = round(s_sl_price_raw / actual_tick) * actual_tick
            print(f"[STOP LOSS] Short SL: raw={s_sl_price_raw}, rounded={s_sl_price}")
            print(f"Placing SHORT Stop Loss on {self.bot.short_account} at {s_sl_price:.2f}...")
            s_sl_ord = StopOrder('BUY', s_qty, s_sl_price, account=self.bot.short_account,
                                tif='GTC', outsideRth=True, orderRef=f"C{cycle_number}_SHORT_SL")

            # Place stop orders
            self.active_trades['long'] = self.ib.placeOrder(self.contract, l_sl_ord)
            self.active_trades['short'] = self.ib.placeOrder(self.contract, s_sl_ord)

            # Create Order records in DB
            @sync_to_async
            def create_stop_orders():
                l_sl_order = Order.objects.create(
                    cycle=self.cycle, order_id=self.active_trades['long'].order.orderId, order_ref=l_sl_ord.orderRef,
                    role='LONG_SL', account=self.bot.long_account, action='SELL', order_type='STP',
                    total_quantity=Decimal(str(l_qty)), stop_price=Decimal(str(l_sl_price)), status='PendingSubmit'
                )
                s_sl_order = Order.objects.create(
                    cycle=self.cycle, order_id=self.active_trades['short'].order.orderId, order_ref=s_sl_ord.orderRef,
                    role='SHORT_SL', account=self.bot.short_account, action='BUY', order_type='STP',
                    total_quantity=Decimal(str(s_qty)), stop_price=Decimal(str(s_sl_price)), status='PendingSubmit'
                )
                return l_sl_order, s_sl_order

            l_sl_db_order, s_sl_db_order = await create_stop_orders()
            self.order_map[self.active_trades['long'].order.orderId] = l_sl_db_order
            self.order_map[self.active_trades['short'].order.orderId] = s_sl_db_order

            # Register fill callbacks for stops
            self.active_trades['long'].fillEvent += self.on_fill
            self.active_trades['short'].fillEvent += self.on_fill

            print("Waiting for Stop Losses to reach live state...")
            armed_statuses = {'PreSubmitted', 'Submitted'}
            while any(t.orderStatus.status not in armed_statuses for t in [self.active_trades['long'], self.active_trades['short']]):
                if not await self.check_bot_status():
                    await self.log_event('CYCLE_ABORTED', 'WARNING', "Bot stop detected during protection setup")
                    return False
                await asyncio.sleep(0.1)
                self.ib.waitOnUpdate()
                # Safety: stop if any order becomes Inactive or Rejected
                if any(t.orderStatus.status in ('Inactive', 'Rejected') for t in [self.active_trades['long'], self.active_trades['short']]):
                    break

            # 4. VERIFY PROTECTION ARMED
            if any(t.orderStatus.status in ('Inactive', 'Rejected') for t in [self.active_trades['long'], self.active_trades['short']]):
                print("\n!!! CRITICAL: PROTECTION FAILURE !!!")
                await self.log_event('PROTECTION_FAILURE', 'ERROR', "Stop loss orders rejected or inactive")
                await self._panic_flatten()
                return False

            print("Protection armed. Hedge is ACTIVE.")
            await self.log_event('HEDGE_ACTIVE', 'INFO',
                                f"Hedge active - Long SL: {l_sl_price:.4f}, Short SL: {s_sl_price:.4f}")

            # 5. MONITOR POSITIONS UNTIL FLAT
            print("\nMonitoring positions until flat...")
            while True:
                if not await self.check_bot_status():
                    await self.log_event('CYCLE_ABORTED', 'WARNING', "Bot stop detected during monitoring")
                    await self._panic_flatten()
                    return False

                await asyncio.sleep(2)
                self.ib.waitOnUpdate()

                # Check if all positions are flat
                pos = [p for p in self.ib.positions()
                      if p.contract.conId == self.contract.conId
                      and p.account in [self.bot.long_account, self.bot.short_account]]

                if not pos or all(p.position == 0 for p in pos):
                    break

            print("\n>>> ALL POSITIONS CLOSED.")
            await self.log_event('POSITIONS_FLAT', 'INFO', "All positions closed")

            # 6. CLEANUP ORPHAN ORDERS
            await self._cleanup_orphan_orders()

            # Final sync
            await asyncio.sleep(2)
            self.ib.waitOnUpdate()

            print("[CYCLE] Cycle completed successfully")
            return True

        except Exception as e:
            print(f"[CYCLE ERROR] {e}")
            await self.log_event('CYCLE_ERROR', 'ERROR', f"Cycle execution error: {str(e)}")
            # Don't flatten on error - smart reconciliation will handle recovery on restart
            # Use PANIC button if immediate position closure is needed
            return False

    async def _panic_flatten(self):
        """
        Flatten all positions for this contract/bot in case of errors.

        If a trailing/benefit order is already active, let it run; otherwise exit ASAP.
        """
        print("\n[PANIC] Flattening all positions (unless trailing is active)...")
        await self.log_event('PANIC_FLATTEN', 'WARNING', "Flattening positions due to error")

        # If IBKR connectivity is down, avoid sending new orders that will fail
        if self.connectivity_down:
            print("[PANIC] Skipping flatten because IBKR connectivity is down; will retry when connection is healthy.")
            return

        # If trailing order is active, prefer to let it execute rather than sending new market orders
        trailing_active = False
        trailing_roles = {'LONG_TRAIL', 'SHORT_TRAIL'}
        if self.cycle:
            trailing_active = await sync_to_async(
                lambda: self.cycle.orders.filter(
                    role__in=trailing_roles,
                    status__in=['Submitted', 'PreSubmitted']
                ).exists()
            )()

        if trailing_active:
            print("[PANIC] Trailing order active; skipping manual flatten to avoid double fills.")
        else:
            for acc in [self.bot.long_account, self.bot.short_account]:
                pos = [p for p in self.ib.positions()
                      if p.contract.conId == self.contract.conId and p.account == acc]
                if pos and pos[0].position != 0:
                    qty = abs(pos[0].position)
                    action = 'SELL' if pos[0].position > 0 else 'BUY'
                    print(f"[PANIC] Flattening {acc}: {action} {qty} shares...")
                    from ib_insync import MarketOrder
                    self.ib.placeOrder(self.contract, MarketOrder(action, qty, account=acc))

        # Cleanup orphan orders (stops, pending entries)
        await self._cleanup_orphan_orders()

    async def _cleanup_orphan_orders(self):
        """Cancel any open orders for this contract/bot"""
        print("[CLEANUP] Cleaning up orphan orders...")
        orphans = []
        for t in self.ib.openTrades():
            if (t.contract.conId == self.contract.conId and
                t.order.account in [self.bot.long_account, self.bot.short_account]):
                print(f"[CLEANUP] Cancelling {t.order.orderType} order {t.order.orderId} on {t.order.account}")
                self.ib.cancelOrder(t.order)
                orphans.append(t)

        if orphans:
            print(f"[CLEANUP] Waiting for {len(orphans)} cancellations...")
            deadline = asyncio.get_event_loop().time() + 10
            while any(not t.isDone() for t in orphans) and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.5)
                self.ib.waitOnUpdate()
