"""
Bot Runner - Executes hedge bot logic for a Bot instance

This module contains the core bot execution logic that can be invoked
by Celery/Procrastinate tasks or management commands.
"""

import asyncio
import math
import random
from datetime import datetime, timezone
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
        self.exec_ids_seen = set()
        self.transitioning = False
        self.transition_done = False

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

    async def check_bot_status(self):
        """Check if bot should continue running"""
        @sync_to_async
        def refresh_bot():
            self.bot.refresh_from_db()
            return self.bot.status

        status = await refresh_bot()
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
        self.transitioning = False
        self.transition_done = False

    def on_error(self, trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        """Handle IBKR errors"""
        if reqId == -1:
            return
        msg = errorString if errorString else str(errorCode)
        code = errorCode if errorString else "INFO"
        self.log_event('IBKR_ERROR', 'WARNING', f"IBKR {code}: {msg} (reqId={reqId})",
                      data={'reqId': reqId, 'errorCode': code})

    def on_commission_report(self, trade, fill, report):
        """Handle commission reports"""
        exec_id = getattr(report, 'execId', None) or fill.execution.execId
        if exec_id in self.exec_ids_seen:
            return

        try:
            execution = Execution.objects.get(exec_id=exec_id)
            execution.commission = Decimal(str(report.commission))
            execution.commission_currency = report.currency or 'USD'
            execution.save()

            # Update cycle P&L
            with transaction.atomic():
                cycle = Cycle.objects.select_for_update().get(pk=self.cycle.pk)
                cycle.total_commission += Decimal(str(report.commission))
                cycle.net_pnl = cycle.total_sells - cycle.total_buys - cycle.total_commission
                cycle.save()
                self.cycle = cycle

            exec_time = self.get_safe_timestamp(fill.execution.time)
            print(f"[{self.fmt_ts(exec_time)}] [COMMISSION]: {report.commission:.2f} {report.currency}")
        except Execution.DoesNotExist:
            pass
        except Exception as e:
            print(f"[ERROR] Commission report handling failed: {e}")

    def on_trailing_stop_status(self, trade):
        """Handle trailing stop status updates"""
        status = trade.orderStatus
        curr_stop = getattr(status, 'stopPrice', 0)
        if curr_stop <= 0:
            curr_stop = getattr(trade.order, 'auxPrice', 0)

        if status.status == 'Filled':
            print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | EXECUTED at {status.avgFillPrice:.4f}")
            self.log_event('TRAILING_UPDATE', 'INFO', f"Trailing stop filled at {status.avgFillPrice:.4f}")
        else:
            price_str = f"{curr_stop:.4f}" if 0 < curr_stop < 1e10 else "Calculating..."
            print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | Current Stop: {price_str}")

    async def on_stop_loss_fill(self, trade, fill):
        """Handle stop loss fill and transition to trailing stop"""
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
            positions = [p for p in self.ib.positions() if p.contract.conId == self.contract.conId and p.account == acc]
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
                self.cycle.status = 'ACTIVE'
                self.cycle.save()
            self.transitioning = False

    def on_fill(self, trade, fill):
        """Handle order fills"""
        exec_id = fill.execution.execId

        if exec_id in self.exec_ids_seen:
            return
        self.exec_ids_seen.add(exec_id)

        # Get the Order from database
        if trade.order.orderId not in self.order_map:
            print(f"Warning: Fill for unknown order {trade.order.orderId}")
            return

        db_order = self.order_map[trade.order.orderId]
        exec_obj = fill.execution

        # Create Execution record
        exec_time = self.get_safe_timestamp(exec_obj.time)

        try:
            with transaction.atomic():
                execution = Execution.objects.create(
                    order=db_order,
                    cycle=self.cycle,
                    exec_id=exec_id,
                    side=exec_obj.side,
                    shares=Decimal(str(exec_obj.shares)),
                    price=Decimal(str(exec_obj.price)),
                    account=trade.order.account,
                    commission=Decimal('0'),
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
                cycle = Cycle.objects.select_for_update().get(pk=self.cycle.pk)
                if exec_obj.side == 'BOT':  # BOT means BUY in IBKR
                    cycle.total_buys += Decimal(str(exec_obj.shares)) * Decimal(str(exec_obj.price))
                else:  # SLD means SELL
                    cycle.total_sells += Decimal(str(exec_obj.shares)) * Decimal(str(exec_obj.price))

                cycle.net_pnl = cycle.total_sells - cycle.total_buys - cycle.total_commission
                cycle.save()
                self.cycle = cycle

            print(f"--- [{self.fmt_ts(exec_time)}] {db_order.role} FILLED on {trade.order.account}: {exec_obj.shares} @ {exec_obj.price:.4f} ---")
            self.log_event('ORDER_FILLED', 'INFO', f"{db_order.role} filled: {exec_obj.shares}@{exec_obj.price:.4f}", order=db_order)

            # Trigger transition if it's a stop loss fill
            if db_order.role in ("LONG_SL", "SHORT_SL") and not self.transitioning and not self.transition_done:
                asyncio.create_task(self.on_stop_loss_fill(trade, fill))
        except Exception as e:
            print(f"[ERROR] Fill handling failed: {e}")
            self.log_event('SYSTEM_ERROR', 'ERROR', f"Fill handling error: {str(e)}")

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

            # Connect to IBKR with unique client ID
            # Formula: (timestamp_ms % 100000) + (bot_id * 100000)
            # This ensures each bot has a unique ID that changes with every connection attempt
            # and doesn't conflict even when multiple bots connect simultaneously
            timestamp_component = int(time.time() * 1000) % 100000  # milliseconds, last 5 digits
            client_id = (self.bot.id * 100000) + timestamp_component

            self.ib = IB()
            self.ib.errorEvent += self.on_error
            self.ib.commissionReportEvent += self.on_commission_report

            print(f"[CONNECTION] Connecting to TWS with clientId={client_id} (bot_id={self.bot.id}, timestamp={timestamp_component})")
            await self.ib.connectAsync('127.0.0.1', self.bot.port, clientId=client_id)
            await self.log_event('BOT_START', 'INFO', f"Bot started: {self.bot.symbol} (clientId={client_id})")

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

            # Main cycle loop
            while not self.should_stop and await self.check_bot_status():
                self.reset_cycle_state()

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
                    print("[CYCLE] Failed or not implemented - waiting 60s before retry")
                    # Mark cycle as failed
                    @sync_to_async
                    def mark_failed():
                        self.cycle.status = 'FAILED'
                        self.cycle.completed_at = datetime.now(timezone.utc)
                        self.cycle.save()
                    await mark_failed()

                    # Sleep with status checking (check every second)
                    for _ in range(60):
                        if not await self.check_bot_status():
                            break
                        await asyncio.sleep(1)
                    continue

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
                    # Only mark as stopped if it's not already in RUNNING state
                    # (i.e., if it was explicitly stopped by user)
                    if self.bot.status != 'RUNNING':
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

            # Wait for fills with timeout
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

            # Long Stop Loss (Sell Stop below entry)
            l_sl_price = self.q_floor(l_price * (1 - float(self.bot.stop_pct) / 100), min_tick)
            print(f"Placing LONG Stop Loss on {self.bot.long_account} at {l_sl_price:.4f}...")
            l_sl_ord = StopOrder('SELL', l_qty, l_sl_price, account=self.bot.long_account,
                                tif='GTC', outsideRth=True, orderRef=f"C{cycle_number}_LONG_SL")

            # Short Stop Loss (Buy Stop above entry)
            s_sl_price = self.q_ceil(s_price * (1 + float(self.bot.stop_pct) / 100), min_tick)
            print(f"Placing SHORT Stop Loss on {self.bot.short_account} at {s_sl_price:.4f}...")
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
            await self._panic_flatten()
            return False

    async def _panic_flatten(self):
        """Flatten all positions for this contract/bot in case of errors"""
        print("\n[PANIC] Flattening all positions...")
        await self.log_event('PANIC_FLATTEN', 'WARNING', "Flattening all positions due to error")

        for acc in [self.bot.long_account, self.bot.short_account]:
            pos = [p for p in self.ib.positions()
                  if p.contract.conId == self.contract.conId and p.account == acc]
            if pos and pos[0].position != 0:
                qty = abs(pos[0].position)
                action = 'SELL' if pos[0].position > 0 else 'BUY'
                print(f"[PANIC] Flattening {acc}: {action} {qty} shares...")
                from ib_insync import MarketOrder
                self.ib.placeOrder(self.contract, MarketOrder(action, qty, account=acc))

        # Cleanup orphan orders
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

    async def _panic_flatten(self):
        """Flatten all positions for this contract/bot in case of errors"""
        print("\n[PANIC] Flattening all positions...")
        await self.log_event('PANIC_FLATTEN', 'WARNING', "Flattening all positions due to error")

        for acc in [self.bot.long_account, self.bot.short_account]:
            pos = [p for p in self.ib.positions()
                  if p.contract.conId == self.contract.conId and p.account == acc]
            if pos and pos[0].position != 0:
                qty = abs(pos[0].position)
                action = 'SELL' if pos[0].position > 0 else 'BUY'
                print(f"[PANIC] Flattening {acc}: {action} {qty} shares...")
                from ib_insync import MarketOrder
                self.ib.placeOrder(self.contract, MarketOrder(action, qty, account=acc))

        # Cleanup orphan orders
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
