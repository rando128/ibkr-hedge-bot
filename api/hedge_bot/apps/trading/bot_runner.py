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
            self.log_event('STOP_LOSS_HIT', 'WARNING', f"Stop loss triggered on {trade.order.account}")

            # Update cycle status
            self.cycle.status = 'TRANSITIONING'
            self.cycle.save()

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
                if surviving_trade.order.orderId in self.order_map:
                    db_order = self.order_map[surviving_trade.order.orderId]
                    db_order.status = surviving_trade.orderStatus.status
                    db_order.save()
                    self.log_event('ORDER_CANCELLED', 'INFO', f"Cancelled {db_order.role} order", order=db_order)

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
            db_order = Order.objects.create(
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
            self.order_map[t_trade.order.orderId] = db_order
            self.log_event('TRANSITION_COMPLETE', 'INFO', f"Placed {label} trailing stop", order=db_order)

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
            await self.log_event('SYSTEM_ERROR', 'CRITICAL', f"Fatal error: {str(e)}")
            if self.bot:
                @sync_to_async
                def set_error():
                    self.bot.status = 'ERROR'
                    self.bot.save()
                await set_error()
            raise
        finally:
            if self.bot:
                @sync_to_async
                def set_stopped():
                    self.bot.status = 'STOPPED'
                    self.bot.stopped_at = datetime.now(timezone.utc)
                    self.bot.save()
                await set_stopped()
            if self.ib:
                self.ib.disconnect()
            print("[BOT] Disconnected. Goodbye!")

    async def _execute_cycle(self, min_tick, cycle_number):
        """Execute a single hedge cycle - extracted for brevity"""
        # This would contain the full cycle logic from v5
        # TODO: Move cycle logic from v5 here

        # For now, simulate a long-running cycle for testing
        print("[CYCLE] Executing placeholder cycle (simulating trading activity)")

        # Simulate trading for 5 minutes with status checking every 2 seconds
        for i in range(150):  # 150 * 2s = 5 minutes
            if not await self.check_bot_status():
                print("[CYCLE] Stop detected during cycle execution")
                return False
            await asyncio.sleep(2)

        print("[CYCLE] Placeholder cycle completed successfully")
        return True
