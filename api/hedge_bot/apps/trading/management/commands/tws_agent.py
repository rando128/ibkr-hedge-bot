from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from hedge_bot.apps.trading.models import Bot, Cycle, Event

logger = logging.getLogger(__name__)


ORDERREF_PREFIX = "HB"


@dataclass(frozen=True)
class OrderRef:
    bot_id: int
    cycle_key: str
    role: str


def build_order_ref(bot_id: int, cycle_key: str, role: str) -> str:
    return f"{ORDERREF_PREFIX}:{bot_id}:{cycle_key}:{role}"


def parse_order_ref(order_ref: str) -> Optional[OrderRef]:
    if not order_ref:
        return None
    # Expected: HB:{bot_id}:{cycle_key}:{role}
    parts = order_ref.split(":")
    if len(parts) != 4:
        return None
    prefix, bot_id_s, cycle_key, role = parts
    if prefix != ORDERREF_PREFIX:
        return None
    try:
        bot_id = int(bot_id_s)
    except ValueError:
        return None
    return OrderRef(bot_id=bot_id, cycle_key=cycle_key, role=role)


def q_floor(price: float, tick: float) -> float:
    return math.floor((price + 1e-12) / tick) * tick


def q_ceil(price: float, tick: float) -> float:
    return math.ceil((price - 1e-12) / tick) * tick


def get_exchange_tick_size(price: float, primary_exchange: str, base_tick: float) -> float:
    """
    Exchange-aware tick sizing (Euronext tiers). Falls back to IB minTick.
    Mirrors the develop BotRunner behavior.
    """
    ex = (primary_exchange or "").upper()
    if ex in {"SBF", "AEB", "EBR"}:
        if price < 50:
            return 0.01
        elif price < 100:
            return 0.05
        elif price < 500:
            return 0.10
        else:
            return 0.50
    return base_tick


class Command(BaseCommand):
    help = "Run the always-on TWS agent (Option B)."

    def add_arguments(self, parser):
        parser.add_argument("--host", default="127.0.0.1")
        parser.add_argument("--port", type=int, default=None, help="Overrides env default (PAPER=7497, LIVE=7496)")
        parser.add_argument("--environment", choices=["PAPER", "LIVE"], default="PAPER")
        parser.add_argument("--client-id", type=int, default=7000, help="IB clientId for the agent connection")
        parser.add_argument("--reconcile-interval", type=float, default=10.0)
        parser.add_argument("--poll-interval", type=float, default=0.5)

    def handle(self, *args, **options):
        from ib_insync import IB, Stock, MarketOrder, StopOrder, Order as IBOrder, util

        util.patchAsyncio()

        host: str = options["host"]
        environment: str = options["environment"]
        port: int = options["port"] or (7497 if environment == "PAPER" else 7496)
        client_id: int = options["client_id"]
        reconcile_interval: float = options["reconcile_interval"]
        poll_interval: float = options["poll_interval"]

        ib = IB()
        connectivity_down = False

        def log_event(
            *,
            level: str,
            event_type: str,
            message: str,
            bot: Optional[Bot] = None,
            cycle: Optional[Cycle] = None,
            from_state: str = "",
            to_state: str = "",
            data: Optional[dict] = None,
        ) -> None:
            try:
                Event.objects.create(
                    bot=bot,
                    cycle=cycle,
                    level=level,
                    event_type=event_type,
                    message=message,
                    data=data,
                    from_state=from_state,
                    to_state=to_state,
                )
            except Exception:
                logger.exception("Failed to write Event: %s", message)

        def transition_cycle(cycle: Cycle, to_state: str, message: str, *, level: str = "INFO", data: Optional[dict] = None) -> Cycle:
            from_state = cycle.state
            if from_state == to_state:
                return cycle
            cycle.state = to_state
            cycle.last_activity_at = timezone.now()
            cycle.save(update_fields=["state", "last_activity_at"])
            log_event(
                level=level,
                event_type="STATE_TRANSITION",
                message=message,
                bot=cycle.bot,
                cycle=cycle,
                from_state=from_state,
                to_state=to_state,
                data=data,
            )
            return cycle

        def load_or_create_cycle(bot: Bot) -> Cycle:
            active = bot.cycles.exclude(state__in=("COMPLETED", "ABORTED")).order_by("-id").first()
            if active:
                return active
            with transaction.atomic():
                max_cycle = bot.cycles.select_for_update().aggregate(Max("cycle_number"))["cycle_number__max"]
                next_cycle_number = (max_cycle or 0) + 1
                cycle = Cycle.objects.create(
                    bot=bot,
                    cycle_number=next_cycle_number,
                    symbol=bot.symbol.upper(),
                    state="INITIALIZING",
                    last_activity_at=timezone.now(),
                )
            log_event(level="INFO", event_type="CYCLE_START", message=f"Cycle {cycle.cycle_number} created", bot=bot, cycle=cycle)
            return cycle

        def get_contract(bot: Bot):
            contract = Stock(bot.symbol.upper(), bot.exchange, bot.currency)
            if bot.primary_exchange:
                contract.primaryExchange = bot.primary_exchange
            return contract

        def with_retries(fn, *, attempts: int = 3, base_delay: float = 0.5, action: str = "action"):
            last_exc = None
            for attempt in range(attempts):
                try:
                    return fn()
                except Exception as exc:
                    last_exc = exc
                    delay = base_delay * (2**attempt)
                    logger.warning("%s failed (attempt %s/%s): %s", action, attempt + 1, attempts, exc)
                    ib.sleep(delay)
            raise last_exc  # type: ignore[misc]

        def request_snapshots():
            ib.reqPositions()
            ib.reqAllOpenOrders()
            ib.sleep(0.5)

        def mark_cycles_recovering():
            """On agent start, mark in-flight cycles as RECOVERING for fresh reconciliation."""
            candidates = Cycle.objects.filter(
                bot__status__in=("RUNNING", "STOPPING"),
                state__in=("INITIALIZING", "ENTERING", "ACTIVE", "TRANSITIONING", "TRAILING", "PNL_CALCULATION", "RECOVERING"),
            )
            for cycle in candidates:
                transition_cycle(cycle, "RECOVERING", "Agent restart: entering RECOVERING")

        def open_trades_for_cycle(contract_conid: int, bot: Bot, cycle: Cycle):
            prefix = f"{ORDERREF_PREFIX}:{bot.id}:{cycle.cycle_key}:"
            trades = [
                t
                for t in ib.openTrades()
                if t.contract.conId == contract_conid
                and t.order.account in (bot.long_account, bot.short_account)
                and (getattr(t.order, "orderRef", "") or "").startswith(prefix)
            ]
            return trades

        def find_open_trade_by_role(contract_conid: int, bot: Bot, cycle: Cycle, role: str):
            target = build_order_ref(bot.id, str(cycle.cycle_key), role)
            for t in ib.openTrades():
                if t.contract.conId != contract_conid:
                    continue
                if t.order.account not in (bot.long_account, bot.short_account):
                    continue
                if getattr(t.order, "orderRef", "") == target:
                    return t
            return None

        def get_position_qty(contract_conid: int, account: str) -> Decimal:
            for p in ib.positions():
                if p.contract.conId == contract_conid and p.account == account:
                    return Decimal(str(p.position or 0))
            return Decimal("0")

        def get_position_avg_cost(contract_conid: int, account: str) -> Optional[Decimal]:
            for p in ib.positions():
                if p.contract.conId == contract_conid and p.account == account and p.position:
                    return Decimal(str(p.avgCost))
            return None

        def ensure_sl_orders(bot: Bot, cycle: Cycle, contract, min_tick: float):
            """
            Ensure there is an SL order per account matching current position qty.
            This is idempotent and safe to run frequently.
            Avoids cancel/replace races by skipping if the existing SL is still pending submit.
            """
            request_snapshots()
            contract_conid = contract.conId
            primary_exch = bot.primary_exchange or getattr(contract, "primaryExchange", "") or bot.exchange

            def tick_for_price(px: float) -> float:
                return get_exchange_tick_size(px, primary_exch, min_tick)

            # Long account: should hold +qty when active
            long_pos = get_position_qty(contract_conid, bot.long_account)
            if long_pos > 0:
                avg_cost = get_position_avg_cost(contract_conid, bot.long_account)
                if avg_cost is not None:
                    tick = tick_for_price(float(avg_cost))
                    stop_price = q_floor(float(avg_cost) * (1 - float(bot.stop_pct)), tick)
                    existing = find_open_trade_by_role(contract_conid, bot, cycle, "LONG_SL")
                    desired_qty = float(abs(long_pos))
                    if not existing or float(existing.order.totalQuantity or 0) != desired_qty:
                        if existing:
                            st = existing.orderStatus.status
                            if st in {"PendingSubmit", "PreSubmitted"}:
                                # Avoid cancel/replace race; retry next tick
                                log_event(
                                    level="DEBUG",
                                    event_type="SL_PENDING",
                                    message=f"LONG_SL still {st}, skip replace this tick",
                                    bot=bot,
                                    cycle=cycle,
                                )
                                return  # wait next tick; don't place replacement yet
                            else:
                                with_retries(lambda: ib.cancelOrder(existing.order), action="cancel LONG_SL")
                        sl = StopOrder(
                            action="SELL",
                            totalQuantity=desired_qty,
                            stopPrice=stop_price,
                            account=bot.long_account,
                            tif="GTC",
                            outsideRth=True,
                            orderRef=build_order_ref(bot.id, str(cycle.cycle_key), "LONG_SL"),
                        )
                        with_retries(lambda: ib.placeOrder(contract, sl), action="place LONG_SL")
                        log_event(
                            level="INFO",
                            event_type="SL_PLACED",
                            message=f"Placed LONG_SL qty={desired_qty} stop={stop_price}",
                            bot=bot,
                            cycle=cycle,
                            data={"qty": desired_qty, "stop_price": stop_price},
                        )
                        cycle.last_activity_at = timezone.now()
                        cycle.save(update_fields=["last_activity_at"])

            # Short account: should hold -qty when active
            short_pos = get_position_qty(contract_conid, bot.short_account)
            if short_pos < 0:
                avg_cost = get_position_avg_cost(contract_conid, bot.short_account)
                if avg_cost is not None:
                    tick = tick_for_price(float(avg_cost))
                    stop_price = q_ceil(float(avg_cost) * (1 + float(bot.stop_pct)), tick)
                    existing = find_open_trade_by_role(contract_conid, bot, cycle, "SHORT_SL")
                    desired_qty = float(abs(short_pos))
                    if not existing or float(existing.order.totalQuantity or 0) != desired_qty:
                        if existing:
                            st = existing.orderStatus.status
                            if st in {"PendingSubmit", "PreSubmitted"}:
                                log_event(
                                    level="DEBUG",
                                    event_type="SL_PENDING",
                                    message=f"SHORT_SL still {st}, skip replace this tick",
                                    bot=bot,
                                    cycle=cycle,
                                )
                                return
                            else:
                                with_retries(lambda: ib.cancelOrder(existing.order), action="cancel SHORT_SL")
                        sl = StopOrder(
                            action="BUY",
                            totalQuantity=desired_qty,
                            stopPrice=stop_price,
                            account=bot.short_account,
                            tif="GTC",
                            outsideRth=True,
                            orderRef=build_order_ref(bot.id, str(cycle.cycle_key), "SHORT_SL"),
                        )
                        with_retries(lambda: ib.placeOrder(contract, sl), action="place SHORT_SL")
                        log_event(
                            level="INFO",
                            event_type="SL_PLACED",
                            message=f"Placed SHORT_SL qty={desired_qty} stop={stop_price}",
                            bot=bot,
                            cycle=cycle,
                            data={"qty": desired_qty, "stop_price": stop_price},
                        )
                        cycle.last_activity_at = timezone.now()
                        cycle.save(update_fields=["last_activity_at"])

        def place_entries(bot: Bot, cycle: Cycle, contract):
            qty = float(bot.qty)
            orders = [
                MarketOrder(
                    action="BUY",
                    totalQuantity=qty,
                    account=bot.long_account,
                    tif="DAY",
                    outsideRth=True,
                    orderRef=build_order_ref(bot.id, str(cycle.cycle_key), "LONG_ENTRY"),
                ),
                MarketOrder(
                    action="SELL",
                    totalQuantity=qty,
                    account=bot.short_account,
                    tif="DAY",
                    outsideRth=True,
                    orderRef=build_order_ref(bot.id, str(cycle.cycle_key), "SHORT_ENTRY"),
                ),
            ]
            for o in orders:
                with_retries(lambda o=o: ib.placeOrder(contract, o), action=f"place {o.orderRef}")
            log_event(level="INFO", event_type="ENTRIES_SUBMITTED", message="Submitted long+short entries", bot=bot, cycle=cycle)
            cycle.last_activity_at = timezone.now()
            cycle.save(update_fields=["last_activity_at"])

        def place_trailing(bot: Bot, cycle: Cycle, contract, surviving_role: str, min_tick: float):
            contract_conid = contract.conId
            request_snapshots()

            if surviving_role == "LONG":
                account = bot.long_account
                pos = get_position_qty(contract_conid, account)
                if pos <= 0:
                    return
                action = "SELL"
                role = "LONG_TRAIL"
            else:
                account = bot.short_account
                pos = get_position_qty(contract_conid, account)
                if pos >= 0:
                    return
                action = "BUY"
                role = "SHORT_TRAIL"

            qty = float(abs(pos))
            trailing_percent = float(bot.trailing_pct) * 100.0
            order = IBOrder(
                action=action,
                totalQuantity=qty,
                orderType="TRAIL",
                trailingPercent=trailing_percent,
                account=account,
                tif="GTC",
                outsideRth=True,
                orderRef=build_order_ref(bot.id, str(cycle.cycle_key), role),
            )
            with_retries(lambda: ib.placeOrder(contract, order), action=f"place {role}")
            log_event(
                level="INFO",
                event_type="TRAIL_PLACED",
                message=f"Placed {role} qty={qty} trailingPercent={trailing_percent}",
                bot=bot,
                cycle=cycle,
                data={"qty": qty, "trailing_percent": trailing_percent},
            )
            cycle.last_activity_at = timezone.now()
            cycle.save(update_fields=["last_activity_at"])

        def cancel_remaining_sl_and_trail(bot: Bot, cycle: Cycle, contract, sl_role: str, min_tick: float):
            contract_conid = contract.conId
            other_role = "SHORT_SL" if sl_role == "LONG_SL" else "LONG_SL"
            surviving = "LONG" if sl_role == "SHORT_SL" else "SHORT"

            def _cancel_other():
                other_trade = find_open_trade_by_role(contract_conid, bot, cycle, other_role)
                if other_trade:
                    ib.cancelOrder(other_trade.order)

            with_retries(_cancel_other, action=f"cancel {other_role}")

            # Wait briefly for cancellation to finalize to avoid races/double orders
            deadline = time.time() + 5
            while time.time() < deadline:
                other_trade = find_open_trade_by_role(contract_conid, bot, cycle, other_role)
                if not other_trade:
                    break
                st = other_trade.orderStatus.status
                if st in {"Cancelled", "ApiCancelled"}:
                    break
                ib.sleep(0.2)

            place_trailing(bot, cycle, contract, surviving_role=surviving, min_tick=min_tick)

        def panic_flatten(bot: Bot, cycle: Optional[Cycle], contract):
            request_snapshots()
            contract_conid = contract.conId
            # Cancel all open trades for bot/accounts on this contract (regardless of cycle)
            for t in list(ib.openTrades()):
                if t.contract.conId != contract_conid:
                    continue
                if t.order.account not in (bot.long_account, bot.short_account):
                    continue
                with_retries(lambda t=t: ib.cancelOrder(t.order), action=f"cancel order {t.order.orderId}")

            request_snapshots()
            # Flatten positions on both accounts
            for account in (bot.long_account, bot.short_account):
                qty = get_position_qty(contract_conid, account)
                if qty == 0:
                    continue
                action = "SELL" if qty > 0 else "BUY"
                with_retries(lambda: ib.placeOrder(contract, MarketOrder(action, float(abs(qty)), account=account)), action=f"flatten {account}")

            log_event(
                level="CRITICAL",
                event_type="PANIC_EXECUTED",
                message="Panic flatten executed (cancel + flatten)",
                bot=bot,
                cycle=cycle,
            )

        def reconcile_bot(bot: Bot):
            contract = get_contract(bot)
            ib.qualifyContracts(contract)
            details = ib.reqContractDetails(contract)
            min_tick = float(details[0].minTick) if details else 0.01

            cycle = bot.cycles.exclude(state__in=("COMPLETED", "ABORTED")).order_by("-id").first()

            if bot.panic_requested:
                if cycle:
                    transition_cycle(cycle, "PANIC", "panic_requested=True")
                panic_flatten(bot, cycle, contract)
                bot.panic_requested = False
                if bot.status == "RUNNING":
                    bot.status = "STOPPING"
                    bot.stopped_at = timezone.now()
                bot.save(update_fields=["status", "stopped_at", "panic_requested"])
                # If we are in STOPPING, and now flat/clean, mark STOPPED
                request_snapshots()
                long_pos = get_position_qty(contract.conId, bot.long_account)
                short_pos = get_position_qty(contract.conId, bot.short_account)
                open_trades_for_bot = [
                    t for t in ib.openTrades()
                    if t.contract.conId == contract.conId and t.order.account in (bot.long_account, bot.short_account)
                ]
                if long_pos == 0 and short_pos == 0 and not open_trades_for_bot:
                    if cycle and cycle.state != "ABORTED":
                        transition_cycle(cycle, "ABORTED", "Cycle closed after panic cleanup", level="WARNING")
                        cycle.completed_at = timezone.now()
                        cycle.save(update_fields=["completed_at"])
                        log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle aborted after panic", bot=bot, cycle=cycle)
                    if bot.status != "STOPPED":
                        bot.status = "STOPPED"
                        bot.stopped_at = timezone.now()
                        bot.save(update_fields=["status", "stopped_at"])
                        log_event(level="INFO", event_type="BOT_STOPPED", message="Bot stopped after panic cleanup", bot=bot, cycle=cycle)
                return

            if bot.status == "STOPPED":
                return

            if bot.status == "ERROR":
                return

            if not cycle and bot.status == "RUNNING":
                cycle = load_or_create_cycle(bot)

            if not cycle:
                # If we're STOPPING and fully flat/clean, mark STOPPED
                if bot.status == "STOPPING":
                    request_snapshots()
                    long_pos = get_position_qty(contract.conId, bot.long_account)
                    short_pos = get_position_qty(contract.conId, bot.short_account)
                    open_trades_for_bot = [
                        t for t in ib.openTrades()
                        if t.contract.conId == contract.conId and t.order.account in (bot.long_account, bot.short_account)
                    ]
                    if long_pos == 0 and short_pos == 0 and not open_trades_for_bot:
                        bot.status = "STOPPED"
                        bot.stopped_at = timezone.now()
                        bot.save(update_fields=["status", "stopped_at"])
                        log_event(level="INFO", event_type="BOT_STOPPED", message="Bot stopped (no active cycle; flat/clean)", bot=bot)
                return

            # Safety timer: only applies in hedge activation states.
            if cycle.state in ("INITIALIZING", "ENTERING"):
                age = timezone.now() - cycle.last_activity_at
                if age.total_seconds() > 30:
                    transition_cycle(cycle, "PANIC", f"Safety timer exceeded (age={age})", level="CRITICAL")
                    panic_flatten(bot, cycle, contract)
                    return

            request_snapshots()
            contract_conid = contract.conId

            long_pos = get_position_qty(contract_conid, bot.long_account)
            short_pos = get_position_qty(contract_conid, bot.short_account)

            long_sl = find_open_trade_by_role(contract_conid, bot, cycle, "LONG_SL")
            short_sl = find_open_trade_by_role(contract_conid, bot, cycle, "SHORT_SL")
            long_trail = find_open_trade_by_role(contract_conid, bot, cycle, "LONG_TRAIL")
            short_trail = find_open_trade_by_role(contract_conid, bot, cycle, "SHORT_TRAIL")

            if cycle.state == "RECOVERING":
                cycle_trades = open_trades_for_cycle(contract_conid, bot, cycle)
                # Hedge intact with SLs
                if long_pos > 0 and short_pos < 0 and long_sl and short_sl:
                    transition_cycle(cycle, "ACTIVE", "Recovered: hedge intact with SLs")
                    return
                # Already trailing
                if long_trail or short_trail:
                    transition_cycle(cycle, "TRAILING", "Recovered: trailing already active")
                    return
                # Flat and clean -> abort cycle
                if long_pos == 0 and short_pos == 0 and not cycle_trades:
                    transition_cycle(cycle, "ABORTED", "Recovered: flat and clean; closing cycle", level="WARNING")
                    cycle.completed_at = timezone.now()
                    cycle.save(update_fields=["completed_at"])
                    log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle aborted/cleaned", bot=bot, cycle=cycle)
                    return
                # Hedge imbalance or missing protections -> panic
                transition_cycle(cycle, "PANIC", "Recovered: hedge incomplete or protections missing; panic", level="CRITICAL")
                try:
                    panic_flatten(bot, cycle, contract)
                except Exception as exc:
                    logger.exception("panic_flatten failed during RECOVERING for bot=%s: %s", bot.id, exc)
                    cycle.state = "ERROR"
                    cycle.save(update_fields=["state"])
                    bot.status = "ERROR"
                    bot.last_error = str(exc)
                    bot.save(update_fields=["status", "last_error"])
                    log_event(level="ERROR", event_type="BOT_ERROR", message=str(exc), bot=bot, cycle=cycle)
                return

            # Recovery: hedge imbalance (one leg missing) outside trailing state -> panic/flatten
            if cycle.state in {"INITIALIZING", "ENTERING", "ACTIVE", "TRANSITIONING"}:
                if (long_pos == 0) != (short_pos == 0):
                    transition_cycle(cycle, "PANIC", "Hedge imbalance detected (one leg missing); panic/flatten", level="CRITICAL")
                    panic_flatten(bot, cycle, contract)
                    return

            # If previous panic/aborted/error cycle is flat and clean, close it so a new cycle can start
            if cycle.state in {"PANIC", "ABORTED", "ERROR"}:
                cycle_trades = open_trades_for_cycle(contract_conid, bot, cycle)
                if long_pos == 0 and short_pos == 0 and not cycle_trades:
                    transition_cycle(cycle, "ABORTED", "Cycle closed after panic/error cleanup", level="WARNING")
                    cycle.completed_at = timezone.now()
                    cycle.save(update_fields=["completed_at"])
                    log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle aborted/cleaned", bot=bot, cycle=cycle)
                    cycle = None
                    if bot.status == "STOPPING":
                        bot.status = "STOPPED"
                        bot.stopped_at = timezone.now()
                        bot.save(update_fields=["status", "stopped_at"])
                        log_event(level="INFO", event_type="BOT_STOPPED", message="Bot stopped after cycle cleanup", bot=bot)
                        return
                    # Will create a fresh cycle below if bot remains RUNNING

            if not cycle and bot.status == "RUNNING":
                cycle = load_or_create_cycle(bot)

            if not cycle:
                return

            if cycle.state not in {"COMPLETED", "ABORTED", "PANIC", "ERROR", "RECOVERING"}:
                transition_cycle(cycle, "RECOVERING", "Agent reconcile: entering RECOVERING")

            if cycle.state == "INITIALIZING":
                transition_cycle(cycle, "ENTERING", "Starting entries + protection")
                place_entries(bot, cycle, contract)

            if cycle.state == "ENTERING":
                ensure_sl_orders(bot, cycle, contract, min_tick=min_tick)
                # When both positions exist and both SL are in place, hedge is active.
                if long_pos > 0 and short_pos < 0 and long_sl and short_sl:
                    transition_cycle(cycle, "ACTIVE", "Both SL protections confirmed in place")
                return

            if cycle.state == "ACTIVE":
                # Keep SL qty aligned to positions (partial fills)
                ensure_sl_orders(bot, cycle, contract, min_tick=min_tick)
                return

            if cycle.state == "TRANSITIONING":
                # Either trailing exists or we retry on next tick.
                if long_trail or short_trail:
                    transition_cycle(cycle, "TRAILING", "Trailing active")
                return

            if cycle.state == "TRAILING":
                # Completion condition: both accounts flat and no open orders for this cycle.
                cycle_trades = open_trades_for_cycle(contract_conid, bot, cycle)
                if long_pos == 0 and short_pos == 0 and not cycle_trades:
                    transition_cycle(cycle, "PNL_CALCULATION", "Positions flat; calculating P&L")
                return

            if cycle.state == "PNL_CALCULATION":
                # Placeholder: rely on IB executions when available.
                transition_cycle(cycle, "COMPLETED", "Cycle completed (PNL calculation placeholder)")
                cycle.completed_at = timezone.now()
                cycle.save(update_fields=["completed_at"])
                log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle completed", bot=bot, cycle=cycle)
                if bot.status == "STOPPING":
                    bot.status = "STOPPED"
                    bot.stopped_at = timezone.now()
                    bot.save(update_fields=["status", "stopped_at"])
                    log_event(level="INFO", event_type="BOT_STOPPED", message="Bot stopped after cycle completion", bot=bot, cycle=cycle)
                return

        def on_error(reqId, errorCode, errorString, contract):
            nonlocal connectivity_down
            if errorCode in (1100, 1101):
                connectivity_down = True
            elif errorCode in (1102, 1103):
                connectivity_down = False
            logger.warning("IB error code=%s reqId=%s msg=%s", errorCode, reqId, errorString)

        def on_exec_details(trade, fill):
            # We only react in real-time to stop-loss fills (SL -> trailing flip).
            try:
                order_ref = getattr(trade.order, "orderRef", "") if trade else ""
                parsed = parse_order_ref(order_ref)
                if not parsed:
                    return
                if parsed.role.endswith("_ENTRY"):
                    bot = Bot.objects.filter(pk=parsed.bot_id).first()
                    if not bot:
                        return
                    cycle = bot.cycles.filter(cycle_key=parsed.cycle_key).first()
                    if not cycle or cycle.state in ("PANIC", "COMPLETED", "ABORTED"):
                        return
                    contract = trade.contract
                    details = ib.reqContractDetails(contract)
                    min_tick = float(details[0].minTick) if details else 0.01
                    ensure_sl_orders(bot, cycle, contract, min_tick=min_tick)
                    request_snapshots()
                    long_pos = get_position_qty(contract.conId, bot.long_account)
                    short_pos = get_position_qty(contract.conId, bot.short_account)
                    long_sl = find_open_trade_by_role(contract.conId, bot, cycle, "LONG_SL")
                    short_sl = find_open_trade_by_role(contract.conId, bot, cycle, "SHORT_SL")
                    if cycle.state in ("INITIALIZING", "ENTERING") and long_pos > 0 and short_pos < 0 and long_sl and short_sl:
                        transition_cycle(cycle, "ACTIVE", "Both SL protections confirmed after entry fill")
                    return

                if not parsed.role.endswith("_SL"):
                    return

                bot = Bot.objects.filter(pk=parsed.bot_id).first()
                if not bot:
                    return
                cycle = bot.cycles.filter(cycle_key=parsed.cycle_key).first()
                if not cycle:
                    return

                if cycle.state in ("PANIC", "COMPLETED", "ABORTED"):
                    return

                contract = trade.contract
                details = ib.reqContractDetails(contract)
                min_tick = float(details[0].minTick) if details else 0.01

                transition_cycle(cycle, "TRANSITIONING", f"{parsed.role} filled -> flip to trailing", level="WARNING")
                cancel_remaining_sl_and_trail(bot, cycle, contract, sl_role=parsed.role, min_tick=min_tick)
                transition_cycle(cycle, "TRAILING", "Trailing active after SL fill")
            except Exception as exc:
                logger.exception("on_exec_details failed: %s", exc)

        ib.errorEvent += on_error
        ib.execDetailsEvent += on_exec_details

        self.stdout.write(self.style.NOTICE(f"[TWS_AGENT] Connecting to {host}:{port} (env={environment}) clientId={client_id}"))
        ib.connect(host, port, clientId=client_id, timeout=10)
        mark_cycles_recovering()
        request_snapshots()

        last_reconcile = 0.0

        try:
            while True:
                # Process IB messages + run DB polling.
                ib.sleep(poll_interval)

                # Minimal resilience: if connectivity is down, avoid aggressive actions and just keep looping.
                now = time.time()
                if now - last_reconcile >= reconcile_interval:
                    last_reconcile = now
                    bots = Bot.objects.filter(environment=environment, status__in=("RUNNING", "STOPPING", "ERROR")).order_by("id")
                    for bot in bots:
                        try:
                            reconcile_bot(bot)
                        except Exception as exc:
                            logger.exception("reconcile_bot failed for bot=%s: %s", bot.id, exc)
                            bot.last_error = str(exc)
                            bot.status = "ERROR"
                            bot.save(update_fields=["last_error", "status"])
                            log_event(level="ERROR", event_type="BOT_ERROR", message=str(exc), bot=bot)
        finally:
            ib.disconnect()
