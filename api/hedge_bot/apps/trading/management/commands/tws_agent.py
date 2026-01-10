from __future__ import annotations

import logging
import math
import time
import threading
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Max
from django.utils import timezone

from hedge_bot.apps.trading.models import Bot, Cycle, Event

logger = logging.getLogger(__name__)
logger.info("Starting TWS agent")

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

        connectivity_down = False

        ib = IB()

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
            # Audit trail improvement: Unified logs
            numeric_level = getattr(logging, level.upper(), logging.INFO)
            ctx = []
            if bot: ctx.append(f"BOT:{bot.id}")
            if cycle:
                ctx.append(f"CYC:{cycle.cycle_number}")
                ctx.append(f"ST:{cycle.state}")

            prefix = f"[{'|'.join(ctx)}]" if ctx else "[SYSTEM]"
            log_line = f"{prefix} {event_type} | {message}"
            if data: log_line += f" | DATA: {data}"

            logger.log(numeric_level, log_line)

            try:
                Event.objects.create(
                    bot=bot,
                    cycle=cycle,
                    level=level.upper(),
                    event_type=event_type,
                    message=message,
                    data=data,
                    from_state=from_state,
                    to_state=to_state,
                )
            except Exception:
                logger.exception(f"{prefix} DB_EVENT_FAILURE | Could not save event to database")

        def transition_cycle(cycle: Cycle, to_state: str, message: str, *, level: str = "INFO",
                             data: Optional[dict] = None) -> Cycle:
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
            log_event(level="INFO", event_type="CYCLE_START", message=f"Cycle {cycle.cycle_number} created", bot=bot,
                      cycle=cycle)
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
                    delay = base_delay * (2 ** attempt)
                    logger.warning("%s failed (attempt %s/%s): %s", action, attempt + 1, attempts, exc)
                    ib.sleep(delay)
            raise last_exc  # type: ignore[misc]

        def finalize_pnl(bot: Bot, cycle: Cycle):
            from ib_insync import ExecutionFilter

            prefix = f"{ORDERREF_PREFIX}:{bot.id}:{cycle.cycle_key}:"

            def fetch_execs(with_filter: bool):
                if not with_filter:
                    return ib.reqExecutions()
                filt = ExecutionFilter()
                filt.clientId = client_id
                try:
                    filt.time = cycle.started_at.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S")
                except Exception:
                    pass
                executions: list = []
                for acct in {bot.long_account, bot.short_account}:
                    if not acct:
                        continue
                    filt.acctCode = acct
                    try:
                        executions.extend(ib.reqExecutions(filt))
                    except Exception:
                        continue
                return executions

            executions = fetch_execs(with_filter=True)
            if not executions:
                executions = fetch_execs(with_filter=False)
            total_buys = Decimal("0")
            total_sells = Decimal("0")
            total_commission = Decimal("0")
            matched = 0
            inspected = 0
            trace = []
            skip_counts = {}
            for ex in executions:
                order_ref = str(getattr(ex, "orderRef", "") or "")
                price = getattr(ex, "price", None)
                if not order_ref or price is None:
                    skip_counts["missing_orderref_or_price"] = skip_counts.get("missing_orderref_or_price", 0) + 1
                    trace.append({"skip": "missing_orderref_or_price"})
                    continue
                parsed = parse_order_ref(order_ref)
                if not parsed:
                    skip_counts["parse_failed"] = skip_counts.get("parse_failed", 0) + 1
                    trace.append({"skip": "parse_failed", "orderRef": order_ref})
                    continue
                if parsed.bot_id != bot.id:
                    skip_counts["other_bot"] = skip_counts.get("other_bot", 0) + 1
                    trace.append({"skip": "other_bot", "orderRef": order_ref})
                    continue
                if parsed.cycle_key != str(cycle.cycle_key):
                    skip_counts["other_cycle"] = skip_counts.get("other_cycle", 0) + 1
                    trace.append({"skip": "other_cycle", "orderRef": order_ref})
                    continue
                role = parsed.role.upper()

                # Define standard roles
                standard_roles = {
                    "LONG_ENTRY", "SHORT_ENTRY",
                    "LONG_SL", "SHORT_SL",
                    "LONG_TRAIL", "SHORT_TRAIL"
                }

                # Accept if it's a standard role OR if it's any PANIC order
                is_valid_role = role in standard_roles or role.startswith("PANIC_")

                if not is_valid_role:
                    skip_counts["unknown_role"] = skip_counts.get("unknown_role", 0) + 1
                    trace.append({"skip": "unknown_role", "orderRef": order_ref, "role": role})
                    continue

                inspected += 1
                matched += 1
                qty = Decimal(str(ex.shares or 0))
                px = Decimal(str(price))
                side = str(ex.side).upper()
                if side in {"BOT", "BUY"}:
                    total_buys += qty * px
                else:
                    total_sells += qty * px
                commission = getattr(ex, "commission", None)
                if commission is not None:
                    total_commission += Decimal(str(commission))
            net_pnl = total_sells - total_buys - total_commission
            cycle.total_buys = total_buys
            cycle.total_sells = total_sells
            cycle.total_commission = total_commission
            cycle.net_pnl = net_pnl
            cycle.completed_at = timezone.now()
            cycle.save(
                update_fields=["total_buys", "total_sells", "total_commission", "net_pnl", "completed_at", "state",
                               "last_activity_at"]
            )
            log_event(
                level="INFO",
                event_type="CYCLE_COMPLETE",
                message=f"Cycle completed (PnL={net_pnl})",
                bot=bot,
                cycle=cycle,
                data={
                    "total_buys": str(total_buys),
                    "total_sells": str(total_sells),
                    "commission": str(total_commission),
                    "net_pnl": str(net_pnl),
                    "executions": matched,
                    "inspected": inspected,
                    "trace": trace[:20],
                    "skips": skip_counts,
                },
            )
            logger.info(
                "PnL finalize bot=%s cycle=%s matched=%s inspected=%s buys=%s sells=%s commission=%s net=%s skips=%s trace=%s",
                bot.id,
                cycle.cycle_key,
                matched,
                inspected,
                total_buys,
                total_sells,
                total_commission,
                net_pnl,
                skip_counts,
                trace[:5],
            )
            if matched == 0:
                log_event(
                    level="WARNING",
                    event_type="PNL_MISSING_EXECUTIONS",
                    message="No executions matched orderRef during PnL calculation",
                    bot=bot,
                    cycle=cycle,
                )
            if bot.status == "STOPPING":
                bot.status = "STOPPED"
                bot.stopped_at = timezone.now()
                bot.save(update_fields=["status", "stopped_at"])
                log_event(level="INFO", event_type="BOT_STOPPED", message="Bot stopped after cycle completion", bot=bot,
                          cycle=cycle)

        def mark_cycles_recovering():
            """On agent start, mark in-flight cycles as RECOVERING for fresh reconciliation."""
            candidates = Cycle.objects.filter(
                bot__status__in=("RUNNING", "STOPPING"),
                state__in=("INITIALIZING", "ENTERING", "ACTIVE", "TRANSITIONING", "TRAILING", "PNL_CALCULATION",
                           "RECOVERING"),
            )
            for cycle in candidates:
                transition_cycle(cycle, "RECOVERING", "Agent restart: entering RECOVERING")

        def open_trades_for_cycle(contract_conid: int, bot: Bot, cycle: Cycle):
            """Optimization: Uses local ib.openTrades() cache."""
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
            """Optimization: Uses local ib.openTrades() cache."""
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
            """Optimization: Uses local ib.positions() cache."""
            for p in ib.positions():
                if p.contract.conId == contract_conid and p.account == account:
                    return Decimal(str(p.position or 0))
            return Decimal("0")

        def get_position_avg_cost(contract_conid: int, account: str) -> Optional[Decimal]:
            """Optimization: Uses local ib.positions() cache."""
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
            # request_snapshots() REMOVED (Optimization: uses loop-level cache)
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
            log_event(level="INFO", event_type="ENTRIES_SUBMITTED", message="Submitted long+short entries", bot=bot,
                      cycle=cycle)
            cycle.last_activity_at = timezone.now()
            cycle.save(update_fields=["last_activity_at"])

        def place_trailing(bot: Bot, cycle: Cycle, contract, surviving_role: str, min_tick: float):
            # request_snapshots() REMOVED (Optimization: uses loop-level cache)
            contract_conid = contract.conId

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
            # request_snapshots() REMOVED (Optimization: uses loop-level cache)
            contract_conid = contract.conId
            # Cancel all open trades for bot/accounts on this contract (regardless of cycle)
            for t in list(ib.openTrades()):
                if t.contract.conId != contract_conid:
                    continue
                if t.order.account not in (bot.long_account, bot.short_account):
                    continue
                with_retries(lambda t=t: ib.cancelOrder(t.order), action=f"cancel order {t.order.orderId}")

            # Flatten positions on both accounts
            for account in (bot.long_account, bot.short_account):
                qty = get_position_qty(contract_conid, account)
                if qty == 0:
                    continue
                action = "SELL" if qty > 0 else "BUY"
                order_ref = build_order_ref(bot.id, str(cycle.cycle_key) if cycle else "panic", f"PANIC_{account}")
                order = MarketOrder(action, float(abs(qty)), account=account, tif="GTC")
                order.orderRef = order_ref
                with_retries(lambda o=order: ib.placeOrder(contract, o), action=f"flatten {account}")

            log_event(
                level="CRITICAL",
                event_type="PANIC_EXECUTED",
                message="Panic flatten executed (cancel + flatten)",
                bot=bot,
                cycle=cycle,
            )

        def get_unrealized_pnl(account: str, conid: int, timeout: float = 2.0) -> Optional[Decimal]:
            """Best-effort unrealized PnL fetch for recovery guard."""
            try:
                sub = ib.reqPnLSingle(account, "", conid)
            except Exception as exc:
                logger.warning("PnL guard: reqPnLSingle failed account=%s conId=%s err=%s", account, conid, exc)
                return None
            deadline = time.time() + timeout
            value: Optional[Decimal] = None
            try:
                while time.time() < deadline:
                    if sub.unrealizedPnL is not None:
                        value = Decimal(str(sub.unrealizedPnL))
                        break
                    ib.sleep(0.1)
            finally:
                try:
                    ib.cancelPnLSingle(account, "", conid)
                except Exception:
                    pass
            return value

        def reconcile_bot(bot: Bot):
            logger.info(
                f"[BOT:{bot.id}][{bot.symbol}] Reconciling bot (Status: {bot.status}, Environment: {bot.environment})"
            )

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

                # Check current state from cache
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
                        log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle aborted after panic",
                                  bot=bot, cycle=cycle)
                    if bot.status != "STOPPED":
                        bot.status = "STOPPED"
                        bot.stopped_at = timezone.now()
                        bot.save(update_fields=["status", "stopped_at"])
                        log_event(level="INFO", event_type="BOT_STOPPED", message="Bot stopped after panic cleanup",
                                  bot=bot, cycle=cycle)
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
                    long_pos = get_position_qty(contract.conId, bot.long_account)
                    short_pos = get_position_qty(contract.conId, bot.short_account)
                    open_trades_for_bot = [
                        t for t in ib.openTrades()
                        if
                        t.contract.conId == contract.conId and t.order.account in (bot.long_account, bot.short_account)
                    ]
                    if long_pos == 0 and short_pos == 0 and not open_trades_for_bot:
                        bot.status = "STOPPED"
                        bot.stopped_at = timezone.now()
                        bot.save(update_fields=["status", "stopped_at"])
                        log_event(level="INFO", event_type="BOT_STOPPED",
                                  message="Bot stopped (no active cycle; flat/clean)", bot=bot)
                return

            # Safety timer: only applies in hedge activation states.
            if cycle.state in ("INITIALIZING", "ENTERING"):
                age = timezone.now() - cycle.last_activity_at
                if age.total_seconds() > 30:
                    transition_cycle(cycle, "PANIC", f"Safety timer exceeded (age={age})", level="CRITICAL")
                    panic_flatten(bot, cycle, contract)
                    return

            # Cache check (Optimization)
            contract_conid = contract.conId

            long_pos = get_position_qty(contract_conid, bot.long_account)
            short_pos = get_position_qty(contract_conid, bot.short_account)

            # If STOPPING and fully flat/clean, mark STOPPED (even if a cycle exists).
            if bot.status == "STOPPING":
                open_trades_for_bot = [
                    t
                    for t in ib.openTrades()
                    if t.contract.conId == contract_conid and t.order.account in (bot.long_account, bot.short_account)
                ]
                if long_pos == 0 and short_pos == 0 and not open_trades_for_bot:
                    if cycle and cycle.state not in ("COMPLETED", "ABORTED"):
                        transition_cycle(cycle, "ABORTED", "Bot stopping: flat/clean; closing cycle", level="WARNING")
                        cycle.completed_at = timezone.now()
                        cycle.save(update_fields=["completed_at"])
                        log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle aborted/cleaned", bot=bot,
                                  cycle=cycle)
                    bot.status = "STOPPED"
                    bot.stopped_at = timezone.now()
                    bot.save(update_fields=["status", "stopped_at"])
                    log_event(level="INFO", event_type="BOT_STOPPED",
                              message="Bot stopped (flat/clean during STOPPING)", bot=bot, cycle=cycle)
                    return

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
                # Hedge imbalance: check unrealized PnL to decide between trailing vs panic.
                if (long_pos > 0 and short_pos == 0) or (short_pos < 0 and long_pos == 0):
                    surviving_role = "LONG" if long_pos > 0 else "SHORT"
                    account = bot.long_account if surviving_role == "LONG" else bot.short_account
                    pnl_val = get_unrealized_pnl(account, contract_conid)
                    if pnl_val is not None and pnl_val >= Decimal("0"):
                        transition_cycle(
                            cycle,
                            "TRANSITIONING",
                            f"Recovered imbalance {surviving_role}; unrealized PnL={pnl_val} >= 0 -> trailing",
                            level="WARNING",
                        )
                        missing_sl_role = "SHORT_SL" if surviving_role == "LONG" else "LONG_SL"
                        cancel_remaining_sl_and_trail(bot, cycle, contract, sl_role=missing_sl_role, min_tick=min_tick)
                        transition_cycle(cycle, "TRAILING", "Trailing active after recovery PnL guard")
                        return
                    log_event(
                        level="WARNING",
                        event_type="RECOVERY_PNL_GUARD",
                        message="Imbalance on recovery: unfavorable or missing unrealized PnL -> panic",
                        bot=bot,
                        cycle=cycle,
                        data={"pnl": str(pnl_val) if pnl_val is not None else None, "surviving": surviving_role},
                    )
                # Flat and clean -> abort cycle
                if long_pos == 0 and short_pos == 0 and not cycle_trades:
                    transition_cycle(cycle, "ABORTED", "Recovered: flat and clean; closing cycle", level="WARNING")
                    cycle.completed_at = timezone.now()
                    cycle.save(update_fields=["completed_at"])
                    log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle aborted/cleaned", bot=bot,
                              cycle=cycle)
                    return
                # Hedge imbalance or missing protections -> panic
                transition_cycle(cycle, "PANIC", "Recovered: hedge incomplete or protections missing; panic",
                                 level="CRITICAL")
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

            # Recovery: hedge imbalance (one leg missing) during hedge activation -> panic/flatten
            if cycle.state in {"INITIALIZING", "ENTERING"}:
                if (long_pos == 0) != (short_pos == 0):
                    missing_side = "LONG" if long_pos == 0 else "SHORT"
                    pending_entry = find_open_trade_by_role(
                        contract_conid, bot, cycle, f"{missing_side}_ENTRY"
                    )
                    if pending_entry:
                        log_event(
                            level="DEBUG",
                            event_type="IMBALANCE_PENDING_ENTRY",
                            message=f"Hedge imbalance but pending {missing_side}_ENTRY; skipping panic",
                            bot=bot,
                            cycle=cycle,
                        )
                    else:
                        transition_cycle(cycle, "PANIC", "Hedge imbalance detected (one leg missing); panic/flatten",
                                         level="CRITICAL")
                        panic_flatten(bot, cycle, contract)
                        return

            # If previous panic/aborted/error cycle is flat and clean, close it so a new cycle can start
            if cycle.state in {"PANIC", "ABORTED", "ERROR"}:
                cycle_trades = open_trades_for_cycle(contract_conid, bot, cycle)
                if long_pos == 0 and short_pos == 0 and not cycle_trades:
                    transition_cycle(cycle, "ABORTED", "Cycle closed after panic/error cleanup", level="WARNING")
                    cycle.completed_at = timezone.now()
                    cycle.save(update_fields=["completed_at"])
                    log_event(level="INFO", event_type="CYCLE_COMPLETE", message="Cycle aborted/cleaned", bot=bot,
                              cycle=cycle)
                    cycle = None
                    if bot.status == "STOPPING":
                        bot.status = "STOPPED"
                        bot.stopped_at = timezone.now()
                        bot.save(update_fields=["status", "stopped_at"])
                        log_event(level="INFO", event_type="BOT_STOPPED", message="Bot stopped after cycle cleanup",
                                  bot=bot)
                        return
                    # Will create a fresh cycle below if bot remains RUNNING

            if not cycle and bot.status == "RUNNING":
                cycle = load_or_create_cycle(bot)

            if not cycle:
                return

            if cycle.state == "INITIALIZING":
                transition_cycle(cycle, "ENTERING", "Starting entries + protection")
                place_entries(bot, cycle, contract)

            if cycle.state == "ENTERING":
                ensure_sl_orders(bot, cycle, contract, min_tick=min_tick)
                # refresh SL presence after placement
                long_sl = find_open_trade_by_role(contract_conid, bot, cycle, "LONG_SL")
                short_sl = find_open_trade_by_role(contract_conid, bot, cycle, "SHORT_SL")
                # When both positions exist and both SL are in place, hedge is active.
                if long_pos > 0 and short_pos < 0 and long_sl and short_sl:
                    transition_cycle(cycle, "ACTIVE", "Both SL protections confirmed in place")
                return

            if cycle.state == "ACTIVE":
                # If one leg vanished, treat it like an SL fill and flip to trailing instead of panicking.
                if (long_pos == 0) != (short_pos == 0):
                    surviving = "LONG" if long_pos > 0 else "SHORT"
                    transition_cycle(cycle, "TRANSITIONING", "Hedge leg missing; flipping surviving leg to trailing")
                    cancel_remaining_sl_and_trail(bot, cycle, contract,
                                                  sl_role="SHORT_SL" if surviving == "LONG" else "LONG_SL",
                                                  min_tick=min_tick)
                    transition_cycle(cycle, "TRAILING", "Trailing active after imbalance flip")
                    return
                # Keep SL qty aligned to positions (partial fills)
                ensure_sl_orders(bot, cycle, contract, min_tick=min_tick)
                return

            if cycle.state == "TRANSITIONING":
                if long_trail or short_trail:
                    transition_cycle(cycle, "TRAILING", "Trailing active")
                    return
                # No trailing yet; try to place based on surviving leg.
                surviving = None
                if long_pos > 0 and short_pos == 0:
                    surviving = "LONG"
                elif short_pos < 0 and long_pos == 0:
                    surviving = "SHORT"
                if surviving:
                    place_trailing(bot, cycle, contract, surviving_role=surviving, min_tick=min_tick)
                    transition_cycle(cycle, "TRAILING", "Trailing placed from TRANSITIONING")
                return

            if cycle.state == "TRAILING":
                # Completion condition: both accounts flat and no open orders for this cycle.
                cycle_trades = open_trades_for_cycle(contract_conid, bot, cycle)
                if long_pos == 0 and short_pos == 0:
                    if not cycle_trades:
                        transition_cycle(cycle, "PNL_CALCULATION", "Positions flat; calculating P&L")
                    else:
                        # Clean up any straggling orders tied to this cycle before moving to PnL
                        for t in cycle_trades:
                            with_retries(lambda t=t: ib.cancelOrder(t.order),
                                         action=f"cancel stray order {t.order.orderId}")
                return

            if cycle.state == "PNL_CALCULATION":
                transition_cycle(cycle, "PNL_CALCULATION", "Calculating realized P&L")
                finalize_pnl(bot, cycle)
                transition_cycle(cycle, "COMPLETED", "Cycle completed (PnL calculated)")
                return

        def on_error(reqId, errorCode, errorString, contract):
            nonlocal connectivity_down
            if errorCode in (1100, 1101):
                connectivity_down = True
            elif errorCode in (1102, 1103):
                connectivity_down = False
            logger.warning("IB error code=%s reqId=%s msg=%s", errorCode, reqId, errorString)

        def _process_exec_details(trade, fill):
            # We only react in real-time to stop-loss fills (SL -> trailing flip).
            order_ref = ""
            if trade and getattr(trade, "order", None):
                order_ref = getattr(trade.order, "orderRef", "") or ""
            if not order_ref and fill and getattr(fill, "execution", None):
                order_ref = getattr(fill.execution, "orderRef", "") or ""
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

                # Manual state check on fill (uses local cache)
                long_pos = get_position_qty(contract.conId, bot.long_account)
                short_pos = get_position_qty(contract.conId, bot.short_account)
                long_sl = find_open_trade_by_role(contract.conId, bot, cycle, "LONG_SL")
                short_sl = find_open_trade_by_role(contract.conId, bot, cycle, "SHORT_SL")
                if cycle.state in ("INITIALIZING",
                                   "ENTERING") and long_pos > 0 and short_pos < 0 and long_sl and short_sl:
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

        def on_exec_details(trade, fill):
            try:
                threading.Thread(target=_process_exec_details, args=(trade, fill), daemon=True).start()
            except Exception as exc:
                logger.exception("on_exec_details failed: %s", exc)

        ib.errorEvent += on_error
        ib.execDetailsEvent += on_exec_details

        # Connection and Initial Sync
        logger.info(f"[TWS_AGENT] Connecting to {host}:{port}...")
        ib.connect(host, port, clientId=client_id, timeout=10)

        # ONE-TIME SNAPSHOT: Populate the local cache at startup
        logger.info("Performing initial state synchronization...")
        ib.reqPositions()
        ib.reqAllOpenOrders()
        ib.sleep(2.0)  # Give TWS time to send the initial dump

        mark_cycles_recovering()
        last_reconcile_by_bot: dict[int, float] = {}

        try:
            while True:
                # NO reqPositions() here!
                # ib_insync updates ib.positions() automatically in the background.

                # This call processes any incoming "pushes" from TWS
                ib.sleep(poll_interval)

                now = time.time()
                bots = Bot.objects.filter(
                    environment=environment,
                    status__in=("RUNNING", "STOPPING", "ERROR")
                ).order_by("id")

                active_ids = {bot.id for bot in bots}
                for bot in bots:
                    # Only reconcile if the interval has passed
                    if now - last_reconcile_by_bot.get(bot.id, 0.0) < reconcile_interval:
                        continue

                    try:
                        reconcile_bot(bot)
                    except Exception as exc:
                        logger.exception(f"reconcile_bot failed for bot={bot.id}")
                        bot.status, bot.last_error = "ERROR", str(exc)
                        bot.save()
                    finally:
                        last_reconcile_by_bot[bot.id] = now

                # Prune stale entries
                for bid in list(last_reconcile_by_bot.keys()):
                    if bid not in active_ids:
                        last_reconcile_by_bot.pop(bid, None)
        finally:
            ib.disconnect()
