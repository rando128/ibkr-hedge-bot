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
        parser.add_argument("--port", type=int, default=None)
        parser.add_argument("--environment", choices=["PAPER", "LIVE"], default="PAPER")
        parser.add_argument("--client-id", type=int, default=7000)
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

        def log_event(*, level, event_type, message, bot=None, cycle=None, from_state="", to_state="", data=None):
            # Audit trail: unified logging for both File and DB
            numeric_level = getattr(logging, level.upper(), logging.INFO)
            ctx = []
            if bot: ctx.append(f"BOT:{bot.id}")
            if cycle:
                ctx.append(f"CYC:{cycle.cycle_number}")
                ctx.append(f"ST:{cycle.state}")

            prefix_str = f"[{'|'.join(ctx)}]" if ctx else "[SYSTEM]"
            log_line = f"{prefix_str} {event_type} | {message}"
            if data: log_line += f" | DATA: {data}"

            logger.log(numeric_level, log_line)

            try:
                Event.objects.create(
                    bot=bot, cycle=cycle, level=level.upper(), event_type=event_type,
                    message=message, data=data, from_state=from_state, to_state=to_state,
                )
            except Exception:
                logger.exception(f"{prefix_str} DB_EVENT_FAILURE | Could not save event to database")

        def transition_cycle(cycle, to_state, message, *, level="INFO", data=None):
            from_state = cycle.state
            if from_state == to_state: return cycle
            cycle.state = to_state
            cycle.last_activity_at = timezone.now()
            cycle.save(update_fields=["state", "last_activity_at"])
            log_event(level=level, event_type="STATE_TRANSITION", message=message, bot=cycle.bot, cycle=cycle,
                      from_state=from_state, to_state=to_state, data=data)
            return cycle

        def load_or_create_cycle(bot):
            active = bot.cycles.exclude(state__in=("COMPLETED", "ABORTED")).order_by("-id").first()
            if active: return active
            with transaction.atomic():
                max_cycle = bot.cycles.select_for_update().aggregate(Max("cycle_number"))["cycle_number__max"]
                next_cycle_number = (max_cycle or 0) + 1
                cycle = Cycle.objects.create(bot=bot, cycle_number=next_cycle_number, symbol=bot.symbol.upper(),
                                             state="INITIALIZING", last_activity_at=timezone.now())
            log_event(level="INFO", event_type="CYCLE_START", message=f"Cycle {cycle.cycle_number} created", bot=bot,
                      cycle=cycle)
            return cycle

        def get_contract(bot):
            contract = Stock(bot.symbol.upper(), bot.exchange, bot.currency)
            if bot.primary_exchange: contract.primaryExchange = bot.primary_exchange
            return contract

        def with_retries(fn, *, attempts=3, base_delay=0.5, action="action"):
            last_exc = None
            for attempt in range(attempts):
                try:
                    return fn()
                except Exception as exc:
                    last_exc = exc
                    delay = base_delay * (2 ** attempt)
                    logger.warning("%s failed (attempt %s/%s): %s", action, attempt + 1, attempts, exc)
                    ib.sleep(delay)
            raise last_exc

        def finalize_pnl(bot, cycle):
            from ib_insync import ExecutionFilter
            # Generic PnL prefix for this bot/cycle
            prefix = f"{ORDERREF_PREFIX}:{bot.id}:{cycle.cycle_key}:"

            def fetch_execs(with_filter):
                if not with_filter: return ib.reqExecutions()
                filt = ExecutionFilter()
                filt.clientId = client_id
                try:
                    filt.time = cycle.started_at.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S")
                except:
                    pass
                executions = []
                for acct in {bot.long_account, bot.short_account}:
                    if not acct: continue
                    filt.acctCode = acct
                    try:
                        executions.extend(ib.reqExecutions(filt))
                    except:
                        continue
                return executions

            executions = fetch_execs(True) or fetch_execs(False)
            total_buys = total_sells = total_commission = Decimal("0")
            matched = inspected = 0
            skip_counts = {}
            for ex in executions:
                order_ref = str(getattr(ex, "orderRef", "") or "")

                # BUG FIX: Use the prefix for efficient filtering
                if not order_ref.startswith(prefix): continue

                price = getattr(ex, "price", None)
                if price is None: continue

                parsed = parse_order_ref(order_ref)
                if not parsed: continue

                role = parsed.role.upper()
                standard_roles = {"LONG_ENTRY", "SHORT_ENTRY", "LONG_SL", "SHORT_SL", "LONG_TRAIL", "SHORT_TRAIL"}

                # Generic role validation: includes original standard roles and any account-specific PANIC role
                if not (role in standard_roles or role.startswith("PANIC_")):
                    skip_counts["unknown_role"] = skip_counts.get("unknown_role", 0) + 1
                    continue

                inspected += 1
                matched += 1
                qty, px = Decimal(str(ex.shares or 0)), Decimal(str(price))
                if str(ex.side).upper() in {"BOT", "BUY"}:
                    total_buys += qty * px
                else:
                    total_sells += qty * px
                comm = getattr(ex, "commission", None)
                if comm is not None: total_commission += Decimal(str(comm))

            net_pnl = total_sells - total_buys - total_commission
            cycle.total_buys, cycle.total_sells, cycle.total_commission = total_buys, total_sells, total_commission
            cycle.net_pnl, cycle.completed_at = net_pnl, timezone.now()
            cycle.save()
            log_event(level="INFO", event_type="CYCLE_COMPLETE", message=f"Cycle completed (PnL={net_pnl})", bot=bot,
                      cycle=cycle)
            if bot.status == "STOPPING":
                bot.status = "STOPPED"
                bot.stopped_at = timezone.now()
                bot.save(update_fields=["status", "stopped_at"])

        def mark_cycles_recovering():
            """On agent start, mark in-flight cycles as RECOVERING for fresh reconciliation."""
            candidates = Cycle.objects.filter(bot__status__in=("RUNNING", "STOPPING"),
                                              state__in=("INITIALIZING", "ENTERING", "ACTIVE", "TRANSITIONING",
                                                         "TRAILING", "PNL_CALCULATION", "RECOVERING"))
            for cycle in candidates: transition_cycle(cycle, "RECOVERING", "Agent restart: entering RECOVERING")

        def open_trades_for_cycle(contract_conid, bot, cycle):
            # Uses ib_insync local cache (Optimization)
            prefix = f"{ORDERREF_PREFIX}:{bot.id}:{cycle.cycle_key}:"
            return [t for t in ib.openTrades() if
                    t.contract.conId == contract_conid and t.order.account in (bot.long_account,
                                                                               bot.short_account) and (
                            getattr(t.order, "orderRef", "") or "").startswith(prefix)]

        def find_open_trade_by_role(contract_conid, bot, cycle, role):
            # Uses ib_insync local cache (Optimization)
            target = build_order_ref(bot.id, str(cycle.cycle_key), role)
            for t in ib.openTrades():
                if t.contract.conId == contract_conid and t.order.account in (bot.long_account,
                                                                              bot.short_account) and getattr(t.order,
                                                                                                             "orderRef",
                                                                                                             "") == target:
                    return t
            return None

        def get_position_qty(contract_conid, account):
            # Uses ib_insync local cache (Optimization)
            for p in ib.positions():
                if p.contract.conId == contract_conid and p.account == account: return Decimal(str(p.position or 0))
            return Decimal("0")

        def get_position_avg_cost(contract_conid, account):
            for p in ib.positions():
                if p.contract.conId == contract_conid and p.account == account and p.position: return Decimal(
                    str(p.avgCost))
            return None

        def ensure_sl_orders(bot, cycle, contract, min_tick):
            contract_conid = contract.conId
            primary_exch = bot.primary_exchange or getattr(contract, "primaryExchange", "") or bot.exchange
            tick_for_price = lambda px: get_exchange_tick_size(px, primary_exch, min_tick)

            for side, action in [('long', 'SELL'), ('short', 'BUY')]:
                account = getattr(bot, f"{side}_account")
                pos = get_position_qty(contract_conid, account)
                if (side == 'long' and pos > 0) or (side == 'short' and pos < 0):
                    avg_cost = get_position_avg_cost(contract_conid, account)
                    if avg_cost is not None:
                        tick = tick_for_price(float(avg_cost))
                        if side == 'long':
                            stop_px = q_floor(float(avg_cost) * (1 - float(bot.stop_pct)), tick)
                        else:
                            stop_px = q_ceil(float(avg_cost) * (1 + float(bot.stop_pct)), tick)

                        role = f"{side.upper()}_SL"
                        existing = find_open_trade_by_role(contract_conid, bot, cycle, role)
                        desired_qty = float(abs(pos))
                        if not existing or float(existing.order.totalQuantity or 0) != desired_qty:
                            if existing:
                                if existing.orderStatus.status in {"PendingSubmit", "PreSubmitted"}: continue
                                with_retries(lambda: ib.cancelOrder(existing.order), action=f"cancel {role}")
                            sl = StopOrder(action=action, totalQuantity=desired_qty, stopPrice=stop_px, account=account,
                                           tif="GTC", outsideRth=True,
                                           orderRef=build_order_ref(bot.id, str(cycle.cycle_key), role))
                            with_retries(lambda: ib.placeOrder(contract, sl), action=f"place {role}")
                            log_event(level="INFO", event_type="SL_PLACED",
                                      message=f"Placed {role} qty={desired_qty} stop={stop_px}", bot=bot, cycle=cycle)
                            cycle.last_activity_at = timezone.now()
                            cycle.save(update_fields=["last_activity_at"])

        def place_entries(bot, cycle, contract):
            qty = float(bot.qty)
            for action, account, role in [("BUY", bot.long_account, "LONG_ENTRY"),
                                          ("SELL", bot.short_account, "SHORT_ENTRY")]:
                o = MarketOrder(action=action, totalQuantity=qty, account=account, tif="DAY", outsideRth=True,
                                orderRef=build_order_ref(bot.id, str(cycle.cycle_key), role))
                with_retries(lambda o=o: ib.placeOrder(contract, o), action=f"place {role}")
            log_event(level="INFO", event_type="ENTRIES_SUBMITTED", message="Submitted long+short entries", bot=bot,
                      cycle=cycle)
            cycle.last_activity_at = timezone.now()
            cycle.save(update_fields=["last_activity_at"])

        def place_trailing(bot, cycle, contract, surviving_role, min_tick):
            account = bot.long_account if surviving_role == "LONG" else bot.short_account
            pos = get_position_qty(contract.conId, account)
            if (surviving_role == "LONG" and pos <= 0) or (surviving_role == "SHORT" and pos >= 0): return
            action, role = ("SELL", "LONG_TRAIL") if surviving_role == "LONG" else ("BUY", "SHORT_TRAIL")
            qty, trail_pct = float(abs(pos)), float(bot.trailing_pct) * 100.0
            order = IBOrder(action=action, totalQuantity=qty, orderType="TRAIL", trailingPercent=trail_pct,
                            account=account, tif="GTC", outsideRth=True,
                            orderRef=build_order_ref(bot.id, str(cycle.cycle_key), role))
            with_retries(lambda: ib.placeOrder(contract, order), action=f"place {role}")
            log_event(level="INFO", event_type="TRAIL_PLACED", message=f"Placed {role} qty={qty} trail={trail_pct}%",
                      bot=bot, cycle=cycle)
            cycle.last_activity_at = timezone.now()
            cycle.save(update_fields=["last_activity_at"])

        def cancel_remaining_sl_and_trail(bot, cycle, contract, sl_role, min_tick):
            other_role = "SHORT_SL" if sl_role == "LONG_SL" else "LONG_SL"
            surviving = "LONG" if sl_role == "SHORT_SL" else "SHORT"
            other_trade = find_open_trade_by_role(contract.conId, bot, cycle, other_role)
            if other_trade: with_retries(lambda: ib.cancelOrder(other_trade.order), action=f"cancel {other_role}")
            ib.sleep(1.0)  # wait for cancellation
            place_trailing(bot, cycle, contract, surviving, min_tick)

        def panic_flatten(bot, cycle, contract):
            conid = contract.conId
            for t in [t for t in ib.openTrades() if
                      t.contract.conId == conid and t.order.account in (bot.long_account, bot.short_account)]:
                with_retries(lambda t=t: ib.cancelOrder(t.order), action="panic cancel")
            for account in (bot.long_account, bot.short_account):
                qty = get_position_qty(conid, account)
                if qty == 0: continue
                order = MarketOrder("SELL" if qty > 0 else "BUY", float(abs(qty)), account=account, tif="GTC")
                order.orderRef = build_order_ref(bot.id, str(cycle.cycle_key) if cycle else "panic", f"PANIC_{account}")
                with_retries(lambda o=order: ib.placeOrder(contract, o), action=f"panic flatten {account}")
            log_event(level="CRITICAL", event_type="PANIC_EXECUTED", message="Panic flatten executed", bot=bot,
                      cycle=cycle)

        def reconcile_bot(bot):
            logger.info(
                f"[BOT:{bot.id}][{bot.symbol}] Reconciling bot (Status: {bot.status}, Environment: {bot.environment})")
            contract = get_contract(bot)
            ib.qualifyContracts(contract)
            details = ib.reqContractDetails(contract)
            min_tick = float(details[0].minTick) if details else 0.01
            conid, cycle = contract.conId, bot.cycles.exclude(state__in=("COMPLETED", "ABORTED")).order_by(
                "-id").first()

            if bot.panic_requested:
                if cycle: transition_cycle(cycle, "PANIC", "panic_requested=True")
                panic_flatten(bot, cycle, contract)
                bot.panic_requested = False
                if bot.status == "RUNNING": bot.status = "STOPPING"
                bot.save()
                return

            if bot.status in ("STOPPED", "ERROR"): return
            if not cycle and bot.status == "RUNNING": cycle = load_or_create_cycle(bot)
            if not cycle: return

            # Safety timer: only applies in hedge activation states.
            if cycle.state in ("INITIALIZING", "ENTERING"):
                if (timezone.now() - cycle.last_activity_at).total_seconds() > 30:
                    transition_cycle(cycle, "PANIC", "Safety timer exceeded", level="CRITICAL")
                    panic_flatten(bot, cycle, contract);
                    return

            long_pos, short_pos = get_position_qty(conid, bot.long_account), get_position_qty(conid, bot.short_account)
            open_trades = open_trades_for_cycle(conid, bot, cycle)

            if cycle.state == "PANIC":
                if long_pos == 0 and short_pos == 0 and not open_trades:
                    transition_cycle(cycle, "ABORTED", "Panic flatten complete; marking aborted", level="WARNING")
                    if bot.status == "STOPPING":
                        bot.status = "STOPPED"
                        bot.stopped_at = timezone.now()
                        bot.save(update_fields=["status", "stopped_at"])
                return

            if cycle.state == "RECOVERING":
                if long_pos > 0 and short_pos < 0 and find_open_trade_by_role(conid, bot, cycle,
                                                                              "LONG_SL") and find_open_trade_by_role(
                    conid, bot, cycle, "SHORT_SL"):
                    transition_cycle(cycle, "ACTIVE", "Recovered: hedge intact")
                elif find_open_trade_by_role(conid, bot, cycle, "LONG_TRAIL") or find_open_trade_by_role(conid, bot,
                                                                                                         cycle,
                                                                                                         "SHORT_TRAIL"):
                    transition_cycle(cycle, "TRAILING", "Recovered: trailing")
                elif long_pos == 0 and short_pos == 0 and not open_trades:
                    transition_cycle(cycle, "ABORTED", "Recovered: flat")
                else:
                    # If imbalance on recovery, check unrealized PnL
                    surviving_role = "LONG" if long_pos > 0 else "SHORT"
                    acc = bot.long_account if surviving_role == "LONG" else bot.short_account
                    pnl_val = get_unrealized_pnl(acc, conid)
                    if pnl_val is not None and pnl_val >= 0:
                        transition_cycle(cycle, "TRANSITIONING", "Recovered imbalance; PnL >= 0 -> trailing",
                                         level="WARNING")
                        cancel_remaining_sl_and_trail(bot, cycle, contract,
                                                      "SHORT_SL" if surviving_role == "LONG" else "LONG_SL", min_tick)
                    else:
                        transition_cycle(cycle, "PANIC", "Recovery failed: inconsistency or negative PnL",
                                         level="CRITICAL")
                        panic_flatten(bot, cycle, contract)
                return

            if cycle.state == "INITIALIZING":
                transition_cycle(cycle, "ENTERING", "Placing entries");
                place_entries(bot, cycle, contract)
            elif cycle.state == "ENTERING":
                ensure_sl_orders(bot, cycle, contract, min_tick)
                if long_pos > 0 and short_pos < 0 and find_open_trade_by_role(conid, bot, cycle,
                                                                              "LONG_SL") and find_open_trade_by_role(
                    conid, bot, cycle, "SHORT_SL"):
                    transition_cycle(cycle, "ACTIVE", "Protections confirmed")
            elif cycle.state == "ACTIVE":
                if (long_pos == 0) != (short_pos == 0):
                    surviving = "LONG" if long_pos > 0 else "SHORT"
                    transition_cycle(cycle, "TRANSITIONING", "Leg missing; trailing")
                    cancel_remaining_sl_and_trail(bot, cycle, contract,
                                                  "SHORT_SL" if surviving == "LONG" else "LONG_SL", min_tick)
                else:
                    ensure_sl_orders(bot, cycle, contract, min_tick)
            elif cycle.state == "TRAILING":
                if long_pos == 0 and short_pos == 0:
                    if not open_trades_for_cycle(conid, bot, cycle):
                        transition_cycle(cycle, "PNL_CALCULATION", "Flat; PnL logic")
            elif cycle.state == "PNL_CALCULATION":
                finalize_pnl(bot, cycle);
                transition_cycle(cycle, "COMPLETED", "Cycle end")

        def on_error(reqId, errorCode, errorString, contract):
            nonlocal connectivity_down
            if errorCode in (1100, 1101):
                connectivity_down = True
            elif errorCode in (1102, 1103):
                connectivity_down = False
            logger.warning("IB error %s: %s", errorCode, errorString)

        ib.errorEvent += on_error
        ib.connect(host, port, clientId=client_id, timeout=10)
        mark_cycles_recovering()

        # Initial snapshot to populate cache
        ib.reqPositions();
        ib.reqAllOpenOrders();
        ib.sleep(1.0)

        last_reconcile_by_bot = {}
        try:
            while True:
                ib.sleep(poll_interval)  # Event-driven: no reqPositions in loop

                if connectivity_down:
                    logger.warning("TWS Connection Down - Waiting...")
                    continue

                now = time.time()
                bots = Bot.objects.filter(environment=environment,
                                          status__in=("RUNNING", "STOPPING", "ERROR")).order_by("id")
                active_ids = {bot.id for bot in bots}

                for bot in bots:
                    if now - last_reconcile_by_bot.get(bot.id, 0.0) >= reconcile_interval:
                        try:
                            reconcile_bot(bot)
                        except Exception:
                            logger.exception(f"Reconcile failed for bot {bot.id}")
                        last_reconcile_by_bot[bot.id] = now

                for bid in list(last_reconcile_by_bot.keys()):
                    if bid not in active_ids: last_reconcile_by_bot.pop(bid)
        finally:
            ib.disconnect()
