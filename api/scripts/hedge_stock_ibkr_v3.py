"""
hedge_stock_ibrk_v3.py — IBKR Hedging Bot (Stocks, ib_insync) — v3 (debuggable, fixed asyncio)
=============================================================================================

Key fixes vs your v3:
- ✅ NEVER use `await self.ib.sleep()` (many ib_insync versions implement IB.sleep via util.run -> run_until_complete -> crashes in running loop)
  -> use `await asyncio.sleep()` everywhere inside async coroutines.
- ✅ No `asyncio.create_task()` from synchronous IB callbacks unless loop is running.
  -> schedule safely via `_schedule()` which uses the running loop if available, else defers.
- ✅ Background tasks (heartbeat/watchdog/fallback) launched only once the async main is running.
- ✅ Kickstart tick scheduled from async main.

Run:
  python hedge_stock_ibrk_v3.py --config config.yaml --log-level DEBUG
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import yaml
from ib_insync import IB, Order, Stock, Trade, MarketOrder, StopOrder, util


# -----------------------------
# Status semantics (spec)
# -----------------------------
LIVE_STATUSES = {"PreSubmitted", "Submitted"}
TERMINAL_STATUSES = {"Filled", "Cancelled", "ApiCancelled", "Inactive", "Rejected"}
BAD_STATUSES = {"Rejected"}  # terminal-bad


# -----------------------------
# Config
# -----------------------------
@dataclass
class Config:
    # Instrument
    symbol: str = "NVDA"
    exchange: str = "SMART"
    currency: str = "USD"

    # Strategy
    notional_per_leg_usd: float = 100.0
    sl_pct: float = 0.03
    trailing_pct: float = 0.15  # 0.15 => 15% trailingPercent
    bar_size: str = "1 min"
    cooldown_bars: int = 1

    # IBKR
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 1

    # Safety / timings
    entering_timeout_sec: float = 30.0
    sl_live_timeout_sec: float = 2.0
    cancel_timeout_sec: float = 2.0
    trailing_live_timeout_sec: float = 2.0
    reconcile_retries: int = 3
    reconcile_retry_delay_sec: float = 0.25

    # Diagnostics / scheduling
    heartbeat_sec: float = 5.0
    bar_stale_warn_sec: float = 90.0          # warn if no bar update callbacks for this long
    clock_fallback_sec: float = 60.0          # run tick every N seconds even if bars not arriving
    kickstart: bool = True

    # Rounding
    price_rounding: str = "cent"

    # Logging
    log_level: str = "INFO"


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config_from_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("config.yaml must be a mapping/dictionary.")
    return data


def _validate_config(cfg: Config) -> None:
    if not cfg.symbol:
        raise ValueError("symbol is required")
    if cfg.notional_per_leg_usd <= 0:
        raise ValueError("notional_per_leg_usd must be > 0")
    if not (0 < cfg.sl_pct < 1):
        raise ValueError("sl_pct must be in (0,1)")
    if not (0 < cfg.trailing_pct < 1):
        raise ValueError("trailing_pct must be in (0,1)")
    if cfg.cooldown_bars < 0:
        raise ValueError("cooldown_bars must be >= 0")
    if cfg.client_id is None:
        raise ValueError("client_id is required (unique per running client)")


def config_from_sources(defaults: Config, yaml_dict: Optional[Dict[str, Any]], cli: argparse.Namespace) -> Config:
    d = dataclasses.asdict(defaults)

    if yaml_dict:
        normalized = dict(yaml_dict)
        if "ibkr" in normalized and isinstance(normalized["ibkr"], dict):
            ibkr = normalized.pop("ibkr")
            normalized = _deep_merge(
                normalized,
                {
                    "host": ibkr.get("host", d["host"]),
                    "port": ibkr.get("port", d["port"]),
                    "client_id": ibkr.get("client_id", d["client_id"]),
                },
            )
        d = _deep_merge(d, normalized)

    # CLI overrides
    if cli.symbol:
        d["symbol"] = cli.symbol
    if cli.host:
        d["host"] = cli.host
    if cli.port is not None:
        d["port"] = cli.port
    if cli.client_id is not None:
        d["client_id"] = cli.client_id

    if cli.notional is not None:
        d["notional_per_leg_usd"] = cli.notional
    if cli.sl_pct is not None:
        d["sl_pct"] = cli.sl_pct
    if cli.trailing_pct is not None:
        d["trailing_pct"] = cli.trailing_pct
    if cli.cooldown_bars is not None:
        d["cooldown_bars"] = cli.cooldown_bars
    if cli.log_level:
        d["log_level"] = cli.log_level

    cfg = Config(**d)
    _validate_config(cfg)
    return cfg


# -----------------------------
# State machine
# -----------------------------
class State(str, Enum):
    IDLE = "IDLE"
    ENTERING = "ENTERING"
    HEDGED = "HEDGED"
    SINGLE_LEG = "SINGLE_LEG"
    COOLDOWN = "COOLDOWN"
    DEGRADED = "DEGRADED"


@dataclass
class Leg:
    parent: Optional[Trade] = None
    sl: Optional[Trade] = None


@dataclass
class ExecLedger:
    parent_filled: Dict[str, float] = field(default_factory=lambda: {"LONG": 0.0, "SHORT": 0.0})
    sl_filled: Dict[str, float] = field(default_factory=lambda: {"LONG": 0.0, "SHORT": 0.0})

    def record_parent_fill(self, leg: str, qty: float) -> None:
        self.parent_filled[leg] += float(qty)

    def record_sl_fill(self, leg: str, qty: float) -> None:
        self.sl_filled[leg] += float(qty)

    def any_sl_fill(self) -> bool:
        return (self.sl_filled["LONG"] > 0) or (self.sl_filled["SHORT"] > 0)

    def loser_leg(self) -> Optional[str]:
        if self.sl_filled["LONG"] > 0 and self.sl_filled["SHORT"] > 0:
            return "BOTH"
        if self.sl_filled["LONG"] > 0:
            return "LONG"
        if self.sl_filled["SHORT"] > 0:
            return "SHORT"
        return None

    def survivor_leg(self) -> Optional[str]:
        loser = self.loser_leg()
        if loser == "LONG":
            return "SHORT"
        if loser == "SHORT":
            return "LONG"
        return None

    def remaining_qty(self, leg: str) -> float:
        return max(0.0, self.parent_filled[leg] - self.sl_filled[leg])


@dataclass
class CycleContext:
    long: Leg = field(default_factory=Leg)
    short: Leg = field(default_factory=Leg)
    trailing: Optional[Trade] = None
    entering_started_at: Optional[datetime] = None
    cycle_ended_at_bar_time: Optional[datetime] = None


# -----------------------------
# Bot
# -----------------------------
class HedgeBotV3:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.ib = IB()

        self.state: State = State.IDLE
        self.ctx = CycleContext()
        self.ledger = ExecLedger()

        self.contract = Stock(cfg.symbol, cfg.exchange, cfg.currency)

        self.last_bar_time: Optional[datetime] = None
        self._last_bars_update_ts: float = 0.0
        self._bars_updates_seen: int = 0

        self.degraded_reason: Optional[str] = None
        self._orderid_to_role: Dict[int, Tuple[str, str]] = {}

        # Loop/task scheduling safety
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._deferred_coros: list = []  # coroutines to schedule once loop exists

        # logger
        self.logger = logging.getLogger(f"hedge-bot-v3:{cfg.symbol}")
        lvl = getattr(logging, cfg.log_level.upper(), logging.INFO)
        self.logger.setLevel(lvl)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(lvl)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.handlers.clear()
        self.logger.addHandler(handler)

    # ---------- scheduling helpers ----------
    def _schedule(self, coro, name: str = "") -> None:
        """
        Schedule a coroutine safely:
        - If an asyncio loop is running, schedule immediately.
        - If not yet running, defer until `_main()` starts.
        """
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro, name=name or None)
            return
        except RuntimeError:
            # no running loop yet
            self._deferred_coros.append((coro, name))

    async def _asleep(self, seconds: float) -> None:
        # IMPORTANT: Do NOT use `await self.ib.sleep()` inside async coroutines (many ib_insync versions break)
        await asyncio.sleep(seconds)

    # ---------- status helpers ----------
    def _status(self, tr: Optional[Trade]) -> Optional[str]:
        if not tr or not tr.orderStatus:
            return None
        return tr.orderStatus.status

    def _is_live(self, tr: Optional[Trade]) -> bool:
        return self._status(tr) in LIVE_STATUSES

    def _is_terminal(self, tr: Optional[Trade]) -> bool:
        st = self._status(tr)
        return (tr is None) or (st in TERMINAL_STATUSES)

    def _is_rejected(self, tr: Optional[Trade]) -> bool:
        return self._status(tr) in BAD_STATUSES

    # ---------- connect / events ----------
    def connect(self) -> None:
        self.logger.info(f"Connecting: host={self.cfg.host} port={self.cfg.port} clientId={self.cfg.client_id}")
        self.ib.connect(self.cfg.host, self.cfg.port, clientId=int(self.cfg.client_id))

        self.ib.disconnectedEvent += self._on_disconnected
        self.ib.connectedEvent += self._on_connected
        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.orderStatusEvent += self._on_order_status

        self.ib.qualifyContracts(self.contract)
        self.logger.info(f"Qualified: {self.contract}")

        bars = self.ib.reqHistoricalData(
            self.contract,
            endDateTime="",
            durationStr="2 D",
            barSizeSetting=self.cfg.bar_size,
            whatToShow="TRADES",
            useRTH=False,
            formatDate=1,
            keepUpToDate=True,
        )
        bars.updateEvent += self._on_bars_update
        self.logger.info(f"Subscribed to bars: {self.cfg.bar_size} (keepUpToDate=True)")

        self._reconcile_sync(reason="startup")

    def _on_disconnected(self) -> None:
        self.logger.warning("Disconnected. Entering DEGRADED.")
        self.state = State.DEGRADED
        self.degraded_reason = "disconnect"

    def _on_connected(self) -> None:
        self.logger.info("Reconnected. Reconciling.")
        self.degraded_reason = None
        self._reconcile_sync(reason="reconnect")
        # Do NOT schedule asyncio tasks here unless loop running; defer safely:
        self._schedule(self._tick_on_bar(), name="tick_on_reconnect")

    def _on_order_status(self, trade: Trade) -> None:
        self.logger.debug(
            f"ORDER_STATUS: id={trade.order.orderId} type={trade.order.orderType} "
            f"status={trade.orderStatus.status} filled={trade.orderStatus.filled} remaining={trade.orderStatus.remaining}"
        )

    def _on_exec_details(self, trade: Trade, fill) -> None:
        order_id = trade.order.orderId
        qty = float(fill.shares)
        price = float(fill.price)

        leg, role = self._orderid_to_role.get(order_id, ("UNKNOWN", "UNKNOWN"))
        self.logger.info(f"EXEC: orderId={order_id} role={role} leg={leg} qty={qty} price={price}")

        if leg in ("LONG", "SHORT") and role == "PARENT":
            self.ledger.record_parent_fill(leg, qty)
        elif leg in ("LONG", "SHORT") and role == "SL":
            self.ledger.record_sl_fill(leg, qty)

        if self.state == State.ENTERING:
            self._schedule(self._check_entering_safety(), name="entering_safety")
        elif self.state == State.HEDGED:
            if self.ledger.any_sl_fill():
                self._schedule(self._handle_hedge_break(), name="hedge_break")
        elif self.state == State.SINGLE_LEG:
            self._schedule(self._reconcile_async(reason="execDetails_single"), name="reconcile_single")

    def _on_bars_update(self, bars, has_new_bar: bool) -> None:
        self._last_bars_update_ts = time.time()
        self._bars_updates_seen += 1

        # Always log at DEBUG so you can see if this callback fires at all
        try:
            last_date = bars[-1].date if bars else None
        except Exception:
            last_date = None
        self.logger.debug(f"BARS_UPDATE: has_new_bar={has_new_bar} len={len(bars) if bars else 0} last_date={last_date}")

        if not has_new_bar or not bars:
            return

        bar = bars[-1]
        bar_time = util.parseIBDatetime(bar.date) if isinstance(bar.date, str) else bar.date
        self.last_bar_time = bar_time
        self.logger.info(f"NEW_BAR: time={bar_time} close={bar.close}")
        self._schedule(self._tick_on_bar(), name="tick_on_bar")

    # ---------- async main & background tasks ----------
    async def _main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self.logger.info("Async main started (event loop running)")

        # Schedule any deferred tasks created before loop existed
        if self._deferred_coros:
            self.logger.debug(f"Scheduling deferred coroutines: n={len(self._deferred_coros)}")
            for coro, name in self._deferred_coros:
                asyncio.create_task(coro, name=name or None)
            self._deferred_coros.clear()

        # Start background diagnostics
        asyncio.create_task(self._heartbeat(), name="heartbeat")
        asyncio.create_task(self._bar_staleness_watchdog(), name="bar_watchdog")
        asyncio.create_task(self._clock_fallback(), name="clock_fallback")

        if self.cfg.kickstart:
            asyncio.create_task(self._tick_on_bar(), name="kickstart_tick")
            self.logger.info("Kickstart: scheduled initial _tick_on_bar()")

        # Keep alive forever
        while True:
            await self._asleep(3600)

    async def _heartbeat(self) -> None:
        while True:
            try:
                net = self._net_position_symbol()
                ot = len(self.ib.openTrades())
                self.logger.debug(
                    f"HEARTBEAT state={self.state} connected={self.ib.isConnected()} "
                    f"barsUpdatesSeen={self._bars_updates_seen} lastBarTime={self.last_bar_time} "
                    f"openTrades={ot} netPos={net}"
                )
            except Exception as e:
                self.logger.debug(f"HEARTBEAT error: {e}")
            await self._asleep(self.cfg.heartbeat_sec)

    async def _bar_staleness_watchdog(self) -> None:
        while True:
            await self._asleep(self.cfg.heartbeat_sec)
            if self._last_bars_update_ts == 0:
                self.logger.warning("No BARS_UPDATE callbacks observed yet.")
                continue
            age = time.time() - self._last_bars_update_ts
            if age >= self.cfg.bar_stale_warn_sec:
                self.logger.warning(
                    f"Bar stream stale: no BARS_UPDATE for {age:.1f}s. "
                    "Either no market data permission or keepUpToDate stream not delivering."
                )

    async def _clock_fallback(self) -> None:
        while True:
            await self._asleep(self.cfg.clock_fallback_sec)
            if self.state != State.DEGRADED:
                self.logger.debug("CLOCK_FALLBACK tick")
                await self._tick_on_bar()

    # ---------- tick ----------
    async def _tick_on_bar(self) -> None:
        self.logger.debug(f"TICK_ON_BAR: state={self.state} last_bar_time={self.last_bar_time}")

        if self.state == State.DEGRADED:
            self.logger.debug("TICK gate: DEGRADED -> skip")
            return

        # Cooldown expiry
        if self.state == State.COOLDOWN and self.ctx.cycle_ended_at_bar_time:
            now = self.last_bar_time or datetime.now()
            if self._bars_elapsed(self.ctx.cycle_ended_at_bar_time, now) >= self.cfg.cooldown_bars:
                self.logger.info("Cooldown elapsed -> IDLE")
                self._reset_cycle()
                self.state = State.IDLE

        # ENTERING timeout
        if self.state == State.ENTERING and self.ctx.entering_started_at:
            if datetime.now() - self.ctx.entering_started_at > timedelta(seconds=self.cfg.entering_timeout_sec):
                await self._flatten_if_ambiguous("ENTERING timeout")

        # Start cycle if idle
        if self.state == State.IDLE:
            await self._start_cycle()

    # ---------- core flow ----------
    async def _start_cycle(self) -> None:
        if self._has_open_orders_symbol():
            self.logger.debug("IDLE gate: has open orders -> skip")
            return

        net = self._net_position_symbol()
        if abs(net) > 1e-9:
            self.logger.warning(f"IDLE gate: net position not flat (net={net}) -> flatten")
            await self._flatten_if_ambiguous("Net position non-zero in IDLE")
            return

        last = await self._get_last_price()
        if not last:
            self.logger.warning("IDLE gate: no last/marketPrice from reqMktData -> skip (market data permission?)")
            return

        qty = max(self.cfg.notional_per_leg_usd / last, 0.0001)
        self.logger.info(f"Starting cycle: symbol={self.cfg.symbol} last={last:.4f} qty≈{qty:.6f}")

        self.state = State.ENTERING
        self.ctx.entering_started_at = datetime.now()

        await self._enter_two_phase(last_price=last, qty=qty)

    async def _enter_two_phase(self, last_price: float, qty: float) -> None:
        # Phase A: LONG bracket
        self.ctx.long = Leg()
        long_parent, long_sl = self._submit_bracket(
            leg="LONG",
            parent_action="BUY",
            stop_action="SELL",
            qty=qty,
            stop_price=last_price * (1 - self.cfg.sl_pct),
        )
        self.ctx.long.parent, self.ctx.long.sl = long_parent, long_sl

        await self._await_trade_live(long_sl, timeout=self.cfg.sl_live_timeout_sec, what="LONG SL live")

        if self.ledger.parent_filled["LONG"] > 0 and not self._is_live(long_sl):
            await self._flatten_if_ambiguous("LONG parent filled before LONG SL live")
            return

        if self._is_rejected(long_sl) or not self._is_live(long_sl):
            await self._flatten_if_ambiguous("LONG SL not live or rejected")
            return

        # Phase B: SHORT bracket
        self.ctx.short = Leg()
        short_parent, short_sl = self._submit_bracket(
            leg="SHORT",
            parent_action="SELL",
            stop_action="BUY",
            qty=qty,
            stop_price=last_price * (1 + self.cfg.sl_pct),
        )
        self.ctx.short.parent, self.ctx.short.sl = short_parent, short_sl

        await self._await_trade_live(short_sl, timeout=self.cfg.sl_live_timeout_sec, what="SHORT SL live")

        await self._check_entering_safety()

        if self._is_rejected(short_sl) or not self._is_live(short_sl):
            await self._flatten_if_ambiguous("SHORT SL not live or rejected")
            return

        self.logger.info("ENTERING -> HEDGED (both initial SLs live)")
        self.state = State.HEDGED

    async def _check_entering_safety(self) -> None:
        long_f = self.ledger.parent_filled["LONG"]
        short_f = self.ledger.parent_filled["SHORT"]

        if (long_f > 0 and short_f == 0) or (short_f > 0 and long_f == 0):
            self.logger.warning(f"ENTERING safety: partial hedge detected (L={long_f}, S={short_f}); grace 0.25s")
            await self._asleep(0.25)
            long_f = self.ledger.parent_filled["LONG"]
            short_f = self.ledger.parent_filled["SHORT"]
            if (long_f > 0 and short_f == 0) or (short_f > 0 and long_f == 0):
                await self._flatten_if_ambiguous("Partial hedge during ENTERING (one parent filled, other not)")

        if self.ledger.parent_filled["LONG"] > 0 and (not self._is_live(self.ctx.long.sl) or self._is_rejected(self.ctx.long.sl)):
            await self._flatten_if_ambiguous("LONG parent filled but LONG SL not live/rejected")
        if self.ledger.parent_filled["SHORT"] > 0 and (not self._is_live(self.ctx.short.sl) or self._is_rejected(self.ctx.short.sl)):
            await self._flatten_if_ambiguous("SHORT parent filled but SHORT SL not live/rejected")

    async def _handle_hedge_break(self) -> None:
        loser = self.ledger.loser_leg()
        if loser == "BOTH":
            await self._flatten_if_ambiguous("Both initial SLs filled (extreme move)")
            return

        survivor = self.ledger.survivor_leg()
        if survivor not in ("LONG", "SHORT"):
            await self._flatten_if_ambiguous("Hedge break but survivor not determinable")
            return

        self.logger.info(f"HEDGE BREAK: loser={loser} survivor={survivor}")
        self.state = State.SINGLE_LEG

        remaining_sl = self.ctx.long.sl if survivor == "LONG" else self.ctx.short.sl
        if remaining_sl and not self._is_terminal(remaining_sl):
            self.logger.info(f"Cancel remaining initial SL for survivor={survivor} orderId={remaining_sl.order.orderId}")
            self.ib.cancelOrder(remaining_sl.order)
            await self._await_trade_terminal(remaining_sl, timeout=self.cfg.cancel_timeout_sec, what="cancel remaining initial SL")

        qty = self.ledger.remaining_qty(survivor)
        if qty <= 0:
            await self._flatten_if_ambiguous("No remaining qty for survivor after hedge break")
            return

        await self._submit_trailing_with_retry(survivor_leg=survivor, qty=qty)
        await self._reconcile_async(reason="post_hedge_break")

    # ---------- orders ----------
    def _submit_bracket(
        self,
        leg: str,
        parent_action: str,
        stop_action: str,
        qty: float,
        stop_price: float,
    ) -> Tuple[Trade, Trade]:
        parent = MarketOrder(parent_action, qty, transmit=False)
        parent.orderId = self.ib.client.getReqId()

        spx = self._round_price(stop_price)
        child = StopOrder(stop_action, qty, stopPrice=spx, transmit=True)
        child.parentId = parent.orderId
        child.orderId = self.ib.client.getReqId()

        self.logger.info(
            f"Submit {leg} bracket: parent {parent_action} MKT qty={qty:.6f} orderId={parent.orderId} "
            f"child {stop_action} STP@{spx:.2f} orderId={child.orderId} parentId={child.parentId}"
        )

        parent_trade = self.ib.placeOrder(self.contract, parent)
        child_trade = self.ib.placeOrder(self.contract, child)

        self._orderid_to_role[parent.orderId] = (leg, "PARENT")
        self._orderid_to_role[child.orderId] = (leg, "SL")
        return parent_trade, child_trade

    async def _submit_trailing_with_retry(self, survivor_leg: str, qty: float) -> None:
        action = "SELL" if survivor_leg == "LONG" else "BUY"
        trailing_percent = max(0.01, self.cfg.trailing_pct * 100.0)

        order = Order(action=action, orderType="TRAIL", totalQuantity=qty, trailingPercent=trailing_percent)

        self.logger.info(f"Submit trailing: action={action} qty={qty:.6f} trailingPercent={trailing_percent:.2f}%")
        tr = self.ib.placeOrder(self.contract, order)
        self.ctx.trailing = tr
        self._orderid_to_role[tr.order.orderId] = ("NA", "TRAIL")

        ok = await self._await_trade_live_or_terminal(tr, timeout=self.cfg.trailing_live_timeout_sec, what="trailing live")
        self.logger.debug(f"Trailing await result: ok={ok} status={self._status(tr)} orderId={tr.order.orderId}")

        if ok and self._is_live(tr):
            return

        self.logger.warning("Trailing not confirmed live; retrying once.")
        tr2 = self.ib.placeOrder(self.contract, order)
        self.ctx.trailing = tr2
        self._orderid_to_role[tr2.order.orderId] = ("NA", "TRAIL")

        ok2 = await self._await_trade_live_or_terminal(tr2, timeout=self.cfg.trailing_live_timeout_sec, what="trailing live retry")
        self.logger.debug(f"Trailing retry await result: ok={ok2} status={self._status(tr2)} orderId={tr2.order.orderId}")

        if ok2 and self._is_live(tr2):
            return

        await self._flatten_if_ambiguous("Trailing could not be confirmed live")

    # ---------- awaits ----------
    async def _await_trade_live(self, tr: Trade, timeout: float, what: str) -> None:
        t0 = time.time()
        last_st = None
        while time.time() - t0 < timeout:
            st = self._status(tr)
            if st != last_st:
                self.logger.debug(f"AWAIT({what}): status={st} orderId={tr.order.orderId}")
                last_st = st
            if self._is_rejected(tr):
                await self._flatten_if_ambiguous(f"{what}: rejected")
                return
            if self._is_live(tr):
                self.logger.debug(f"AWAIT({what}): LIVE achieved status={st}")
                return
            await self._asleep(0.05)
        await self._flatten_if_ambiguous(f"{what}: timeout (not live), last_status={self._status(tr)}")

    async def _await_trade_terminal(self, tr: Trade, timeout: float, what: str) -> None:
        t0 = time.time()
        last_st = None
        while time.time() - t0 < timeout:
            st = self._status(tr)
            if st != last_st:
                self.logger.debug(f"AWAIT({what}): status={st} orderId={tr.order.orderId}")
                last_st = st
            if self._is_terminal(tr):
                self.logger.debug(f"AWAIT({what}): TERMINAL status={st}")
                return
            await self._asleep(0.05)
        await self._flatten_if_ambiguous(f"{what}: timeout (not terminal), last_status={self._status(tr)}")

    async def _await_trade_live_or_terminal(self, tr: Trade, timeout: float, what: str) -> bool:
        t0 = time.time()
        last_st = None
        while time.time() - t0 < timeout:
            st = self._status(tr)
            if st != last_st:
                self.logger.debug(f"AWAIT({what}): status={st} orderId={tr.order.orderId}")
                last_st = st
            if self._is_live(tr) or self._is_terminal(tr):
                return True
            await self._asleep(0.05)
        self.logger.warning(f"{what}: timeout waiting for live/terminal; last_status={self._status(tr)}")
        return False

    # ---------- reconcile / flatten ----------
    def _reconcile_sync(self, reason: str) -> None:
        self.logger.info(f"Reconcile(sync): {reason}")
        if self.state == State.DEGRADED:
            if self._has_open_orders_symbol() or abs(self._net_position_symbol()) > 1e-9:
                self.logger.warning("DEGRADED with exposure/orders: flattening.")
                self._schedule(self._flatten_if_ambiguous("DEGRADED sync reconcile"), name="flatten_degraded")
            else:
                self.state = State.IDLE

    async def _reconcile_async(self, reason: str) -> None:
        self.logger.debug(f"RECONCILE(async): {reason}")
        for i in range(self.cfg.reconcile_retries):
            if self.state == State.DEGRADED:
                return
            if self.ctx.trailing and self._status(self.ctx.trailing) == "Filled":
                await self._end_cycle("Trailing filled")
                return
            self.logger.debug(
                f"RECONCILE(async): pass {i+1}/{self.cfg.reconcile_retries} "
                f"trailingStatus={self._status(self.ctx.trailing)}"
            )
            await self._asleep(self.cfg.reconcile_retry_delay_sec)

    async def _flatten_if_ambiguous(self, reason: str) -> None:
        self.logger.error(f"FLATTEN_IF_AMBIGUOUS: {reason}")

        for tr in self.ib.openTrades():
            if tr.contract.symbol == self.cfg.symbol:
                try:
                    self.logger.warning(
                        f"CANCEL: orderId={tr.order.orderId} type={tr.order.orderType} status={tr.orderStatus.status}"
                    )
                    self.ib.cancelOrder(tr.order)
                except Exception as e:
                    self.logger.warning(f"Cancel error: {e}")

        await self._asleep(0.2)

        net = self._net_position_symbol()
        if abs(net) > 1e-9:
            action = "SELL" if net > 0 else "BUY"
            qty = abs(net)
            self.logger.warning(f"Flattening net position: {action} {qty}")
            self.ib.placeOrder(self.contract, MarketOrder(action, qty))

        await self._end_cycle(f"Flattened: {reason}")
        self.state = State.COOLDOWN

    async def _end_cycle(self, reason: str) -> None:
        self.logger.info(f"Cycle ended: {reason}")
        self.ctx.cycle_ended_at_bar_time = self.last_bar_time or datetime.now()

        for tr in self.ib.openTrades():
            if tr.contract.symbol == self.cfg.symbol:
                try:
                    self.ib.cancelOrder(tr.order)
                except Exception:
                    pass

    def _reset_cycle(self) -> None:
        self.ctx = CycleContext()
        self.ledger = ExecLedger()
        self._orderid_to_role = {}

    # ---------- market/account ----------
    async def _get_last_price(self) -> Optional[float]:
        self.logger.debug("REQ_MKTDATA: requesting last/marketPrice")
        ticker = self.ib.reqMktData(self.contract, "", False, False)
        await self._asleep(0.6)

        last = None
        if ticker.last:
            last = float(ticker.last)
            self.logger.debug(f"REQ_MKTDATA: got last={last}")
        else:
            mp = ticker.marketPrice()
            if mp:
                last = float(mp)
                self.logger.debug(f"REQ_MKTDATA: got marketPrice={last}")
            else:
                self.logger.debug(
                    f"REQ_MKTDATA: no last/marketPrice; bid={ticker.bid} ask={ticker.ask} close={ticker.close}"
                )

        try:
            self.ib.cancelMktData(ticker.contract)
        except Exception:
            pass
        return last

    def _has_open_orders_symbol(self) -> bool:
        for tr in self.ib.openTrades():
            if tr.contract.symbol != self.cfg.symbol:
                continue
            st = tr.orderStatus.status
            if st not in TERMINAL_STATUSES:
                return True
        return False

    def _net_position_symbol(self) -> float:
        net = 0.0
        for p in self.ib.positions():
            if getattr(p.contract, "symbol", None) == self.cfg.symbol:
                net += float(p.position)
        return net

    def _round_price(self, px: float) -> float:
        # Stocks: safe default cents
        return round(px, 2)

    @staticmethod
    def _bars_elapsed(start: datetime, now: datetime) -> int:
        return int((now - start).total_seconds() // 60)

    # ---------- run ----------
    def run(self) -> None:
        self.logger.info("Running. Ctrl+C to stop.")
        try:
            # ib_insync can drive the asyncio loop by running an awaitable here
            self.ib.run(self._main())
        except KeyboardInterrupt:
            self.logger.info("Stopping. Cancelling and disconnecting.")
            try:
                for tr in self.ib.openTrades():
                    if tr.contract.symbol == self.cfg.symbol:
                        self.ib.cancelOrder(tr.order)
            finally:
                self.ib.disconnect()


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=str, default="config.yaml")
    p.add_argument("--symbol", type=str, default=None)
    p.add_argument("--host", type=str, default=None)
    p.add_argument("--port", type=int, default=None)
    p.add_argument("--client-id", type=int, default=None)

    p.add_argument("--notional", type=float, default=None)
    p.add_argument("--sl-pct", type=float, default=None)
    p.add_argument("--trailing-pct", type=float, default=None)
    p.add_argument("--cooldown-bars", type=int, default=None)
    p.add_argument("--log-level", type=str, default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    defaults = Config()

    yaml_dict = None
    try:
        yaml_dict = load_config_from_yaml(args.config)
    except FileNotFoundError:
        yaml_dict = None

    cfg = config_from_sources(defaults, yaml_dict, args)

    bot = HedgeBotV3(cfg)
    bot.connect()
    bot.run()


if __name__ == "__main__":
    main()
