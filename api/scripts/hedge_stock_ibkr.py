"""
hedge_stock_ibrk.py — IBKR Hedging Bot (Stocks, ib_insync) — Final v2
===============================================================

Design goals (spec-conformant):
- IBKR stocks are NETTED per symbol -> do NOT assume two simultaneous positions.
- Strategy is ORDER/EXECUTION-ledger driven:
  1) Executions (fills) are truth
  2) Open orders validate protection
  3) positions() is safety sanity check only
- Two-phase entry (best-possible atomicity on IBKR):
  - Submit Leg A bracket, wait SL child is LIVE (PreSubmitted/Submitted)
  - If parent fills before SL live -> ABORT+FLATTEN
  - Submit Leg B bracket, wait SL live
- Strict SL "live" definition: {"PreSubmitted","Submitted"} only
- Hedge break occurs on ANY stop fill (partial included) on either initial SL
- On hedge break:
  - Determine loser/survivor via execution ledger (not positions())
  - Cancel remaining initial SL and await terminal
  - Submit trailing stop for survivor sized from ledger remaining qty
  - If ambiguity persists -> flatten (global invariant)
- Non-blocking waits: use ib.sleep(), no time.sleep() in event loop
- Trailing stop created via generic Order(orderType="TRAIL", trailingPercent=...)

Dependencies:
  pip install ib_insync pyyaml

Run:
  python hedge_stock_ibrk.py --config config.yaml
Optional overrides:
  python hedge_stock_ibrk.py --config config.yaml --symbol NVDA --port 7497 --client-id 7
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import yaml
from ib_insync import (
    IB,
    Order,
    Stock,
    Trade,
    MarketOrder,
    StopOrder,
    util,
)

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
    sl_pct: float = 0.03          # 3%
    trailing_pct: float = 0.15    # 15% (we convert to trailingPercent=15.0)
    bar_size: str = "1 min"
    cooldown_bars: int = 1        # 1 bar = 1 min

    # IBKR connection defaults (configurable)
    host: str = "127.0.0.1"
    port: int = 7497              # TWS paper default; gateway paper often 4002
    client_id: int = 1            # REQUIRED; any int but unique per running client

    # Safety / timings
    entering_timeout_sec: float = 30.0
    sl_live_timeout_sec: float = 2.0
    cancel_timeout_sec: float = 2.0
    trailing_live_timeout_sec: float = 2.0
    reconcile_retries: int = 3
    reconcile_retry_delay_sec: float = 0.25

    # Optional correctness
    price_rounding: str = "cent"  # "cent" or "minTick" (minTick not always accessible reliably)

    # Logging
    log_level: str = "INFO"       # DEBUG / INFO / WARNING / ERROR


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
        raise ValueError("sl_pct must be in (0,1) e.g. 0.03")
    if not (0 < cfg.trailing_pct < 1):
        raise ValueError("trailing_pct must be in (0,1) e.g. 0.15")
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
            normalized = _deep_merge(normalized, {
                "host": ibkr.get("host", d["host"]),
                "port": ibkr.get("port", d["port"]),
                "client_id": ibkr.get("client_id", d["client_id"]),
            })
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
    DEGRADED = "DEGRADED"  # disconnected / unsafe


@dataclass
class Leg:
    parent: Optional[Trade] = None
    sl: Optional[Trade] = None


@dataclass
class ExecLedger:
    # Cumulative fills per leg
    parent_filled: Dict[str, float] = field(default_factory=lambda: {"LONG": 0.0, "SHORT": 0.0})
    sl_filled: Dict[str, float] = field(default_factory=lambda: {"LONG": 0.0, "SHORT": 0.0})

    def record_parent_fill(self, leg: str, qty: float) -> None:
        self.parent_filled[leg] += float(qty)

    def record_sl_fill(self, leg: str, qty: float) -> None:
        self.sl_filled[leg] += float(qty)

    def any_sl_fill(self) -> bool:
        return (self.sl_filled["LONG"] > 0) or (self.sl_filled["SHORT"] > 0)

    def loser_leg(self) -> Optional[str]:
        # Loser is the leg whose SL has any fill
        if self.sl_filled["LONG"] > 0 and self.sl_filled["SHORT"] > 0:
            # extremely violent move; ambiguous - handled by flatten
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
        # Remaining exposure on a leg = parent fills - SL fills on same leg
        # (SL fills represent exits for that leg)
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


class HedgeBotV2:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.ib = IB()

        self.state: State = State.IDLE
        self.ctx = CycleContext()
        self.ledger = ExecLedger()

        self.contract = Stock(cfg.symbol, cfg.exchange, cfg.currency)
        self.last_bar_time: Optional[datetime] = None
        self.degraded_reason: Optional[str] = None

        self._orderid_to_role: Dict[int, Tuple[str, str]] = {}  # orderId -> (leg, role) where role ∈ {"PARENT","SL","TRAIL"}

        self.logger = logging.getLogger("hedge-bot-v2")
        self.logger.setLevel(getattr(logging, cfg.log_level.upper(), logging.INFO))
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        if not self.logger.handlers:
            self.logger.addHandler(handler)

    # -------- status helpers --------

    def _status(self, tr: Optional[Trade]) -> Optional[str]:
        if not tr or not tr.orderStatus:
            return None
        return tr.orderStatus.status

    def _is_live(self, tr: Optional[Trade]) -> bool:
        st = self._status(tr)
        return st in LIVE_STATUSES

    def _is_terminal(self, tr: Optional[Trade]) -> bool:
        st = self._status(tr)
        return (tr is None) or (st in TERMINAL_STATUSES)

    def _is_rejected(self, tr: Optional[Trade]) -> bool:
        return self._status(tr) in BAD_STATUSES

    # -------- broker connect / events --------

    def connect(self) -> None:
        self.logger.info(f"Connecting: host={self.cfg.host} port={self.cfg.port} clientId={self.cfg.client_id}")
        self.ib.connect(self.cfg.host, self.cfg.port, clientId=int(self.cfg.client_id))

        self.ib.disconnectedEvent += self._on_disconnected
        self.ib.connectedEvent += self._on_connected
        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.orderStatusEvent += self._on_order_status

        self.ib.qualifyContracts(self.contract)
        self.logger.info(f"Qualified: {self.contract}")

        # Bars used only for cooldown cadence, NOT for stop inference
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

        # Initial sanity
        self._reconcile_sync(reason="startup")

    def _on_disconnected(self) -> None:
        self.logger.warning("Disconnected. Entering DEGRADED.")
        self.state = State.DEGRADED
        self.degraded_reason = "disconnect"

    def _on_connected(self) -> None:
        self.logger.info("Reconnected. Reconciling.")
        self.degraded_reason = None
        self._reconcile_sync(reason="reconnect")

    def _on_order_status(self, trade: Trade) -> None:
        # lightweight timeout checks can be placed here if desired
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

        # Update ledger
        if leg in ("LONG", "SHORT") and role == "PARENT":
            self.ledger.record_parent_fill(leg, qty)
        elif leg in ("LONG", "SHORT") and role == "SL":
            self.ledger.record_sl_fill(leg, qty)
        elif role == "TRAIL":
            # trailing fill ends cycle; handled by reconcile
            pass

        # Event-driven transitions
        if self.state == State.ENTERING:
            # If any parent fills before its SL is live -> this is unsafe; flatten.
            # We'll check both legs.
            self.ib.createTask(self._check_entering_safety())
        elif self.state == State.HEDGED:
            # Any SL fill (partial) => hedge break
            if self.ledger.any_sl_fill():
                self.ib.createTask(self._handle_hedge_break())
        elif self.state == State.SINGLE_LEG:
            # trailing may fill -> reconcile will end cycle
            self.ib.createTask(self._reconcile_async(reason="execDetails_single"))

    def _on_bars_update(self, bars, has_new_bar: bool) -> None:
        if not has_new_bar or not bars:
            return
        bar = bars[-1]
        bar_time = util.parseIBDatetime(bar.date) if isinstance(bar.date, str) else bar.date
        self.last_bar_time = bar_time
        self.ib.createTask(self._tick_on_bar())

    # -------- periodic/cooldown tick --------

    async def _tick_on_bar(self) -> None:
        if self.state == State.DEGRADED:
            return

        # Cooldown expiry
        if self.state == State.COOLDOWN and self.ctx.cycle_ended_at_bar_time and self.last_bar_time:
            if self._bars_elapsed(self.ctx.cycle_ended_at_bar_time, self.last_bar_time) >= self.cfg.cooldown_bars:
                self.logger.info("Cooldown elapsed -> IDLE")
                self._reset_cycle()
                self.state = State.IDLE

        # Entering timeout (wall-clock)
        if self.state == State.ENTERING and self.ctx.entering_started_at:
            if datetime.now() - self.ctx.entering_started_at > timedelta(seconds=self.cfg.entering_timeout_sec):
                await self._flatten_if_ambiguous("ENTERING timeout")

        # Start cycle if idle
        if self.state == State.IDLE:
            await self._start_cycle()

    # -------- core flow --------

    async def _start_cycle(self) -> None:
        # Preconditions: no open orders for this symbol and position sanity (net should be ~0)
        if self._has_open_orders_symbol():
            return

        # Net position sanity check only (not strategy truth)
        if abs(self._net_position_symbol()) > 1e-9:
            self.logger.warning("Net position not flat while IDLE; flattening.")
            await self._flatten_if_ambiguous("Net position non-zero in IDLE")
            return

        last = await self._get_last_price()
        if not last:
            self.logger.warning("No last price; skipping entry.")
            return

        qty = max(self.cfg.notional_per_leg_usd / last, 0.0001)
        self.logger.info(f"Starting cycle: symbol={self.cfg.symbol} last={last:.4f} qty≈{qty:.6f}")

        self.state = State.ENTERING
        self.ctx.entering_started_at = datetime.now()

        # Two-phase submission (best-possible atomicity)
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
        self.ctx.long.parent = long_parent
        self.ctx.long.sl = long_sl

        await self._await_trade_live(long_sl, timeout=self.cfg.sl_live_timeout_sec, what="LONG SL live")

        # If long parent filled but long SL not live -> abort
        if self.ledger.parent_filled["LONG"] > 0 and not self._is_live(long_sl):
            await self._flatten_if_ambiguous("LONG parent filled before LONG SL live")
            return

        # If SL rejected/missing -> abort (await already checks rejection, but keep hard guard)
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
        self.ctx.short.parent = short_parent
        self.ctx.short.sl = short_sl

        await self._await_trade_live(short_sl, timeout=self.cfg.sl_live_timeout_sec, what="SHORT SL live")

        # Partial fill symmetry rule during ENTERING:
        # If either parent has filled but both have not filled within a small window -> flatten.
        await self._check_entering_safety()

        # If short SL rejected/missing -> abort
        if self._is_rejected(short_sl) or not self._is_live(short_sl):
            await self._flatten_if_ambiguous("SHORT SL not live or rejected")
            return

        # If we reach here: both SLs are live; hedge considered active
        self.logger.info("ENTERING -> HEDGED (both initial SLs live)")
        self.state = State.HEDGED

    async def _check_entering_safety(self) -> None:
        # Enforce "never partial hedge"
        long_f = self.ledger.parent_filled["LONG"]
        short_f = self.ledger.parent_filled["SHORT"]

        # If one side has any fill and the other has zero after a short grace -> abort
        if (long_f > 0 and short_f == 0) or (short_f > 0 and long_f == 0):
            # allow a tiny grace window for the other parent to fill
            await self.ib.sleep(0.25)
            long_f = self.ledger.parent_filled["LONG"]
            short_f = self.ledger.parent_filled["SHORT"]
            if (long_f > 0 and short_f == 0) or (short_f > 0 and long_f == 0):
                await self._flatten_if_ambiguous("Partial hedge during ENTERING (one parent filled, other not)")

        # If any initial SL is rejected while parent has fills -> abort
        if self.ledger.parent_filled["LONG"] > 0 and (not self._is_live(self.ctx.long.sl) or self._is_rejected(self.ctx.long.sl)):
            await self._flatten_if_ambiguous("LONG parent filled but LONG SL not live/rejected")
        if self.ledger.parent_filled["SHORT"] > 0 and (not self._is_live(self.ctx.short.sl) or self._is_rejected(self.ctx.short.sl)):
            await self._flatten_if_ambiguous("SHORT parent filled but SHORT SL not live/rejected")

    async def _handle_hedge_break(self) -> None:
        # If both SLs have fills -> ambiguous extreme -> flatten (spec safety)
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

        # Cancel remaining initial SL (the survivor's initial SL is still active, must be replaced)
        remaining_sl = self.ctx.long.sl if survivor == "LONG" else self.ctx.short.sl
        if remaining_sl and not self._is_terminal(remaining_sl):
            self.ib.cancelOrder(remaining_sl.order)
            await self._await_trade_terminal(remaining_sl, timeout=self.cfg.cancel_timeout_sec, what="cancel remaining initial SL")

        # After cancel, compute remaining qty from ledger
        qty = self.ledger.remaining_qty(survivor)
        if qty <= 0:
            await self._flatten_if_ambiguous("No remaining qty for survivor after hedge break")
            return

        # Submit trailing (retry once if needed)
        await self._submit_trailing_with_retry(survivor_leg=survivor, qty=qty)

        # Reconcile after protection install
        await self._reconcile_async(reason="post_hedge_break")

    # -------- orders --------

    def _submit_bracket(
        self,
        leg: str,
        parent_action: str,
        stop_action: str,
        qty: float,
        stop_price: float,
    ) -> Tuple[Trade, Trade]:
        """
        One-leg bracket: parent MarketOrder (transmit=False) + child StopOrder (transmit=True).
        This provides atomicity WITHIN the leg, and we do two-phase to minimize cross-leg risk.
        """
        parent = MarketOrder(parent_action, qty, transmit=False)
        parent.orderId = self.ib.client.getReqId()

        spx = self._round_price(stop_price)
        child = StopOrder(stop_action, qty, stopPrice=spx, transmit=True)
        child.parentId = parent.orderId
        child.orderId = self.ib.client.getReqId()

        self.logger.info(f"Submit {leg} bracket: parent {parent_action} MKT qty={qty:.6f} "
                         f"child {stop_action} STP @ {spx:.4f} parentId={parent.orderId}")

        parent_trade = self.ib.placeOrder(self.contract, parent)
        child_trade = self.ib.placeOrder(self.contract, child)

        self._orderid_to_role[parent.orderId] = (leg, "PARENT")
        self._orderid_to_role[child.orderId] = (leg, "SL")

        return parent_trade, child_trade

    async def _submit_trailing_with_retry(self, survivor_leg: str, qty: float) -> None:
        action = "SELL" if survivor_leg == "LONG" else "BUY"
        trailing_percent = max(0.01, self.cfg.trailing_pct * 100.0)

        order = Order(
            action=action,
            orderType="TRAIL",
            totalQuantity=qty,
            trailingPercent=trailing_percent,
        )

        self.logger.info(f"Submit trailing: action={action} qty={qty:.6f} trailingPercent={trailing_percent:.2f}%")
        tr = self.ib.placeOrder(self.contract, order)
        self.ctx.trailing = tr
        self._orderid_to_role[tr.order.orderId] = ("NA", "TRAIL")

        # Wait for live or terminal quickly; if rejected/inactive, retry once
        ok = await self._await_trade_live_or_terminal(tr, timeout=self.cfg.trailing_live_timeout_sec, what="trailing live")
        if ok and self._is_live(tr):
            return

        # Retry once if not live
        self.logger.warning("Trailing not live; retrying once.")
        tr2 = self.ib.placeOrder(self.contract, order)
        self.ctx.trailing = tr2
        self._orderid_to_role[tr2.order.orderId] = ("NA", "TRAIL")

        ok2 = await self._await_trade_live_or_terminal(tr2, timeout=self.cfg.trailing_live_timeout_sec, what="trailing live retry")
        if ok2 and self._is_live(tr2):
            return

        await self._flatten_if_ambiguous("Trailing could not be confirmed live")

    # -------- awaits (non-blocking) --------

    async def _await_trade_live(self, tr: Trade, timeout: float, what: str) -> None:
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self._is_rejected(tr):
                await self._flatten_if_ambiguous(f"{what}: rejected")
                return
            if self._is_live(tr):
                return
            await self.ib.sleep(0.05)
        await self._flatten_if_ambiguous(f"{what}: timeout (not live)")

    async def _await_trade_terminal(self, tr: Trade, timeout: float, what: str) -> None:
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self._is_terminal(tr):
                return
            await self.ib.sleep(0.05)
        await self._flatten_if_ambiguous(f"{what}: cancel timeout (not terminal)")

    async def _await_trade_live_or_terminal(self, tr: Trade, timeout: float, what: str) -> bool:
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self._is_live(tr):
                return True
            if self._is_terminal(tr):
                return True
            await self.ib.sleep(0.05)
        self.logger.warning(f"{what}: timeout waiting for live/terminal")
        return False

    # -------- reconcile & flatten invariant --------

    def _reconcile_sync(self, reason: str) -> None:
        # Minimal sync reconcile: used at startup/reconnect
        self.logger.info(f"Reconcile(sync): {reason}")
        # If we reconnect mid-exposure and can't safely adopt, flatten.
        if self.state == State.DEGRADED:
            if self._has_open_orders_symbol() or abs(self._net_position_symbol()) > 1e-9:
                self.logger.warning("DEGRADED with exposure/orders: flattening.")
                self.ib.createTask(self._flatten_if_ambiguous("DEGRADED sync reconcile"))
            else:
                self.state = State.IDLE

    async def _reconcile_async(self, reason: str) -> None:
        # Non-blocking reconcile loop; if ambiguous beyond retries -> flatten
        for _ in range(self.cfg.reconcile_retries):
            if self.state == State.DEGRADED:
                return
            # If trailing exists and is filled -> end cycle
            if self.ctx.trailing and self._status(self.ctx.trailing) == "Filled":
                await self._end_cycle("Trailing filled")
                return
            await self.ib.sleep(self.cfg.reconcile_retry_delay_sec)

    async def _flatten_if_ambiguous(self, reason: str) -> None:
        """
        Global invariant: if safety cannot be proven, cancel everything and market-flatten.
        """
        self.logger.error(f"FLATTEN_IF_AMBIGUOUS: {reason}")

        # Cancel all open orders for this symbol
        for tr in self.ib.openTrades():
            if tr.contract.symbol == self.cfg.symbol:
                try:
                    self.ib.cancelOrder(tr.order)
                except Exception as e:
                    self.logger.warning(f"Cancel error: {e}")

        await self.ib.sleep(0.2)

        # Flatten net position if any (sanity layer)
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
        # Keep ledger for debugging; reset on cooldown expiry
        # Also cancel any remaining tracked orders (best-effort)
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

    # -------- market data / account queries --------

    async def _get_last_price(self) -> Optional[float]:
        ticker = self.ib.reqMktData(self.contract, "", False, False)
        await self.ib.sleep(0.4)
        last = None
        if ticker.last:
            last = float(ticker.last)
        else:
            mp = ticker.marketPrice()
            if mp:
                last = float(mp)
        self.ib.cancelMktData(ticker.contract)
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
        # Sanity check only (stocks net)
        net = 0.0
        for p in self.ib.positions():
            if getattr(p.contract, "symbol", None) == self.cfg.symbol:
                net += float(p.position)
        return net

    def _round_price(self, px: float) -> float:
        # Practical safe default: cents
        if self.cfg.price_rounding == "cent":
            return round(px, 2)
        return round(px, 2)

    @staticmethod
    def _bars_elapsed(start: datetime, now: datetime) -> int:
        return int((now - start).total_seconds() // 60)

    # -------- run --------

    def run(self) -> None:
        self.logger.info("Running. Ctrl+C to stop.")
        try:
            self.ib.run()
        except KeyboardInterrupt:
            self.logger.info("Stopping. Cancelling and disconnecting.")
            try:
                # best-effort cancel symbol orders
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

    bot = HedgeBotV2(cfg)
    bot.connect()
    bot.run()


if __name__ == "__main__":
    main()
