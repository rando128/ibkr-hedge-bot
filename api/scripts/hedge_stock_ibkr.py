# hedge_stock_ibkr.py
"""
IBKR Hedging Bot (Paper/Live compatible) — ib_insync + config.yaml
==================================================================

Implements:
- Configurable stock symbol (default NVDA)
- Enter TWO independent bracket legs (long + short), each with attached stop-loss (slPct)
- Broker (IBKR) is authoritative for stop executions (event-driven)
- After one stop triggers, bot cancels the remaining initial SL and submits a trailing stop (trailingPct)
- Overnight allowed (no forced flat)
- Cooldown = 1 bar (1 minute) after cycle ends
- Fractional shares allowed
- Failure guards: disconnect freeze, partial fill abort/flatten, missing stops flatten, reconcile after fills
- Minimal config.yaml loader + CLI override

Usage:
  pip install ib_insync pyyaml
  python hedge_stock_ibkr.py --config config.yaml
  python hedge_stock_ibkr.py --config config.yaml --symbol AAPL --port 7497

Notes:
- IBKR requires a clientId. Any integer is fine, but it must be unique per running client.
- IBKR positions for stocks are netted per symbol. This bot is therefore ORDER/EVENT-driven.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any

import yaml  # pip install pyyaml
from ib_insync import (
    IB,
    Stock,
    MarketOrder,
    StopOrder,
    TrailingStopOrder,
    Trade,
    BarDataList,
    util,
)

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
    sl_pct: float = 0.03            # 3%
    trailing_pct: float = 0.15      # 15% (we convert to IB trailingPercent units)
    bar_size: str = "1 min"
    cooldown_bars: int = 1          # 1 bar = 1 min

    # IBKR connection defaults (configurable)
    host: str = "127.0.0.1"
    port: int = 7497                # TWS paper default; Gateway paper often 4002
    client_id: int = 1              # Required. Any int, but unique per bot instance.

    # Operational safety
    entering_timeout_sec: int = 30
    reconcile_retries: int = 3
    reconcile_retry_delay_sec: float = 1.0

    # Logging
    log_level: str = "DEBUG"         # DEBUG / INFO / WARNING / ERROR


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge overlay into base (overlay wins)."""
    out = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config_from_yaml(path: str) -> Dict[str, Any]:
    """Load YAML into a plain dict. Minimal and strict-ish."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("config.yaml must contain a mapping/dictionary at the top level.")
    return data


def config_from_sources(defaults: Config, yaml_dict: Optional[Dict[str, Any]], cli: argparse.Namespace) -> Config:
    """
    Build Config from:
      1) defaults (dataclass)
      2) YAML (optional)
      3) CLI overrides (optional)
    """
    d = dataclasses.asdict(defaults)

    # Allow YAML nesting for ibkr: {host, port, client_id}
    if yaml_dict:
        normalized = dict(yaml_dict)

        if "ibkr" in normalized and isinstance(normalized["ibkr"], dict):
            ibkr = normalized.pop("ibkr")
            # Map YAML keys to flat Config keys
            if "host" in ibkr:
                normalized["host"] = ibkr["host"]
            if "port" in ibkr:
                normalized["port"] = ibkr["port"]
            if "client_id" in ibkr:
                normalized["client_id"] = ibkr["client_id"]

        d = _deep_merge(d, normalized)

    # CLI overrides (only if provided)
    if cli.symbol:
        d["symbol"] = cli.symbol
    if cli.host:
        d["host"] = cli.host
    if cli.port is not None:
        d["port"] = cli.port
    if cli.client_id is not None:
        d["client_id"] = cli.client_id

    # Strategy numeric overrides
    if cli.notional is not None:
        d["notional_per_leg_usd"] = cli.notional
    if cli.sl_pct is not None:
        d["sl_pct"] = cli.sl_pct
    if cli.trailing_pct is not None:
        d["trailing_pct"] = cli.trailing_pct
    if cli.cooldown_bars is not None:
        d["cooldown_bars"] = cli.cooldown_bars

    # Logging override
    if cli.log_level:
        d["log_level"] = cli.log_level

    cfg = Config(**d)
    _validate_config(cfg)
    return cfg


def _validate_config(cfg: Config) -> None:
    if not cfg.symbol or not isinstance(cfg.symbol, str):
        raise ValueError("symbol must be a non-empty string.")
    if cfg.notional_per_leg_usd <= 0:
        raise ValueError("notional_per_leg_usd must be > 0.")
    if not (0 < cfg.sl_pct < 1):
        raise ValueError("sl_pct must be in (0, 1). Example: 0.03")
    if not (0 < cfg.trailing_pct < 1):
        raise ValueError("trailing_pct must be in (0, 1). Example: 0.15")
    if cfg.cooldown_bars < 0:
        raise ValueError("cooldown_bars must be >= 0.")
    if cfg.client_id is None:
        raise ValueError("client_id is required (any integer, but unique per running client).")


# -----------------------------
# State machine
# -----------------------------

class State(str, Enum):
    IDLE = "IDLE"
    ENTERING = "ENTERING"
    HEDGED = "HEDGED"
    SINGLE_LEG = "SINGLE_LEG"
    COOLDOWN = "COOLDOWN"
    DEGRADED = "DEGRADED"  # disconnect/unsafe


@dataclass
class LegOrders:
    parent_trade: Optional[Trade] = None
    sl_trade: Optional[Trade] = None


@dataclass
class CycleContext:
    long_leg: LegOrders = dataclasses.field(default_factory=LegOrders)
    short_leg: LegOrders = dataclasses.field(default_factory=LegOrders)
    survivor: Optional[str] = None            # "LONG" or "SHORT"
    trailing_trade: Optional[Trade] = None
    entering_started_at: Optional[datetime] = None
    cycle_ended_at_bar_time: Optional[datetime] = None


# -----------------------------
# Bot
# -----------------------------

class HedgeBot:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.ib = IB()
        self.state: State = State.IDLE
        self.ctx = CycleContext()

        self.contract = Stock(cfg.symbol, cfg.exchange, cfg.currency)

        self.bars: Optional[BarDataList] = None
        self.last_bar_time: Optional[datetime] = None

        self.degraded_reason: Optional[str] = None

        self.logger = logging.getLogger("hedge-bot")
        self.logger.setLevel(self._parse_log_level(cfg.log_level))
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def _parse_log_level(self, s: str) -> int:
        return getattr(logging, s.upper(), logging.INFO)

    # -------- Connection / subscriptions --------

    def connect(self) -> None:
        self.logger.info(f"Connecting to IBKR: host={self.cfg.host} port={self.cfg.port} clientId={self.cfg.client_id}")
        self.ib.connect(self.cfg.host, self.cfg.port, clientId=int(self.cfg.client_id))

        self.ib.disconnectedEvent += self._on_disconnected
        self.ib.connectedEvent += self._on_connected
        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.orderStatusEvent += self._on_order_status

        self._qualify_contract()
        self._subscribe_bars()

        self.reconcile_and_adopt_state(reason="startup")

    def _qualify_contract(self) -> None:
        self.ib.qualifyContracts(self.contract)
        self.logger.info(f"Qualified contract: {self.contract}")

    def _subscribe_bars(self) -> None:
        self.bars = self.ib.reqHistoricalData(
            self.contract,
            endDateTime="",
            durationStr="2 D",
            barSizeSetting=self.cfg.bar_size,
            whatToShow="TRADES",
            useRTH=False,             # overnight allowed
            formatDate=1,
            keepUpToDate=True,
        )
        self.bars.updateEvent += self._on_bars_update
        self.logger.info(f"Subscribed to {self.cfg.bar_size} bars (keepUpToDate=True).")

    # -------- Event handlers --------

    def _on_disconnected(self) -> None:
        self.logger.warning("Disconnected from IBKR. Freezing trading actions.")
        self.state = State.DEGRADED
        self.degraded_reason = "disconnect"

    def _on_connected(self) -> None:
        self.logger.info("Reconnected to IBKR. Reconciling state with broker.")
        self.degraded_reason = None
        self.reconcile_and_adopt_state(reason="reconnect")

    def _on_exec_details(self, trade: Trade, fill) -> None:
        self.logger.info(
            f"EXEC: {trade.contract.symbol} {trade.order.action} qty={fill.shares} price={fill.price} "
            f"orderId={trade.order.orderId}"
        )
        # Reconcile after fills (paper/live parity guard)
        if self.state in (State.HEDGED, State.SINGLE_LEG, State.ENTERING):
            self.reconcile_and_adopt_state(reason="execDetails")

    def _on_order_status(self, trade: Trade) -> None:
        # Keep this debug-level; it can be noisy
        s = trade.orderStatus.status
        self.logger.debug(
            f"ORDER_STATUS: orderId={trade.order.orderId} status={s} "
            f"filled={trade.orderStatus.filled} remaining={trade.orderStatus.remaining}"
        )

    def _on_bars_update(self, bars: BarDataList, has_new_bar: bool) -> None:
        if not has_new_bar or not bars:
            return
        bar = bars[-1]
        bar_time = util.parseIBDatetime(bar.date) if isinstance(bar.date, str) else bar.date
        if bar_time is None:
            return
        self.last_bar_time = bar_time
        self._tick_on_bar(bar_time)

    # -------- Core loop logic --------

    def _tick_on_bar(self, bar_time: datetime) -> None:
        if self.state == State.DEGRADED:
            return

        # Cooldown expiry
        if self.state == State.COOLDOWN and self.ctx.cycle_ended_at_bar_time is not None:
            if self._bars_elapsed(self.ctx.cycle_ended_at_bar_time, bar_time) >= self.cfg.cooldown_bars:
                self.logger.info("Cooldown elapsed -> IDLE")
                self._reset_cycle_context(keep_state=True)
                self.state = State.IDLE

        # ENTERING timeout
        if self.state == State.ENTERING and self.ctx.entering_started_at is not None:
            if datetime.now() - self.ctx.entering_started_at > timedelta(seconds=self.cfg.entering_timeout_sec):
                self.logger.warning("ENTERING timeout: aborting and flattening any partial exposure.")
                self._abort_and_flatten(reason="entering_timeout")
                return

        # Start a new cycle
        if self.state == State.IDLE:
            self._start_new_cycle()

    def _start_new_cycle(self) -> None:
        if not self._is_flat_on_symbol():
            self.logger.info("Not flat; skipping entry.")
            return
        if self._has_open_orders_on_symbol():
            self.logger.info("Open orders exist; skipping entry.")
            return

        last = self._get_last_price()
        if last is None or last <= 0:
            self.logger.warning("No valid last price; skipping entry.")
            return

        qty = self.cfg.notional_per_leg_usd / last
        qty = max(qty, 0.0001)  # allow fractional

        self.logger.info(
            f"Entering new cycle: symbol={self.cfg.symbol} last={last:.4f} qty≈{qty:.6f} "
            f"slPct={self.cfg.sl_pct:.4f} trailingPct={self.cfg.trailing_pct:.4f}"
        )

        self.state = State.ENTERING
        self.ctx.entering_started_at = datetime.now()

        try:
            self._submit_leg_bracket(
                leg_name="LONG",
                parent_action="BUY",
                qty=qty,
                stop_action="SELL",
                stop_price=last * (1 - self.cfg.sl_pct),
                store_to=self.ctx.long_leg,
            )
            self._submit_leg_bracket(
                leg_name="SHORT",
                parent_action="SELL",
                qty=qty,
                stop_action="BUY",
                stop_price=last * (1 + self.cfg.sl_pct),
                store_to=self.ctx.short_leg,
            )
        except Exception as e:
            self.logger.exception(f"Failed to submit brackets: {e}")
            self._abort_and_flatten(reason="submit_failed")
            return

        self.reconcile_and_adopt_state(reason="post_submit")

    def _submit_leg_bracket(
        self,
        leg_name: str,
        parent_action: str,
        qty: float,
        stop_action: str,
        stop_price: float,
        store_to: LegOrders,
    ) -> None:
        parent = MarketOrder(parent_action, qty, transmit=False)
        parent.orderId = self.ib.client.getReqId()

        child = StopOrder(stop_action, qty, stopPrice=self._round_price(stop_price), transmit=True)
        child.parentId = parent.orderId
        child.orderId = self.ib.client.getReqId()

        self.logger.info(
            f"Submitting {leg_name} bracket: parent {parent_action} MKT qty={qty:.6f} "
            f"child {stop_action} STP @ {child.auxPrice:.4f} (parentId={parent.orderId})"
        )

        store_to.parent_trade = self.ib.placeOrder(self.contract, parent)
        store_to.sl_trade = self.ib.placeOrder(self.contract, child)

    # -------- Reconciliation & state adoption --------

    def reconcile_and_adopt_state(self, reason: str) -> None:
        for attempt in range(1, self.cfg.reconcile_retries + 1):
            try:
                positions = self.ib.positions()
                sym_positions = [p for p in positions if getattr(p.contract, "symbol", None) == self.cfg.symbol]
                net_qty = sum(p.position for p in sym_positions)

                open_trades = self.ib.openTrades()
                sym_trades = [t for t in open_trades if t.contract.symbol == self.cfg.symbol]

                self.logger.debug(f"Reconcile({reason}) attempt={attempt}: net_qty={net_qty} openTrades={len(sym_trades)}")

                # Recover from degraded: safest is flatten if any exposure/orders
                if self.state == State.DEGRADED:
                    if self._has_any_position(sym_positions) or self._has_open_orders_on_symbol():
                        self.logger.warning("Was DEGRADED with exposure/orders. Flattening for safety.")
                        self._abort_and_flatten(reason="degraded_recover_flatten")
                    else:
                        self.state = State.IDLE
                    return

                # ENTERING -> HEDGED
                if self.state == State.ENTERING:
                    if self._both_parents_filled() and self._both_initial_stops_live():
                        self.logger.info("ENTERING -> HEDGED (parents filled, both initial stops live)")
                        self.state = State.HEDGED
                        return

                    # Partial fill abort rule
                    if self._any_parent_filled() and not self._both_parents_filled():
                        self.logger.warning("Partial fill detected during ENTERING. Aborting and flattening.")
                        self._abort_and_flatten(reason="partial_fill_entering")
                        return

                # HEDGED -> SINGLE_LEG when any initial stop fills
                if self.state == State.HEDGED:
                    if self._any_initial_stop_filled():
                        self.ctx.survivor = self._infer_survivor_from_fills()
                        self.logger.info(f"Hedge break detected by IBKR stop fill. Survivor={self.ctx.survivor}")
                        self._install_trailing_and_cancel_initial(reason="hedge_break")
                        self.state = State.SINGLE_LEG
                        return

                # SINGLE_LEG -> COOLDOWN when trailing fills or flat/no orders
                if self.state == State.SINGLE_LEG:
                    if self._trade_filled(self.ctx.trailing_trade):
                        self.logger.info("Trailing filled -> COOLDOWN")
                        self._end_cycle()
                        self.state = State.COOLDOWN
                        return

                    if (not self._has_open_orders_on_symbol()) and (not self._has_any_position(sym_positions)):
                        self.logger.info("No orders and flat -> COOLDOWN")
                        self._end_cycle()
                        self.state = State.COOLDOWN
                        return

                # Safety reset: active state but broker shows nothing
                if self.state in (State.ENTERING, State.HEDGED, State.SINGLE_LEG) and \
                   (not self._has_open_orders_on_symbol()) and (not self._has_any_position(sym_positions)):
                    self.logger.warning(f"State={self.state} but broker shows flat/no orders. Reset -> IDLE")
                    self._reset_cycle_context(keep_state=True)
                    self.state = State.IDLE
                    return

                return

            except Exception as e:
                self.logger.warning(f"Reconcile failed attempt {attempt}/{self.cfg.reconcile_retries}: {e}")
                time.sleep(self.cfg.reconcile_retry_delay_sec)

        self.logger.error("Reconcile failed repeatedly. Flattening for safety.")
        self._abort_and_flatten(reason="reconcile_failed")

    # -------- Hedge break handling --------

    def _install_trailing_and_cancel_initial(self, reason: str) -> None:
        # Cancel remaining initial SL (the one that did NOT fill)
        for leg_name, leg in (("LONG", self.ctx.long_leg), ("SHORT", self.ctx.short_leg)):
            if leg.sl_trade and leg.sl_trade.orderStatus.status not in ("Filled", "Cancelled", "ApiCancelled", "Inactive"):
                self.logger.info(f"Cancelling remaining initial SL for {leg_name} (orderId={leg.sl_trade.order.orderId})")
                self.ib.cancelOrder(leg.sl_trade.order)

        qty = self._qty_estimate()
        if qty <= 0:
            self.logger.warning("Could not estimate qty for trailing; flattening for safety.")
            self._abort_and_flatten(reason="no_qty_for_trailing")
            return

        if self.ctx.survivor == "LONG":
            action = "SELL"
        elif self.ctx.survivor == "SHORT":
            action = "BUY"
        else:
            self.logger.warning("No survivor set; flattening for safety.")
            self._abort_and_flatten(reason="no_survivor")
            return

        trailing_percent = max(0.01, self.cfg.trailing_pct * 100.0)  # IB expects percent units
        order = TrailingStopOrder(action, qty, trailingPercent=trailing_percent)

        self.logger.info(f"Submitting trailing stop: action={action} qty={qty:.6f} trailingPercent={trailing_percent:.2f}%")
        self.ctx.trailing_trade = self.ib.placeOrder(self.contract, order)

        self.reconcile_and_adopt_state(reason=f"install_trailing:{reason}")

    # -------- Helpers --------

    def _trade_filled(self, trade: Optional[Trade]) -> bool:
        return bool(trade and trade.orderStatus.status == "Filled")

    def _both_parents_filled(self) -> bool:
        return self._trade_filled(self.ctx.long_leg.parent_trade) and self._trade_filled(self.ctx.short_leg.parent_trade)

    def _any_parent_filled(self) -> bool:
        return self._trade_filled(self.ctx.long_leg.parent_trade) or self._trade_filled(self.ctx.short_leg.parent_trade)

    def _both_initial_stops_live(self) -> bool:
        long_ok = self.ctx.long_leg.sl_trade is not None and self.ctx.long_leg.sl_trade.orderStatus.status not in ("Cancelled", "ApiCancelled", "Inactive")
        short_ok = self.ctx.short_leg.sl_trade is not None and self.ctx.short_leg.sl_trade.orderStatus.status not in ("Cancelled", "ApiCancelled", "Inactive")
        return long_ok and short_ok

    def _any_initial_stop_filled(self) -> bool:
        return self._trade_filled(self.ctx.long_leg.sl_trade) or self._trade_filled(self.ctx.short_leg.sl_trade)

    def _infer_survivor_from_fills(self) -> Optional[str]:
        # If long SL filled => long lost => survivor SHORT
        if self._trade_filled(self.ctx.long_leg.sl_trade):
            return "SHORT"
        if self._trade_filled(self.ctx.short_leg.sl_trade):
            return "LONG"
        return None

    def _has_open_orders_on_symbol(self) -> bool:
        for t in self.ib.openTrades():
            if t.contract.symbol == self.cfg.symbol and t.orderStatus.status not in ("Filled", "Cancelled", "ApiCancelled", "Inactive"):
                return True
        return False

    def _has_any_position(self, sym_positions) -> bool:
        return any(abs(p.position) > 1e-9 for p in sym_positions)

    def _is_flat_on_symbol(self) -> bool:
        positions = self.ib.positions()
        sym_positions = [p for p in positions if getattr(p.contract, "symbol", None) == self.cfg.symbol]
        return abs(sum(p.position for p in sym_positions)) < 1e-9

    def _qty_estimate(self) -> float:
        for t in (self.ctx.long_leg.parent_trade, self.ctx.short_leg.parent_trade):
            if t and t.order and t.order.totalQuantity:
                return float(t.order.totalQuantity)
        return 0.0

    def _get_last_price(self) -> Optional[float]:
        ticker = self.ib.reqMktData(self.contract, "", False, False)
        self.ib.sleep(0.5)
        last = None
        if ticker.last:
            last = float(ticker.last)
        else:
            mp = ticker.marketPrice()
            if mp:
                last = float(mp)
        self.ib.cancelMktData(ticker.contract)
        return last

    def _round_price(self, px: float) -> float:
        return round(px, 2)

    def _bars_elapsed(self, start: datetime, now: datetime) -> int:
        return int((now - start).total_seconds() // 60)

    # -------- Abort / flatten / cycle end --------

    def _abort_and_flatten(self, reason: str) -> None:
        self.logger.error(f"ABORT+FLATTEN: {reason}")
        self._cancel_all_tracked_orders()
        self._flatten_symbol_position()
        self._reset_cycle_context(keep_state=True)
        self.state = State.COOLDOWN
        self.ctx.cycle_ended_at_bar_time = self.last_bar_time or datetime.now()

    def _cancel_all_tracked_orders(self) -> None:
        trades = [
            self.ctx.long_leg.parent_trade, self.ctx.long_leg.sl_trade,
            self.ctx.short_leg.parent_trade, self.ctx.short_leg.sl_trade,
            self.ctx.trailing_trade,
        ]
        for tr in trades:
            if tr and tr.orderStatus.status not in ("Filled", "Cancelled", "ApiCancelled", "Inactive"):
                try:
                    self.logger.info(f"Cancelling orderId={tr.order.orderId} type={tr.order.orderType}")
                    self.ib.cancelOrder(tr.order)
                except Exception as e:
                    self.logger.warning(f"Cancel failed for orderId={tr.order.orderId}: {e}")

    def _flatten_symbol_position(self) -> None:
        positions = self.ib.positions()
        sym_positions = [p for p in positions if getattr(p.contract, "symbol", None) == self.cfg.symbol]
        net_qty = sum(p.position for p in sym_positions)
        if abs(net_qty) < 1e-9:
            return
        action = "SELL" if net_qty > 0 else "BUY"
        qty = abs(net_qty)
        self.logger.warning(f"Flattening net position: action={action} qty={qty}")
        self.ib.placeOrder(self.contract, MarketOrder(action, qty))

    def _end_cycle(self) -> None:
        self.ctx.cycle_ended_at_bar_time = self.last_bar_time or datetime.now()

    def _reset_cycle_context(self, keep_state: bool = False) -> None:
        old = self.state
        self.ctx = CycleContext()
        if keep_state:
            self.state = old

    # -------- Run --------

    def run(self) -> None:
        self.logger.info("Bot running. Ctrl+C to stop.")
        try:
            self.ib.run()
        except KeyboardInterrupt:
            self.logger.info("Stopping. Cancelling orders and disconnecting.")
            try:
                self._cancel_all_tracked_orders()
            finally:
                self.ib.disconnect()


# -----------------------------
# Entrypoint
# -----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=str, default="config.yaml", help="Path to config.yaml")
    # Common overrides
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
    if args.config:
        try:
            yaml_dict = load_config_from_yaml(args.config)
        except FileNotFoundError:
            # Allow running without YAML if desired
            yaml_dict = None

    cfg = config_from_sources(defaults, yaml_dict, args)

    bot = HedgeBot(cfg)
    bot.connect()
    bot.run()


if __name__ == "__main__":
    main()
