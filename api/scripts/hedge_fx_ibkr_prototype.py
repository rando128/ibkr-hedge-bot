from __future__ import annotations

import time
import math
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple

from ib_insync import (
    IB,
    Forex,
    LimitOrder,
    MarketOrder,
    StopOrder,
    Trade,
    Ticker,
    util,
)

# ============================================================
# CONFIG
# ============================================================
HOST = "127.0.0.1"
PORT = 7497              # TWS paper commonly 7497 (live often 7496)
CLIENT_ID = 17

PAIR = "EURUSD"
EXCHANGE = "IDEALPRO"

BAR_SECONDS = 60

# Strategy params (similar spirit to your HB config)
NOTIONAL_PER_LEG_USD = 2_000.0
ENTRY_OFFSET_PCT = 0.0001
ENTRY_TIMEOUT_BARS = 6

ENTRY_REQUOTE_EVERY_BARS = 1
ENTRY_MAX_REQUOTES = 4
ENTRY_REQUOTE_OFFSETS = [0.0001, 0.00005, 0.0, -0.0001]  # maker-ish -> cross-ish

ENTRY_MAX_ADVERSE_PCT = 0.0010   # 0.10%
SL_PCT = 0.0010                 # 0.10%
SLA_PCT = 0.0015                # 0.15%

TRAIL_MODIFY_THRESHOLD_PCT = 0.0002   # only modify if stop moves enough
COOLDOWN_BARS = 1

# Throttling: do not spam cancels/modifies
MIN_ORDER_OP_INTERVAL_S = 1.0

# OrderRef tagging (critical for reconciliation)
ORDERREF_PREFIX = "HEDGEFX"

# FX price formatting
PRICE_DECIMALS = 5  # EURUSD typically 5 decimals on IDEALPRO
MIN_TICK = 0.00005  # conservative; exact varies by venue/stream

TIF = "GTC"


# ============================================================
# UTIL
# ============================================================
def now_s() -> float:
    return time.time()


def round_price(px: float) -> float:
    # quantize to tick-ish and decimals
    if px <= 0:
        return px
    ticks = round(px / MIN_TICK)
    px2 = ticks * MIN_TICK
    return round(px2, PRICE_DECIMALS)


def mid_from_ticker(t: Ticker) -> Optional[float]:
    b = t.bid
    a = t.ask
    if b is None or a is None:
        return None
    if b <= 0 or a <= 0:
        return None
    return (b + a) / 2.0


def pct_move(a: float, b: float) -> float:
    # (a-b)/a style helper
    if a == 0:
        return 0.0
    return (a - b) / a


# ============================================================
# CONNECTIVITY & RESILIENCE
# ============================================================
class IBClientManager:
    def __init__(self, host: str, port: int, client_id: int):
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id

        self.degraded = True
        self._last_connect_attempt = 0.0
        self._backoff_s = 1.0
        self._max_backoff_s = 20.0

        # on disconnect, mark degraded
        self.ib.disconnectedEvent += self._on_disconnect

    def connect(self) -> bool:
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=5)
            self.degraded = False
            self._backoff_s = 1.0
            print("[IB] connected")
            return True
        except Exception as e:
            self.degraded = True
            print(f"[IB] connect failed: {e}")
            return False

    def _on_disconnect(self):
        self.degraded = True
        print("[IB] DISCONNECTED -> degraded")

    def ensure_connected(self) -> bool:
        if self.ib.isConnected() and not self.degraded:
            return True

        # backoff reconnect attempts
        t = now_s()
        if t - self._last_connect_attempt < self._backoff_s:
            return False

        self._last_connect_attempt = t
        ok = self.connect()
        if not ok:
            self._backoff_s = min(self._max_backoff_s, self._backoff_s * 1.8)
        return ok


# ============================================================
# MARKET DATA + BAR BUILDER
# ============================================================
@dataclass
class Bar:
    ts_start: float
    open: float
    high: float
    low: float
    close: float


class BarAggregator:
    def __init__(self, bar_seconds: int):
        self.bar_seconds = bar_seconds
        self._bar_start: Optional[float] = None
        self._bar: Optional[Bar] = None

    def update(self, price: float, t: float) -> Optional[Bar]:
        if self._bar is None:
            self._bar_start = t
            self._bar = Bar(ts_start=t, open=price, high=price, low=price, close=price)
            return None

        # roll bar
        if (t - (self._bar_start or t)) >= self.bar_seconds:
            finished = self._bar
            # start new bar with current tick
            self._bar_start = t
            self._bar = Bar(ts_start=t, open=price, high=price, low=price, close=price)
            return finished

        # update in-flight bar
        self._bar.high = max(self._bar.high, price)
        self._bar.low = min(self._bar.low, price)
        self._bar.close = price
        return None


class MarketDataEngine:
    def __init__(self, ib: IB, contract):
        self.ib = ib
        self.contract = contract
        self.ticker: Optional[Ticker] = None

        self.last_mid: Optional[float] = None
        self.last_mid_ts: Optional[float] = None

        self.agg = BarAggregator(BAR_SECONDS)

    def subscribe(self):
        # idempotent best-effort
        self.ticker = self.ib.reqMktData(self.contract, "", False, False)
        print("[MD] subscribed reqMktData")

    def data_ok(self, stale_s: float = 5.0) -> bool:
        if self.last_mid is None or self.last_mid_ts is None:
            return False
        return (now_s() - self.last_mid_ts) <= stale_s

    def poll(self) -> Optional[Bar]:
        if self.ticker is None:
            return None
        m = mid_from_ticker(self.ticker)
        if m is None:
            return None
        t = now_s()
        self.last_mid = m
        self.last_mid_ts = t
        return self.agg.update(m, t)


# ============================================================
# EXECUTION ENGINE (orders, tracking, throttling)
# ============================================================
class ThrottleGuard:
    def __init__(self, min_interval_s: float):
        self.min_interval_s = min_interval_s
        self._last_op_ts = 0.0

    def allow(self) -> bool:
        t = now_s()
        if (t - self._last_op_ts) >= self.min_interval_s:
            self._last_op_ts = t
            return True
        return False


class ExecutionEngine:
    def __init__(self, ib: IB, contract, throttle: ThrottleGuard):
        self.ib = ib
        self.contract = contract
        self.throttle = throttle

    def place(self, order, order_ref: str) -> Trade:
        order.orderRef = order_ref
        order.tif = getattr(order, "tif", None) or TIF
        trade = self.ib.placeOrder(self.contract, order)
        return trade

    def cancel(self, trade: Optional[Trade]) -> bool:
        if trade is None:
            return False
        if not self.throttle.allow():
            return False
        try:
            self.ib.cancelOrder(trade.order)
            return True
        except Exception as e:
            print(f"[EXEC] cancel failed: {e}")
            return False

    def replace(self, trade: Optional[Trade], new_order, order_ref: str) -> Trade:
        # cancel+new (throttled)
        if trade is not None:
            self.cancel(trade)
        # ensure at least one throttle tick between cancel and new
        # (if throttle blocks, we still place; IB may accept but this is a prototype)
        new_order.orderRef = order_ref
        new_order.tif = getattr(new_order, "tif", None) or TIF
        return self.ib.placeOrder(self.contract, new_order)

    def modify_stop_price(self, trade: Trade, new_stop_price: float) -> bool:
        # IB modifications are done by re-sending order with same orderId.
        if not self.throttle.allow():
            return False
        try:
            o = trade.order
            # only for StopOrder-like
            o.auxPrice = new_stop_price
            o.orderRef = o.orderRef or ""
            self.ib.placeOrder(self.contract, o)
            return True
        except Exception as e:
            print(f"[EXEC] modify failed (will need cancel+replace): {e}")
            return False

    def flatten_fx_position_best_effort(self, qty_base: float, action: str, order_ref: str) -> Optional[Trade]:
        # market order to close net exposure
        if qty_base <= 0:
            return None
        o = MarketOrder(action, qty_base)
        return self.place(o, order_ref)


# ============================================================
# VIRTUAL LEGS
# ============================================================
@dataclass
class VirtualLeg:
    name: str                 # "LONG" or "SHORT"
    entry_action: str         # BUY or SELL
    exit_action: str          # SELL or BUY
    qty: float

    entry_trade: Optional[Trade] = None
    stop_trade: Optional[Trade] = None

    entry_avg: Optional[float] = None
    entry_filled_qty: float = 0.0

    def entry_status(self) -> str:
        if self.entry_trade is None:
            return "NONE"
        return (self.entry_trade.orderStatus.status or "UNKNOWN").upper()

    def stop_status(self) -> str:
        if self.stop_trade is None:
            return "NONE"
        return (self.stop_trade.orderStatus.status or "UNKNOWN").upper()

    def entry_filled(self) -> bool:
        return self.entry_status() == "FILLED"

    def stop_filled(self) -> bool:
        return self.stop_status() == "FILLED"


# ============================================================
# STRATEGY STATE MACHINE
# ============================================================
class HedgeFXStrategy:
    def __init__(self, mgr: IBClientManager):
        self.mgr = mgr
        self.ib = mgr.ib

        self.contract = Forex(PAIR, exchange=EXCHANGE)

        self.md = MarketDataEngine(self.ib, self.contract)
        self.exec = ExecutionEngine(self.ib, self.contract, ThrottleGuard(MIN_ORDER_OP_INTERVAL_S))

        # state
        self.state = "BOOT"
        self.cycle_id = 0
        self.bar_count = 0
        self.cooldown_left = 0

        # entry pending tracking
        self.entry_start_bar: Optional[int] = None
        self.requote_count = 0
        self.last_requote_bar: Optional[int] = None

        # legs
        self.long_leg: Optional[VirtualLeg] = None
        self.short_leg: Optional[VirtualLeg] = None

        # trailing
        self.direction_after_break: Optional[str] = None  # "long" or "short"
        self.highest_since_break: Optional[float] = None
        self.lowest_since_break: Optional[float] = None
        self.trailing_trade: Optional[Trade] = None
        self.trailing_stop_px: Optional[float] = None

        # safety / fault
        self.fault_reason: Optional[str] = None

    # ----------------------------
    # Startup reconciliation (flatten mode)
    # ----------------------------
    def reconcile_flatten(self):
        print("[RECON] flatten-mode: cancel tagged orders + flatten exposure best-effort")

        # cancel open orders with our prefix (by orderRef)
        try:
            open_trades = self.ib.openTrades()
            for tr in open_trades:
                ref = getattr(tr.order, "orderRef", "") or ""
                if ref.startswith(ORDERREF_PREFIX):
                    try:
                        self.ib.cancelOrder(tr.order)
                        print(f"[RECON] cancel orderRef={ref} orderId={tr.order.orderId}")
                    except Exception as e:
                        print(f"[RECON] cancel failed orderRef={ref}: {e}")
        except Exception as e:
            print(f"[RECON] openTrades() failed: {e}")

        # flatten net FX position best-effort (positions() often includes CASH contracts)
        # For EURUSD, base currency is EUR. If position in EUR is positive, sell EURUSD to flatten; if negative, buy.
        try:
            pos = None
            for p in self.ib.positions():
                c = p.contract
                if getattr(c, "secType", "") == "CASH":
                    # CASH contract symbols: symbol=EUR currency=USD for EURUSD-like
                    if getattr(c, "symbol", "") == "EUR" and getattr(c, "currency", "") == "USD":
                        pos = p
                        break
            if pos is None:
                print("[RECON] no EUR.USD CASH position found (ok)")
                return

            qty = float(pos.position or 0.0)
            if abs(qty) < 1e-9:
                print("[RECON] EUR.USD position is ~0 (ok)")
                return

            action = "SELL" if qty > 0 else "BUY"
            qty_abs = abs(qty)
            tr = self.exec.flatten_fx_position_best_effort(
                qty_base=qty_abs,
                action=action,
                order_ref=f"{ORDERREF_PREFIX}-RECON-FLATTEN",
            )
            print(f"[RECON] flatten sent action={action} qty={qty_abs} orderId={tr.order.orderId if tr else None}")
        except Exception as e:
            print(f"[RECON] flatten failed: {e}")

    # ----------------------------
    # Core loop
    # ----------------------------
    def start(self):
        # qualify contract
        self.ib.qualifyContracts(self.contract)
        self.md.subscribe()

        self.reconcile_flatten()

        self.state = "IDLE"
        print("[STATE] -> IDLE")

    def on_bar(self, bar: Bar):
        self.bar_count += 1

        # Connectivity gate
        if self.mgr.degraded:
            print("[GATE] IB degraded; skipping bar actions")
            return

        # Data gate
        if not self.md.data_ok():
            print("[GATE] DATA_NOT_OK (stale/missing mid); skipping bar actions")
            return

        close = bar.close
        print(f"[BAR] #{self.bar_count} O={bar.open:.5f} H={bar.high:.5f} L={bar.low:.5f} C={bar.close:.5f} state={self.state}")

        if self.state == "COOLDOWN":
            self.cooldown_left -= 1
            if self.cooldown_left <= 0:
                self.state = "IDLE"
                print("[STATE] -> IDLE")
            return

        if self.state == "FAULT":
            print(f"[FAULT] {self.fault_reason}")
            return

        if self.state == "IDLE":
            self._enter_entry_pending(close)
            return

        if self.state == "ENTRY_PENDING":
            self._handle_entry_pending(bar)
            return

        if self.state == "HEDGED":
            self._handle_hedged(bar)
            return

        if self.state == "TRAILING":
            self._handle_trailing(bar)
            return

    # ----------------------------
    # Phase 1: Entry placement
    # ----------------------------
    def _enter_entry_pending(self, mid_ref: float):
        self.cycle_id += 1
        self.entry_start_bar = self.bar_count
        self.requote_count = 0
        self.last_requote_bar = self.bar_count

        # qty in base currency EUR
        buy_px = round_price(mid_ref * (1.0 - ENTRY_OFFSET_PCT))
        sell_px = round_price(mid_ref * (1.0 + ENTRY_OFFSET_PCT))

        qty_buy = NOTIONAL_PER_LEG_USD / buy_px
        qty_sell = NOTIONAL_PER_LEG_USD / sell_px
        # keep symmetric qty (use min to reduce netting distortions)
        qty = float(math.floor(min(qty_buy, qty_sell)))

        if qty <= 0:
            self._fault(f"qty computed <= 0 (mid={mid_ref})")
            return

        self.long_leg = VirtualLeg(name="LONG", entry_action="BUY", exit_action="SELL", qty=qty)
        self.short_leg = VirtualLeg(name="SHORT", entry_action="SELL", exit_action="BUY", qty=qty)

        o_long = LimitOrder("BUY", qty, buy_px)
        o_short = LimitOrder("SELL", qty, sell_px)
        o_long.tif = TIF
        o_short.tif = TIF

        self.long_leg.entry_trade = self.exec.place(o_long, f"{ORDERREF_PREFIX}-C{self.cycle_id}-LONG-ENTRY")
        self.short_leg.entry_trade = self.exec.place(o_short, f"{ORDERREF_PREFIX}-C{self.cycle_id}-SHORT-ENTRY")

        self.state = "ENTRY_PENDING"
        print(f"[STATE] -> ENTRY_PENDING (cycle={self.cycle_id}) entry BUY@{buy_px:.5f} SELL@{sell_px:.5f} qty={qty}")

    # ----------------------------
    # Phase 2: Entry monitoring (requote + adverse kill + timeout)
    # ----------------------------
    def _handle_entry_pending(self, bar: Bar):
        assert self.entry_start_bar is not None
        assert self.long_leg and self.short_leg
        bars_waited = self.bar_count - self.entry_start_bar

        # Update entry avgFillPrice best-effort
        self.long_leg.entry_avg = float(self.long_leg.entry_trade.orderStatus.avgFillPrice or 0.0) or None
        self.short_leg.entry_avg = float(self.short_leg.entry_trade.orderStatus.avgFillPrice or 0.0) or None

        long_filled = self.long_leg.entry_filled()
        short_filled = self.short_leg.entry_filled()

        # If both filled -> go HEDGED and place stops
        if long_filled and short_filled:
            ref_entry = ((self.long_leg.entry_avg or bar.close) + (self.short_leg.entry_avg or bar.close)) / 2.0
            self._place_initial_stops(ref_entry)
            self.state = "HEDGED"
            print(f"[STATE] -> HEDGED ref_entry={ref_entry:.5f}")
            return

        # One-sided logic
        mid = bar.close

        # adverse kill: if one filled, price moves against that filled leg by ENTRY_MAX_ADVERSE_PCT
        if long_filled and not short_filled:
            v = self.long_leg.entry_avg or mid
            adverse = pct_move(v, mid)  # (v-mid)/v ; adverse if mid fell
            if adverse >= ENTRY_MAX_ADVERSE_PCT:
                print(f"[ENTRY_ADVERSE] long_only adverse={adverse:.6f} >= {ENTRY_MAX_ADVERSE_PCT} -> flatten+abort")
                self._abort_and_flatten()
                return

        if short_filled and not long_filled:
            v = self.short_leg.entry_avg or mid
            adverse = pct_move(mid, v)  # (mid-v)/mid-ish; adverse if mid rose above short entry
            # better: (mid - v)/v
            if v != 0:
                adverse = (mid - v) / v
            if adverse >= ENTRY_MAX_ADVERSE_PCT:
                print(f"[ENTRY_ADVERSE] short_only adverse={adverse:.6f} >= {ENTRY_MAX_ADVERSE_PCT} -> flatten+abort")
                self._abort_and_flatten()
                return

        # requote missing leg (max 1 per N bars, capped)
        if self.last_requote_bar is None:
            self.last_requote_bar = self.bar_count

        can_requote = (self.bar_count - self.last_requote_bar) >= ENTRY_REQUOTE_EVERY_BARS
        if can_requote and self.requote_count < ENTRY_MAX_REQUOTES:
            idx = min(self.requote_count, len(ENTRY_REQUOTE_OFFSETS) - 1)
            off = ENTRY_REQUOTE_OFFSETS[idx]

            if long_filled and not short_filled:
                # requote missing short: cancel+replace SELL closer
                new_px = round_price(mid * (1.0 + off))
                print(f"[REQUOTE] missing SHORT attempt={self.requote_count+1}/{ENTRY_MAX_REQUOTES} new SELL@{new_px:.5f}")
                self.exec.replace(
                    self.short_leg.entry_trade,
                    LimitOrder("SELL", self.short_leg.qty, new_px),
                    f"{ORDERREF_PREFIX}-C{self.cycle_id}-SHORT-ENTRY-R{self.requote_count+1}",
                )
                self.requote_count += 1
                self.last_requote_bar = self.bar_count

            elif short_filled and not long_filled:
                # requote missing long: cancel+replace BUY closer
                new_px = round_price(mid * (1.0 - off))
                print(f"[REQUOTE] missing LONG attempt={self.requote_count+1}/{ENTRY_MAX_REQUOTES} new BUY@{new_px:.5f}")
                self.exec.replace(
                    self.long_leg.entry_trade,
                    LimitOrder("BUY", self.long_leg.qty, new_px),
                    f"{ORDERREF_PREFIX}-C{self.cycle_id}-LONG-ENTRY-R{self.requote_count+1}",
                )
                self.requote_count += 1
                self.last_requote_bar = self.bar_count

        # timeout
        if bars_waited >= ENTRY_TIMEOUT_BARS:
            print(f"[ENTRY_TIMEOUT] waited={bars_waited} -> cancel remaining + flatten")
            self._abort_and_flatten()
            return

        print(f"[ENTRY_WAIT] waited={bars_waited} long={self.long_leg.entry_status()} short={self.short_leg.entry_status()} requotes={self.requote_count}")

    def _abort_and_flatten(self):
        assert self.long_leg and self.short_leg

        # cancel entry orders
        self.exec.cancel(self.long_leg.entry_trade)
        self.exec.cancel(self.short_leg.entry_trade)

        # flatten best-effort: if one filled and the other not, close net exposure with market
        # (in netting FX, this approximates the exposure cleanup)
        # If long filled only => SELL qty; if short filled only => BUY qty
        long_filled = self.long_leg.entry_filled()
        short_filled = self.short_leg.entry_filled()

        if long_filled and not short_filled:
            self.exec.flatten_fx_position_best_effort(
                qty_base=self.long_leg.qty,
                action="SELL",
                order_ref=f"{ORDERREF_PREFIX}-C{self.cycle_id}-ABORT-FLATTEN-SELL",
            )
        elif short_filled and not long_filled:
            self.exec.flatten_fx_position_best_effort(
                qty_base=self.short_leg.qty,
                action="BUY",
                order_ref=f"{ORDERREF_PREFIX}-C{self.cycle_id}-ABORT-FLATTEN-BUY",
            )

        self._enter_cooldown()

    # ----------------------------
    # Phase 3: Create stops (hedged)
    # ----------------------------
    def _place_initial_stops(self, ref_entry: float):
        assert self.long_leg and self.short_leg

        long_sl = round_price(ref_entry * (1.0 - SL_PCT))
        short_sl = round_price(ref_entry * (1.0 + SL_PCT))

        # Stop for long exposure: SELL stop below
        self.long_leg.stop_trade = self.exec.place(
            StopOrder("SELL", self.long_leg.qty, long_sl),
            f"{ORDERREF_PREFIX}-C{self.cycle_id}-LONG-SL",
        )
        # Stop for short exposure: BUY stop above
        self.short_leg.stop_trade = self.exec.place(
            StopOrder("BUY", self.short_leg.qty, short_sl),
            f"{ORDERREF_PREFIX}-C{self.cycle_id}-SHORT-SL",
        )

        print(f"[STOPS] longSL={long_sl:.5f} shortSL={short_sl:.5f}")

    # ----------------------------
    # Phase 4: Detect break
    # ----------------------------
    def _handle_hedged(self, bar: Bar):
        assert self.long_leg and self.short_leg

        long_sl_filled = self.long_leg.stop_filled()
        short_sl_filled = self.short_leg.stop_filled()

        if not long_sl_filled and not short_sl_filled:
            return

        # Cancel the remaining stop (safety)
        if long_sl_filled and not short_sl_filled:
            print("[BREAK] long SL filled -> remaining exposure is SHORT (net)")
            self.direction_after_break = "short"
            self.lowest_since_break = bar.low
            self.exec.cancel(self.short_leg.stop_trade)
        elif short_sl_filled and not long_sl_filled:
            print("[BREAK] short SL filled -> remaining exposure is LONG (net)")
            self.direction_after_break = "long"
            self.highest_since_break = bar.high
            self.exec.cancel(self.long_leg.stop_trade)
        else:
            # both filled same bar (rare but possible)
            print("[BREAK] both stops filled -> cycle ends")
            self._enter_cooldown()
            return

        # Place initial trailing stop order for remaining exposure
        self._place_or_reset_trailing(bar)
        self.state = "TRAILING"
        print(f"[STATE] -> TRAILING dir={self.direction_after_break}")

    # ----------------------------
    # Phase 5: Trailing stop (bar-based stop modifications)
    # ----------------------------
    def _place_or_reset_trailing(self, bar: Bar):
        assert self.direction_after_break in ("long", "short")
        qty = self.long_leg.qty if self.long_leg else 0.0
        qty = float(qty)

        if self.direction_after_break == "long":
            self.highest_since_break = bar.high if self.highest_since_break is None else max(self.highest_since_break, bar.high)
            stop_px = round_price(self.highest_since_break * (1.0 - SLA_PCT))
            # remaining exposure is long => protective SELL stop
            self.trailing_trade = self.exec.place(
                StopOrder("SELL", qty, stop_px),
                f"{ORDERREF_PREFIX}-C{self.cycle_id}-TRAIL-SELL",
            )
            self.trailing_stop_px = stop_px
            print(f"[TRAIL_INIT] LONG stop={stop_px:.5f} (highest={self.highest_since_break:.5f})")

        else:
            self.lowest_since_break = bar.low if self.lowest_since_break is None else min(self.lowest_since_break, bar.low)
            stop_px = round_price(self.lowest_since_break * (1.0 + SLA_PCT))
            # remaining exposure is short => protective BUY stop
            self.trailing_trade = self.exec.place(
                StopOrder("BUY", qty, stop_px),
                f"{ORDERREF_PREFIX}-C{self.cycle_id}-TRAIL-BUY",
            )
            self.trailing_stop_px = stop_px
            print(f"[TRAIL_INIT] SHORT stop={stop_px:.5f} (lowest={self.lowest_since_break:.5f})")

    def _handle_trailing(self, bar: Bar):
        if self.trailing_trade is None or self.direction_after_break not in ("long", "short"):
            self._fault("TRAILING state but no trailing_trade/direction")
            return

        st = (self.trailing_trade.orderStatus.status or "UNKNOWN").upper()
        if st == "FILLED":
            print("[TRAIL_EXIT] trailing stop filled -> COOLDOWN")
            self._enter_cooldown()
            return
        if st in ("CANCELLED", "INACTIVE"):
            # Try to re-place once (safest: flatten, but for prototype we re-place)
            print(f"[TRAIL_WARN] trailing order {st}; re-place trailing")
            self._place_or_reset_trailing(bar)
            return

        # Update high/low
        if self.direction_after_break == "long":
            self.highest_since_break = max(self.highest_since_break or bar.high, bar.high)
            new_stop = round_price(self.highest_since_break * (1.0 - SLA_PCT))

            # Only tighten (for long, stop should move UP)
            if self.trailing_stop_px is None or new_stop > self.trailing_stop_px:
                # threshold gate
                moved = abs(new_stop - (self.trailing_stop_px or new_stop)) / max(new_stop, 1e-9)
                if moved >= TRAIL_MODIFY_THRESHOLD_PCT:
                    ok = self.exec.modify_stop_price(self.trailing_trade, new_stop)
                    if ok:
                        self.trailing_stop_px = new_stop
                        print(f"[TRAIL_MOD] LONG new_stop={new_stop:.5f} (highest={self.highest_since_break:.5f})")
                    else:
                        # fallback: cancel+replace (throttled)
                        self.exec.cancel(self.trailing_trade)
                        self.trailing_trade = self.exec.place(
                            StopOrder("SELL", self.long_leg.qty if self.long_leg else 0.0, new_stop),
                            f"{ORDERREF_PREFIX}-C{self.cycle_id}-TRAIL-SELL-R",
                        )
                        self.trailing_stop_px = new_stop
                        print(f"[TRAIL_REPLACE] LONG new_stop={new_stop:.5f}")

        else:
            self.lowest_since_break = min(self.lowest_since_break or bar.low, bar.low)
            new_stop = round_price(self.lowest_since_break * (1.0 + SLA_PCT))

            # Only tighten (for short, stop should move DOWN)
            if self.trailing_stop_px is None or new_stop < self.trailing_stop_px:
                moved = abs(new_stop - (self.trailing_stop_px or new_stop)) / max(new_stop, 1e-9)
                if moved >= TRAIL_MODIFY_THRESHOLD_PCT:
                    ok = self.exec.modify_stop_price(self.trailing_trade, new_stop)
                    if ok:
                        self.trailing_stop_px = new_stop
                        print(f"[TRAIL_MOD] SHORT new_stop={new_stop:.5f} (lowest={self.lowest_since_break:.5f})")
                    else:
                        self.exec.cancel(self.trailing_trade)
                        self.trailing_trade = self.exec.place(
                            StopOrder("BUY", self.short_leg.qty if self.short_leg else 0.0, new_stop),
                            f"{ORDERREF_PREFIX}-C{self.cycle_id}-TRAIL-BUY-R",
                        )
                        self.trailing_stop_px = new_stop
                        print(f"[TRAIL_REPLACE] SHORT new_stop={new_stop:.5f}")

    # ----------------------------
    # Common transitions
    # ----------------------------
    def _enter_cooldown(self):
        self.state = "COOLDOWN"
        self.cooldown_left = COOLDOWN_BARS
        self.entry_start_bar = None
        self.requote_count = 0
        self.last_requote_bar = None
        self.direction_after_break = None
        self.highest_since_break = None
        self.lowest_since_break = None
        self.trailing_trade = None
        self.trailing_stop_px = None
        self.long_leg = None
        self.short_leg = None
        print(f"[STATE] -> COOLDOWN bars={self.cooldown_left}")

    def _fault(self, reason: str):
        self.state = "FAULT"
        self.fault_reason = reason
        print(f"[STATE] -> FAULT reason={reason}")


# ============================================================
# MAIN
# ============================================================
def main():
    mgr = IBClientManager(HOST, PORT, CLIENT_ID)
    last_heartbeat = time.time()

    # initial connect
    mgr.ensure_connected()
    if mgr.degraded:
        print("[MAIN] could not connect; exiting.")
        return

    strat = HedgeFXStrategy(mgr)
    strat.start()

    print("[MAIN] running... (Ctrl+C to stop)")
    try:
        while True:
            print(f"[MAIN] tick={time.time()}")
            # keep connection alive / attempt reconnect
            ok = mgr.ensure_connected()
            print(f"[MAIN] connected={mgr.ib.isConnected()} degraded={mgr.degraded}")
            if ok and not strat.md.ticker:
                # after reconnect, resubscribe
                try:
                    strat.ib.qualifyContracts(strat.contract)
                    strat.md.subscribe()
                except Exception as e:
                    print(f"[MAIN] resubscribe failed: {e}")

            # process incoming updates (drives ticker bid/ask)
            got_update = mgr.ib.waitOnUpdate(timeout=1.0)
            print(f"[MAIN] got_update={got_update}")
            # 👇 ADD THIS HEARTBEAT
            if time.time() - last_heartbeat >= 5:
                print(
                    f"[HEARTBEAT] connected={mgr.ib.isConnected()} "
                    f"degraded={mgr.degraded} "
                    f"got_update={got_update} "
                    f"mid={strat.md.last_mid}"
                )
                last_heartbeat = time.time()

            # poll market data; if a bar completed, run strategy
            bar = strat.md.poll()
            if bar is not None:
                strat.on_bar(bar)

    except KeyboardInterrupt:
        print("\n[MAIN] stopping...")

    finally:
        try:
            mgr.ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    # If you run inside a Jupyter notebook, uncomment:
    # util.startLoop()
    main()
