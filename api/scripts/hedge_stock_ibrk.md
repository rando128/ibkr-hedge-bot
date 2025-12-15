# Long/Short Hedging Strategy — Product Specification (IBKR Stocks) v2

## 0. Scope and Authority

This specification defines a **broker-executed, event-driven** hedging strategy for **stocks on IBKR** (paper trading and live trading share the same logic).

**Authority order (truth sources):**
1) **Executions** (fills) — primary truth
2) **Open orders** — validate protection and working exits
3) **Positions()** — sanity check only (stocks are netted)

**Hard invariant:**
If the system cannot prove that exposure is protected and quantities are known, it must **cancel orders and flatten**.

---

## 1. IBKR Equity Netting Constraint (Critical)

IBKR equities are **netted per symbol** at the account level.

- The strategy must **not** assume that long and short legs appear as distinct open positions.
- Strategy state and remaining quantities must be derived from an **execution ledger** plus open orders.
- `positions()` is used only to detect accidental leftover exposure (e.g., net != 0 when strategy expects flat).

---

## 2. Parameters (Configurable)

- `symbol` — stock symbol (default: NVDA)
- `notionalPerLegUSD` — notional per leg (default: 100 USD)
- `slPct` — initial stop-loss percentage (default: 0.03)
- `trailingPct` — trailing percentage for the surviving exposure (default: 0.15)
- `barSize` — cooldown cadence (default: 1 minute bars)
- `cooldownBars` — delay between cycles (default: 1 bar)
- `sessionMode` — **OVERNIGHT_ALLOWED** (no forced flat)
- IBKR connectivity: `host`, `port`, `clientId` (clientId required, any unique integer)

---

## 3. Strategy Cycle Overview

A full cycle has three phases:

1. **Entry Phase** — establish both legs as independent bracket orders (each leg has attached SL)
2. **Hedge Break Phase** — one initial SL gets any fill; the hedge is broken
3. **Final Exit Phase** — replace remaining initial SL with a trailing stop; cycle ends when trailing stop fills

---

## 4. Order Model

### 4.1 Leg Bracket (Per Leg)

Each leg is a **bracket-like chain**:

- Parent: Market order (transmit = False)
- Child: Stop order (transmit = True, parentId = parent.orderId)

This guarantees **atomicity within a leg** (parent + SL submitted together).

### 4.2 Cross-Leg Atomicity Limit

IBKR cannot guarantee a single atomic transaction across **two independent parents**.
Therefore the system must implement **Two-Phase Entry**.

---

## 5. State Machine

States:

- `IDLE` — no active cycle
- `ENTERING` — submitting legs and confirming stop protection
- `HEDGED` — both initial stops live; waiting for hedge break via broker execution
- `SINGLE_LEG` — hedge broken; trailing stop manages remaining exposure
- `COOLDOWN` — wait N bars before next cycle
- `DEGRADED` — disconnect/unsafe; freeze, reconcile, and possibly flatten

---

## 6. Entry Phase (Two-Phase Entry)

### 6.1 Two-Phase Entry Procedure

1) Submit **Leg A bracket** (parent + SL)
2) Wait for **Leg A SL to be LIVE**
3) If Leg A parent fills before SL is LIVE → **abort + flatten**
4) Submit **Leg B bracket**
5) Wait for **Leg B SL to be LIVE**
6) If any parent fills while its SL is not LIVE → **abort + flatten**
7) If both SLs are LIVE → transition to `HEDGED`

### 6.2 Definition: “SL Live”

An SL is considered **LIVE** only if order status is:

- `PreSubmitted` or `Submitted`

Not live:
- `PendingSubmit`, `PendingCancel`, or any error-like status

Terminal-bad:
- `Rejected` (always triggers abort/flatten)

---

## 7. Partial Fill Policy

### 7.1 During ENTERING

- Partial hedge is forbidden.
- If one parent has any fill while the other does not fill within a short grace window → **cancel + flatten**.

### 7.2 During HEDGED

- A hedge break occurs when **any initial SL has any fill** (`filled > 0`), even partial.
- If **both** initial SLs have any fill (extreme move) and ordering cannot be proven safe → **flatten**.

---

## 8. Hedge Break Logic (Execution-Ledger Driven)

### 8.1 Execution Ledger

Maintain cumulative fills:

- `parentFilledQty[LONG|SHORT]`
- `slFilledQty[LONG|SHORT]`

### 8.2 Determine Loser/Survivor

- Loser = leg whose initial SL has any fill
- Survivor = opposite leg

If loser cannot be determined unambiguously → flatten.

### 8.3 Remaining Quantity

Remaining survivor exposure:

remainingQty = parentFilledQty[survivor] - slFilledQty[survivor]

yaml
Copy code

If remainingQty <= 0 or ambiguous → flatten.

---

## 9. Replace Protection Safely (Cancel-Then-Replace)

On hedge break:

1) Cancel the remaining initial SL (the survivor leg’s initial SL)
2) Await terminal cancel status (timeout)
3) If cancellation does not complete in time → flatten
4) Submit trailing stop sized to `remainingQty`
5) Confirm trailing stop is LIVE; if rejected/inactive:
   - retry once
   - if still not live → flatten

---

## 10. Trailing Stop (Final Exit)

Trailing stop is placed broker-side:

- `orderType="TRAIL"`
- `trailingPercent = trailingPct * 100`

Final exit when trailing stop fills → cycle ends → enter `COOLDOWN`.

---

## 11. Disconnect and Safety Rules

### 11.1 On Disconnect

- Enter `DEGRADED`
- Stop placing/modifying orders

### 11.2 On Reconnect

- Reconcile open orders and net position
- If any ambiguity remains → flatten

### 11.3 Global Invariant: Flatten If Ambiguous

At any point if the system cannot prove:
- exposure is protected, and
- quantities are known, and
- order state is safe,

then it must:
- cancel all orders for the symbol
- market-flatten any residual net position
- enter `COOLDOWN`

Capital preservation overrides continuity.

---

## 12. Paper Trading

Paper trading must use the **same live execution logic**:

- no bar-inferred stop logic
- no tie-breakers
- broker events and the ledger are authoritative

Minor paper quirks (lag/idealized fills) are handled by:
- ledger-driven state
- short non-blocking waits
- flatten-if-ambiguous invariant
