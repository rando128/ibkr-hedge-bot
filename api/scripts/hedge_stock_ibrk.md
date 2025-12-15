# Hedging Strategy — Unified State Machine & Checklist
**IBKR Live & Paper Trading (Bracket-Based, Event-Driven)**

It’s a low-frequency volatility capture strategy with insurance symmetry (A volatility-triggered directional capture strategy with temporary neutrality.)

> **Principle**
> - IBKR paper trading uses **live execution logic**.
> - **Broker events are authoritative** (orders, fills, positions).
> - **No bar-based stop inference**, no tie-breaker logic.
> - Paper and live share **identical code paths**.

---

## 1. State Machine

### State Diagram

┌──────────┐
│ IDLE │
└────┬─────┘
│ entry conditions met
▼
┌──────────┐
│ ENTERING │
└────┬─────┘
│ both brackets live & parents filled
▼
┌──────────┐
│ HEDGED │
└────┬─────┘
│ IBKR stop execution (paper/live)
▼
┌────────────┐
│ SINGLE_LEG │
└────┬───────┘
│ trailing stop execution
▼
┌──────────┐
│ COOLDOWN │
└────┬─────┘
│ delay elapsed
▼
┌──────────┐
│ IDLE │
└──────────┘

yaml
Copy code

---

### State Definitions

#### `IDLE`
- No positions
- No open orders

**Transition → `ENTERING`**
- Trading window open
- Cooldown elapsed
- Account reconciled (positions = 0, open orders = 0)

---

#### `ENTERING`
- Create hedge using **two independent bracket orders**

**Actions (atomic)**
- **Long Bracket**
  - Parent: BUY market
  - Child: SELL stop @ `entry * (1 - slPct)`
- **Short Bracket**
  - Parent: SELL market
  - Child: BUY stop @ `entry * (1 + slPct)`

**Rules**
- Entry + SL submitted together
- No naked exposure possible

**Failure Handling**
- If any bracket fails or partial:
  - Cancel everything
  - Return to `IDLE`

**Transition → `HEDGED`**
- Both parents filled
- Both SL children acknowledged live by IBKR

---

#### `HEDGED`
- Long and short both open
- Initial SLs fully broker-managed

**Monitoring**
- Listen to IBKR:
  - Order status events
  - Execution reports
  - Position updates

**Hedge Break**
- One SL executes at IBKR (paper or live)

**Paper/Live Guard**
- Immediately **reconcile**:
  - Query positions
  - Query open orders

**Transition → `SINGLE_LEG`**
- Exactly one position remains open

---

#### `SINGLE_LEG`
- One directional leg remains

**Actions**
- Cancel remaining leg’s **initial SL**
- Submit **trailing stop** for surviving leg

**Trailing**
- Long: trail below highest favorable price
- Short: trail above lowest favorable price
- Update once per bar (non-time-critical)

**Exit**
- Trailing stop filled by IBKR

**Paper/Live Guard**
- Re-query positions to confirm flat

**Transition → `COOLDOWN`**

---

#### `COOLDOWN`
- Cycle finished
- Re-entry blocked

**Transition → `IDLE`**
- Cooldown time/bars elapsed
- Account confirmed flat

---

## 2. Execution Checklist (Paper = Live)

### 2.1 Startup / Mode Declaration
- [ ] `mode ∈ { PAPER, LIVE }` set and logged
- [ ] Connected to correct IBKR endpoint
- [ ] Strategy logic identical for both modes

---

### 2.2 Pre-Trade Checks

**Connectivity & Account**
- [ ] IBKR Gateway / TWS connected
- [ ] US stocks enabled
- [ ] Short selling enabled
- [ ] Fractional shares enabled
- [ ] No manual positions on symbol

**Instrument Validation**
- [ ] Contract resolved (SMART / USD)
- [ ] Shortable flag = true
- [ ] Trading hours loaded (RTH vs ETH)

**Strategy Parameters**
- [ ] `notionalPerLeg = $100`
- [ ] `slPct = 3–4%`
- [ ] `trailingPct` defined
- [ ] `barSize = 1m`
- [ ] `cooldown` defined
- [ ] `sessionMode ∈ { RTH_ONLY, OVERNIGHT_ALLOWED }`

---

### 2.3 Entry (`IDLE → ENTERING`)

**Preconditions**
- [ ] State = `IDLE`
- [ ] Positions = 0
- [ ] Open orders = 0
- [ ] Trading window open
- [ ] Cooldown elapsed

**Bracket Construction**

_Long Bracket_
- [ ] BUY market
- [ ] Qty = `100 / last_price`
- [ ] Attached SELL stop @ `entry * (1 - slPct)`

_Short Bracket_
- [ ] SELL market
- [ ] Qty = `100 / last_price`
- [ ] Attached BUY stop @ `entry * (1 + slPct)`

**Submission Validation**
- [ ] Both parents filled
- [ ] Both SL children live

**Failure**
- [ ] Cancel all orders
- [ ] Return to `IDLE`

---

### 2.4 Hedge Monitoring (`HEDGED`)
- [ ] Listen for IBKR execution events
- [ ] Detect SL execution on one leg
- [ ] **Immediately reconcile** positions & open orders
- [ ] Confirm exactly one position remains

> No bar-based SL logic
> No tie-breaker (paper = live)

---

### 2.5 Hedge Break Handling (`HEDGED → SINGLE_LEG`)
- [ ] Identify surviving leg
- [ ] Cancel its **initial SL**
- [ ] Submit **trailing stop**

---

### 2.6 Trailing Phase (`SINGLE_LEG`)
- [ ] Update favorable extreme per bar
- [ ] Adjust trailing stop (one direction only)
- [ ] Trailing stop filled
- [ ] **Re-query positions**
- [ ] Confirm flat
- [ ] Cancel residual orders

---

### 2.7 Session Handling

**RTH_ONLY**
- [ ] At session end:
  - Close any open positions
  - Cancel all orders
  - Reconcile
  - Reset state

**OVERNIGHT_ALLOWED**
- [ ] Accept gap risk
- [ ] Resume next session normally

---

### 2.8 Safety Rules (Always On)
- [ ] Never infer stops from bars
- [ ] Never override broker executions
- [ ] Always reconcile after:
  - Stop fills
  - Trailing fills
  - Reconnects
- [ ] Paper trading ≠ backtesting (same execution rules)

---

### 2.9 Post-Cycle Accounting
- [ ] Record entry fills
- [ ] Record stop / trailing fills
- [ ] Compute PnL per leg and net
- [ ] Tag outcome: long-win / short-win / forced-exit
- [ ] Mark mode: PAPER or LIVE

---

## 4. Summary
- Bracket-based entry with attached SLs
- IBKR is the single source of truth
- Paper trading uses live logic
- Deterministic, safe, and go-live ready


# Failure-Mode Matrix
**IBKR Live & Paper Trading (Event-Driven, Bracket-Based)**

> Principle:
> **Broker state is authoritative.**
> On any anomaly → *freeze, reconcile, then decide.*

---

## 3. Connectivity Failures

### A. Temporary Disconnect (TWS / Gateway)

| Situation | Detection | Immediate Action | Recovery |
|---------|----------|------------------|----------|
| Disconnect while `IDLE` | API disconnect event | Freeze | Reconnect → resume |
| Disconnect while `ENTERING` | Disconnect before both parents filled | Freeze | Reconnect → cancel all orders → `IDLE` |
| Disconnect while `HEDGED` | Disconnect with 2 legs open | Freeze | Reconnect → reconcile positions & orders |
| Disconnect while `SINGLE_LEG` | Disconnect with trailing stop live | Freeze | Reconnect → verify trailing stop exists |

**Rules**
- Never assume orders failed or filled during disconnect
- Always re-query:
  - Positions
  - Open orders
  - Executions

---

### B. Reconnect with Unexpected State

| Observed State | Expected | Resolution |
|---------------|----------|-----------|
| 0 positions, no orders | Any active state | Reset → `IDLE` |
| 1 position, no stop | `SINGLE_LEG` | Immediately submit trailing stop |
| 2 positions, no SLs | `HEDGED` | Emergency close both legs |
| Orders exist, no positions | Any | Cancel orders → reconcile |

---

## 2. Partial Fills & Order Anomalies

### A. Parent Order Partial Fill

| Scenario | Action |
|--------|--------|
| Long parent partial, short not filled | Cancel both brackets → `IDLE` |
| Short parent partial, long not filled | Cancel both brackets → `IDLE` |
| Both parents partially filled | Cancel everything → flatten → `IDLE` |

**Rule**
- Strategy **never allows partial hedges**
- Symmetry is mandatory

---

### B. Child Stop Rejected or Missing

| Scenario | Action |
|--------|--------|
| SL rejected on one leg | Cancel entire hedge → flatten |
| SL not acknowledged | Cancel parent (if possible) |
| SL disappears unexpectedly | Immediate market close |

---

## 3. Hedge-Break Edge Cases

### A. Both Stops Trigger (Extreme Move)

| Outcome | Action |
|-------|--------|
| Both legs closed | Accept full loss → `COOLDOWN` |
| One leg remains | Treat as normal hedge break |
| Broker order sequencing unclear | Reconcile → trust final positions |

**Rule**
- Do **not** attempt reconstruction
- Final positions = truth

---

### B. Stop Executes but Position Still Shows Open (Paper Lag)

| Detection | Action |
|---------|--------|
| Stop filled, position still visible | Wait → re-query |
| Position persists after retry | Market close immediately |

---

## 4. Trailing Stop Failures

### A. Trailing Stop Rejected

| Scenario | Action |
|--------|--------|
| Rejected on submit | Retry once |
| Still rejected | Market close |
| API error | Flatten |

---

### B. Trailing Stop Missing After Reconnect

| Scenario | Action |
|--------|--------|
| Position open, no trailing stop | Re-submit trailing stop immediately |
| Cannot confirm | Market close |

---

## 5. Session Edge Cases (Stocks)

### A. Session End (RTH_ONLY)

| State | Action |
|-----|--------|
| `ENTERING` | Cancel orders |
| `HEDGED` | Market close both legs |
| `SINGLE_LEG` | Market close remaining |
| `COOLDOWN` | No action |

---

### B. Market Halt / LULD

| Detection | Action |
|---------|--------|
| Halt during `HEDGED` | Freeze |
| Halt during `SINGLE_LEG` | Freeze |
| Resume | Reconcile → re-apply stops if needed |

---

## 6. Paper-Trading Specific Quirks

| Issue | Mitigation |
|-----|-----------|
| Event lag | Always reconcile after executions |
| Idealized fills | Do not rely on fill quality |
| Borrow always available | Still validate short leg exists |

---

## 7. Global Invariants (Never Violated)

- ❌ Never hold unprotected positions
- ❌ Never infer execution order from bars
- ❌ Never continue with partial hedge
- ✅ Always reconcile after anomalies
- ✅ Flatten if state is ambiguous

---

## 8. Escalation Rule (Simple & Safe)

> **If the bot cannot prove the current state is safe → flatten everything.**

Capital preservation > strategy purity.

---

## Final Note

This matrix completes the system.

You now have:
- ✅ State machine
- ✅ Execution checklist
- ✅ Paper = live parity
- ✅ **Failure-mode matrix**

This is now **production-grade logic**, not just a strategy idea.

If you want next:
- Convert this matrix into **code-level guards**
- Add **unit-test scenarios per failure**
- Produce a **one-page “kill-switch policy”**

Just say the word.
