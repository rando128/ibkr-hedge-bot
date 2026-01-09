# Refactor PRD (trading system)

## Goals
- Simplify the trading runtime dramatically (ditch the current over-engineered worker/state/job approach).
- Keep only the truly real-time requirement: when a stop loss (SL) fills, flip the remaining protection to a trailing order ASAP.
- Make the system resilient to Django/procrastinate crashes and TWS disconnects/restarts.
- Minimize background jobs and synchronization complexity.
- We do not need to preserve existing DB data: we can reset the schema and start fresh.

## Conventions (locked)
- Percentage fields use decimal ratios: `0.02 == 2%` (not `2 == 2%`).
- “Reasonable time (30s)” is measured from the most recent of:
  - last entry fill time, or
  - last SL placement/update time
  (i.e., from `last_activity_at`).
- Profitability optimization on recovery:
  - If TWS can provide current unrealized P&L for the open position, use it.
  - If not available (no market data / no P&L API), do not attempt the optimization: go `PANIC`.
- Retry policy: default **3 retries** for any side-effecting action (cancel SL, place trailing, panic flatten, etc.). After retries are exhausted, mark cycle `ERROR` and transition to `PANIC` (unless TWS connectivity is down, see below).

## Strategy reminder (core)
- Place one long and one short entry.
- Place one SL for each leg.
- On first SL execution, cancel the remaining SL and replace it with a trailing order to capture profit.

## Cycle phases vs states (locked)
To avoid confusion:
- A **phase** is a higher-level grouping (hedge activation, profit activation, pnl calculation).
- A **state** is the persisted `Cycle.state` value used for transitions, admin visibility, and recovery.

### Phases (conceptual only)
- `HEDGE_ACTIVATION` phase: cycle is getting into “both entries + both SL protections placed”.
- `PROFIT_ACTIVATION` phase: cycle is switching the surviving leg protection to trailing and waiting for exit.
- `PNL_CALCULATION` phase: compute final realized P&L and commissions from TWS history.

### States (`Cycle.state`, persisted)
We keep (and slightly extend) the existing status vocabulary because it maps well to operational transitions:
- `INITIALIZING`: cycle created, preflight checks, preparing orders.
- `ENTERING`: placing entries and establishing protection.
  - Entries: place long/short entries and wait for fills (partial fills allowed).
  - Protection: place SL protection orders as soon as possible and regardless of partial vs full fills (i.e., do not wait for full fills to place SL).
- `ACTIVE`: hedge is active; both SL protections are in place.
- `TRANSITIONING`: an SL was hit; cancel remaining SL and place trailing (this is the only “ASAP” path).
- `TRAILING`: trailing order is active; waiting for final exit.
- `RECOVERING`: bot/agent restarted or TWS state ambiguous; reconciling from TWS snapshots.
- `PANIC`: emergency exit/cancel; flatten positions and cancel orders.
- `PNL_CALCULATION`: computing final realized P&L and commissions.
- `COMPLETED`: cycle closed and pnl recorded.
- `ABORTED`: cycle intentionally aborted (operator action) with cleanup performed.
- `ERROR`: unrecoverable internal error (should generally lead to `PANIC` unless TWS connectivity is down).

### Phase ↔ state mapping (for operator mental model)
- `HEDGE_ACTIVATION` phase == `INITIALIZING` → `ENTERING` → `ACTIVE`
- `PROFIT_ACTIVATION` phase == `TRANSITIONING` → `TRAILING`
- `PNL_CALCULATION` phase == `PNL_CALCULATION` → `COMPLETED`

## Operational requirements (locked)
- Start bot: hedge activation must execute immediately (no “wait 30s” to place orders).
- Stop bot gracefully: finish the current cycle (including taking profit) and do not start a new cycle.
- Restart safety: after a crash/restart, the bot should enter `RECOVERING` quickly and converge to the correct state.
- Observability:
  - State transitions must be visible in Django admin for each bot/cycle.
  - Operators must be able to access per-bot logs quickly (admin “log” link/button).

## Architecture (Option B: major simplification)
### Overview
Split responsibilities into:
1) **TWS Agent (single always-on process)**:
   - One IB connection used for:
     - placing/canceling orders,
     - receiving callbacks (we only “react” to SL fills in real-time),
     - periodic reconciliation snapshots (open orders, positions).
   - Owns all interaction with TWS to maximize resilience and avoid multi-client races.

2) **Django/Procrastinate orchestration** (minimum jobs):
   - A single periodic job every 30s that enqueues reconciliation for all running bots.
   - Immediate “start bot” and “stop bot” operations that only mutate DB state and enqueue a command for the agent.

### Why a dedicated agent process
Procrastinate tasks are not a great fit for an always-on callback listener that must keep an IB connection open while also servicing requests. A dedicated agent process (implemented as a Django management command) is the simplest resilient approach and keeps Procrastinate usage minimal and predictable.

## TWS synchronization rules
### Source of truth
- Django is the source of truth for the **cycle state**.
- TWS is the source of truth for **orders/positions/executions**.
- The reconciler periodically reads TWS snapshots and updates Django state accordingly.

### Event handling (real-time)
- The agent subscribes to IB callbacks and only reacts to “SL filled” events.
- On “SL filled”:
  - cancel the remaining SL,
  - place trailing order for the surviving leg,
  - update cycle state to `PROFIT_ACTIVATION`,
  - write a state transition record (admin-visible).

### Mapping IB events to a bot/cycle (resilience requirement)
We must be resilient to restarts and to multiple concurrent bots.

Canonical mapping (locked):
- Every order placed by the agent MUST include a deterministic `orderRef` encoding:
  - `bot_id`
  - `cycle_key` (UUID)
  - `role` (e.g., `LONG_ENTRY`, `SHORT_SL`, `LONG_TRAIL`, ...)
  - example format: `HB:{bot_id}:{cycle_uuid}:{role}`
- On restart, the agent reconstructs context purely from TWS open orders + `orderRef` parsing; it must not depend on in-memory state.
- Reconciliation should primarily key off `orderRef`; `orderId` is treated as ephemeral (changes across new orders) while `orderRef` is stable.

## Recovery and resilience (locked behaviors)
### If Django/procrastinate crashes or restarts
- If trailing is already active in TWS (`PROFIT_ACTIVATION` achieved), we are safe; we just reconcile and compute final P&L when the position closes.
- If hedge activation is incomplete (only one entry filled, or SL protections incomplete) at recovery time:
  - transition to `PANIC` and force exits/cancel.
- If hedge activation completed but flip-to-trailing did not occur:
  - If TWS can provide unrealized P&L for the surviving position and it is favorable, we may still flip.
  - Otherwise, go `PANIC`.

### If TWS connectivity is down
- The system should NOT spam failing actions.
- Actions that require TWS must retry (3 times with backoff); if connectivity is down, we keep the cycle in `RECOVERING` (or `ERROR`) and require operator intervention if it doesn’t recover.

## Safety timers
We maintain `last_activity_at` for each cycle:
- Update it on:
  - any entry fill,
  - any SL placement/replace,
  - any trailing placement.
- If hedge activation is not fully completed within 30s of `last_activity_at`, go `PANIC`.

## Data model (fresh schema; no backward compatibility)
We are allowed to reset DB tables.

Recommended minimal persistent models:
- `Bot`:
  - configuration (symbol, accounts, qty, stop_pct, trailing_pct, etc.)
  - desired status: `RUNNING` / `STOPPING` / `STOPPED` / `ERROR`
- `Cycle`:
  - `uuid` (cycle_key for orderRef)
  - `state` (the states defined above)
  - timestamps: `started_at`, `last_activity_at`, `completed_at`
  - result fields: realized P&L, commissions, net P&L (final)
  - minimal reconciliation hints (optional): contract `conId`
- `Event` (append-only, admin-friendly):
  - includes state transitions + operational logs (start/stop/panic/recovery actions).
  - this replaces the “JSON log” idea for easier filtering/debugging in admin.

We explicitly drop the existing `Order`/`Execution` persistence unless we later prove it is necessary (we can compute P&L from TWS history instead).

## Retry policy (locked)
- Each external side-effect action uses:
  - `max_attempts=3`,
  - exponential backoff (e.g., 0.5s, 1s, 2s),
  - “give up” transitions the cycle to `ERROR` then `PANIC` (unless connectivity down).

## Procrastinate jobs (minimum)
- `reconcile_running_bots` (periodic, every 30s): finds running bots and enqueues reconciliation commands for the agent.
- `request_start_bot(bot_id)` (immediate): sets bot desired status and enqueues “start cycle now” command.
- `request_stop_bot(bot_id)` (immediate): sets bot desired status to stop-after-cycle and enqueues a “do not start next cycle” command.

The agent process is responsible for consuming commands and doing the IB work.

## Implementation plan (Option B)
This is the concrete implementation breakdown for the refactor. It assumes **no DB/data preservation** and prioritizes shipping a simpler, resilient core first.

### Phase 0 — Lock the PRD (this doc)
- Confirm the state machine and timer rule (`last_activity_at`).
- Confirm transition logging: use the `Event` table.

### Phase 1 — Reset the schema (no data preservation)
- Replace the trading app models with the new minimal schema (`Bot`, `Cycle`, and chosen transition log).
- Create new migrations that drop old tables and create new tables (we can squash/recreate since data is disposable).
- Update admin screens to match the new models and keep operator UX (start/stop/panic + logs).

### Phase 2 — Implement the TWS Agent (always-on)
- Add a Django management command (e.g., `manage.py tws_agent`) that:
  - connects to TWS,
  - subscribes to callbacks,
  - reconstructs context from TWS open orders on startup,
  - processes only “SL filled” events in real-time (flip-to-trailing),
  - polls/consumes DB commands (start cycle, panic, reconcile bot).
- Define command format and idempotency keys (e.g., `(bot_id, cycle_uuid, command_type)`).

### Phase 3 — Minimal Procrastinate orchestration
- Add `reconcile_running_bots` periodic task (every 30s) that queues reconcile commands.
- Add `request_start_bot`/`request_stop_bot` tasks used by admin actions (immediate effect).
- Ensure no long-running per-bot workers remain.

### Phase 4 — Trading flow implementation (agent-side)
- `start_cycle_now`:
  - create cycle UUID,
  - place both entries,
  - place both SL protections during `ENTERING` (partial fills allowed; do not wait for full fills),
  - update `last_activity_at` on each fill/SL placement,
  - transition cycle → `HEDGE_ACTIVATION`.
- `on_sl_filled`:
  - cancel remaining SL (3 retries),
  - place trailing order (3 retries),
  - transition → `PROFIT_ACTIVATION`.
- `reconcile_bot`:
  - read TWS snapshots (positions/open orders),
  - infer best state, apply safety timer, and trigger `PANIC` when required.
- `pnl_calculation`:
  - compute realized P&L + commissions from TWS order/execution history for the cycle’s `orderRef` prefix.

### Phase 5 — Rollout safety
- Add a “dry-run reconcile” mode (log-only) to compare inferred state vs DB state before enabling writes.
- Add a hard “kill switch” in admin (force `PANIC` / force stop) that works even if a cycle is mid-flight.
