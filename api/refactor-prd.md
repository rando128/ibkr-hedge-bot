# Current situation
While the bot_runner.py strategy logic is propery working, I'm facing a lot of unstability due to an overly complexy mode/state/worker/job implementation. So I want to simplify the implementation, reduce the synchronisation complexity between django and TWS and limit the number of jobs to the minimum.

# Core logic reminder
In essence the hedge strategy is simple:
- place one long and one short order
- associate stop loss to each
- on first SL executed, flip the SL of the remaining leg to a trailing order to take as much profit as possible.

## Cycle state management
From a state and real-time management standpoint, what matters is only to make sure we cancel the remaining SL and make it a trailing order.
All the rest doesn't really require true real-time.

In terms of strategy execution, for each executed cyle, we can define 3 phases:
- Hedge activation: place short/long orders (we accept the spread) + SL protections placed
- Profit activation: SL was successfully flipped in a trailing order and we wait for benefits to be taken
- P&L calculation: based on TWS orders history (of this cycle and contract), compute the total commissions and final P&L

A fourth state is PANIC to exit from any open positions or pending orders:
- If the SL protection is not fully completed (both legs) after a reasonable time (30s) or only of SL remains and we are losing money (see resilience handling below), then we should go in panic mode (closing any open positions of contract's cycle).
- or if the admin requests PANIC (from the django admin)

A fifth state is RECOVERING state whenever something went wrong from a strategy execution standpoint and we are trying to fix it (see next section).


## Problems resilience

Let's discuss cases where django/procrastinate crashs or TWS connection is lost.

As long as profit activation is completed on TWS, we don't care what happens on django bot/workers/procrastinate/connection. We can even shutdown django without any issue. Once problem disappears (django restarted, connection to TWS reestablished), we simply need to check on TWS how the trailing order resolved and compute P&L.

If shit happens on django/TWS while hedge activation is still in process (only one entry order gets filled, or one SL is placed), when we recover, we go in panic state (forced exits)

If only hedge activation got completed on TWS at the time of the issue, it won't cause too much losses (thx to the dual stop loses - obviously not the best scenario, but not a disaster). But we can recovery smartly by detecting from TWS orders history:
	- if both SL are still active, great. We keep waiting for one leg SL to execute (we stay in hedge activation state)
	- if one SL got executed and we couldn't flip the other leg to trail order (django was down, django-TWS connectivity was down...), the price might have already changed making the remaining leg not profitable, in which case we go in panic to exit ASAP. If there is profit (computed by a price difference without commissions to simplify the logic), we flip the SL to trail to go in profit activation
	- if both SL got executed, worse scenario: we dictate cycle is closed and compute the negative P&L (moving effectively to P&L calculation state)

## Partial fills handling
We place the SL protection independently of whether the leg is partially or totally filled.

## Operational aspects
- As a bot manager, I want to be able to start a bot (resuming the existing bot model) and see the hedge activation state getting executed IMMEDIATELY (or near real-time, i.e. no wait for 30s for long/short orders to be placed)
- As a bot manager, I want to be able to stop a bot gracefully so once it completes its current cycle (and take profit), no new cycle is started
- As a devops, I want to be able to kill and restart my server (or django processes) and see immediately the RECOVERING state, which gets updated as soon as recovery logic happens
- As devops and bot manager, I want to access logs/traces in django admin of any state transitions to understand the cycle execution timeline.
- As devops and bot manager, I can easily access log filtered by bot from a log button in the bot admin


# Implementation guidelines
I propose the following approach:

## Models
2 models only:
- Bot with the current model (without idle state)
- Cycle with the current model

## TWS synchronisation
As learned from the previous implementation, we need to use a a global callback listener (to be resilient to django/TWS restarts), but now we ONLY process Stop-loss callbacks to flip the trail ASAP. We silently discard any other callbacks.

The source of truth of state management is django. The source of truth for orders status is TWS. We periodically fetch from TWS open positions and pending orders (every 30s) to determine in which state we are now (for each bot).
