import asyncio
import argparse
import math
from datetime import datetime, timezone
from ib_insync import IB, Stock, Crypto, MarketOrder, StopOrder, Order, TagValue, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

# Global state to manage the legs, P&L, and order roles
active_trades = {'long': None, 'short': None}
order_role_map = {} # Maps orderId -> "LONG_ENTRY", "SHORT_SL", etc.

pnl_stats = {
    'total_buys': 0.0,
    'total_sells': 0.0,
    'total_commission': 0.0,
    'fills': [],
    'exec_ids': set(),
    'symbol': '',
    'min_tick': 0.01,
    'leg_details': {} # Maps role -> {'price': sum, 'qty': sum, 'time': last_fill_time}
}

config = {
    'trailing_pct': 2.0,
    'ib': None,
    'long_account': '',
    'short_account': '',
    'transitioning': False,
    'transition_done': False,
    'cycle_count': 0
}

def reset_cycle_state():
    """Reset per-cycle state to allow multiple runs in one process."""
    global order_role_map, pnl_stats
    active_trades['long'] = None
    active_trades['short'] = None
    order_role_map.clear()

    pnl_stats.update({
        'total_buys': 0.0,
        'total_sells': 0.0,
        'total_commission': 0.0,
        'fills': [],
        'exec_ids': set(),
        'symbol': '',
        'min_tick': 0.01,
        'leg_details': {}
    })

    config['transitioning'] = False
    config['transition_done'] = False
    config['cycle_count'] += 1
    print(f"\n--- CYCLE {config['cycle_count']} STATE RESET ---")

def q_floor(price, tick):
    return math.floor((price + 1e-12) / tick) * tick

def q_ceil(price, tick):
    return math.ceil((price - 1e-12) / tick) * tick

def onError(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    if reqId == -1: return
    msg = errorString if errorString else errorCode
    code = errorCode if errorString else "INFO"
    print(f"\n[IBKR {code}]: {msg} (reqId={reqId})")

def report_pnl(is_final=False):
    net_pnl = pnl_stats['total_sells'] - pnl_stats['total_buys'] - pnl_stats['total_commission']
    status = "FINAL" if is_final else "INTERIM"

    print(f"\n========================================")
    print(f"{status} HEDGE P&L REPORT (Cycle #{config['cycle_count']} - {pnl_stats['symbol']})")
    print(f"----------------------------------------")

    # Print per-leg summaries
    for role, details in pnl_stats['leg_details'].items():
        avg_p = details['price'] / details['qty']
        time_str = details['time'].strftime('%H:%M:%S')
        print(f"{role:12} | {details['qty']:5} @ {avg_p:8.4f} | {time_str}")

    print(f"----------------------------------------")
    print(f"Total Cash Out (Buys):  {pnl_stats['total_buys']:.2f}")
    print(f"Total Cash In (Sells):  {pnl_stats['total_sells']:.2f}")
    print(f"Total Commissions:      {pnl_stats['total_commission']:.2f}")
    print(f"----------------------------------------")
    print(f"NET REALIZED P&L:       {net_pnl:.2f}")
    print(f"STATUS: {'CLOSED' if is_final else 'OPEN'}")
    print(f"========================================\n")

def onCommissionReport(trade, fill, report):
    # Only process commissions for executions we have tracked in this session
    # We use getattr/or fallback because report.execId is sometimes missing
    exec_id = getattr(report, 'execId', None) or fill.execution.execId
    if exec_id not in pnl_stats['exec_ids']:
        return
    pnl_stats['total_commission'] += report.commission
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [COMMISSION]: {report.commission:.2f} {report.currency}")

def onTrailingStopStatus(trade):
    status = trade.orderStatus
    curr_stop = getattr(status, 'stopPrice', 0)
    if curr_stop <= 0: curr_stop = getattr(trade.order, 'auxPrice', 0)

    if status.status == 'Filled':
        print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | EXECUTED at {status.avgFillPrice:.4f}")
    else:
        price_str = f"{curr_stop:.4f}" if 0 < curr_stop < 1e10 else "Calculating..."
        print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | Current Stop: {price_str}")

async def onStopLossFill(trade, fill):
    # Prevent double entry into transition logic
    if config.get('transitioning') or config.get('transition_done'):
        return
    config['transitioning'] = True
    transitioned = False

    try:
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\n>>>> [{now_str}] STOP LOSS TRIGGERED on {trade.order.account} <<<<")

        hit_leg = 'long' if trade.order.account == config['long_account'] else 'short'
        surviving_leg = 'short' if hit_leg == 'long' else 'long'
        surviving_trade = active_trades[surviving_leg]
        acc = config['long_account'] if surviving_leg == 'long' else config['short_account']
        label = surviving_leg.upper()

        if surviving_trade and not surviving_trade.isDone():
            print(f"Cancelling surviving Stop Loss on {acc} ({label})...")
            config['ib'].cancelOrder(surviving_trade.order)

            # 4. Wait until cancelled
            deadline = asyncio.get_event_loop().time() + 10
            while not surviving_trade.isDone() and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.1)
                config['ib'].waitOnUpdate()

            if not surviving_trade.isDone():
                print(f"Cancel timeout on {acc}; flattening survivor position defensively.")
                pos = [p for p in config['ib'].positions() if p.contract.conId == trade.contract.conId and p.account == acc]
                if pos and pos[0].position != 0:
                    qty = pos[0].position
                    action = 'SELL' if qty > 0 else 'BUY'
                    config['ib'].placeOrder(trade.contract, MarketOrder(action, abs(qty), account=acc))
                return

            # Extra sync to ensure local cache is updated with latest position after cancel
            config['ib'].waitOnUpdate()
            await asyncio.sleep(0)
            config['ib'].waitOnUpdate()

            # Verify it was actually cancelled and not filled during the wait
            st = surviving_trade.orderStatus.status
            if st not in ('Cancelled', 'ApiCancelled'):
                print(f"Surviving Stop Loss was not cancelled (Status: {st}). It likely filled. Not placing trailing stop.")
                return
        else:
            if not surviving_trade:
                print(f"No surviving SL trade object found for {acc} ({label}). Checking position.")
            else:
                print(f"Surviving SL on {acc} ({label}) is already {surviving_trade.orderStatus.status}. Checking position.")

        # Place Trailing Stop based on actual Position
        action = 'SELL' if label == "LONG" else 'BUY'

        # 5. Determine Quantity from actual Position (Resilient to partials)
        # Find the position for this specific account and symbol
        positions = [p for p in config['ib'].positions() if p.contract.conId == trade.contract.conId and p.account == acc]
        if not positions or positions[0].position == 0:
            print(f"No active position found on {acc}. Not placing trailing stop.")
            return

        qty = abs(positions[0].position)
        market_price = fill.execution.price

        tick = pnl_stats['min_tick']
        if label == "LONG":
            trail_price = q_floor(market_price * (1 - (config['trailing_pct'] / 100)), tick)
        else:
            trail_price = q_ceil(market_price * (1 + (config['trailing_pct'] / 100)), tick)

        print(f"Switching {label} leg to {config['trailing_pct']}% Trailing Stop (Est: {trail_price:.4f})...")
        trail_order = Order(
            action=action, totalQuantity=qty, orderType='TRAIL',
            trailingPercent=config['trailing_pct'], account=acc, tif='GTC', outsideRth=True
        )
        t_trade = config['ib'].placeOrder(trade.contract, trail_order)
        t_trade.statusEvent += onTrailingStopStatus
        active_trades[surviving_leg] = t_trade

        # Register the role using the trade object's orderId
        order_role_map[t_trade.order.orderId] = f"{label}_TRAIL"
        transitioned = True
        print("Trailing Stop submitted. Protection transitioned.")
    finally:
        if transitioned:
            config['transition_done'] = True
        config['transitioning'] = False

def onFill(trade, fill):
    exec = fill.execution

    # Track the unique execution ID for robust commission matching
    # and to ensure we don't process same fill twice across cycles
    if exec.execId in pnl_stats['exec_ids']:
        return

    pnl_stats['fills'].append(fill)
    pnl_stats['exec_ids'].add(exec.execId)

    if trade.order.action == 'BUY': pnl_stats['total_buys'] += exec.shares * exec.price
    else: pnl_stats['total_sells'] += exec.shares * exec.price

    account = trade.order.account
    role = order_role_map.get(trade.order.orderId, "UNKNOWN")
    now_ts = datetime.now()
    print(f"--- [{now_ts.strftime('%H:%M:%S')}] {role} FILLED on {account}: {exec.shares} @ {exec.price:.4f} ---")

    # Record detailed leg data
    if role not in pnl_stats['leg_details']:
        pnl_stats['leg_details'][role] = {'price': 0.0, 'qty': 0.0, 'time': now_ts}

    pnl_stats['leg_details'][role]['price'] += exec.shares * exec.price
    pnl_stats['leg_details'][role]['qty'] += exec.shares
    pnl_stats['leg_details'][role]['time'] = now_ts

    # Trigger transition if it's a stop loss fill
    if "SL" in role and not config.get('transitioning') and not config.get('transition_done'):
        asyncio.create_task(onStopLossFill(trade, fill))

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--qty', type=float, required=True)
    parser.add_argument('--stopPct', type=float, default=1.0)
    parser.add_argument('--trailingPct', type=float, default=2.0)
    parser.add_argument('--longAccount', required=True)
    parser.add_argument('--shortAccount', required=True)
    parser.add_argument('--useAlgo', action='store_true')
    parser.add_argument('--port', type=int, default=7497)
    args = parser.parse_args()

    config.update({'trailing_pct': args.trailingPct, 'long_account': args.longAccount, 'short_account': args.shortAccount})
    config['transitioning'] = False
    config['transition_done'] = False
    ib = IB()
    config['ib'] = ib
    ib.errorEvent += onError
    ib.commissionReportEvent += onCommissionReport

    try:
        await ib.connectAsync('127.0.0.1', args.port, clientId=10)

        # Contract setup
        if args.symbol.upper() == 'AIR':
            contract = Stock('AIR', 'SMART', 'SBF', 'EUR')
        else:
            contract = Stock(args.symbol.upper(), 'SMART', 'USD')
        await ib.qualifyContractsAsync(contract)

        details = await ib.reqContractDetailsAsync(contract)
        min_tick = details[0].minTick if details else 0.01

        while True:
            reset_cycle_state()
            pnl_stats['symbol'] = contract.symbol
            pnl_stats['min_tick'] = min_tick
            tick = min_tick

            # 1. Concurrent Entries
            print(f"\n>>> SUBMITTING CONCURRENT ENTRIES (Qty: {args.qty})...")
            l_ord = MarketOrder('BUY', args.qty, account=args.longAccount, tif='GTC')
            s_ord = MarketOrder('SELL', args.qty, account=args.shortAccount, tif='GTC')
            if args.useAlgo and contract.currency == 'USD':
                for o in [l_ord, s_ord]:
                    o.algoStrategy = 'Adaptive'
                    o.algoParams = [TagValue('priority', 'Normal')]

            l_trade = ib.placeOrder(contract, l_ord)
            s_trade = ib.placeOrder(contract, s_ord)

            # Immediate mapping of roles using orderId from the trade objects
            order_role_map[l_trade.order.orderId] = "LONG_ENTRY"
            order_role_map[s_trade.order.orderId] = "SHORT_ENTRY"

            l_trade.fillEvent += onFill
            s_trade.fillEvent += onFill

            print("Waiting for both entries to fill...")
            deadline = asyncio.get_event_loop().time() + 30
            while not (l_trade.isDone() and s_trade.isDone()) and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.5)
                ib.waitOnUpdate()

            # 3. VERIFY ENTRY COMPLETION (Panic Exit if one failed)
            l_stat, s_stat = l_trade.orderStatus.status, s_trade.orderStatus.status
            l_filled, s_filled = l_trade.orderStatus.filled, s_trade.orderStatus.filled

            if l_stat != 'Filled' or s_stat != 'Filled' or l_filled == 0 or s_filled == 0:
                print(f"\n!!! CRITICAL: ENTRY FAILURE !!!")
                print(f"Long Status: {l_stat} (Filled: {l_filled})")
                print(f"Short Status: {s_stat} (Filled: {s_filled})")
                print("Aborting hedge and flattening positions...")

                # Panic Flattening based on actual positions
                for acc in [args.longAccount, args.shortAccount]:
                    pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account == acc]
                    if pos and pos[0].position != 0:
                        qty = pos[0].position
                        action = 'SELL' if qty > 0 else 'BUY'
                        print(f"Flattening position on {acc}: {abs(qty)} shares...")
                        ib.placeOrder(contract, MarketOrder(action, abs(qty), account=acc))

                # Orphan Cleanup
                print("Cleaning up orphan orders...")
                orphans = []
                for t in ib.openTrades():
                    if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                        ib.cancelOrder(t.order)
                        orphans.append(t)

                if orphans:
                    print(f"Waiting for {len(orphans)} cancellations...")
                    deadline = asyncio.get_event_loop().time() + 10
                    while any(not t.isDone() for t in orphans) and asyncio.get_event_loop().time() < deadline:
                        await asyncio.sleep(0.5)
                        ib.waitOnUpdate()

                print("Cycle aborted due to entry failure.")
                await asyncio.sleep(5)
                continue

            if l_filled != s_filled:
                print(f"\nWARNING: Quantity mismatch! Long: {l_filled}, Short: {s_filled}. Proceeding with asymmetric protection.")

            # 4. Place Stop Losses after successful entry
            print("\n>>> BOTH ENTRIES FILLED. PLACING PROTECTION...")
            l_qty, s_qty = l_filled, s_filled
            l_price, s_price = l_trade.orderStatus.avgFillPrice, s_trade.orderStatus.avgFillPrice

            # Long SL (Sell Stop)
            l_sl_p = q_floor(l_price * (1 - args.stopPct/100), tick)
            print(f"Placing LONG Stop Loss on {args.longAccount} at {l_sl_p:.4f}...")
            l_sl_o = StopOrder('SELL', l_qty, l_sl_p, account=args.longAccount, tif='GTC', outsideRth=True)

            # Short SL (Buy Stop)
            s_sl_p = q_ceil(s_price * (1 + args.stopPct/100), tick)
            print(f"Placing SHORT Stop Loss on {args.shortAccount} at {s_sl_p:.4f}...")
            s_sl_o = StopOrder('BUY', s_qty, s_sl_p, account=args.shortAccount, tif='GTC', outsideRth=True)

            active_trades['long'] = ib.placeOrder(contract, l_sl_o)
            active_trades['short'] = ib.placeOrder(contract, s_sl_o)

            # Register Stop Loss roles
            order_role_map[active_trades['long'].order.orderId] = "LONG_SL"
            order_role_map[active_trades['short'].order.orderId] = "SHORT_SL"

            for t in [active_trades['long'], active_trades['short']]: t.fillEvent += onFill

            print("Waiting for Stop Losses to reach live state...")
            armed_statuses = {'PreSubmitted', 'Submitted'}
            while any(t.orderStatus.status not in armed_statuses for t in [active_trades['long'], active_trades['short']]):
                await asyncio.sleep(0.1)
                ib.waitOnUpdate()
                # Safety: If any order becomes Inactive or Rejected, stop waiting
                if any(t.orderStatus.status in ('Inactive', 'Rejected') for t in [active_trades['long'], active_trades['short']]):
                    break

            # 5. VERIFY PROTECTION ARMED (Panic Exit if one failed)
            if any(t.orderStatus.status in ('Inactive', 'Rejected') for t in [active_trades['long'], active_trades['short']]):
                print("\n!!! CRITICAL: PROTECTION FAILURE !!!")
                print("One or more Stop Loss orders were rejected or became Inactive.")
                print("Aborting hedge and flattening positions...")

                # Flatten both accounts for this contract
                for acc in [args.longAccount, args.shortAccount]:
                    pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account == acc]
                    if pos and pos[0].position != 0:
                        qty = pos[0].position
                        action = 'SELL' if qty > 0 else 'BUY'
                        print(f"Flattening position on {acc}: {abs(qty)} shares...")
                        ib.placeOrder(contract, MarketOrder(action, abs(qty), account=acc))

                # Defensive Cleanup of any orphan orders
                print("Cleaning up orphan orders...")
                orphans = []
                for t in ib.openTrades():
                    if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                        ib.cancelOrder(t.order)
                        orphans.append(t)

                if orphans:
                    print(f"Waiting for {len(orphans)} cancellations...")
                    deadline = asyncio.get_event_loop().time() + 10
                    while any(not t.isDone() for t in orphans) and asyncio.get_event_loop().time() < deadline:
                        await asyncio.sleep(0.5)
                        ib.waitOnUpdate()

                await asyncio.sleep(2)
                report_pnl(is_final=True)
                print("Cycle aborted due to protection failure.")
                await asyncio.sleep(5)
                continue

            # 6. Monitor until positions are FLAT
            print("\nHedge is ACTIVE. Monitoring positions...")
            while True:
                await asyncio.sleep(2)
                ib.waitOnUpdate()
                pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account in [args.longAccount, args.shortAccount]]
                if not pos or all(p.position == 0 for p in pos):
                    break

            print("\n>>> ALL POSITIONS CLOSED.")

            # 7. Defensive Cleanup of any orphan orders
            print("Cleaning up any remaining orphan orders...")
            orphans = []
            for t in ib.openTrades():
                if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                    print(f"Cancelling orphan {t.order.orderType} order {t.order.orderId} on {t.order.account}...")
                    ib.cancelOrder(t.order)
                    orphans.append(t)

            if orphans:
                print(f"Waiting for {len(orphans)} cancellations...")
                deadline = asyncio.get_event_loop().time() + 10
                while any(not t.isDone() for t in orphans) and asyncio.get_event_loop().time() < deadline:
                    await asyncio.sleep(0.5)
                    ib.waitOnUpdate()

            await asyncio.sleep(2) # Final sync
            report_pnl(is_final=True)
            print("\n>>> CYCLE COMPLETE. Waiting before next cycle...")
            await asyncio.sleep(10)

    finally:
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
