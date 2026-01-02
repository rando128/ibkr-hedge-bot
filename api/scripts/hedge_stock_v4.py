import asyncio
import argparse
import math
from datetime import datetime, timezone
from ib_insync import IB, Stock, Crypto, MarketOrder, StopOrder, Order, TagValue, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

# Global state to manage the legs, P&L, and order roles
active_trades = {
    'long': None,
    'short': None
}

order_role_map = {} # Maps orderId -> "LONG_ENTRY", "SHORT_SL", etc.

pnl_stats = {
    'total_buys': 0.0,      # Sum of all BUY amounts (cash out)
    'total_sells': 0.0,     # Sum of all SELL amounts (cash in)
    'total_commission': 0.0,
    'fills': [],
    'exec_ids': set(),      # Set of execId strings to track commissions robustly
    'symbol': '',
    'min_tick': 0.01        # Will be updated from contract details
}

config = {
    'trailing_pct': 2.0,
    'ib': None,
    'long_account': '',
    'short_account': ''
}

def onError(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    if reqId == -1: return
    msg = errorString if errorString else errorCode
    code = errorCode if errorString else "INFO"
    print(f"\n[IBKR {code}]: {msg} (reqId={reqId})")

def report_pnl(is_final=False):
    net_pnl = pnl_stats['total_sells'] - pnl_stats['total_buys'] - pnl_stats['total_commission']
    status = "FINAL" if is_final else "INTERIM"

    print(f"\n========================================")
    print(f"{status} HEDGE P&L REPORT ({pnl_stats['symbol']})")
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
    if report.execId not in pnl_stats['exec_ids']:
        return
    pnl_stats['total_commission'] += report.commission
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [COMMISSION]: {report.commission:.2f} {report.currency}")

def onTrailingStopStatus(trade):
    status = trade.orderStatus
    curr_stop = getattr(status, 'stopPrice', 0)
    if curr_stop <= 0: curr_stop = getattr(trade.order, 'auxPrice', 0)

    if status.status == 'Filled':
        print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | EXECUTED at {status.avgFillPrice}")
    else:
        price_str = str(curr_stop) if 0 < curr_stop < 1e10 else "Calculating..."
        print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | Current Stop: {price_str}")

async def onStopLossFill(trade, fill):
    now_str = datetime.now().strftime("%H:%M:%S")
    print(f"\n>>>> [{now_str}] STOP LOSS TRIGGERED on {trade.order.account} <<<<")

    hit_leg = 'long' if trade.order.account == config['long_account'] else 'short'
    surviving_leg = 'short' if hit_leg == 'long' else 'long'
    surviving_trade = active_trades[surviving_leg]

    if surviving_trade and not surviving_trade.isDone():
        acc = surviving_trade.order.account
        label = "LONG" if acc == config['long_account'] else "SHORT"

        print(f"Cancelling surviving Stop Loss on {acc} ({label})...")
        config['ib'].cancelOrder(surviving_trade.order)

        # 4. Wait until cancelled
        while not surviving_trade.isDone():
            await asyncio.sleep(0.1)
            config['ib'].waitOnUpdate()

        # Verify it was actually cancelled and not filled during the wait
        st = surviving_trade.orderStatus.status
        if st not in ('Cancelled', 'ApiCancelled'):
            print(f"Surviving Stop Loss was not cancelled (Status: {st}). It likely filled. Not placing trailing stop.")
            return

        # Place Trailing Stop
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
            trail_price = math.floor(market_price * (1 - (config['trailing_pct'] / 100)) / tick) * tick
        else:
            trail_price = math.ceil(market_price * (1 + (config['trailing_pct'] / 100)) / tick) * tick

        print(f"Switching {label} leg to {config['trailing_pct']}% Trailing Stop (Est: {trail_price})...")
        trail_order = Order(
            action=action, totalQuantity=qty, orderType='TRAIL',
            trailingPercent=config['trailing_pct'], account=acc, tif='GTC', outsideRth=True
        )
        t_trade = config['ib'].placeOrder(trade.contract, trail_order)
        t_trade.statusEvent += onTrailingStopStatus
        active_trades[surviving_leg] = t_trade

        # Register the role using the trade object's orderId
        order_role_map[t_trade.order.orderId] = f"{label}_TRAIL"
        print("Trailing Stop submitted. Protection transitioned.")

def onFill(trade, fill):
    exec = fill.execution
    pnl_stats['fills'].append(fill)
    # Track the unique execution ID for robust commission matching
    pnl_stats['exec_ids'].add(exec.execId)

    if trade.order.action == 'BUY': pnl_stats['total_buys'] += exec.shares * exec.price
    else: pnl_stats['total_sells'] += exec.shares * exec.price

    account = trade.order.account
    role = order_role_map.get(trade.order.orderId, "UNKNOWN")
    print(f"--- [{datetime.now().strftime('%H:%M:%S')}] {role} FILLED on {account}: {exec.shares} @ {exec.price} ---")

    # Trigger transition if it's a stop loss fill
    if "SL" in role:
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
        pnl_stats['symbol'] = contract.symbol

        details = await ib.reqContractDetailsAsync(contract)
        pnl_stats['min_tick'] = details[0].minTick if details else 0.01
        tick = pnl_stats['min_tick']

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
        while not (l_trade.isDone() and s_trade.isDone()):
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

            # Panic Close Long
            if l_filled > 0:
                print(f"Flattening partial LONG position ({l_filled} shares)...")
                ib.placeOrder(contract, MarketOrder('SELL', l_filled, account=args.longAccount))
            # Panic Close Short
            if s_filled > 0:
                print(f"Flattening partial SHORT position ({s_filled} shares)...")
                ib.placeOrder(contract, MarketOrder('BUY', s_filled, account=args.shortAccount))

            # Orphan Cleanup
            for t in ib.openTrades():
                if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                    ib.cancelOrder(t.order)

            return # Exit script

        if l_filled != s_filled:
            print(f"\nWARNING: Quantity mismatch! Long: {l_filled}, Short: {s_filled}. Proceeding with asymmetric protection.")

        # 4. Place Stop Losses after successful entry
        print("\n>>> BOTH ENTRIES FILLED. PLACING PROTECTION...")
        l_qty, s_qty = l_filled, s_filled
        l_price, s_price = l_trade.orderStatus.avgFillPrice, s_trade.orderStatus.avgFillPrice

        # Long SL (Sell Stop)
        l_sl_p = math.floor(l_price * (1 - args.stopPct/100) / tick) * tick
        l_sl_p = round(l_sl_p, 4)
        print(f"Placing LONG Stop Loss on {args.longAccount} at {l_sl_p}...")
        l_sl_o = StopOrder('SELL', l_qty, l_sl_p, account=args.longAccount, tif='GTC', outsideRth=True)

        # Short SL (Buy Stop)
        s_sl_p = math.ceil(s_price * (1 + args.stopPct/100) / tick) * tick
        s_sl_p = round(s_sl_p, 4)
        print(f"Placing SHORT Stop Loss on {args.shortAccount} at {s_sl_p}...")
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
                print("One or more Stop Loss orders were rejected or became Inactive.")
                break

        # 5. Monitor until positions are FLAT
        print("\nHedge is ACTIVE. Monitoring positions...")
        while True:
            await asyncio.sleep(2)
            ib.waitOnUpdate()
            pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account in [args.longAccount, args.shortAccount]]
            if not pos or all(p.position == 0 for p in pos):
                break

        print("\n>>> ALL POSITIONS CLOSED.")

        # 6. Defensive Cleanup of any orphan orders
        print("Cleaning up any remaining orphan orders...")
        for t in ib.openTrades():
            if t.contract.conId == contract.conId and t.order.account in [args.longAccount, args.shortAccount]:
                print(f"Cancelling orphan {t.order.orderType} order {t.order.orderId} on {t.order.account}...")
                ib.cancelOrder(t.order)

        await asyncio.sleep(2) # Final sync
        report_pnl(is_final=True)

    finally:
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
