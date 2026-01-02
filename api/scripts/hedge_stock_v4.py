import asyncio
import argparse
import math
from datetime import datetime, timezone
from ib_insync import IB, Stock, Crypto, MarketOrder, StopOrder, Order, TagValue, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

# Tracks when the script started
script_start_time = datetime.now(timezone.utc)

# Global state to manage the legs and P&L
active_trades = {
    'long': None,
    'short': None
}

pnl_stats = {
    'total_buys': 0.0,      # Sum of all BUY amounts (cash out)
    'total_sells': 0.0,     # Sum of all SELL amounts (cash in)
    'total_commission': 0.0,
    'fills': [],
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
    if fill not in pnl_stats['fills']: return
    pnl_stats['total_commission'] += report.commission
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [COMMISSION]: {report.commission:.2f} {report.currency}")

def onTrailingStopStatus(trade):
    status = trade.orderStatus
    curr_stop = getattr(status, 'stopPrice', 0)
    if curr_stop <= 0: curr_stop = getattr(trade.order, 'auxPrice', 0)
    price_str = f"{curr_stop:.2f}" if 0 < curr_stop < 1e10 else "Calculating..."
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

        # Place Trailing Stop
        action = 'SELL' if label == "LONG" else 'BUY'
        qty = surviving_trade.order.totalQuantity
        market_price = fill.execution.price

        tick = pnl_stats['min_tick']
        if label == "LONG":
            trail_price = math.floor(market_price * (1 - (config['trailing_pct'] / 100)) / tick) * tick
        else:
            trail_price = math.ceil(market_price * (1 + (config['trailing_pct'] / 100)) / tick) * tick

        print(f"Switching {label} leg to {config['trailing_pct']}% Trailing Stop (Est: {trail_price:.2f})...")
        trail_order = Order(
            action=action, totalQuantity=qty, orderType='TRAIL',
            trailingPercent=config['trailing_pct'], account=acc, tif='GTC', outsideRth=True
        )
        t_trade = config['ib'].placeOrder(trade.contract, trail_order)
        t_trade.statusEvent += onTrailingStopStatus
        active_trades[surviving_leg] = t_trade

def onFill(trade, fill):
    exec = fill.execution
    pnl_stats['fills'].append(fill)
    if trade.order.action == 'BUY': pnl_stats['total_buys'] += exec.shares * exec.price
    else: pnl_stats['total_sells'] += exec.shares * exec.price

    leg = "LONG" if trade.order.account == config['long_account'] else "SHORT"
    role = "ENTRY" if trade.order.orderId in [active_trades['l_ent_id'], active_trades['s_ent_id']] else "EXIT"
    print(f"--- [{datetime.now().strftime('%H:%M:%S')}] {leg} {role} FILLED: {exec.shares} @ {exec.price} ---")

    # Trigger transition if it's a stop loss fill
    if role == "EXIT" and trade.order.orderType == 'STP':
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
        l_trade.fillEvent += onFill
        s_trade.fillEvent += onFill
        active_trades['l_ent_id'] = l_ord.orderId
        active_trades['s_ent_id'] = s_ord.orderId

        print("Waiting for both entries to fill...")
        while not (l_trade.isDone() and s_trade.isDone()):
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        # 3. Place Stop Losses after both fills
        print("\n>>> BOTH ENTRIES DONE. PLACING PROTECTION...")
        l_qty, s_qty = l_trade.orderStatus.filled, s_trade.orderStatus.filled
        l_price, s_price = l_trade.orderStatus.avgFillPrice, s_trade.orderStatus.avgFillPrice

        # Long SL (Sell Stop)
        l_sl_p = math.floor(l_price * (1 - args.stopPct/100) / tick) * tick
        l_sl_o = StopOrder('SELL', l_qty, round(l_sl_p, 2), account=args.longAccount, tif='GTC', outsideRth=True)
        # Short SL (Buy Stop)
        s_sl_p = math.ceil(s_price * (1 + args.stopPct/100) / tick) * tick
        s_sl_o = StopOrder('BUY', s_qty, round(s_sl_p, 2), account=args.shortAccount, tif='GTC', outsideRth=True)

        active_trades['long'] = ib.placeOrder(contract, l_sl_o)
        active_trades['short'] = ib.placeOrder(contract, s_sl_o)
        for t in [active_trades['long'], active_trades['short']]: t.fillEvent += onFill

        print("Waiting for Stop Losses to reach live state...")
        while any(t.orderStatus.status == 'PendingSubmit' for t in [active_trades['long'], active_trades['short']]):
            await asyncio.sleep(0.1)
            ib.waitOnUpdate()

        # 5. Monitor until positions are FLAT
        print("\nHedge is ACTIVE. Monitoring positions...")
        while True:
            await asyncio.sleep(2)
            ib.waitOnUpdate()
            pos = [p for p in ib.positions() if p.contract.conId == contract.conId and p.account in [args.longAccount, args.shortAccount]]
            if not pos or all(p.position == 0 for p in pos):
                break

        print("\n>>> ALL POSITIONS CLOSED.")
        await asyncio.sleep(2) # Final sync
        report_pnl(is_final=True)

    finally:
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
