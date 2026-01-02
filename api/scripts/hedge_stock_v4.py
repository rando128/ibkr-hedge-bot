import asyncio
import argparse
import math
from ib_insync import IB, Stock, Crypto, MarketOrder, StopOrder, Order, TagValue, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

def onError(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    """
    Improved error handler that filters system info and handles None values.
    """
    if reqId == -1:
        return

    msg = errorString if errorString else errorCode
    code = errorCode if errorString else "INFO"
    print(f"\n[IBKR {code}]: {msg} (reqId={reqId})")

# Global state to manage the two legs and P&L
active_trades = {
    'long': None,
    'short': None
}

pnl_stats = {
    'entry_cost': 0.0,      # Money spent on entries (Long Buy + Short Sell proceeds)
    'exit_proceeds': 0.0,   # Money received from exits
    'total_commission': 0.0,# Sum of all commissions
    'fills': [],
    'symbol': ''
}

def report_pnl():
    """
    Calculates and prints the combined P&L across all accounts.
    """
    gross_pnl = pnl_stats['exit_proceeds'] - pnl_stats['entry_cost']
    net_pnl = gross_pnl - pnl_stats['total_commission']

    print(f"\n========================================")
    print(f"FULL CYCLE P&L REPORT ({pnl_stats['symbol']})")
    print(f"----------------------------------------")
    print(f"Total Entry Basis: {pnl_stats['entry_cost']:.2f}")
    print(f"Total Exit Value:  {pnl_stats['exit_proceeds']:.2f}")
    print(f"Total Commissions: {pnl_stats['total_commission']:.2f}")
    print(f"----------------------------------------")
    print(f"GROSS REALIZED:    {gross_pnl:.2f}")
    print(f"NET REALIZED P&L:  {net_pnl:.2f}")
    print(f"========================================\n")

def onCommissionReport(trade, fill, report):
    """
    Callback when IBKR reports the actual commission for a fill.
    """
    pnl_stats['total_commission'] += report.commission
    print(f"[COMMISSION]: {report.commission:.2f} {report.currency} for {trade.contract.symbol}")
    # Update report after commission arrives
    if len(pnl_stats['fills']) >= 3:
        report_pnl()

def onStopLossFill(trade, fill):
    """
    Triggered when one of the Stop Losses is filled.
    """
    print(f"\n>>>> STOP LOSS TRIGGERED on {trade.order.account} <<<<")

    # Identify which leg was hit and which one survived
    hit_leg = 'long' if trade == active_trades['long'] else 'short'
    surviving_leg = 'short' if hit_leg == 'long' else 'long'

    surviving_trade = active_trades[surviving_leg]

    if surviving_trade and not surviving_trade.isDone():
        print(f"Cancelling surviving Stop Loss on {surviving_trade.order.account}...")
        trade.ib.cancelOrder(surviving_trade.order)

        # Place Trailing Stop on the surviving leg
        # Action must be the same as the original SL (SELL for long, BUY for short)
        action = surviving_trade.order.action
        qty = surviving_trade.order.totalQuantity
        acc = surviving_trade.order.account

        print(f"Switching {surviving_leg.upper()} leg to 2% Trailing Stop on account {acc}...")
        trail_order = Order(
            action=action,
            totalQuantity=qty,
            orderType='TRAIL',
            trailingPercent=2.0,
            account=acc,
            tif='GTC',
            outsideRth=True
        )
        trade.ib.placeOrder(trade.contract, trail_order)
        print("Trailing Stop submitted. Protection transitioned.")

def onFill(trade, fill):
    """
    Callback for all order fills. Tracks P&L.
    """
    exec = fill.execution
    amount = exec.shares * exec.price
    action = trade.order.action

    print(f"\n--- EVENT: ORDER FILLED ---")
    print(f"Account: {trade.order.account} | Action: {action} | Qty: {exec.shares} @ {exec.price}")

    # P&L LOGIC
    # Entry: Buying for Long, Selling for Short
    # Exit: Selling for Long, Buying for Short

    # We use a simple accounting approach:
    # BUY is always a negative cash flow (paying money)
    # SELL is always a positive cash flow (receiving money)
    if action == 'BUY':
        pnl_stats['entry_cost'] += amount
    else: # SELL
        pnl_stats['exit_proceeds'] += amount

    pnl_stats['fills'].append(fill)

    if trade.isDone():
        print(f"Status: {trade.orderStatus.status}")
        # Only report P&L if we have an exit
        if len(pnl_stats['fills']) >= 3:
            report_pnl()
    print(f"---------------------------\n")

async def main():
    parser = argparse.ArgumentParser(description='Place a buy order with an automated stop loss.')
    parser.add_argument('--symbol', type=str, required=True, help='Ticker symbol (e.g., AAPL, AIR, BTC)')
    parser.add_argument('--cashQty', type=float, help='USD amount to spend (REQUIRED for Crypto)')
    parser.add_argument('--qty', type=float, help='Number of shares to buy (REQUIRED for Stocks)')
    parser.add_argument('--stopPct', type=float, default=1.0, help='Stop loss percentage (e.g., 1.0 for 1%%)')
    parser.add_argument('--longAccount', type=str, required=True, help='Account for LONG leg')
    parser.add_argument('--shortAccount', type=str, required=True, help='Account for SHORT leg')
    parser.add_argument('--port', type=int, default=7497, help='TWS/Gateway port')

    args = parser.parse_args()

    ib = IB()
    # Attach global handlers
    ib.errorEvent += onError
    ib.commissionReportEvent += onCommissionReport
    try:
        print(f"Connecting to IBKR on port {args.port}...")
        ib.connect('127.0.0.1', args.port, clientId=10)
        print("Connected!")

        # 1. Determine Contract Type
        # If it's a known crypto or 3-letter symbol we might need more logic,
        # but we'll try to qualify it as a Stock first, then Crypto.
        print(f"Searching for contract: {args.symbol}...")

        # Simple heuristic: if symbol is BTC, ETH etc, use Crypto.
        # Otherwise try Stock SMART.
        if args.symbol.upper() in ['BTC', 'ETH', 'LTC', 'BCH']:
            contract = Crypto(symbol=args.symbol.upper(), exchange='PAXOS', currency='USD')
        elif args.symbol.upper() == 'AIR':
             contract = Stock(symbol='AIR', exchange='SMART', primaryExchange='SBF', currency='EUR')
        else:
            contract = Stock(symbol=args.symbol.upper(), exchange='SMART', currency='USD')

        ib.qualifyContracts(contract)
        print(f"Contract qualified: {contract}")
        pnl_stats['symbol'] = contract.symbol

        # 2. Determine Contract Details (Tick Size)
        print("Fetching contract details for tick size...")
        details = await ib.reqContractDetailsAsync(contract)
        min_tick = 0.01 # Default
        if details:
            min_tick = details[0].minTick

        is_paxos = (contract.exchange == 'PAXOS')
        active_stop_losses = []

        # ---------------------------------------------------------
        # LEG 1: LONG (BUY) on longAccount
        # ---------------------------------------------------------
        print(f"\n>>> EXECUTING LONG LEG on {args.longAccount}...")
        if is_paxos:
            if not args.cashQty:
                print("Error: --cashQty is required for Crypto.")
                return
            long_order = MarketOrder(action='BUY', totalQuantity=0, account=args.longAccount, cashQty=args.cashQty, tif='IOC')
        else:
            if not args.qty:
                print("Error: --qty is required for Stocks.")
                return
            long_order = MarketOrder(action='BUY', totalQuantity=args.qty, account=args.longAccount, tif='GTC')
            if contract.currency == 'USD':
                long_order.algoStrategy = 'Adaptive'
                long_order.algoParams = [TagValue('priority', 'Normal')]

        long_trade = ib.placeOrder(contract, long_order)
        long_trade.fillEvent += onFill

        while not long_trade.isDone():
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        if long_trade.orderStatus.status == 'Filled':
            avg_price = long_trade.orderStatus.avgFillPrice
            # Compliance for high-priced Euronext
            leg_tick = 0.05 if (avg_price >= 200 and contract.currency == 'EUR') else min_tick

            # Stop Loss: BUY price -> SELL STOP below
            sl_price = math.floor(avg_price * (1 - (args.stopPct / 100)) / leg_tick) * leg_tick
            sl_price = round(sl_price, 2)

            print(f"Placing LONG Stop Loss at {sl_price}...")
            sl_long = StopOrder(action='SELL', totalQuantity=long_trade.orderStatus.filled, stopPrice=sl_price, account=args.longAccount, tif='GTC', outsideRth=True)
            sl_long_trade = ib.placeOrder(contract, sl_long)
            sl_long_trade.fillEvent += onFill
            sl_long_trade.fillEvent += onStopLossFill # Logic Switcher
            active_trades['long'] = sl_long_trade

        # ---------------------------------------------------------
        # LEG 2: SHORT (SELL) on shortAccount
        # ---------------------------------------------------------
        print(f"\n>>> EXECUTING SHORT LEG on {args.shortAccount}...")
        if is_paxos:
            # PAXOS Shorting is usually not supported in the same way, but we follow the logic
            short_order = MarketOrder(action='SELL', totalQuantity=args.qty or 0, account=args.shortAccount, tif='IOC')
        else:
            short_order = MarketOrder(action='SELL', totalQuantity=args.qty, account=args.shortAccount, tif='GTC')
            if contract.currency == 'USD':
                short_order.algoStrategy = 'Adaptive'
                short_order.algoParams = [TagValue('priority', 'Normal')]

        short_trade = ib.placeOrder(contract, short_order)
        short_trade.fillEvent += onFill

        while not short_trade.isDone():
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        if short_trade.orderStatus.status == 'Filled':
            avg_price = short_trade.orderStatus.avgFillPrice
            leg_tick = 0.05 if (avg_price >= 200 and contract.currency == 'EUR') else min_tick

            # Stop Loss: SELL price -> BUY STOP above
            sl_price = math.ceil(avg_price * (1 + (args.stopPct / 100)) / leg_tick) * leg_tick
            sl_price = round(sl_price, 2)

            print(f"Placing SHORT Stop Loss at {sl_price}...")
            sl_short = StopOrder(action='BUY', totalQuantity=short_trade.orderStatus.filled, stopPrice=sl_price, account=args.shortAccount, tif='GTC', outsideRth=True)
            sl_short_trade = ib.placeOrder(contract, sl_short)
            sl_short_trade.fillEvent += onFill
            sl_short_trade.fillEvent += onStopLossFill # Logic Switcher
            active_trades['short'] = sl_short_trade

        # ---------------------------------------------------------
        # 6. Final confirmation & Monitoring
        # ---------------------------------------------------------
        print("\nAll legs submitted. Waiting for Stop Losses to reach live state...")
        for sl_t in [active_trades['long'], active_trades['short']]:
            if not sl_t: continue
            while sl_t.orderStatus.status == 'PendingSubmit':
                await asyncio.sleep(0.1)
                ib.waitOnUpdate()

        print("\nBoth Stop Losses are ACTIVE. Listening for events... (Ctrl+C to stop)")
        # Continue listening as long as any trade is still alive
        while True:
            await asyncio.sleep(1)
            ib.waitOnUpdate()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing connection...")
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
