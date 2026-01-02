import asyncio
import argparse
import math
from datetime import datetime, timezone
from ib_insync import IB, Stock, Crypto, MarketOrder, StopOrder, Order, TagValue, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

# Tracks when the script started to ignore historical fills
script_start_time = datetime.now(timezone.utc)

def onError(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    """
    Improved error handler that filters system info and handles None values.
    """
    if reqId == -1:
        return

    msg = errorString if errorString else errorCode
    code = errorCode if errorString else "INFO"
    print(f"\n[IBKR {code}]: {msg} (reqId={reqId})")

# Global state to manage the two legs, P&L, and parameters
active_trades = {
    'long': None,
    'short': None
}

pnl_stats = {
    'total_buys': 0.0,      # Sum of all BUY amounts (cash out)
    'total_sells': 0.0,     # Sum of all SELL amounts (cash in)
    'total_commission': 0.0,
    'fills': [],
    'symbol': ''
}

config = {
    'trailing_pct': 2.0,    # Default trailing percentage
    'ib': None,             # Will store the IB instance
    'long_account': '',
    'short_account': ''
}

def report_pnl():
    """
    Calculates and prints the combined P&L across all accounts.
    """
    net_pnl = pnl_stats['total_sells'] - pnl_stats['total_buys'] - pnl_stats['total_commission']

    num_fills = len(pnl_stats['fills'])
    status = "INTERIM" if num_fills < 4 else "FINAL"

    print(f"\n========================================")
    print(f"{status} HEDGE P&L REPORT ({pnl_stats['symbol']})")
    print(f"----------------------------------------")
    print(f"Total Cash Out (Buys):  {pnl_stats['total_buys']:.2f}")
    print(f"Total Cash In (Sells):  {pnl_stats['total_sells']:.2f}")
    print(f"Total Commissions:      {pnl_stats['total_commission']:.2f}")
    print(f"----------------------------------------")
    print(f"NET REALIZED P&L:       {net_pnl:.2f}")
    if num_fills < 4:
        print(f"STATUS: CYCLE INCOMPLETE ({num_fills}/4 fills)")
    else:
        print(f"STATUS: FULL HEDGE CYCLE COMPLETED")
    print(f"========================================\n")

def onCommissionReport(trade, fill, report):
    """
    Callback when IBKR reports the actual commission for a fill.
    """
    # Only process commissions for fills we have tracked in this session
    if fill not in pnl_stats['fills']:
        return

    pnl_stats['total_commission'] += report.commission
    print(f"[COMMISSION]: {report.commission:.2f} {report.currency} for {trade.contract.symbol}")
    report_pnl()

def onTrailingStopStatus(trade):
    """
    Called when the Trailing Stop order status or price changes.
    """
    status = trade.orderStatus
    # IBKR stores the dynamic trigger price in different fields depending on state.
    # 1. status.stopPrice is the official live trigger price
    # 2. trade.order.auxPrice is the initial submission price
    curr_stop = status.stopPrice if status.stopPrice > 0 else getattr(trade.order, 'auxPrice', 0)

    # Handle IBKR's Double.MAX_VALUE placeholder
    price_str = f"{curr_stop:.2f}" if 0 < curr_stop < 1e10 else "Calculating..."

    print(f"[TRAILING UPDATE] Account: {trade.order.account} | Status: {status.status} | Current Stop: {price_str}")

def onStopLossFill(trade, fill):
    """
    Triggered when one of the Stop Losses is filled.
    """
    print(f"\n>>>> STOP LOSS TRIGGERED on {trade.order.account} <<<<")

    # identify the surviving leg
    hit_leg = 'long' if trade.order.account == config['long_account'] else 'short'
    surviving_leg = 'short' if hit_leg == 'long' else 'long'
    surviving_trade = active_trades[surviving_leg]

    if surviving_trade and not surviving_trade.isDone():
        status = surviving_trade.orderStatus.status
        # Use account info to determine correct label for logging
        acc = surviving_trade.order.account
        label = "LONG" if acc == config['long_account'] else "SHORT"

        if status not in ('PendingCancel', 'Cancelled', 'ApiCancelled'):
            print(f"Cancelling surviving Stop Loss ({status}) on {acc} ({label})...")
            config['ib'].cancelOrder(surviving_trade.order)

        # Switch to Trailing Stop (2%)
        acc = surviving_trade.order.account
        action = surviving_trade.order.action # Keep same exit direction
        qty = surviving_trade.order.totalQuantity

        # Calculate initial estimated trail price for logging
        # We try to get the current price from the IB cache
        ticker = config['ib'].ticker(trade.contract)
        market_price = ticker.marketPrice() if ticker.marketPrice() > 0 else ticker.close

        # Determine tick for rounding
        leg_tick = 0.05 if (market_price >= 200 and trade.contract.currency == 'EUR') else 0.01

        trail_price = 0.0
        if action == 'SELL': # Closing a LONG
            trail_price = math.floor(market_price * (1 - (config['trailing_pct'] / 100)) / leg_tick) * leg_tick
        else: # BUY to cover a SHORT
            trail_price = math.ceil(market_price * (1 + (config['trailing_pct'] / 100)) / leg_tick) * leg_tick

        print(f"Switching {label} leg to {config['trailing_pct']}% Trailing Stop on account {acc} (Estimated initial stop: {trail_price:.2f})...")

        trail_order = Order(
            action=action,
            totalQuantity=qty,
            orderType='TRAIL',
            trailingPercent=config['trailing_pct'],
            account=acc,
            tif='GTC',
            outsideRth=True
        )
        trail_trade = config['ib'].placeOrder(trade.contract, trail_order)
        # Attach the status listener to see price updates
        trail_trade.statusEvent += onTrailingStopStatus
        # Attach to global tracker so we can wait for it
        active_trades[surviving_leg] = trail_trade
        print("Trailing Stop submitted. Protection transitioned.")

def onFill(trade, fill):
    """
    Callback for all order fills. Tracks P&L across both legs.
    """
    exec = fill.execution
    amount = exec.shares * exec.price
    action = trade.order.action
    account = trade.order.account

    print(f"\n--- EVENT: ORDER FILLED ---")
    # Determine leg type for better logging
    leg_type = "LONG" if account == config['long_account'] else "SHORT"
    role = "ENTRY" if ((action == 'BUY' and leg_type == 'LONG') or (action == 'SELL' and leg_type == 'SHORT')) else "EXIT"

    print(f"[{leg_type} {role}] Account: {account} | {trade.contract.symbol} {action} {exec.shares} @ {exec.price}")

    # Cash-flow logic:
    # BUY is always money leaving the account (cost)
    # SELL is always money entering the account (revenue)
    if action == 'BUY':
        pnl_stats['total_buys'] += amount
    else: # SELL
        pnl_stats['total_sells'] += amount

    pnl_stats['fills'].append(fill)

    if trade.isDone():
        print(f"Trade {trade.order.orderId} finished. Status: {trade.orderStatus.status}")

        # A full cycle requires exactly 4 fills
        num_fills = len(pnl_stats['fills'])
        report_pnl()

        if num_fills == 4:
            print(">>> ALL LEGS CLOSED. HEDGE COMPLETE.")

    print(f"---------------------------\n")

async def main():
    parser = argparse.ArgumentParser(description='Place a buy order with an automated stop loss.')
    parser.add_argument('--symbol', type=str, required=True, help='Ticker symbol (e.g., AAPL, AIR, BTC)')
    parser.add_argument('--cashQty', type=float, help='USD amount to spend (REQUIRED for Crypto)')
    parser.add_argument('--qty', type=float, help='Number of shares to buy (REQUIRED for Stocks)')
    parser.add_argument('--stopPct', type=float, default=1.0, help='Stop loss percentage (e.g., 1.0 for 1%%)')
    parser.add_argument('--trailingPct', type=float, default=2.0, help='Trailing stop percentage (e.g., 2.0 for 2%%)')
    parser.add_argument('--longAccount', type=str, required=True, help='Account for LONG leg')
    parser.add_argument('--shortAccount', type=str, required=True, help='Account for SHORT leg')
    parser.add_argument('--useAlgo', action='store_true', help='Use IBKR Adaptive Algo (primarily US Stocks)')
    parser.add_argument('--port', type=int, default=7497, help='TWS/Gateway port')

    args = parser.parse_args()
    config['trailing_pct'] = args.trailingPct
    config['long_account'] = args.longAccount
    config['short_account'] = args.shortAccount

    # Update start time slightly in the past to ensure we don't miss the first immediate fill
    global script_start_time
    script_start_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    ib = IB()
    config['ib'] = ib # Store for callbacks
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

            # Use Adaptive Algo only if explicitly requested
            if args.useAlgo and contract.currency == 'USD':
                print(f"Applying Adaptive Algo (Normal priority)...")
                long_order.algoStrategy = 'Adaptive'
                long_order.algoParams = [TagValue('priority', 'Normal')]

        long_trade = ib.placeOrder(contract, long_order)
        long_trade.fillEvent += onFill

        while not long_trade.isDone():
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        if long_trade.orderStatus.status == 'Filled':
            avg_price = long_trade.orderStatus.avgFillPrice
            print(f"--- [LONG ENTRY] FILLED at {avg_price} ---")

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

            # Use Adaptive Algo only if explicitly requested
            if args.useAlgo and contract.currency == 'USD':
                print(f"Applying Adaptive Algo (Normal priority)...")
                short_order.algoStrategy = 'Adaptive'
                short_order.algoParams = [TagValue('priority', 'Normal')]

        short_trade = ib.placeOrder(contract, short_order)
        short_trade.fillEvent += onFill

        while not short_trade.isDone():
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        if short_trade.orderStatus.status == 'Filled':
            avg_price = short_trade.orderStatus.avgFillPrice
            print(f"--- [SHORT ENTRY] FILLED at {avg_price} ---")

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
        # Continue listening until both legs in active_trades are Done
        while any(t and not t.isDone() for t in active_trades.values()):
            await asyncio.sleep(1)
            ib.waitOnUpdate()

        print("\n>>> ALL LEGS CLOSED. HEDGE COMPLETE.")
        report_pnl()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing connection...")
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
