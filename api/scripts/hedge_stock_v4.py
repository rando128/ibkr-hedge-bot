import asyncio
import argparse
import math
from ib_insync import IB, Stock, Crypto, MarketOrder, StopOrder, TagValue, util

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

def onFill(trade, fill):
    """
    Callback for all order fills.
    """
    print(f"\n--- EVENT: ORDER FILLED ---")
    print(f"Account: {trade.order.account}")
    print(f"Action: {trade.order.action}")
    print(f"Symbol: {trade.contract.symbol}")
    print(f"Qty: {fill.execution.shares} @ {fill.execution.price}")
    if trade.isDone():
        print(f"Status: {trade.orderStatus.status}")
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
    # Attach global error handler
    ib.errorEvent += onError
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
            active_stop_losses.append(sl_long_trade)

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
            active_stop_losses.append(sl_short_trade)

        # ---------------------------------------------------------
        # 6. Final confirmation & Monitoring
        # ---------------------------------------------------------
        print("\nAll legs submitted. Waiting for Stop Losses to reach live state...")
        for sl_t in active_stop_losses:
            while sl_t.orderStatus.status == 'PendingSubmit':
                await asyncio.sleep(0.1)
                ib.waitOnUpdate()

        print("\nBoth Stop Losses are ACTIVE. Listening for events... (Ctrl+C to stop)")
        while any(not t.isDone() for t in active_stop_losses):
            await asyncio.sleep(1)
            ib.waitOnUpdate()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing connection...")
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
