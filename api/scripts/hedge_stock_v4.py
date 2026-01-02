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
    parser.add_argument('--stopPct', type=float, default=1.0, help='Stop loss percentage below entry (e.g., 1.0 for 1%%)')
    parser.add_argument('--account', type=str, default='DUP073403', help='IBKR Account ID')
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

        # 2. Define Primary Order
        is_paxos = (contract.exchange == 'PAXOS')

        if is_paxos:
            # Crypto BUY: Use cashQty and IOC
            if not args.cashQty:
                print("Error: --cashQty is required for Crypto contracts.")
                return
            order = MarketOrder(action='BUY', totalQuantity=0, account=args.account)
            order.cashQty = args.cashQty
            order.tif = 'IOC'
        else:
            # Stock BUY: Use manual qty
            if not args.qty:
                print("Error: --qty is required for Stock contracts.")
                return

            order = MarketOrder(action='BUY', totalQuantity=args.qty, account=args.account)

            # Use Adaptive Algo for US Stocks
            if contract.currency == 'USD':
                print(f"Applying Adaptive Algo (Normal priority)...")
                order.algoStrategy = 'Adaptive'
                order.algoParams = [TagValue('priority', 'Normal')]

            order.tif = 'GTC'

        # 3. Place Primary Order
        print(f"Placing Buy Order for {args.symbol}...")
        trade = ib.placeOrder(contract, order)
        trade.fillEvent += onFill

        # 4. Wait for Fill
        print("Waiting for primary order execution...")
        while not trade.isDone():
            await asyncio.sleep(1)
            if trade.orderStatus.status == 'Inactive':
                print("Order became Inactive. Likely rejected.")
                break

        if trade.orderStatus.status == 'Filled':
            avg_price = trade.orderStatus.avgFillPrice
            filled_qty = trade.orderStatus.filled

            print(f"\nPrimary Order Filled at {avg_price}. Quantity: {filled_qty}")

            # 5. Place Stop Loss Order
            # FETCH CONTRACT DETAILS FOR MIN TICK (Fix for Error 78110)
            print("Fetching contract details for tick size...")
            details = await ib.reqContractDetailsAsync(contract)
            min_tick = 0.01 # Default
            if details:
                min_tick = details[0].minTick

            # Euronext MiFID II compliance
            if avg_price >= 200:
                print(f"Price {avg_price} >= 200. Using 0.05 tick size for compliance.")
                min_tick = 0.05
            elif min_tick < 0.01:
                min_tick = 0.01

            stop_price_raw = avg_price * (1 - (args.stopPct / 100))
            # Round DOWN to nearest tick
            stop_price = math.floor(stop_price_raw / min_tick) * min_tick
            stop_price = round(stop_price, 2)

            print(f"Placing Stop Loss at {stop_price} ({args.stopPct}% below entry, Tick: {min_tick})...")

            sl_order = StopOrder(
                action='SELL',
                totalQuantity=filled_qty,
                stopPrice=stop_price,
                account=args.account
            )
            sl_order.tif = 'GTC'
            sl_order.outsideRth = True

            sl_trade = ib.placeOrder(contract, sl_order)
            sl_trade.fillEvent += onFill

            # FORCE SYNC
            while sl_trade.orderStatus.status == 'PendingSubmit':
                await asyncio.sleep(0.1)
                ib.waitOnUpdate()

            print(f"Stop Loss is now ACTIVE. Status: {sl_trade.orderStatus.status}")

            # 6. Keep listening
            print(f"\nStop Loss is now ACTIVE at {stop_price}.")
            print("Listening for Stop Loss triggers or status changes... (Ctrl+C to stop)")

            while not sl_trade.isDone():
                # waitOnUpdate() is crucial; it processes messages and updates statuses
                await ib.updateEvent
                if sl_trade.orderStatus.status in ('Submitted', 'PreSubmitted'):
                    # Only print once when it reaches a stable live state
                    print(f"Current Stop Loss Status: {sl_trade.orderStatus.status}")
                    break

            # Now continue to wait until it's actually filled or cancelled
            while not sl_trade.isDone():
                await ib.updateEvent
        else:
            print(f"Primary order failed or was cancelled. Status: {trade.orderStatus.status}")
            for entry in trade.log:
                if entry.message: print(f"Reason: {entry.message}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing connection...")
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
