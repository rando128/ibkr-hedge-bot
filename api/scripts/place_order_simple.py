import asyncio
import math
from ib_insync import IB, Stock, StopOrder, MarketOrder, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

def onError(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    """
    Global error handler for all trades.
    """
    # System messages often have reqId -1 and aren't true "errors"
    if reqId == -1:
        return

    # Sometimes ib_insync swaps errorCode and errorString for certain messages
    msg = errorString if errorString else errorCode
    code = errorCode if errorString else "INFO"

    print(f"\n[IBKR {code}]: {msg} (reqId={reqId})")

async def main():
    ib = IB()
    # Attach the error handler globally to the IB instance
    ib.errorEvent += onError
    account_id = 'DUP073403'
    symbol = 'AIR'

    try:
        print(f"Connecting to IBKR (Account: {account_id})...")
        await ib.connectAsync('127.0.0.1', 7497, clientId=10)

        contract = Stock(symbol=symbol, exchange='SMART', primaryExchange='SBF', currency='EUR')
        await ib.qualifyContractsAsync(contract)
        print(f"Contract qualified: {contract.symbol}")

        # 1. Place the BUY Market Order
        print(f"Placing Buy Market Order for {symbol}...")
        buy_order = MarketOrder(action='BUY', totalQuantity=1, account=account_id, tif='GTC')
        buy_trade = ib.placeOrder(contract, buy_order)

        # 2. Wait for the BUY to fill (we need the price for the Stop Loss)
        print("Waiting for buy order to fill...")
        while not buy_trade.isDone():
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        if buy_trade.orderStatus.status != 'Filled':
            print(f"Buy failed with status: {buy_trade.orderStatus.status}")
            return

        avg_price = buy_trade.orderStatus.avgFillPrice
        print(f"Success! Bought at {avg_price}")

        # 3. Place the Stop Loss Order
        # FETCH CONTRACT DETAILS FOR MIN TICK (Fix for Error 78110)
        print("Fetching contract details for tick size...")
        details = await ib.reqContractDetailsAsync(contract)
        if not details:
            print("Failed to fetch contract details.")
            return
        min_tick = details[0].minTick

        # AIR/Euronext Paris uses dynamic tick sizes (MiFID II).
        # For stocks between 200 and 500 EUR, the tick size is usually 0.05.
        if avg_price >= 200:
            print(f"Price {avg_price} is >= 200 EUR. Using 0.05 tick size for Euronext compliance.")
            min_tick = 0.05
        elif min_tick < 0.01:
            min_tick = 0.01

        # Calculate tick-compliant stop price
        stop_price_raw = avg_price * 0.99
        # Round DOWN to the nearest valid tick increment
        stop_price = math.floor(stop_price_raw / min_tick) * min_tick
        # Ensure clean decimals for IBKR
        stop_price = round(stop_price, 2)

        print(f"MinTick: {min_tick}. Placing Stop Loss at {stop_price} (Raw: {stop_price_raw:.4f})...")

        sl_order = StopOrder(
            action='SELL',
            totalQuantity=1,
            stopPrice=stop_price,
            account=account_id
        )
        # Removed parentId as advised
        sl_order.tif = 'GTC'
        sl_order.outsideRth = True

        sl_trade = ib.placeOrder(contract, sl_order)

        # 4. Final confirmation
        print("Waiting for Stop Loss acknowledgment...")
        while sl_trade.orderStatus.status == 'PendingSubmit':
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        print(f"Stop Loss is now live! Status: {sl_trade.orderStatus.status}")
        print("You can now see it in the TWS 'Orders' tab.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
