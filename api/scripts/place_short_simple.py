import asyncio
import math
from ib_insync import IB, Stock, StopOrder, MarketOrder, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

def onError(trade, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    """
    Global error handler for all trades.
    """
    if reqId == -1:
        return

    msg = errorString if errorString else errorCode
    code = errorCode if errorString else "INFO"
    print(f"\n[IBKR {code}]: {msg} (reqId={reqId})")

async def main():
    ib = IB()
    ib.errorEvent += onError
    account_id = 'DUP073403'
    symbol = 'AIR'

    try:
        print(f"Connecting to IBKR (Account: {account_id})...")
        await ib.connectAsync('127.0.0.1', 7497, clientId=11) # Unique clientId

        contract = Stock(symbol=symbol, exchange='SMART', primaryExchange='SBF', currency='EUR')
        await ib.qualifyContractsAsync(contract)
        print(f"Contract qualified: {contract.symbol}")

        # 1. Place the SELL (Short) Market Order
        # Shorting means selling something you don't own.
        print(f"Placing Short Market Order for {symbol}...")
        sell_order = MarketOrder(action='SELL', totalQuantity=1, account=account_id, tif='GTC')
        sell_trade = ib.placeOrder(contract, sell_order)

        # 2. Wait for the Short to fill
        print("Waiting for short order to fill...")
        while not sell_trade.isDone():
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        if sell_trade.orderStatus.status != 'Filled':
            print(f"Short failed with status: {sell_trade.orderStatus.status}")
            return

        avg_price = sell_trade.orderStatus.avgFillPrice
        print(f"Success! Shorted at {avg_price}")

        # 3. Place the Buy Stop Order (Stop Loss for a Short)
        print("Fetching contract details for tick size...")
        details = await ib.reqContractDetailsAsync(contract)
        min_tick = details[0].minTick if details else 0.01

        # MiFID II / Euronext compliance
        if avg_price >= 200:
            min_tick = 0.05
        elif min_tick < 0.01:
            min_tick = 0.01

        # For a SHORT position, the Stop Loss must be HIGHER than the entry price.
        stop_price_raw = avg_price * 1.01 # 1% above entry

        # Round UP to the nearest valid tick (to be safe/conservative on a stop loss for short)
        stop_price = math.ceil(stop_price_raw / min_tick) * min_tick
        stop_price = round(stop_price, 2)

        print(f"MinTick: {min_tick}. Placing Buy Stop Loss at {stop_price} (1% above {avg_price})...")

        # Action is 'BUY' to cover the short position
        sl_order = StopOrder(
            action='BUY',
            totalQuantity=1,
            stopPrice=stop_price,
            account=account_id
        )
        sl_order.tif = 'GTC'
        sl_order.outsideRth = True

        sl_trade = ib.placeOrder(contract, sl_order)

        # 4. Final confirmation
        while sl_trade.orderStatus.status == 'PendingSubmit':
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()

        print(f"Stop Loss is now live! Status: {sl_trade.orderStatus.status}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
