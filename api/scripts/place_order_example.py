import asyncio
from ib_insync import IB, Stock, Crypto, MarketOrder, StopOrder, Order, LimitOrder, TagValue, util

# MANDATORY: Patch asyncio for ib_insync
util.patchAsyncio()

def onFill(trade, fill):
    print(f"\n--- EVENT: ORDER FILLED ---")
    print(f"Account: {trade.order.account}")
    print(f"Order: {trade.order.action} {trade.order.totalQuantity} {trade.contract.symbol}")
    print(f"Fill Price: {fill.execution.price}")
    print(f"Remaining: {trade.orderStatus.remaining}")
    if trade.isDone():
        print(f"Trade is finished. Final Status: {trade.orderStatus.status}")
    print(f"---------------------------\n")

async def main():
    ib = IB()
    try:
        # 1. Connect
        print("Connecting to IBKR on localhost:7497...")
        # Use connectAsync for pure async/await flow
        await ib.connectAsync('127.0.0.1', 7497, clientId=10)
        print("Connected!")

        # 2. Define Contract (Comment/Uncomment as needed)

        # --- EUROPE (Airbus on Euronext Paris) ---
        contract = Stock(symbol='AIR', exchange='SMART', primaryExchange='SBF', currency='EUR')

        # --- US (Apple on NASDAQ) ---
        # contract = Stock(symbol='AAPL', exchange='SMART', primaryExchange='NASDAQ', currency='USD')

        # --- JAPAN (Sony on Tokyo Stock Exchange) ---
        # contract = Stock(symbol='6758', exchange='SMART', primaryExchange='TSEJ', currency='JPY')

        # --- CRYPTO (Bitcoin on Paxos - Requires Live Account Permissions) ---
        # contract = Crypto(symbol='BTC', exchange='PAXOS', currency='USD')

        # 3. Qualify
        print(f"Qualifying contract: {contract.symbol}...")
        await ib.qualifyContractsAsync(contract)
        print(f"Contract qualified: {contract}")

        # 4. Define and Place Primary Order
        # We use a simple sequential approach: Buy first, then Stop Loss.
        print(f"Placing Buy Market Order for {contract.symbol}...")
        order = MarketOrder(action='BUY', totalQuantity=1, account='DUP073403', tif='GTC')
        trade = ib.placeOrder(contract, order)
        trade.fillEvent += onFill

        # 6. Wait for fill
        print("Waiting for fill...")
        while not trade.isDone():
            await asyncio.sleep(0.5)
            ib.waitOnUpdate()
            print(f"Current Status: {trade.orderStatus.status}")

        # 7. Check Results and Place Stop Loss
        status = trade.orderStatus.status
        if status == 'Filled':
            print(f"\nSUCCESS: Order filled successfully!")

            total_shares = 0
            total_cost = 0.0
            for fill in trade.fills:
                ex = fill.execution
                print(f"- Fill: {ex.shares} shares @ {ex.price}")
                total_shares += ex.shares
                total_cost += ex.shares * ex.price

            avg_price = total_cost / total_shares
            print(f"Average Execution Price: {avg_price:.2f}")

            # 8. Add Stop Loss at 1% below
            # For a BUY order, the SL is a SELL STOP
            stop_price = round(avg_price * 0.99, 2)
            print(f"\nPlacing Stop Loss at {stop_price} (1% below {avg_price:.2f})...")

            sl_order = StopOrder('SELL', total_shares, stop_price, account='DUP073403')
            # LINK to parent to ensure it goes through
            sl_order.parentId = order.orderId
            sl_order.tif = 'GTC'
            sl_order.outsideRth = True

            sl_trade = ib.placeOrder(contract, sl_order)
            sl_trade.fillEvent += onFill

            # FORCE SYNC: Wait until the order is acknowledged by TWS
            print("Sending Stop Loss to TWS...")
            while sl_trade.orderStatus.status == 'PendingSubmit':
                await asyncio.sleep(0.1)
                ib.waitOnUpdate()

            print(f"Stop Loss placed. Status: {sl_trade.orderStatus.status}")

            # 9. Add Trailing Stop at 2%
            # This locks in profits by following the price up and selling if it drops 2% from its peak.
            # print(f"\nPlacing Trailing Stop at 2%...")
            # trail_order = Order(
            #     action='SELL',
            #     totalQuantity=total_shares,
            #     orderType='TRAIL',
            #     trailingPercent=0.5,  # 2% trailing
            #     account='DUP073403',
            #     tif='GTC',
            #     outsideRth=True
            # )
            # trail_trade = ib.placeOrder(contract, trail_order)
            # trail_trade.fillEvent += onFill  # 2. Attach listener
            #
            # print(f"Trailing Stop submitted. Status: {trail_trade.orderStatus.status}")

            # 10. Stay connected to listen for events
            print("\nListening for Stop Loss / Trailing Stop fills... (Ctrl+C to stop)")
            while True:
                await asyncio.sleep(1)
                ib.waitOnUpdate()

        elif status in ('Cancelled', 'ApiCancelled', 'Rejected', 'Inactive'):
            print(f"\nORDER FAILED: {status}")
            for entry in trade.log:
                if entry.message:
                    print(f"Reason: {entry.message}")
        else:
            print(f"\nOrder finished with status: {status}")

    except Exception as e:
        print(f"An error occurred: {e}")
        # If the error is 'bool' await, print more info
        import traceback
        traceback.print_exc()
    finally:
        print("Disconnecting...")
        ib.disconnect()

if __name__ == '__main__':
    # Using a simple run
    asyncio.run(main())
