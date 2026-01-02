import asyncio
from ib_insync import IB, MarketOrder, util

# MANDATORY: Patch asyncio to allow nested event loops (fixes "This event loop is already running")
util.patchAsyncio()

async def main():
    ib = IB()
    account_id = 'DUP073403'
    target_symbol = '6758'  # Example: Sony Group

    try:
        print(f"Connecting to IBKR (Account: {account_id})...")
        await ib.connectAsync('127.0.0.1', 7497, clientId=2)
        print("Connected!")

        # 1. Fetch all positions
        print(f"Fetching positions for account {account_id}...")
        all_positions = ib.positions()

        # Filter for your specific account
        positions = [p for p in all_positions if p.account == account_id]

        print("\n--- Current Positions ---")
        target_pos = None
        for p in positions:
            print(f"Ticker: {p.contract.localSymbol}, Qty: {p.position}, Avg Cost: {p.avgCost}")
            if p.contract.symbol == target_symbol or p.contract.localSymbol == target_symbol:
                target_pos = p

        if not target_pos:
            print(f"\nNo position found for '{target_symbol}' in account {account_id}. Nothing to sell.")
            return

        print(f"\nTarget found: {target_pos.position} shares of {target_pos.contract.localSymbol}")

        # 2. Determine the closing action
        action = 'SELL' if target_pos.position > 0 else 'BUY'
        quantity = abs(target_pos.position)

        print(f"Closing position: {action} {quantity} shares...")

        # 3. Create and place the order
        order = MarketOrder(action=action, totalQuantity=quantity, account=account_id)
        trade = ib.placeOrder(target_pos.contract, order)

        # 4. Monitor status
        print("Waiting for order to fill...")
        while not trade.isDone():
            await ib.sleep(1)
            print(f"Current Status: {trade.orderStatus.status}")

        if trade.orderStatus.status == 'Filled':
            print(f"\nSUCCESS: Position closed.")
        else:
            print(f"\nOrder finished with status: {trade.orderStatus.status}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Disconnecting...")
        ib.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
