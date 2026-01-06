#!/usr/bin/env python3
"""
Test script to validate order callback recovery after TWS reconnection.

This script:
1. Connects to TWS
2. Places a market order
3. Waits for fill
4. Places a stop-loss order at 2% from executed price
5. Disconnects from TWS
6. Reconnects and retrieves the pending SL order
7. Waits for manual cancellation from TWS to capture the callback

Usage:
    python test_order_recovery.py --symbol AAPL --qty 1 --account DU123456 --port 7497
"""

import asyncio
import argparse
from datetime import datetime
from ib_insync import IB, Stock, MarketOrder, StopOrder


class OrderRecoveryTest:
    def __init__(self, symbol='AAPL', qty=10, account='DUP073404', port=7497, host='127.0.0.1'):
        self.symbol = symbol
        self.qty = qty
        self.account = account
        self.port = port
        self.host = host
        self.ib = None
        self.contract = None
        self.market_order_id = None
        self.stop_order_id = None
        self.executed_price = None
        self.client_id = 999  # Use a fixed client ID for testing

    def on_order_status(self, trade):
        """Callback for order status changes"""
        print(f"\n[ORDER STATUS] Order {trade.order.orderId}: {trade.orderStatus.status}")
        print(f"  Filled: {trade.orderStatus.filled}/{trade.order.totalQuantity}")
        print(f"  Remaining: {trade.orderStatus.remaining}")
        print(f"  AvgFillPrice: {trade.orderStatus.avgFillPrice}")

    def on_exec_details(self, trade, fill):
        """Callback for execution details"""
        print(f"\n[EXECUTION] Order {trade.order.orderId} executed:")
        print(f"  ExecId: {fill.execution.execId}")
        print(f"  Time: {fill.execution.time}")
        print(f"  Side: {fill.execution.side}")
        print(f"  Shares: {fill.execution.shares}")
        print(f"  Price: {fill.execution.price}")
        print(f"  AvgPrice: {fill.execution.avgPrice}")

        # Store executed price for stop order
        if trade.order.orderId == self.market_order_id:
            self.executed_price = fill.execution.avgPrice

    def on_error(self, reqId, errorCode, errorString, contract):
        """Callback for errors"""
        print(f"\n[ERROR] ReqId: {reqId}, Code: {errorCode}, Msg: {errorString}")

    def on_cancel_order(self, trade):
        """Callback for cancelled orders"""
        print(f"\n[CANCELLED] Order {trade.order.orderId} was cancelled")
        print(f"  Status: {trade.orderStatus.status}")

    async def connect(self):
        """Connect to TWS"""
        print(f"\n{'='*60}")
        print(f"[CONNECT] Connecting to TWS at {self.host}:{self.port} with clientId={self.client_id}")
        print(f"{'='*60}")

        self.ib = IB()

        # Register callbacks
        self.ib.orderStatusEvent += self.on_order_status
        self.ib.execDetailsEvent += self.on_exec_details
        self.ib.errorEvent += self.on_error
        self.ib.cancelOrderEvent += self.on_cancel_order

        await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
        print(f"[CONNECT] Connected successfully")

        # Setup contract
        self.contract = Stock(self.symbol, 'SMART', 'USD')
        await self.ib.qualifyContractsAsync(self.contract)
        print(f"[CONTRACT] {self.contract}")

    async def disconnect(self):
        """Disconnect from TWS"""
        print(f"\n{'='*60}")
        print(f"[DISCONNECT] Disconnecting from TWS...")
        print(f"{'='*60}")

        if self.ib:
            self.ib.disconnect()
            self.ib = None
        print(f"[DISCONNECT] Disconnected")

    async def place_market_order(self):
        """Place a market order"""
        print(f"\n{'='*60}")
        print(f"[MARKET ORDER] Placing market order: BUY {self.qty} {self.symbol}")
        print(f"{'='*60}")

        order = MarketOrder('BUY', self.qty, account=self.account)
        order.outsideRth = True  # Allow outside regular trading hours
        order.tif = 'GTC'  # Good Till Cancelled (works outside RTH)
        trade = self.ib.placeOrder(self.contract, order)
        self.market_order_id = trade.order.orderId

        print(f"[MARKET ORDER] Order placed with ID: {self.market_order_id}")

        # Wait for fill
        print(f"[MARKET ORDER] Waiting for fill...")
        timeout = 60  # 60 seconds timeout
        start = asyncio.get_event_loop().time()

        while not trade.isDone():
            if asyncio.get_event_loop().time() - start > timeout:
                print(f"[MARKET ORDER] Timeout waiting for fill - current status: {trade.orderStatus.status}")
                if trade.orderStatus.status == 'Cancelled':
                    raise Exception("Market order was cancelled")
                break
            await asyncio.sleep(0.1)

        if self.executed_price:
            print(f"[MARKET ORDER] Order filled at avg price: {self.executed_price}")
        else:
            print(f"[MARKET ORDER] Order not filled - status: {trade.orderStatus.status}")
            raise Exception(f"Market order not filled: {trade.orderStatus.status}")

        return self.executed_price

    async def place_stop_order(self, exec_price):
        """Place a stop-loss order at 2% below executed price"""
        print(f"\n{'='*60}")
        print(f"[STOP ORDER] Placing stop-loss order 2% below {exec_price}")
        print(f"{'='*60}")

        # Calculate stop price (2% below for a long position)
        stop_price = round(exec_price * 0.98, 2)

        print(f"[STOP ORDER] Stop price: {stop_price}")

        order = StopOrder('SELL', self.qty, stopPrice=stop_price, account=self.account)
        order.outsideRth = True
        order.tif = 'GTC'
        trade = self.ib.placeOrder(self.contract, order)
        self.stop_order_id = trade.order.orderId

        print(f"[STOP ORDER] Order placed with ID: {self.stop_order_id}")

        # Wait a bit to ensure order is accepted
        await asyncio.sleep(2)

        print(f"[STOP ORDER] Order status: {trade.orderStatus.status}")

        if trade.orderStatus.status == 'Cancelled':
            raise Exception(f"Stop order was cancelled: {trade.orderStatus.status}")

    async def retrieve_pending_orders(self):
        """Retrieve pending orders after reconnection"""
        print(f"\n{'='*60}")
        print(f"[RETRIEVE] Retrieving pending orders...")
        print(f"{'='*60}")

        # Request all open orders
        await self.ib.reqAllOpenOrdersAsync()
        await asyncio.sleep(1)

        open_orders = self.ib.openOrders()
        print(f"[RETRIEVE] Found {len(open_orders)} open orders")

        for order in open_orders:
            print(f"  Order {order.orderId}: {order.action} {order.totalQuantity} @ {order.orderType}")
            if hasattr(order, 'auxPrice'):
                print(f"    Stop price: {order.auxPrice}")

        # Find our stop order
        stop_trade = None
        for trade in self.ib.openTrades():
            if trade.order.orderId == self.stop_order_id:
                stop_trade = trade
                break

        if stop_trade:
            print(f"\n[RETRIEVE] Successfully retrieved stop order {self.stop_order_id}")
            print(f"  Status: {stop_trade.orderStatus.status}")
            return True
        else:
            print(f"\n[RETRIEVE] WARNING: Could not find stop order {self.stop_order_id}")
            return False

    async def wait_for_cancellation(self):
        """Wait for manual cancellation from TWS"""
        print(f"\n{'='*60}")
        print(f"[WAIT] Waiting for manual cancellation from TWS...")
        print(f"  Please cancel order {self.stop_order_id} from TWS")
        print(f"{'='*60}")

        # Monitor for cancellation
        start_time = asyncio.get_event_loop().time()
        timeout = 300  # 5 minutes

        while True:
            # Check if stop order still exists
            stop_exists = False
            for trade in self.ib.openTrades():
                if trade.order.orderId == self.stop_order_id:
                    stop_exists = True
                    if trade.orderStatus.status in ['Cancelled', 'Inactive']:
                        print(f"\n[SUCCESS] Order {self.stop_order_id} cancelled!")
                        print(f"  Status: {trade.orderStatus.status}")
                        return True
                    break

            if not stop_exists:
                # Check if order was cancelled and removed from openTrades
                print(f"\n[SUCCESS] Order {self.stop_order_id} no longer in open trades (cancelled)")
                return True

            # Check timeout
            if asyncio.get_event_loop().time() - start_time > timeout:
                print(f"\n[TIMEOUT] Waited {timeout} seconds without cancellation")
                return False

            await asyncio.sleep(1)

    async def run(self):
        """Run the full test sequence"""
        try:
            # Step 1: Connect and place market order
            await self.connect()
            exec_price = await self.place_market_order()

            # Step 2: Place stop order
            await self.place_stop_order(exec_price)

            # Step 3: Disconnect
            await self.disconnect()
            await asyncio.sleep(3)

            # Step 4: Reconnect
            await self.connect()

            # Step 5: Retrieve pending orders
            retrieved = await self.retrieve_pending_orders()

            if not retrieved:
                print("\n[FAILED] Could not retrieve stop order after reconnection")
                return

            # Step 6: Wait for manual cancellation
            cancelled = await self.wait_for_cancellation()

            if cancelled:
                print("\n" + "="*60)
                print("[TEST COMPLETED] Successfully captured cancellation callback")
                print("="*60)
            else:
                print("\n[TEST INCOMPLETE] Did not receive cancellation callback")

        except Exception as e:
            print(f"\n[EXCEPTION] {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.ib and self.ib.isConnected():
                self.ib.disconnect()


def main():
    parser = argparse.ArgumentParser(description='Test order callback recovery after reconnection')
    parser.add_argument('--symbol', default='AAPL', help='Stock symbol (default: AAPL)')
    parser.add_argument('--qty', type=int, default=1, help='Quantity to trade (default: 1)')
    parser.add_argument('--account', default='DUP073404', help='IBKR account number (default: DU9518341)')
    parser.add_argument('--port', type=int, default=7497, help='TWS port (default: 7497 for paper trading)')
    parser.add_argument('--host', default='127.0.0.1', help='TWS host (default: 127.0.0.1)')

    args = parser.parse_args()

    print("\n" + "="*60)
    print("ORDER RECOVERY TEST")
    print("="*60)
    print(f"Symbol: {args.symbol}")
    print(f"Quantity: {args.qty}")
    print(f"Account: {args.account}")
    print(f"Connection: {args.host}:{args.port}")
    print("="*60)

    test = OrderRecoveryTest(
        symbol=args.symbol,
        qty=args.qty,
        account=args.account,
        port=args.port,
        host=args.host
    )

    asyncio.run(test.run())


if __name__ == '__main__':
    main()
