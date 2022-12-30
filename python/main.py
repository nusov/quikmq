import asyncio
import time

from tinkoff.invest import OrderType

from stockmq.rpc import RPCClient
from stockmq.api import Quik, TimeInForce, Side, QuikTable


import zmq
import msgpack

rpc = RPCClient("tcp://10.211.55.3:8004")
api = Quik("tcp://10.211.55.3:8004")

client = "CLIENT"
board = "SPBFUT"
ticker = "VBU2"



async def main():
    # Create transaction to BUY and wait for completion
    tx = await api.create_order(client, board, ticker, TimeInForce.DAY, Side.BUY, 1600.0, 1)
    print(tx)
    print(tx.updated_ts - tx.created_ts)

    # Create transaction to cancel the order
    tx = await api.cancel_order(client, board, ticker, tx.order_id, timeout=4.0)
    print(tx)
    print(tx.updated_ts - tx.created_ts)

    # Create transaction to BUY and wait for completion
    tx = await api.create_stop_order(client, board, ticker, TimeInForce.DAY, Side.BUY, 1600.0, 1599.0, 1)
    print(tx)
    print(tx.updated_ts - tx.created_ts)

    # Create transaction to cancel the stop order
    tx = await api.cancel_stop_order(client, board, ticker, tx.order_id, timeout=4.0)
    print(tx)
    print(tx.updated_ts - tx.created_ts)


if __name__ == '__main__':
    asyncio.run(tx())
