import time
import asyncio

from enum import Enum
from datetime import datetime
from pydantic import BaseModel
from typing import Any
from typing_extensions import Self

from stockmq.rpc import RPCClient


class TxTimeoutError(Exception):
    pass


class TxRejectedError(Exception):
    pass



class Action(str, Enum):
    NEW_ORDER = "NEW_ORDER"
    NEW_STOP_ORDER = "NEW_STOP_ORDER"
    KILL_ORDER = "KILL_ORDER"
    KILL_STOP_ORDER = "KILL_STOP_ORDER"


class OrderType(str, Enum):
    LIMIT = 'L'
    MARKET = 'M'

class TimeInForce(str, Enum):
    FOK = "FILL_OR_KILL"
    IOC = "KILL_BALANCE"
    DAY = "PUT_IN_QUEUE"


class Side(str, Enum):
    BUY = 'B'
    SELL = 'S'


class TransactionState(str, Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    EXECUTED = "EXECUTED"


class Transaction(BaseModel):
    id: int
    action: Action
    board: str
    order_id: int
    created_ts: float
    updated_ts: float
    state: TransactionState
    message: str

class Order(BaseModel):
    id: int
    board: str


class QuikTable:
    def __init__(self, rpc: RPCClient, name: str):
        self.rpc = rpc
        self.name = name

    def __len__(self):
        return int(self.rpc.call("getNumberOf", self.name))

    def __getitem__(self, index):
        r = self.rpc.call("stockmq_get_item", self.name, index)
        if r is None:
            raise IndexError
        return r


class Quik(RPCClient):
    def repl(self, s: str) -> Any:
        return self.call("stockmq_repl", s)

    def get_table(self, name: str) -> QuikTable:
        return QuikTable(self, name)

    TX_SLEEP_TIMEOUT = 0.01

    async def wait_tx(self, tx: Transaction, timeout=1.0) -> Transaction:
        t0 = time.time()
        while True:
            if time.time() - t0 >= timeout:
                raise TxTimeoutError()
            elif tx.state == TransactionState.EXECUTED:
                return tx
            elif tx.state == TransactionState.REJECTED:
                print(tx)
                raise TxRejectedError(tx.message)
            elif tx.state == TransactionState.ACCEPTED:
                await asyncio.sleep(self.TX_SLEEP_TIMEOUT)
                tx = self.update_transaction(tx)
                print(tx)

    def create_order_tx(self, client: str, board: str, ticker: str, tif: TimeInForce, side: Side, price: float, quantity: int) -> Transaction:
        return Transaction(**self.call("stockmq_create_order", client, board, ticker, tif.value, side.value, price, quantity))

    def create_stop_order_tx(self, client: str, board: str, ticker: str, tif: TimeInForce, side: Side, price: float, stop_price: float, quantity: int) -> Transaction:
        return Transaction(**self.call("stockmq_create_simple_stop_order", client, board, ticker, tif.value, side.value, price, stop_price, quantity))

    def cancel_order_tx(self, client: str, board: str, ticker: str, order_id: int) -> Transaction:
        return Transaction(**self.call("stockmq_cancel_order", client, board, ticker, order_id))

    def cancel_stop_order_tx(self, client: str, board: str, ticker: str, order_id: int) -> Transaction:
        return Transaction(**self.call("stockmq_cancel_stop_order", client, board, ticker, order_id))

    def update_transaction(self, tx: Transaction) -> Transaction:
        return Transaction(**self.call("stockmq_update_tx", tx.dict()))

    async def create_order(self, client: str, board: str, ticker: str, tif: TimeInForce, side: Side, price: float, quantity: int, timeout: float = 1.0) -> Transaction:
        return await self.wait_tx(self.create_order_tx(client, board, ticker, tif, side, price, quantity), timeout)

    async def create_stop_order(self, client: str, board: str, ticker: str, tif: TimeInForce, side: Side, price: float, stop_price: float, quantity: int, timeout: float = 1.0) -> Transaction:
        return await self.wait_tx(self.create_stop_order_tx(client, board, ticker, tif, side, price, stop_price, quantity), timeout)

    async def cancel_order(self, client: str, board: str, ticker: str, order_id: int, timeout: float = 1.0) -> Transaction:
        return await self.wait_tx(self.cancel_order_tx(client, board, ticker, order_id), timeout)

    async def cancel_stop_order(self, client: str, board: str, ticker: str, order_id: int, timeout: float = 1.0) -> Transaction:
        return await self.wait_tx(self.cancel_stop_order_tx(client, board, ticker, order_id), timeout)
