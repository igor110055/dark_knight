import time
from functools import lru_cache
from uuid import uuid4

from ..clients.rest_client import get_session
from ..utils import get_logger
from .binance_helpers import SYMBOLS, allow_async, get, post

logger = get_logger(__file__)


REST_URL = "https://api.binance.com"
WS_URL = "wss://stream.binance.com:9443/ws"

session = get_session(REST_URL)


class Binance:
    def create_order(self, side, order_type, symbol, quantity, on_quote=False):
        timestamp = int(time.time() * 1000)
        data = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "timestamp": timestamp,
        }
        if on_quote:
            data["quoteOrderQty"] = quantity
        else:
            data["quantity"] = quantity
        return post("api/v3/order", data)

    def get_order(self, symbol, order_id):
        timestamp = int(time.time() * 1000)
        params = {
            "symbol": symbol,
            "orderId": order_id,
            "recvWindow": 5000,
            "timestamp": timestamp,
        }

        return get("api/v3/order", params=params)

    def load_markets(self):
        return get("api/v3/exchangeInfo")


@lru_cache
def get_min_quantity(symbol):
    if not (symbol := SYMBOLS.get(symbol)):
        return None
    for filter in eval(symbol["filters"]):
        if filter["filterType"] == "LOT_SIZE":
            return filter["minQty"].rstrip("0")


def get_client(init_symbols=False):
    return Binance(init_symbols)


@allow_async
def get_order_book(symbol):
    request_uuid = uuid4()
    logger.info(f"[{request_uuid}] GET order book request for {symbol}")
    resp = get(f"api/v3/depth?symbol={symbol}", raw=True)
    logger.info(
        f"[{request_uuid}] GET order book response for {symbol}: {resp.content.decode()[:40]}"
    )
    return resp.json()


@allow_async
def get_orders(symbol, limit=100):
    timestamp = int(time.time() * 1000)
    params = {
        "symbol": symbol,
        "limit": limit,
        "recvWindow": 5000,
        "timestamp": timestamp,
    }

    return get("api/v3/allOrders", params=params)


@allow_async
def get_balances():
    timestamp = int(time.time() * 1000)
    params = {"recvWindow": 5000, "timestamp": timestamp}

    data = get("api/v3/account", params)
    balances = {
        balance["asset"]: balance["free"]
        for balance in data["balances"]
        if _is_larger_than_zero(balance["free"])
    }
    return balances


def _is_larger_than_zero(balance):
    return all((balance != "0.00000000", balance != "0.00"))
