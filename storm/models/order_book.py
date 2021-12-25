import simplejson as json
from typing import Dict

from ..clients.redis_client import get_client


class OrderBook:
    _registry: Dict[str, OrderBook] = {}

    @classmethod
    def get(cls, symbol):
        return cls._registry.setdefault(symbol, cls(symbol))

    def __init__(self, symbol) -> None:
        self.symbol = symbol
        self.bids_key = f"{self.symbol}:bids"
        self.asks_key = f"{self.symbol}:asks"
        self.redis = get_client()

    def clear(self):
        self.redis.delete(self.bids_key, self.asks_key)
        self.redis.delete(f"best_prices:{self.symbol}:*")

    def save(self, book):
        self.redis.delete(self.bids_key, self.asks_key)
        self.redis.hmset(self.bids_key, book["bids"])
        self.redis.hmset(self.asks_key, book["asks"])

    def get_book(self):
        return {
            "bids": {
                float(price): amount
                for price, amount in self.redis.hgetall(self.bids_key).items()
            },
            "asks": {
                float(price): amount
                for price, amount in self.redis.hgetall(self.asks_key).items()
            },
        }

    # Important: for arbitrage
    @property
    def best_prices(self):
        prices = self.redis.get(f"best_prices:{self.symbol}")
        if not prices:
            return None
        return json.loads(prices)

    @best_prices.setter
    def best_prices(self, best_prices):
        self.redis.set(f"best_prices:{self.symbol}", json.dumps(best_prices), 1)
