from redis_client import get_client

import pdb


class OrderBook:
    _registry = {}

    @classmethod
    def get(cls, symbol):
        return cls._registry.setdefault(symbol, cls(symbol))

    def __init__(self, symbol) -> None:
        self.symbol = symbol
        self.bids_key = f"{self.symbol}:bids"
        self.asks_key = f"{self.symbol}:asks"
        self.book = None
        self.redis = get_client()
        OrderBook._registry[symbol] = self

    def populate(self, bids: dict, asks: dict):
        self.clear()

        self.redis.hmset(f"{self.bids_key}", bids)
        self.redis.hmset(f"{self.asks_key}", asks)

        self.redis.sadd(f"{self.bids_key}:prices", *bids)
        self.redis.sadd(f"{self.asks_key}:prices", *asks)

    def delete(self, side, price):
        self.redis.srem(f"{self.symbol}:{side}:prices", price)
        self.redis.hdel(f"{self.symbol}:{side}", price)

    def set(self, side, price, amount):
        self.redis.sadd(f"{self.symbol}:{side}:prices", price)
        self.redis.hset(f"{self.symbol}:{side}", price, amount)

    def clear(self):
        self.redis.delete(self.bids_key, self.asks_key, f"{self.bids_key}:prices", f"{self.asks_key}:prices")

    # TODO: apply all operations at once
    def save(self):
        self.populate(self.book['bids'], self.book['asks'])

    def get_best(self, top=-1):
        return {'bids': self.get_best_bids(top), 'asks': self.get_best_asks(top)}

    def get_best_bids(self, top=-1):
        bids_prices = self.redis.sort(f"{self.bids_key}:prices", 0, top, desc=True)
        if not bids_prices:
            return []

        bids = list(zip(bids_prices, self.redis.hmget(self.bids_key, bids_prices)))
        return bids

    def get_best_asks(self, top=-1):
        asks_prices = self.redis.sort(f"{self.asks_key}:prices", 0, top)
        if not asks_prices:
            return []

        asks = list(zip(asks_prices, self.redis.hmget(self.asks_key, asks_prices)))
        return asks
