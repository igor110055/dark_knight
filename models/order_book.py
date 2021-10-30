from redis_client import get_client


class OrderBook:
    _registry = {}

    @classmethod
    def get(cls, symbol):
        return cls._registry.setdefault(symbol, cls(symbol))

    def __init__(self, symbol) -> None:
        self.symbol = symbol
        self.bids_key = f"{self.symbol}:bids"
        self.asks_key = f"{self.symbol}:asks"
        self.redis = get_client()
        self.book = None

        OrderBook._registry[symbol] = self

    def populate(self, bids: dict, asks: dict):
        self.clear()

        if bids:
            self.redis.hmset(f"{self.bids_key}", bids)

        if asks:
            self.redis.hmset(f"{self.asks_key}", asks)

    # def delete(self, side, price):
    #     self.redis.srem(f"{self.symbol}:{side}:prices", price)
    #     self.redis.hdel(f"{self.symbol}:{side}", price)

    # def set(self, side, price, amount):
    #     self.redis.sadd(f"{self.symbol}:{side}:prices", price)
    #     self.redis.hset(f"{self.symbol}:{side}", price, amount)

    def clear(self, deep=True):
        if deep:
            self.redis.delete(self.bids_key, self.asks_key)
        self.redis.delete(f"best_prices:{self.symbol}:*")

    def save(self, book):
        self.populate(book['bids'], book['asks'])

    def get_best(self, top=None):
        return {'bids': self.get_best_bids(top), 'asks': self.get_best_asks(top)}

    def get_best_bids(self, top=None):
        if not (bids_prices := self.redis.hkeys(self.bids_key)):
            return []

        bids_prices = sorted([float(price) for price in bids_prices], reverse=True)

        if top is not None:
            bids_prices = bids_prices[:top]

        bids = list(zip(bids_prices, self.redis.hmget(
            self.bids_key, bids_prices)))
        return bids

    def get_best_asks(self, top=None):
        if not (asks_prices := self.redis.hkeys(self.asks_key)):
            return []

        asks_prices = sorted(float(price) for price in asks_prices)

        if top is not None:
            asks_prices = asks_prices[:top]

        asks = list(zip(asks_prices, self.redis.hmget(
            self.asks_key, asks_prices)))
        return asks

    def get_book(self):
        return {
            'bids': {float(price): amount for price, amount in self.get_best_bids()},
            'asks': {float(price): amount for price, amount in self.get_best_asks()}
        }

    # Important: for arbitrage
    @property
    def best_prices(self):
        bids = self.redis.hget(f"best_orders:{self.symbol}", "bids") or 0
        asks = self.redis.hget(f"best_orders:{self.symbol}", "asks") or 0
        return dict(bids=float(bids), asks=float(asks))

    @best_prices.setter
    def best_prices(self, best_prices):
        self.redis.hset(f"best_orders:{self.symbol}",
                        "asks", best_prices['asks'])
        self.redis.hset(f"best_orders:{self.symbol}",
                        "bids", best_prices['bids'])
