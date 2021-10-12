from ..redis_client import get_client


class OrderBook:
    def __init__(self, symbol) -> None:
        self.symbol = symbol
        self.bids_key = f"{self.symbol}:bids"
        self.asks_key = f"{self.symbol}:asks"
        self.redis = get_client()

    def populate(self, bids: dict, asks: dict):
        _bids = {amount: float(price) for price, amount in bids.items()}
        _asks = {amount: float(price) for price, amount in asks.items()}

        self.redis.zadd(self.bids_key, _bids)
        self.redis.zadd(self.asks_key, _asks)

    def get(self) -> dict:
        bids = self.get_best_bids()
        asks = self.get_best_asks()
        return {'bids': bids, 'asks': asks}

    def get_best_bids(self, top=-1):
        return self.redis.zrange(self.bids_key, 0, top, desc=True, withscores=True)

    def get_best_asks(self, top=-1):
        return self.redis.zrange(self.asks_key, 0, top, desc=False, withscores=True)
