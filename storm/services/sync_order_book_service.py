from time import sleep

import simplejson as json

from ..exchanges.binance import get_client
from ..models.order_book import OrderBook
from ..utils import get_logger, symbol_lock

logger = get_logger(__file__)
binance = get_client()


class SyncOrderBookService:
    def __init__(self, redis, binance):
        self.redis = redis
        self.subscriber = redis.pubsub()
        self.binance = binance

        self.subscriber.subscribe('order_book_snapshot')

    # TODO: check passing string or retrieve from redis faster
    def update_order_book(self, message: str, from_cache=False, last_sequence=None):
        response = json.loads(message)

        symbol = response['s']
        epoch = response['E']  # TODO: add to price
        start_sequence = response['U']
        end_sequence = response['u']

        if not self.redis.hget('initialized', symbol) and not from_cache:
            self.redis.publish('order_book_websocket', json.dumps(
                {'type': 'subscribe', 'symbol': symbol}))

            self.redis.rpush('cached_responses:'+symbol, json.dumps(response))
            self.get_order_book_snapshot(symbol)
            return False

        logger.info(
            f"{symbol} update: start sequence {start_sequence}, last sequence {self.redis.hget('last_sequences', symbol)}")

        last_sequence = last_sequence or int(
            self.redis.hget('last_sequences', symbol))
        if start_sequence != last_sequence + 1:
            if not from_cache:
                logger.warning(
                    f"{symbol} reset: {start_sequence}, {self.redis.hget('last_sequences', symbol)}")
                self.redis.hdel('initialized', symbol)
                self.redis.hdel('last_sequences', symbol)
                self.redis.rpush('cached_responses:'+symbol,
                                 json.dumps(response))
                # self.get_order_book_snapshot(symbol)
            return False

        self.sync_orders(response, from_cache)

        return True

    def sync_orders(self, response, from_cache=False):
        symbol = response['s']
        end_sequence = response['u']

        self.redis.hset('last_sequences', symbol, end_sequence)

        order_book_ob = OrderBook.get(symbol)
        order_book = order_book_ob.get_book()  # TODO: cache local order book

        # TODO: update best bid ask
        for price, amount in response['b']:
            price = float(price)
            if float(amount):
                order_book['bids'][price] = amount
            else:
                order_book['bids'].pop(price, None)

        for price, amount in response['a']:
            price = float(price)
            if float(amount):
                order_book['asks'][price] = amount
            else:
                order_book['asks'].pop(price, None)

        order_book_ob.save(order_book)

        # FIXME: not sure if this is necessary
        if from_cache:
            return

        self.consolidate_order_book(response, order_book, order_book_ob)

    def consolidate_order_book(self, response, order_book, order_book_ob):
        if order_book['bids'] and order_book['asks']:
            symbol = response['s']

            best_bid = max(order_book['bids'])
            best_ask = min(order_book['asks'])

            if best_bid > best_ask:
                logger.warning(
                    f"{symbol} bid ask cross! best bid {best_bid}, best ask {best_ask}")
                self.redis.hdel('initialized', symbol)

            else:
                best_prices = order_book_ob.best_prices
                if best_prices and best_prices['bids'] == best_bid and best_prices['asks'] == best_ask:
                    return

                self.redis.hset('updated_best_prices', symbol, 1)
                logger.info(
                    f'Update best prices for {symbol}: best bid {best_bid}, best ask {best_ask}')
                order_book_ob.best_prices = {
                    'bids': best_bid, 'asks': best_ask}

    def get_order_book_snapshot(self, symbol, sync=False):
        if sync:
            data = binance.get_order_book(symbol)
        else:
            self.redis.publish('order_book_websocket', json.dumps(
                {'type': 'retrieve', 'symbol': symbol}))
            while True:
                message = self.subscriber.get_message()
                if message and message['type'] == 'message':
                    if symbol in message['data']:
                        data = json.loads(message['data'])[symbol]
                        data = json.loads(data)
                        break
                sleep(0.01)

        if self.redis.hget('initialized', symbol):
            return

        if not (last_update_id := data.pop('lastUpdateId', None)):
            return

        # now = int(time() * 1000)

        # bids = {float(price): [size, now] for price, size in data['bids']}
        # asks = {float(price): [size, now] for price, size in data['asks']}

        if self.apply_cached_response(symbol, last_update_id):
            order_book_ob = OrderBook.get(symbol)
            order_book_ob.clear()
            order_book = order_book_ob.get_book()

            for price, amount in data['bids']:
                order_book['bids'][float(price)] = amount

            for price, amount in data['asks']:
                order_book['asks'][float(price)] = amount

            order_book_ob.save(order_book)

    def apply_cached_response(self, symbol, last_update_id):
        if not (responses := self.redis.lrange('cached_responses:'+symbol, 0, -1)):
            return

        with symbol_lock(self.redis, symbol):
            logger.info(f'Got the lock for {symbol}')

            for raw_response in responses:
                response = json.loads(raw_response)

                last_sequence = response['u']
                if last_sequence <= last_update_id:
                    continue

                if not self.redis.hget('initialized', symbol):
                    if not self.has_order_book_initialized(response, last_update_id):
                        continue

                    logger.info(f"Initialized {symbol}")
                    self.redis.hset('initialized', symbol, 1)

                elif not self.is_subsequent_response(response, last_sequence):
                    logger.info(f'Uninitialized {symbol}')
                    # reset initialized status
                    self.redis.hdel('initialized', symbol)
                    break

                # TODO: continue only when update order book succeeded
                self.update_order_book(
                    raw_response, from_cache=True, last_sequence=last_sequence)
                self.redis.hset('last_sequences', symbol, last_sequence)

        self.redis.delete('cached_responses:'+symbol)  # remove stale responses
        logger.info(f'Release the lock for {symbol}')
        return True

    def has_order_book_initialized(self, response, last_update_id):
        if response is None:
            return False
        symbol = response['s']
        start_sequence = response['U']
        end_sequence = response['u']

        logger.info(
            f"Initialized check: {symbol} {start_sequence}, {last_update_id}, {end_sequence}")
        return start_sequence <= int(last_update_id) + 1 <= end_sequence

    def is_subsequent_response(self, response, last_sequence):
        symbol = response['s']
        start_sequence = response['U']

        if not (start_sequence == int(last_sequence) + 1):
            logger.info(
                f"Subsequent response: {symbol}, {start_sequence}, {int(last_sequence) + 1}")
        return start_sequence == int(last_sequence) + 1
