import asyncio
from concurrent.futures import ProcessPoolExecutor

import simplejson as json

from ..clients.redis_client import get_client
from ..exchanges.binance import get_client as get_binance_client
from ..models.order_book import OrderBook
from ..utils import get_logger, symbol_lock

redis = get_client(a_sync=False)
binance = get_binance_client()
logger = get_logger(__file__)


POOL = ProcessPoolExecutor(4)
# POOL = None


def update_order_book(response: dict, redis=redis, from_cache=False, last_sequence=None):
    symbol = response['s']
    epoch = response['E']  # TODO: add to price
    start_sequence = response['U']
    end_sequence = response['u']

    if not redis.hget('initialized', symbol) and not from_cache:
        # TODO: too slow, need to separate response receive and handling
        redis.rpush('cached_responses:'+symbol, json.dumps(response))
        loop = asyncio.get_event_loop()
        # TODO: move to process
        return loop.run_in_executor(POOL, get_order_book_snapshot, symbol)

    logger.info(
        f"{symbol} update: start sequence {start_sequence}, last sequence {redis.hget('last_sequences', symbol)}")

    last_sequence = last_sequence or int(redis.hget('last_sequences', symbol))
    if start_sequence != last_sequence + 1:
        if not from_cache:
            logger.warning(
                f"{symbol} reset: {start_sequence}, {redis.hget('last_sequences', symbol)}")
            redis.hdel('initialized', symbol)
            redis.hdel('last_sequences', symbol)
            redis.rpush('cached_responses:'+symbol, json.dumps(response))
        return

    sync_orders(response)


def sync_orders(response):
    symbol = response['s']
    end_sequence = response['u']

    redis.hset('last_sequences', symbol, end_sequence)

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
    consolidate_order_book(response, order_book, order_book_ob)


def consolidate_order_book(response, order_book, order_book_ob):
    if order_book['bids'] and order_book['asks']:
        symbol = response['s']

        best_bid = max(order_book['bids'])
        best_ask = min(order_book['asks'])

        if best_bid > best_ask:
            logger.warning(
                f"{symbol} bid ask cross! best bid {best_bid}, best ask {best_ask}")
            redis.hdel('initialized', symbol)

        else:
            best_prices = order_book_ob.best_prices
            if best_prices['bids'] == best_bid and best_prices['asks'] == best_ask:
                return

            redis.hset('updated_best_prices', symbol, 1)
            logger.info(
                f'Update best prices for {symbol}: best bid {best_bid}, best ask {best_ask}')
            order_book_ob.best_prices = {'bids': best_bid, 'asks': best_ask}


def get_order_book_snapshot(symbol):
    data = binance.get_order_book(symbol)
    if redis.hget('initialized', symbol):
        return

    if not (last_update_id := data.pop('lastUpdateId')):
        return

    # now = int(time() * 1000)

    # bids = {float(price): [size, now] for price, size in data['bids']}
    # asks = {float(price): [size, now] for price, size in data['asks']}

    if apply_cached_response(symbol, last_update_id):
        order_book_ob = OrderBook.get(symbol)
        order_book_ob.clear()
        order_book = order_book_ob.get_book()

        for price, amount in data['bids']:
            order_book['bids'][float(price)] = amount

        for price, amount in data['asks']:
            order_book['asks'][float(price)] = amount

        order_book_ob.save(order_book)


def apply_cached_response(symbol, last_update_id):
    if not (responses := redis.lrange('cached_responses:'+symbol, 0, -1)):
        return

    with symbol_lock(redis, symbol):
        logger.info(f'Got the lock for {symbol}')

        for response in responses:
            response = json.loads(response)

            last_sequence = response['u']
            if last_sequence <= last_update_id:
                continue

            if not redis.hget('initialized', symbol):
                if not has_order_book_initialized(response, last_update_id):
                    continue

                logger.info(f"Initialized {symbol}")
                redis.hset('initialized', symbol, 1)

            elif not is_subsequent_response(response, last_sequence):
                logger.info(f'Uninitialized {symbol}')
                redis.hdel('initialized', symbol)  # reset initialized status
                return False

            update_order_book(response, from_cache=True,
                              last_sequence=last_sequence)
            redis.hset('last_sequences', symbol, last_sequence)

    redis.delete('cached_responses:'+symbol)  # remove stale responses
    logger.info(f'Release the lock for {symbol}')
    return True


def has_order_book_initialized(response, last_update_id):
    if response is None:
        return False
    symbol = response['s']
    start_sequence = response['U']
    end_sequence = response['u']

    logger.info(
        f"Initialized check: {symbol} {start_sequence}, {last_update_id}, {end_sequence}")
    return start_sequence <= int(last_update_id) + 1 <= end_sequence


def is_subsequent_response(response, last_sequence):
    symbol = response['s']
    start_sequence = response['U']

    if not (start_sequence == int(last_sequence) + 1):
        logger.info(
            f"Subsequent response: {symbol}, {start_sequence}, {int(last_sequence) + 1}")
    return start_sequence == int(last_sequence) + 1


async def handle_response():
    while True:
        responses = redis.rpop('responses', 20)
        if not responses:
            await asyncio.sleep(0)
            continue

        for response in responses:
            response = json.loads(response)
            update_order_book(response)
