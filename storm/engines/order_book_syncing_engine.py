import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import simplejson as json
import zmq

from ..clients.redis_client import get_client
from ..exchanges.binance import get_client as get_binance_client
from ..models.order_book import OrderBook
from ..utils import get_logger, symbol_lock

redis = get_client(a_sync=False)
binance = get_binance_client()
logger = get_logger(__file__)
loop = asyncio.get_event_loop()

SLOW_POOL = None
FAST_POOL = None

# TODO: check passing string or retrieve from redis faster
def update_order_book(message: str, from_cache=False, last_sequence=None, redis=redis):
    response = json.loads(message)

    symbol = response['s']
    epoch = response['E']  # TODO: add to price
    start_sequence = response['U']
    end_sequence = response['u']

    if not redis.hget('initialized', symbol) and not from_cache:
        redis.rpush('cached_responses:'+symbol, json.dumps(response))

        get_order_book_snapshot(symbol)
        if redis.setnx('getting_snapshot:'+symbol, 30):
            get_order_book_snapshot(symbol)
            # loop.run_in_executor(SLOW_POOL, get_order_book_snapshot, symbol)
            redis.delete('getting_snapshot:'+symbol)
        return 
        # return get_order_book_snapshot(symbol)

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
            if best_prices and best_prices['bids'] == best_bid and best_prices['asks'] == best_ask:
                return

            redis.hset('updated_best_prices', symbol, 1)
            logger.info(
                f'Update best prices for {symbol}: best bid {best_bid}, best ask {best_ask}')
            order_book_ob.best_prices = {'bids': best_bid, 'asks': best_ask}

import pdb
def get_order_book_snapshot(symbol):
    # data = binance.get_order_book(symbol)

    # pdb.set_trace()
    order_book_socket.send_json({'type': 'retrieve_order_book'})
    data = json.loads(order_book_socket.recv_json())
    if redis.hget('initialized', symbol):
        return

    if not (last_update_id := data.pop('lastUpdateId', None)):
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

        for raw_response in responses:
            response = json.loads(raw_response)

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

            loop.run_in_executor(SLOW_POOL, update_order_book, raw_response, True, last_sequence)
            # update_order_book(raw_response, from_cache=True, last_sequence=last_sequence)
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


if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://127.0.0.1:5555')

    loop = asyncio.get_event_loop()

    # FAST_POOL = ProcessPoolExecutor(16)
    FAST_POOL = ThreadPoolExecutor(8)
    SLOW_POOL = ThreadPoolExecutor(16)

    # import pdb; pdb.set_trace()
    order_book_socket = context.socket(zmq.REQ)
    order_book_socket.connect('tcp://127.0.0.1:5556')

    # import time
    # for symbol in ['BTCUSDT', 'ETHUSDT', 'MATICUSDT', 'LUNAUSDT']:
    #     order_book_socket.send_json({'type': 'update_symbol', 'symbol': symbol})
    #     message = order_book_socket.recv_string()
    #     logger.info(message)

    #     for _ in range(100):

    #         order_book_socket.send_json({'type': 'retrieve_order_book'})
    #         message = order_book_socket.recv_string()
    #         logger.info(message)

            # order_book_socket.send_json({'type': 'get_order_book', 'symbol': symbol})
            # message = order_book_socket.recv_string()
            # logger.info(message)

        # time.sleep(0.4)
        

    logger.info('Ready to handle order book update')
    while True:
        message = socket.recv_string()
        # update_order_book(message)

        # TODO: fix zmq connection in process
        loop.run_in_executor(FAST_POOL, update_order_book, message)

        socket.send_string('')
