# from celery_app import app
from binance import Binance
import simplejson as json
from models.order_book import OrderBook
from redis_client import get_client
from multiprocessing import Pool, Manager


redis = get_client()

import pdb

def callback(ob):
    pdb.set_trace()
    print(ob)

# @app.task
def update_order_book(response: dict):
    symbol = response['s']
    epoch = response['E']  # TODO: add to price
    start_sequence = response['U']
    end_sequence = response['u']

    # move to one off check
    if not has_order_book_initialized(response):
        redis.lpush('cached_responses:'+symbol, json.dumps(response))  # TODO: to slow, need to separate response receive and handling
        # return pool.apply(get_order_book_snapshot, (symbol, ))
        # pool.apply(get_order_book_snapshot, (symbol, ))
        get_order_book_snapshot(symbol)
        return

    if redis.hget('last_sequences', symbol):
        if start_sequence != int(redis.hget('last_sequences', symbol)) + 1:  # TODO: use incr
            redis.hdel('last_update_ids', symbol)  # invalidate has_order_book_initialized
            return

    sync_orders(response)

def sync_orders(response):
    symbol = response['s']
    end_sequence = response['u']

    redis.hset('last_sequences', symbol, end_sequence)

    order_book_ob = OrderBook.get(symbol)
    order_book = order_book_ob.get_book()

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

    print(order_book)
    # FIXME: not sure if this is necessary
    consolidate_order_book(response, order_book, order_book_ob)


def consolidate_order_book(response, order_book, order_book_ob):
    symbol = response['s']
    if order_book['bids'] and order_book['asks']:
        best_bid = max(order_book['bids'])
        best_ask = min(order_book['asks'])

        if best_bid > best_ask:
            pass
            # last_update_ids[symbol] = None
            # order_book_ob.best_prices = {'bids': 0, 'asks': 0}
            # order_book_ob.clear()
            # cached_responses.setdefault(symbol, []).append(response)
            # get_order_book_snapshot(symbol)

        else:
            order_book_ob.best_prices = {'bids': best_bid, 'asks': best_ask}


# @app.task
def get_order_book_snapshot(symbol):
    # print('Get order book for:', symbol)
    data = Binance.get_order_book(symbol, 9999)
    # print(data)

    # TODO: use the DATA!!!!
    last_update_id = data.pop('lastUpdateId', None)

    if last_update_id is None:
        # return pool.apply_async(get_order_book_snapshot, (symbol, ))
        return

    redis.hset('last_update_ids', symbol, last_update_id)

    order_book_ob = OrderBook.get(symbol)
    order_book_ob.clear()
    order_book = order_book_ob.get_book()

    for price, amount in data['bids']:
        price = float(price)
        order_book['bids'][float(price)] = float(amount)

    for price, amount in data['asks']:
        price = float(price)
        order_book['asks'][float(price)] = float(amount)

    # return order_book
    # now = int(time() * 1000)

    # bids = {float(price): [size, now] for price, size in data['bids']}
    # asks = {float(price): [size, now] for price, size in data['asks']}

    apply_cached_response(symbol)


def apply_cached_response(symbol):
    cache_applied = False

    for response in redis.lrange('cached_responses:'+symbol, 0, -1):
        response = json.loads(response)
        if not has_order_book_initialized(response):
            continue

        if not is_subsequent_response(response):
            # TODO: clear responses cache?
            return
            # return pool.apply_async(get_order_book_snapshot, (symbol, ))

        update_order_book(response)
        redis.hset('last_sequences', symbol, response['u'])
        cache_applied = True

    if cache_applied:
        redis.hdel('last_sequences', symbol)
        redis.delete('cached_responses:'+symbol)


def has_order_book_initialized(response):
    symbol = response['s']
    end_sequence = response['u']

    if not (last_update_id := redis.hget('last_update_ids', symbol)):
        return False

    return end_sequence > int(last_update_id)


def is_subsequent_response(response):
    symbol = response['s']
    start_sequence = response['U']
    end_sequence = response['u']

    if not redis.hget('last_sequences', symbol):
        return False

    return start_sequence <= int(redis.hget('last_sequences', symbol)) + 1 <= end_sequence


# last_update_ids = manager.dict()
# last_sequences = manager.dict()
# cached_responses = manager.dict()

if __name__ == '__main__':
    import threading
    import asyncio

    from binance import websocket_pool
        
    threading.Thread(target=asyncio.run, args=(websocket_pool(), )).start()

    response = {
        "e":"depthUpdate",
        "E":1635587664771,
        "s":"BNBETH",
        "U":1024042672,
        "u":1024042672,
        "b":[],
        "a":[
            ["0.12210000","213.67300000"]
        ]
    }
    update_order_book(response)
