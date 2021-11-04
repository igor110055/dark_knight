import asyncio

import simplejson as json

from .exchanges.binance import Binance
from .exchanges.binance import get_client as get_binance_client
from .models.order_book import OrderBook
from .clients.redis_client import get_client

redis = get_client(a_sync=False)


def update_order_book(response: dict, redis=redis, from_cache=False):
    symbol = response['s']
    epoch = response['E']  # TODO: add to price
    start_sequence = response['U']
    end_sequence = response['u']

    # move to one off check
    # while not response or not has_order_book_initialized(response):
    if not redis.hget('initialized', symbol):
        redis.rpush('cached_responses:'+symbol, json.dumps(response))  # TODO: to slow, need to separate response receive and handling
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(None, get_order_book_snapshot, symbol)

    # if redis.hget('last_sequences', symbol):
    # if redis.hget('initialized', symbol):
    print(symbol, 'start', start_sequence, redis.hget('last_sequences', symbol))
    if start_sequence != int(redis.hget('last_sequences', symbol)) + 1:  # TODO: use incr
        if not from_cache:
            print('reset', start_sequence, redis.hget('last_sequences', symbol))
            redis.hdel('initialized', symbol)
            redis.hdel('last_update_ids', symbol)  # invalidate has_order_book_initialized
            redis.hdel('last_sequences', symbol)
            redis.rpush('cached_responses:'+symbol, json.dumps(response))
        return

    sync_orders(response)

def sync_orders(response):
    print('success')
    symbol = response['s']
    end_sequence = response['u']

    redis.hset('last_sequences', symbol, end_sequence)

    order_book_ob = OrderBook.get(symbol)
    order_book = order_book_ob.get_book()
    # print(order_book)

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

    # print(order_book)
    # FIXME: not sure if this is necessary
    # consolidate_order_book(response, order_book, order_book_ob)


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


binance = get_binance_client()
def get_order_book_snapshot(symbol):
    # print('Get order book for:', symbol)
    data = binance.get_order_book(symbol)

    if redis.hget('initialized', symbol):
        return

    # TODO: use the DATA!!!!
    last_update_id = data.pop('lastUpdateId', None)

    if last_update_id is None:
        return None

    redis.hset('last_update_ids', symbol, last_update_id)

    order_book_ob = OrderBook.get(symbol)
    order_book_ob.clear()
    order_book = order_book_ob.get_book()

    for price, amount in data['bids']:
        order_book['bids'][float(price)] = amount

    for price, amount in data['asks']:
        order_book['asks'][float(price)] = amount

    order_book_ob.save(order_book)

    # print(symbol)

    # print(order_book)
    # return order_book
    # now = int(time() * 1000)

    # bids = {float(price): [size, now] for price, size in data['bids']}
    # asks = {float(price): [size, now] for price, size in data['asks']}

    return apply_cached_response(symbol)


def apply_cached_response(symbol, count=10):
    # if not (responses := redis.rpop('cached_responses:'+symbol, count)):
    #     return
    
    # TODO: compare rpop, lrange and stream performance
    if not (responses := redis.lrange('cached_responses:'+symbol, 0, -1)):
        return

    if not redis.setnx(f'working_on_{symbol}', 1):
        print('Working')
        return

    print('Got the lock')

    for response in responses:
        response = json.loads(response)

        if response['u'] <= int(redis.hget('last_update_ids', symbol)):
            continue

        initialized = redis.hget('initialized', symbol)
        if not initialized:
            if has_order_book_initialized(response):
                redis.hset('initialized', symbol, 1)
                redis.hset('last_sequences', symbol, response['u'])
                print('Initialized')

                # special handling for the first valid response
                update_order_book(response, from_cache=True)
                redis.hset('last_sequences', symbol, response['u'])
            
            continue

        if not is_subsequent_response(response):
            print('Del')
            redis.hdel('initialized', symbol)  # reset initialized status
            return

        update_order_book(response, from_cache=True)
        redis.hset('last_sequences', symbol, response['u'])

    # redis.delete('cached_responses:'+symbol)  # remove stale responses

    redis.delete(f'working_on_{symbol}')
    print('Release Lock')

    # redis.delete('cached_responses:'+symbol)



def has_order_book_initialized(response):
    if response is None:
        return False
    symbol = response['s']
    start_sequence = response['U']
    end_sequence = response['u']

    if not (last_update_id := redis.hget('last_update_ids', symbol)):
        return False

    print('initialized check', start_sequence, last_update_id, end_sequence, start_sequence <= int(last_update_id) + 1 <= end_sequence)
    return start_sequence <= int(last_update_id) + 1 <= end_sequence


def is_subsequent_response(response):
    symbol = response['s']
    start_sequence = response['U']
    # end_sequence = response['u']

    if not (last_sequence := redis.hget('last_sequences', symbol)):
        return False
    if not (start_sequence == int(last_sequence) + 1):
        print('subsequent', start_sequence, int(last_sequence) + 1)
    return start_sequence == int(last_sequence) + 1


async def handle_response():
    # await redis.delete('last_update_ids', 'last_sequences', 'cached_responses', 'initialized')
    loop = asyncio.get_event_loop()
    while True:
        responses = redis.rpop('responses', 10)
        if not responses:
            await asyncio.sleep(0)
            continue
        # if not (responses := redis.rpop('responses', 1)):
        #     await asyncio.sleep(1)
        #     continue
        
        # if responses.empty():
        #     continue
        # response = responses.get()
        if responses:
            for response in responses:
                response = json.loads(response)
                # print(response)
                update_order_book(response)
                # await loop.run_in_executor(None, update_order_book, response)

                # pool.apply(update_order_book, (response, ))
            # symbol = response['s']
                # print(redis.llen('responses'))
            # print(symbol, Binance.get_order_book(symbol))

import concurrent.futures
from multiprocessing import Pool, Process

# pool = Pool()


if __name__ == '__main__':
    redis.delete('last_update_ids', 'last_sequences', 'cached_responses', 'initialized')
    from .order_book_websocket_update_receiver import WS_URL, connect
    
    loop = asyncio.get_event_loop()
    redis.delete('working_on_ETHUSDT', 'working_on_LUNAUSDT', 'initialized')

    loop.create_task(connect(WS_URL, ['ETHUSDT', 'LUNAUSDT']))
    loop.create_task(handle_response())
    loop.run_forever()
