import asyncio
from binance import Binance
import simplejson as json
from models.order_book import OrderBook
from redis_client import get_client


redis = get_client()


def update_order_book(response: dict):
    symbol = response['s']
    epoch = response['E']  # TODO: add to price
    start_sequence = response['U']
    end_sequence = response['u']

    # move to one off check
    # while not response or not has_order_book_initialized(response):
    if not redis.hget('initialized', symbol):
        redis.rpush('cached_responses:'+symbol, json.dumps(response))  # TODO: to slow, need to separate response receive and handling
        return get_order_book_snapshot(symbol)

    if redis.hget('last_sequences', symbol):
        if start_sequence != int(redis.hget('last_sequences', symbol)) + 1:  # TODO: use incr
            redis.hdel('last_update_ids', symbol)  # invalidate has_order_book_initialized
            redis.hdel('last_sequences', symbol)
            redis.rpush('cached_responses:'+symbol, json.dumps(response))
            return

        sync_orders(response)

def sync_orders(response):
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


def get_order_book_snapshot(symbol):
    # print('Get order book for:', symbol)
    data = Binance.get_order_book(symbol, 9999)
    # print(data)

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

    print(symbol)

    # print(order_book)
    # return order_book
    # now = int(time() * 1000)

    # bids = {float(price): [size, now] for price, size in data['bids']}
    # asks = {float(price): [size, now] for price, size in data['asks']}

    return apply_cached_response(symbol)


def apply_cached_response(symbol):
    cache_applied = False
    for response in redis.lrange('cached_responses:'+symbol, 0, -1):
        if response == 'null':
            break
        response = json.loads(response)

        initialized = redis.hget('initialized', symbol)
        if not initialized:
            if has_order_book_initialized(response):
                redis.hset('initialized', symbol, 1)
                redis.hset('last_sequences', symbol, response['u'])
            else:
                continue
        
        if response['U'] <= int(redis.hget('last_sequences', symbol)):
            continue

        if not is_subsequent_response(response):
            cache_applied = False
            break

        update_order_book(response)
        redis.hset('last_sequences', symbol, response['u'])
        cache_applied = True

    # redis.delete('cached_responses:'+symbol)  # remove stale responses
    if not cache_applied:
        redis.hdel('initialized', symbol)  # reset initialized status
        return None
    
    return response



def has_order_book_initialized(response):
    if response is None:
        return False
    symbol = response['s']
    start_sequence = response['U']
    end_sequence = response['u']

    if not (last_update_id := redis.hget('last_update_ids', symbol)):
        return False

    return start_sequence <= int(last_update_id) + 1 <= end_sequence


def is_subsequent_response(response):
    symbol = response['s']
    start_sequence = response['U']
    # end_sequence = response['u']

    if not (last_sequence := redis.hget('last_sequences', symbol)):
        return False

    return start_sequence == int(last_sequence) + 1


async def handle_response():
    loop = asyncio.get_event_loop()
    with concurrent.futures.ProcessPoolExecutor(8) as pool:
        while True:
            if not (responses := redis.rpop('responses', 1)):
                continue
            
            # if responses.empty():
            #     continue
            # response = responses.get()
            if responses:
                for response in responses:
                    response = json.loads(response)
                    await loop.run_in_executor(pool, update_order_book, response)

                    # pool.apply(update_order_book, (response, ))
                # symbol = response['s']
                    # print(redis.llen('responses'))
                # print(symbol, Binance.get_order_book(symbol))


import concurrent.futures

if __name__ == '__main__':
    redis.delete('last_update_ids', 'last_sequences', 'cached_responses', 'initialized')
    # asyncio.run(handle_response())

    from multiprocessing import Process, Pool

    pool = Pool()

    asyncio.run(handle_response())
    # for _ in range(4):
    #     Process(target=handle_response).start()


    # import threading
    # import asyncio

    # from binance import websocket_pool
        
    # threading.Thread(target=asyncio.run, args=(websocket_pool(), )).start()

    # response = {
    #     "e":"depthUpdate",
    #     "E":1635587664771,
    #     "s":"BNBETH",
    #     "U":1024042672,
    #     "u":1024042672,
    #     "b":[],
    #     "a":[
    #         ["0.12210000","213.67300000"]
    #     ]
    # }
    # update_order_book(response)
