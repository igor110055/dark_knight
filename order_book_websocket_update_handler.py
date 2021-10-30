import asyncio

import concurrent.futures


import simplejson as json
import websockets

from multiprocessing import Manager, Process, Pool

from models.order_book import OrderBook
from tasks.order_book_task import update_order_book
from tasks.order_task import check_arbitrage
from utils import chunks

from binance import Binance, websocket_pool

from time import time

from redis_client import get_client

redis = get_client()

WS_URL = "wss://stream.binance.com:9443/ws"

import pdb
async def connect(url, symbols, callback=None, timeout=60*15):
    params = []
    order_books = {}
    for symbol in symbols:
        params.append(f'{symbol.lower()}@depth@100ms')
        order_book = OrderBook.get(symbol)
        order_book.clear(deep=True)
        order_books[symbol] = order_book

    payload = {
        "method": "SUBSCRIBE",
        'params': params,
        "id": 1
    }

    async with websockets.connect(url, ping_timeout=timeout) as websocket:
        await websocket.send(json.dumps(payload))

        # ack
        await websocket.recv()

        async for message in websocket:
            # responses.put_nowait(message)
            redis.lpush('responses', message)
            # update_order_book.delay(response)
            # symbol = response['s']
            # callback(symbol)

            # print(symbol, order_books[symbol].best_prices)
            # print(symbol, order_books[symbol].get_best(1))

order_books = {}

def run(symbols, callback):
    # tasks = []
    # tasks.append(asyncio.create_task(connect(WS_URL, symbols, callback)))
    # await asyncio.gather(*tasks)
    asyncio.run(connect(WS_URL, symbols, callback))


def trading(symbol):
    if symbol in ['LUNAUSDT', 'LUNABNB', 'BNBUSDT']:
        symbol = 'LUNAUSDT'
        synthetic = {
            'LUNABNB': {'normal': True},
            'BNBUSDT': {'normal': True}
        }
        check_arbitrage(symbol, synthetic, 0.3)

def handle_response():
    start = time()
    while True:
        if not (response := redis.rpop('responses', 10)):
            continue
        
        # if responses.empty():
        #     continue
        # response = responses.get()
        if response:
            for _response in response:
                response = json.loads(_response)
                update_order_book(response)
                # pool.apply(update_order_book, (response, ))
            # symbol = response['s']
                print(redis.llen('responses'))
            # print(symbol, Binance.get_order_book(symbol))


if __name__ == '__main__':
    import sys
    pool = Pool(4)

    try:
        # Process(target=asyncio.run, args=(websocket_pool(), )).start()
        for _ in range(4):
            Process(target=handle_response).start()
        asyncio.run(websocket_pool())
    except KeyboardInterrupt:
        sys.exit(1)
