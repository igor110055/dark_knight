import asyncio

import concurrent.futures


import simplejson as json
import websockets

from multiprocessing import Manager, Process

from models.order_book import OrderBook
from tasks.order_book_task import update_order_book
from tasks.order_task import check_arbitrage
from utils import chunks

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
            responses.put_nowait(message)
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

from exchanges.binance import Binance, websocket_pool

from time import time

def handle_response():
    start = time()
    while True:
        if responses.empty():
            continue
        response = responses.get()
        if response:
            # response = json.loads(response)
            # update_order_book(response)
            # symbol = response['s']
            print(responses.qsize())
            # print(symbol, Binance.get_order_book(symbol))
        
        # if time() - start > 2:
        #     break
            # trading(symbol)
        # print(responses.qsize())

# TODO?
def multi_handle_response():
    for i in range(2):
        threading.Thread(target=handle_response).start()


async def main():
    from triangular_finder import get_symbols

    all_symbols = get_symbols()

    tasks = []
    # loop = asyncio.get_event_loop()

    for symbols in chunks(all_symbols, 200):
        tasks.append(asyncio.create_task(connect(WS_URL, symbols)))

    try:
        await asyncio.gather(*tasks)
        # for task in tasks:
        #     loop.run_until_complete(task)
    except:
        for t in tasks:
            t.cancel()


if __name__ == '__main__':
    import sys
    from threading import Thread
    from queue import Queue

    manager = Manager()
    responses = manager.Queue()
    try:
        loop = asyncio.get_event_loop()
        # loop.create_task(websocket_pool())
        # threading.Thread(target=asyncio.run, args=(websocket_pool(), )).start()
        # for _ in range(10):
        #     threading.Thread(target=handle_response).start()
        for _ in range(4):
            Process(target=handle_response).start()
        loop.create_task(main())
        loop.run_forever()
        # main()
    except KeyboardInterrupt:
        sys.exit(1)
