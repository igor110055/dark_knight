import asyncio

import simplejson as json
import websockets

from .models.order_book import OrderBook
from .tasks.order_task import check_arbitrage
from .utils import chunks

from .redis_client import get_client

redis = get_client(a_sync=False)

WS_URL = "wss://stream.binance.com:9443/ws"

async def connect(url, symbols, callback=None, timeout=60*15, redis=redis):
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
        message = await websocket.recv()

        while True:
            message = await websocket.recv()
        # async for message in websocket:
            # responses.put_nowait(message)
            # print(message)
            redis.lpush('responses', message)
            await asyncio.sleep(0)
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

async def main():
    from triangular_finder import get_symbols

    all_symbols = get_symbols()

    tasks = []
    # loop = asyncio.get_event_loop()

    # tasks.append(asyncio.create_task(connect(WS_URL, ['LUNAUSDT'])))
    # print('hi')
    for symbols in chunks(all_symbols, 10):
        tasks.append(asyncio.create_task(connect(WS_URL, symbols)))
        break

    try:
        await asyncio.gather(*tasks)
    except:
        for t in tasks:
            t.cancel()


if __name__ == '__main__':
    import sys

    redis.flushdb()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)