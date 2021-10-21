import asyncio

import simplejson as json
import websockets

from models.order_book import OrderBook
from tasks.order_book_task import update_order_book

WS_URL = "wss://stream.binance.com:9443/ws"


async def connect(url, symbols, callback, timeout=60*15):
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
            response = json.loads(message)
            update_order_book.delay(response)
            symbol = response['s']
            callback(symbol)

            # print(symbol, order_books[symbol].best_prices)
            # print(symbol, order_books[symbol].get_best(1))

order_books = {}

async def run(symbols, callback):
    # tasks = []
    # tasks.append(asyncio.create_task(connect(WS_URL, symbols, callback)))
    # await asyncio.gather(*tasks)
    await connect(WS_URL, symbols, callback)