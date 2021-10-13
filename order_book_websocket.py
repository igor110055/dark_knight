import asyncio

import simplejson as json
import websockets

from models.order_book import OrderBook
from tasks.order_book_task import update_order_book

WS_URL = "wss://stream.binance.com:9443/ws"


async def connect(url, symbol, timeout=60*15):
    payload = {
        "method": "SUBSCRIBE",
        'params': [f'{symbol.lower()}@depth@100ms'],
        "id": 1
    }

    order_book = OrderBook(symbol)
    order_book.clear()
    async with websockets.connect(url, ping_timeout=timeout) as websocket:
        await websocket.send(json.dumps(payload))

        # ack
        await websocket.recv()

        while True:
            message = await websocket.recv()
            response = json.loads(message)
            update_order_book.delay(response)
            print(symbol, order_book.get_best(1))


async def run(symbols):
    tasks = []
    for symbol in symbols:
        tasks.append(asyncio.create_task(connect(WS_URL, symbol)))

    await asyncio.gather(*tasks)
