import asyncio
from random import randint
from typing import List

import simplejson as json
import websockets
from storm.clients.redis_client import FRedis, get_client
from storm.tasks.order_task import check_arbitrage
from storm.utils import chunks, get_logger

redis = get_client(a_sync=False)
logger = get_logger(__file__)


async def stream_symbols(url: str, symbols: List[str], stream_id: int = randint(1, 99), timeout: int = 60*15, redis: FRedis = redis) -> None:
    """Stream symbols update from websocket, and cache into redis

    Args:
        url (str): Exchange websocket url
        symbols (List[str]): symbols to stream update
        stream_id (int, optional): stream id. Default to randint(1, 99)
        timeout (int, optional): ping timeout. Defaults to 60*15.
        redis (FRedis, optional): redis client. Defaults to redis.
    """
    params = [f'{symbol.lower()}@depth@100ms' for symbol in symbols]

    payload = {
        "method": "SUBSCRIBE",
        'params': params,
        "id": stream_id
    }

    # auto reconnect
    async for websocket in websockets.connect(url, ping_timeout=timeout):
        await websocket.send(json.dumps(payload))

        # ack
        message = await websocket.recv()

        async for message in websocket:
        # while True:
        #     message = await websocket.recv()

            logger.info(f'websocket update received on {message[19:52]} ...')
            redis.lpush('responses', message)

            # print(symbol, order_books[symbol].best_prices)
            # print(symbol, order_books[symbol].get_best(1))


def trading(symbol):
    if symbol in ['LUNAUSDT', 'LUNABNB', 'BNBUSDT']:
        symbol = 'LUNAUSDT'
        synthetic = {
            'LUNABNB': {'normal': True},
            'BNBUSDT': {'normal': True}
        }
        check_arbitrage(symbol, synthetic, 0.3)


if __name__ == '__main__':
    from storm.exchanges.binance import WS_URL
    from triangular_finder import get_symbols

    all_symbols = get_symbols()

    loop = asyncio.get_event_loop()

    tasks = []
    for symbols in chunks(all_symbols, 10):
        tasks.append(loop.create_task(stream_symbols(WS_URL, symbols)))
        break

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        for t in tasks:
            t.cancel()
