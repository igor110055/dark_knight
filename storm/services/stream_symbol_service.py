import asyncio
from random import randint
from typing import List

import simplejson as json
import websockets

from ..clients.redis_client import get_client
from ..utils import chunks, get_logger

redis = get_client()
logger = get_logger(__file__)


async def stream_symbols(
    url: str,
    symbols: List[str],
    stream_id: int = randint(1, 99),
    timeout: int = 60 * 15,
    redis=redis,
) -> None:
    """Stream symbols update from websocket, and cache into redis

    Args:
        url (str): Exchange websocket url
        symbols (List[str]): symbols to stream update
        stream_id (int, optional): stream id. Default to randint(1, 99)
        timeout (int, optional): ping timeout. Defaults to 60*15.
        redis (FRedis, optional): redis client. Defaults to redis.
    """

    # auto reconnect
    async for websocket in websockets.connect(url, ping_timeout=timeout):
        for symbol in symbols:
            payload = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@depth@100ms"],
                "id": stream_id,
            }
            await websocket.send(json.dumps(payload))

            # ack
            # message = await websocket.recv()

            while not redis.hget("initialized", symbol):
                message = await websocket.recv()
                if "result" in message:
                    continue
                logger.info(f"websocket update received on {message[19:52]} ...")
                redis.lpush("responses", message)

        async for message in websocket:
            if "result" in message:
                continue
            logger.info(f"websocket update received on {message[19:52]} ...")
            redis.lpush("responses", message)


if __name__ == "__main__":
    from ..exchanges.binance import WS_URL
    from ..helpers.triangular_finder import get_symbols

    loop = asyncio.get_event_loop()

    tasks = []
    all_symbols = get_symbols()
    for symbols in chunks(all_symbols, 10):
        tasks.append(loop.create_task(stream_symbols(WS_URL, symbols)))
        break

    # tasks.append(loop.create_task(stream_symbols(WS_URL, ['LUNDUSDT'])))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        for t in tasks:
            t.cancel()
