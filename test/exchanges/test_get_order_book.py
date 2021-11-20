import asyncio
from threading import Thread
from time import sleep

import pytest
from storm.clients.redis_client import get_client
from storm.exchanges.binance import WS_URL
from storm.services.stream_symbol_service import stream_symbols

redis = get_client(9)


def get_order_book_update(symbols):
    Thread(
        target=asyncio.run,
        args=(stream_symbols(WS_URL, symbols, redis=redis),),
        daemon=True,
    ).start()


@pytest.mark.skip
def test_receive_order_book_update():
    redis.flushdb()
    assert not redis.lrange("responses", 0, 10)

    symbols = ["LUNAUSDT"]

    get_order_book_update(symbols)

    while True:
        if not redis.lrange("responses", 0, 10):
            sleep(0.1)
            continue
        break

    assert redis.lrange("responses", 0, 10)
