import asyncio
from time import sleep
from order_book_websocket_update_receiver import connect, WS_URL
# from order_book_task import 
from redis_client import get_client  # test get redis
from threading import Thread


redis = get_client(9)

def get_order_book_update(symbols):
    Thread(target=asyncio.run, args=(connect(WS_URL, symbols, redis=redis), ), daemon=True).start()

def test_receive_order_book_update():
    redis.flushdb()
    assert not redis.lrange('responses', 0, 10)

    symbols = ['LUNAUSDT']

    get_order_book_update(symbols)

    while True:
        if not redis.lrange('responses', 0, 10):
            sleep(0.1)
            continue
        break

    assert redis.lrange('responses', 0, 10)
