import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import zmq

from ..clients.redis_client import get_client
from ..exchanges.binance import get_client as get_binance_client
from ..services.sync_order_book_service import SyncOrderBookService
from ..utils import get_logger

redis = get_client(a_sync=False)
binance = get_binance_client()
logger = get_logger(__file__)


if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://127.0.0.1:5555')

    loop = asyncio.get_event_loop()

    FAST_POOL = ThreadPoolExecutor(32)

    order_book_socket = context.socket(zmq.REQ)
    order_book_socket.connect('tcp://127.0.0.1:5556')

    logger.info('Ready to handle order book update')

    service = SyncOrderBookService(redis, binance, order_book_socket)
    while True:
        message = socket.recv_string()
        # update_order_book(message)

        # TODO: fix zmq connection in process
        loop.run_in_executor(FAST_POOL, service.update_order_book, message)

        socket.send_string('')
