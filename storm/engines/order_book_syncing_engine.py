from concurrent.futures import ThreadPoolExecutor

import zmq

from ..clients.redis_client import get_client
from ..exchanges.binance import get_client as get_binance_client
from ..services.sync_order_book_service import SyncOrderBookService
from ..utils import get_logger

redis = get_client(a_sync=False)
binance = get_binance_client()
logger = get_logger(__file__)


if __name__ == '__main__':
    FAST_POOL = ThreadPoolExecutor(32)

    context = zmq.Context()
    order_book_socket = context.socket(zmq.REQ)
    order_book_socket.connect('tcp://127.0.0.1:5556')

    logger.info('Ready to handle order book update')

    service = SyncOrderBookService(redis, binance, order_book_socket)

    while True:
        responses = redis.rpop('responses', 10)
        if not responses:
            continue

        # TODO: fix zmq connection in process, use ProcessPool
        FAST_POOL.map(service.update_order_book, responses)
