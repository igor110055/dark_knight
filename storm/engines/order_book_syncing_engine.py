from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import zmq

from ..clients.redis_client import get_client
from ..exchanges.binance import get_client as get_binance_client
from ..services.sync_order_book_service import SyncOrderBookService
from ..utils import get_logger

redis = get_client(a_sync=False)
binance = get_binance_client()
logger = get_logger(__file__)


if __name__ == '__main__':
    FAST_POOL = ThreadPoolExecutor(10)

    logger.info('Ready to handle order book update')

    service = SyncOrderBookService(redis, binance)

    redis.delete('responses')

    while True:
        responses = redis.rpop('responses', 10)
        if not responses:
            continue

        # TODO: fix zmq connection in process, use ProcessPool
        FAST_POOL.map(service.update_order_book, responses)
        # for response in responses:
        #     service.update_order_book(response)
