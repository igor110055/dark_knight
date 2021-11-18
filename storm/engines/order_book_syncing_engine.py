from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from multiprocessing import Process
from ..clients.redis_client import get_client
from ..services.sync_order_book_service import SyncOrderBookService
from ..utils import get_logger

redis = get_client(a_sync=False)
logger = get_logger(__file__)


def main(r_num=16):
    FAST_POOL = ThreadPoolExecutor(r_num)

    service = SyncOrderBookService()

    while True:
        responses = redis.rpop('responses', r_num)
        if not responses:
            continue

        # TODO: can cache response by symbols, to enable parallel processing
        # TODO: fix zmq connection in process, use ProcessPool
        FAST_POOL.map(service.update_order_book, responses)


if __name__ == '__main__':
    logger.info('Ready to handle order book update')
    redis.delete('responses')

    for _ in range(4):
        Process(target=main).start()
