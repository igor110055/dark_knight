from multiprocessing import Process
from time import sleep

from ..clients.redis_client import get_client
from ..services.sync_order_book_service import SyncOrderBookService
from ..utils import get_logger

redis = get_client()
logger = get_logger(__file__)


def main():
    service = SyncOrderBookService()

    while True:
        response = redis.brpop("responses", 0.001)
        if response:
            service.update_order_book(response[1])
        else:
            sleep(0.001)

        # TODO: can cache response by symbols, to enable parallel processing
        # TODO: fix zmq connection in process, use ProcessPool


if __name__ == "__main__":
    logger.info("Ready to handle order book update")
    redis.delete("responses")

    for _ in range(6):
        Process(target=main).start()
