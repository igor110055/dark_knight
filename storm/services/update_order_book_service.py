import asyncio

import simplejson as json

import zmq

from storm.celery_app import app
from storm.clients.redis_client import get_client
from storm.exchanges.binance import get_client as get_binance_client
from storm.models.order_book import OrderBook
from storm.utils import get_logger, symbol_lock

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect('tcp://127.0.0.1:5555')

redis = get_client(a_sync=False)
binance = get_binance_client()
logger = get_logger(__file__)


# POOL = None

# TODO: move to the upper engine
async def handle_response():
    while True:
        responses = redis.rpop('responses', 4)
        if not responses:
            await asyncio.sleep(0)
            continue

        for response in responses:
            await send_response(response)


async def send_response(response):
    socket.send_string(response)
    socket.recv()
