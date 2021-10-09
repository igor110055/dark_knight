import asyncio
import csv
import hashlib
import hmac
import logging
import os
from decimal import Decimal
from functools import lru_cache
from time import time
from urllib.parse import urlencode, urljoin

import aiohttp
import requests
import simplejson as json
from celery import Celery, Task
from celery.utils.log import get_task_logger
from dotenv import load_dotenv
from redis_client import get_client

logging.basicConfig(level=logging.INFO)

redis = get_client()

load_dotenv()

api_key = os.getenv('API_KEY')
secret_key = os.getenv('SECRET_KEY')


BASE_URL = 'https://api.binance.com'


logger = get_task_logger(__name__)

app = Celery('binance', broker='redis://localhost:6379/1',
             backend='redis://localhost:6379/2')

session = requests.Session()

loop = asyncio.get_event_loop()

headers = {'X-MBX-APIKEY': api_key}
message_hash_key = secret_key.encode('utf-8')


@app.task
def _request(method, path, params):
    # TODO: suspect hashing key is a bit slow
    # params['timestamp'] = timestamp
    # params['recvWindow'] = 60000
    if params:
        query_string = urlencode(params)

        msg = query_string.encode('utf-8')
        params['signature'] = hmac.new(
            message_hash_key, msg, digestmod=hashlib.sha256).hexdigest()

    url = urljoin(BASE_URL, path)
    response = session.request(method, url, headers=headers, params=params)
    return response.json()


class Binance:
    SYMBOLS = None

    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

        self.headers = {'X-MBX-APIKEY': api_key}
        self.message_hash_key = secret_key.encode('utf-8')

        if not Binance.SYMBOLS:
            Binance.SYMBOLS = load_symbols()

    def create_order(self, side, order_type, symbol, quantity, on_quote=False):
        timestamp = int(time() * 1000)
        data = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'timestamp': timestamp
        }
        if on_quote:
            data['quoteOrderQty'] = quantity
        else:
            data['quantity'] = quantity
        return self.post('api/v3/order', data)

    def get_balances(self):
        timestamp = int(time() * 1000)
        params = {
            'recvWindow': 5000,
            'timestamp': timestamp
        }

        data = self.get('api/v3/account', params=params)
        balances = {balance['asset']: balance['free']
                    for balance in data['balances'] if self.__is_larger_than_zero(balance['free'])}
        return balances

    def get_order_book(self, symbol):
        return self.get(f"api/v3/depth?symbol={symbol}", raw=True)

    def load_markets(self):
        return self.get('api/v3/exchangeInfo')
        # return session.get(urljoin(BASE_URL, 'api/v3/exchangeInfo')).json()

    def __is_larger_than_zero(self, balance):
        return all((balance != '0.00000000', balance != '0.00'))

    @staticmethod
    def get(path, params=None, raw=False):
        if raw:
            url = urljoin(BASE_URL, path)
            return session.get(url, params=params).json()
        return _request('GET', path, params)

    @staticmethod
    def post(path, params=None):
        return _request.delay('POST', path, params)

    @staticmethod
    @lru_cache
    def get_min_quantity(symbol):
        if not (symbol := Binance.SYMBOLS.get(symbol)):
            return
        for filter in eval(symbol['filters']):
            if filter['filterType'] == 'LOT_SIZE':
                return filter['minQty']


def load_symbols():
    with open('symbols.csv') as csv_file:
        return {symbol['symbol']: symbol for symbol in csv.DictReader(csv_file)}


def get_client():
    return Binance(api_key, secret_key)


binance_client = get_client()


def update_order_book(order_book, response):
    end_sequence = response['u']
    symbol = response['s']

    last_update_id = Decimal(redis.hget('last_update_ids', symbol).decode() or 'Infinity')
    if Decimal(end_sequence) < last_update_id:
        return

    start_sequence = response['U']

    epoch = response['E']

    for price, size in response['b']:
        price = Decimal(price)
        size = Decimal(size)
        if size:
            order_book['bids'][price] = (size, epoch)
        else:
            order_book['bids'].pop(price, None)

    for price, size in response['a']:
        price = Decimal(price)
        size = Decimal(size)
        if size:
            # update event, reconcile with trade data
            order_book['asks'][price] = (size, epoch)
        else:
            # cancel event
            order_book['asks'].pop(price, None)


def apply_cached_response(order_book, symbol):
    # raw_ob = redis.hget('order_books', symbol)
    # order_book = json.loads(raw_ob)

    cached_responses = redis.lrange(f"cached_responses:{symbol}", 0, -1)
    cached_responses = [json.loads(response) for response in cached_responses]
    for response in cached_responses:
        update_order_book(order_book, response)

    redis.delete(f"cached_responses:{symbol}")

def cache_order_book(symbol, data):
    if 'lastUpdateId' not in data:
        logging.warning(
            f"['Binance Client'] invalid order book response {data}")
        return

    redis.hset('last_update_ids', symbol, data['lastUpdateId'])

    now = int(time() * 1000)

    bids = {Decimal(price): (
        Decimal(size), now) for price, size in data['bids']}
    asks = {Decimal(price): (
        Decimal(size), now) for price, size in data['asks']}

    # redis.hset('order_books', symbol, json.dumps({'bids': bids, 'asks': asks}))

    order_book = {'bids': bids, 'asks': asks}

    apply_cached_response(order_book, symbol)

    redis.hset('order_books', symbol, json.dumps(order_book))


@app.task()
def get_order_book_snapshot(symbol):
    data = binance_client.get_order_book(symbol)
    cache_order_book(symbol, data)
