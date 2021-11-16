import csv
import hashlib
import hmac
import os
import time
from functools import lru_cache
from queue import Queue
from urllib.parse import urlencode, urljoin
from uuid import uuid4

import requests
import requests.adapters
from dotenv import load_dotenv
from storm.clients.redis_client import get_client as get_redis_client

from ..utils import get_logger

load_dotenv()

logger = get_logger(__file__)

api_key = os.getenv('API_KEY')
secret_key = os.getenv('SECRET_KEY')

REST_URL = 'https://api.binance.com'
WS_URL = "wss://stream.binance.com:9443/ws"

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=100)
session.mount(REST_URL, adapter)

headers = {'X-MBX-APIKEY': api_key}
message_hash_key = secret_key.encode('utf-8')


redis = get_redis_client()


def _request(method, path, params):
    # TODO: suspect hashing key is a bit slow
    # params['timestamp'] = timestamp
    # params['recvWindow'] = 60000
    if params:
        query_string = urlencode(params)

        msg = query_string.encode('utf-8')
        params['signature'] = hmac.new(
            message_hash_key, msg, digestmod=hashlib.sha256).hexdigest()

    url = urljoin(REST_URL, path)
    response = session.request(method, url, headers=headers, params=params)
    return response.json()


class Binance:
    SYMBOLS = None
    websockets = Queue()

    def __init__(self, api_key, secret_key, init_symbols=False):
        self.api_key = api_key
        self.secret_key = secret_key

        self.headers = {'X-MBX-APIKEY': api_key}
        self.message_hash_key = secret_key.encode('utf-8')

        self.websocket = None
        self.websocket_thread = None
        self.health_check_thread = None

        if not Binance.SYMBOLS and init_symbols:
            Binance.SYMBOLS = load_symbols()

    def create_order(self, side, order_type, symbol, quantity, on_quote=False):
        timestamp = int(time.time() * 1000)
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

    def get_order(self, symbol, order_id):
        timestamp = int(time.time() * 1000)
        params = {
            'symbol': symbol,
            'orderId': order_id,
            'recvWindow': 5000,
            'timestamp': timestamp
        }

        return self.get('api/v3/order', params=params)

    def get_orders(self, symbol, limit=100):
        timestamp = int(time.time() * 1000)
        params = {
            'symbol': symbol,
            'limit': limit,
            'recvWindow': 5000,
            'timestamp': timestamp
        }

        return self.get('api/v3/allOrders', params=params)

    def get_balances(self):
        timestamp = int(time.time() * 1000)
        params = {
            'recvWindow': 5000,
            'timestamp': timestamp
        }

        data = self.get('api/v3/account', params=params)
        balances = {balance['asset']: balance['free']
                    for balance in data['balances'] if self.__is_larger_than_zero(balance['free'])}
        return balances

    def get_order_book(self, symbol):
        request_uuid = uuid4()
        logger.info(f'[{request_uuid}] GET order book request for {symbol}')
        resp = self.get(f"api/v3/depth?symbol={symbol}", raw=True)
        logger.info(
            f'[{request_uuid}] GET order book response for {symbol}: {resp.content.decode()[:40]}')
        return resp.json()

    def load_markets(self):
        return self.get('api/v3/exchangeInfo')
        # return session.get(urljoin(REST_URL, 'api/v3/exchangeInfo')).json()

    def __is_larger_than_zero(self, balance):
        return all((balance != '0.00000000', balance != '0.00'))

    @staticmethod
    def get(path, params=None, raw=False):
        if raw:
            url = urljoin(REST_URL, path)
            return session.get(url, params=params)
        return _request('GET', path, params)

    @staticmethod
    def post(path, params=None):
        return _request('POST', path, params)

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


def get_client(init_symbols=False):
    return Binance(api_key, secret_key, init_symbols)
