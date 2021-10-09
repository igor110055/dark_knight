import asyncio
import csv
import hashlib
import hmac
import os
from decimal import Decimal
from functools import lru_cache
from time import time
from urllib.parse import urlencode, urljoin

import aiohttp
import requests
import simplejson as json
from celery import Celery
from celery.utils.log import get_task_logger
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('API_KEY')
secret_key = os.getenv('SECRET_KEY')


BASE_URL = 'https://api.binance.com'


logger = get_task_logger(__name__)

app = Celery('binance', broker='redis://localhost:6379/0',
             backend='redis://localhost:6379/0')

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
