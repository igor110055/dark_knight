import asyncio
import csv
import hashlib
import hmac
import logging
import os
import random
from decimal import Decimal
from functools import lru_cache
from time import time
from urllib.parse import urlencode, urljoin

import requests
import requests.adapters
import simplejson as json
import websockets
from celery import Celery
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
adapter = requests.adapters.HTTPAdapter(pool_connections=500, pool_maxsize=500)
session.mount(BASE_URL, adapter)


# loop = asyncio.get_event_loop()

headers = {'X-MBX-APIKEY': api_key}
message_hash_key = secret_key.encode('utf-8')


# @app.task
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
            response = session.get(url, params=params)
            if response.status_code == 200:
                return json.loads(session.get(url, params=params).content)
            else:
                return None
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
    ob = order_book.get()
    epoch = response['E']

    for bid_price, size in response['b']:
        bid_price = float(bid_price)
        if float(size):
            ob['bids'][bid_price] = size
            order_book.set('bids', bid_price, size)  # TODO: aggregate the changes at once

            # ob['bids'][bid_price] = [size, epoch]
        else:
            ob['bids'].pop(bid_price, None)
            order_book.delete('bids', bid_price)

    for ask_price, size in response['a']:
        ask_price = float(ask_price)
        if float(size):
            # update event, reconcile with trade data
            ob['asks'][ask_price] = size
            order_book.set('asks', ask_price, size)
        else:
            ob['asks'].pop(ask_price, None)
            order_book.delete('asks', ask_price)

    if not ob['bids'] or not ob['asks']:
        return

    best_bid = max(ob['bids'])
    best_ask = min(ob['asks'])

    # cross spread
    if best_bid > best_ask:
        ob['asks'] = {price: size for price, size in ob['asks'].items() if price > best_bid}
        ob['bids'] = {price: size for price, size in ob['bids'].items() if price < best_ask}

    if ob['bids'] and ob['asks']:
        best_bid = max(ob['bids'])
        best_ask = min(ob['asks'])
    else:
        print('error')
        return

    if best_bid > best_ask:
        raise Exception(str(response))

    print(best_bid, ob['bids'][best_bid], best_ask, ob['asks'][best_ask])

    # order_book.save()

    return True

def apply_cached_response(order_book, symbol):
    # raw_ob = redis.hget('order_books', symbol)
    # order_book = json.loads(raw_ob)

    cached_responses = redis.lrange(f"cached_responses:{symbol}", 0, -1)
    cached_responses = [json.loads(response) for response in cached_responses]

    last_update_id = Decimal(redis.hget('last_update_ids', symbol) or 'Infinity')

    cache_applied = False
    for response in cached_responses:
        start_sequence = response['U']
        end_sequence = response['u']
        symbol = response['s']

        if Decimal(end_sequence) < last_update_id:
            continue

        if not Decimal(start_sequence) <= last_update_id + 1 <= Decimal(end_sequence):
            # return
            return get_order_book_snapshot(symbol)

        if update_order_book(order_book, response):
            last_update_id = end_sequence
            cache_applied = True

    if cache_applied:
        # bids = {Decimal(price): size for price, size in order_book['bids'].items()}
        # asks = {Decimal(price): size for price, size in order_book['asks'].items()}
        # order_book['bids'] = sorted(bids, reverse=True)[5:]
        # order_book['asks'] = sorted(asks)[5:]
        redis.hset('last_update_ids', symbol, last_update_id)
        redis.delete(f"cached_responses:{symbol}")


def cache_order_book(symbol, data):
    last_update_id = data.pop('lastUpdateId', None)

    if last_update_id is None:
        logging.warning(
            f"['Binance Client'] invalid order book response {data}")
        return

    redis.hset('last_update_ids', symbol, last_update_id)

    now = int(time() * 1000)

    bids = {float(price): [size, now] for price, size in data['bids']}
    asks = {float(price): [size, now] for price, size in data['asks']}

    bs = {size: float(price) for price, size in data['bids']}

    order_book = dict(bids=bids, asks=asks)

    apply_cached_response(order_book, symbol)


import websocket


def _get_order_book(symbol, id=random.randint(1, 100)):
    ws = websocket.WebSocket(enable_multithread=True)

    ws.connect("wss://stream.binance.com:9443/ws", timeout=60*15)

    payload = {
        "method": "SUBSCRIBE",
        'params': [f'{symbol.lower()}@depth20@100ms'],
        "id": id
    }

    ws.send(json.dumps(payload))

    ws.recv()

    message = ws.recv()
    ws.close()

    return json.loads(message)


import concurrent.futures

executor = concurrent.futures.ThreadPoolExecutor(max_workers=100)


def get_order_book(symbol, id):
    future = executor.submit(_get_order_book, symbol, id)
    return_value = future.result()
    return return_value

@app.task()
def get_order_book_snapshot(symbol):
    data = get_order_book(symbol, 9999)
    cache_order_book(symbol, data)
