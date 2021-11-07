import asyncio
import csv
import hashlib
import hmac
import os
from time import sleep
import _thread
from functools import lru_cache
from queue import Queue
from time import time
from urllib.parse import urlencode, urljoin
from uuid import uuid4
from collections import deque

import requests
import requests.adapters
import simplejson as json
import websockets
from dotenv import load_dotenv
from storm.clients.redis_client import get_client
from .binance_websocket import get_binance_websocket

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


redis = get_client()


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

    def stream_symbol(self, symbol):
        self.websocket = self.websocket or get_binance_websocket()
        self.websocket.symbol = symbol

        self.websocket_thread = Thread(target=self.websocket.run_forever, kwargs={'ping_interval': 60, 'ping_timeout' : 30})
        self.websocket_thread.start()

    def stop_streaming(self):
        self.websocket.close()

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

    def get_order_book(self, symbol, use_websocket=True):
        if use_websocket:
            redis.lpush('working_on_symbols', symbol)
            while not (snapshot := redis.hget('snapshots', symbol)):
                continue

            redis.hdel('snapshots', symbol)
            return json.loads(snapshot)

        request_uuid = uuid4()
        logger.info(f'[{request_uuid}] GET order book request for {symbol}')
        resp = self.get(f"api/v3/depth?symbol={symbol}", raw=True)
        logger.info(f'[{request_uuid}] GET order book response for {symbol}: {resp.content.decode()[:40]}')
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



import websocket
import threading
import time

loaded = {}

def on_message(ws, message):
    # for symbol in ['LUNAUSDT', 'ETHUSDT']:
    # print(message)

    if ws.symbol:
        if ws.symbol not in loaded:
            loaded[ws.symbol] = True
            return
        logger.info(message)
        payload = {
            "method": "UNSUBSCRIBE",
            'params': [f"{ws.symbol.lower()}@depth10@100ms"],
            "id": 0
        }

        ws.send(json.dumps(payload))
        ws.symbol = None

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

from threading import Thread

symbols = Queue()

def on_open(ws):
    ws.symbol = None
    def run(*args):

        while True:
            symbol = symbols.get()
            if symbol is None:
                break
            payload = {
                "method": "SUBSCRIBE",
                'params': [f'{symbol.lower()}@depth10@100ms'],
                "id": 0
            }
            ws.symbol = symbol
            ws.send(json.dumps(payload))
            time.sleep(0.1)
            # ws.close()
        logger.info("thread terminating...")
    Thread(target=run).start()
    # _thread.start_new_thread(run, ())


if __name__ == '__main__':
    redis.delete('payloads', 'snapshots', 'initialized')
    # from threading import Thread
    # from multiprocessing import Process

    # # task = Process(target=asyncio.run, args=(connect(WS_URL), ))
    # # task.start()

    # for i in range(100):
    #     print(asyncio.run(Binance.get_order_book_ws('BTCUSDT')))
    #     print(asyncio.run(Binance.get_order_book_ws('ETHUSDT')))
    #     print(asyncio.run(Binance.get_order_book_ws('LUNAUSDT')))

    # websocket.enableTrace(True)
    ws = websocket.WebSocketApp(WS_URL,
                                on_open = on_open,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    # ws.run_forever()
    task = Thread(target=ws.run_forever)
    task.start()

    symbols.put('LUNAUSDT')
    symbols.put('ETHUSDT')
    symbols.put(None)

    # task.join()

    print('done')

    # asyncio.run(websocket_pool())
