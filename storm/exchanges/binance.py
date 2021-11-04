import asyncio
from asyncio import tasks
import csv
import hashlib
import hmac
import os
from functools import lru_cache
from queue import Queue
from time import time
from urllib.parse import urlencode, urljoin

from multiprocessing import Manager, Process

import requests
import requests.adapters
import simplejson as json
import websockets
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('API_KEY')
secret_key = os.getenv('SECRET_KEY')

BASE_URL = 'https://api.binance.com'

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=500, pool_maxsize=500)
session.mount(BASE_URL, adapter)

headers = {'X-MBX-APIKEY': api_key}
message_hash_key = secret_key.encode('utf-8')

from ..redis_client import get_client

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

    url = urljoin(BASE_URL, path)
    response = session.request(method, url, headers=headers, params=params)
    return response.json()

if os.getenv('WS_POOL'):
    pass

class Binance:
    SYMBOLS = None

    def __init__(self, api_key, secret_key, init_symbols=False):
        self.api_key = api_key
        self.secret_key = secret_key

        self.headers = {'X-MBX-APIKEY': api_key}
        self.message_hash_key = secret_key.encode('utf-8')

        if not Binance.SYMBOLS and init_symbols:
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

    @staticmethod
    def get_order_book(symbol, id):
        payload = {
            "method": "SUBSCRIBE",
            'params': [f'{symbol.lower()}@depth20@100ms'],
            "id": id
        }

        redis.lpush('payloads', payload)

        while True:
            print('hi')
            message = redis.hget(id)
            if message:
                break

        payload['method'] = 'UNSUBSCRIBE'
        redis.lpush('payloads', payload)

        return json.loads(message)
        # return self.get(f"api/v3/depth?symbol={symbol}", raw=True)

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


async def websocket_pool(num=5):
    tasks = []
    for _ in range(num):
        tasks.append(asyncio.create_task(connect("wss://stream.binance.com:9443/ws")))

    await asyncio.gather(*tasks)


async def connect(url, timeout=60*15):
    async with websockets.connect(url, ping_timeout=timeout) as websocket:
        while True:
            payload = redis.rpop('responses')
            if not payload:
                await asyncio.sleep(0)
                continue
            await websocket.send(json.dumps(payload))

            # ack
            await websocket.recv()

            message = await websocket.recv()
            if not 'result' in message:
                redis.hset(payload['id'], message)
            # break


# import threading
# threading.Thread(target=asyncio.run, args=(websocket_pool(), )).start()


if __name__ == '__main__':
    import threading
    
    redis.delete('payloads')
    threading.Thread(target=asyncio.run, args=(websocket_pool(), )).start()

    print(Binance.get_order_book('LUNAUSDT'))
    print(Binance.get_order_book('ETHUSDT'))
    print(Binance.get_order_book('BTCUSDT'))
