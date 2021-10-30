import asyncio
import csv
import hashlib
import hmac
import os
from functools import lru_cache
from time import time
from urllib.parse import urlencode, urljoin

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

from redis_client import get_client

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

from time import sleep

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
    def get_order_book(symbol, id=999999):
        redis.delete('payloads')
        payload = {
            "method": "SUBSCRIBE",
            'params': [f'{symbol.lower()}@depth20@100ms'],
            "id": id
        }

        redis.lpush('payloads', json.dumps(payload))

        while not (message := redis.hget('snapshots', id)):
            continue

        redis.hdel('snapshots', id)
        resp = json.loads(message)
        # print(resp['lastUpdateId'])
        return resp
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


async def websocket_pool(num=10):
    tasks = []
    for _ in range(num):
        tasks.append(asyncio.create_task(connect("wss://stream.binance.com:9443/ws")))

    await asyncio.gather(*tasks)


async def connect(url, timeout=60*15):
    async with websockets.connect(url, ping_timeout=timeout) as websocket:
        while True:
            while not (payload := redis.rpop('payloads')):
                await asyncio.sleep(0)

            message_id = json.loads(payload)['id']
            await websocket.send(payload)

            message_received = False
            async for message in websocket:
                if not 'result' in message:  # juice
                    redis.hset('snapshots', message_id, message)
                    await websocket.send(payload.replace('SUBSCRIBE', 'UNSUBSCRIBE'))
                    message_received = True
                elif message_received:  # cleanup
                    if 'result' in message:
                        break
                else:
                    print(message)  #ack



if __name__ == '__main__':
    redis.delete('payloads', 'snapshots')
    import threading
        
    threading.Thread(target=asyncio.run, args=(websocket_pool(), )).start()

    for i in range(10):
        print(Binance.get_order_book('BTCUSDT', i))
        print(Binance.get_order_book('ETHUSDT', i + 10))
        print(Binance.get_order_book('LUNAUSDT', i + 20))
     
    print('done')
    # print(Binance.get_order_book('BTCUSDT', 200))
