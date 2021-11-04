import asyncio
import csv
import hashlib
import hmac
import os
from functools import lru_cache
from time import time
from urllib.parse import urlencode, urljoin
import random
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

from .redis_client import get_client

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
import pdb
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
    def get_order_book(symbol, id=None):
        # redis.delete('payloads')
        id = id or random.randint(1,100)
        payload = {
            "method": "SUBSCRIBE",
            'params': [f'{symbol.lower()}@depth10@100ms'],
            "id": id
        }

        redis.lpush('payloads', json.dumps(payload))

        while not (message := redis.hget('snapshots', id)):
            continue

        redis.hdel('snapshots', id)
        resp = json.loads(message)
        # print(resp['lastUpdateId'])
        return resp
    
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
                return response.json()
                # return json.loads(session.get(url, params=params).content)
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


async def websocket_pool(num=8):
    tasks = []
    for _ in range(num):
        tasks.append(asyncio.create_task(connect("wss://stream.binance.com:9443/ws")))

    await asyncio.gather(*tasks)

from queue import Queue


async def handle(websocket, sleep_duration):
    while True:
        if not (payload := redis.rpop('payloads')):
            await asyncio.sleep(0)
            continue

        message_id = json.loads(payload)['id']
        await websocket.send(payload)

        message_received = False
        while not message_received:
            message = await websocket.recv()
            if not 'result' in message:  # juice
                redis.hset('snapshots', message_id, message)
                payload = json.loads(payload)
                payload['method'] = 'UNSUBSCRIBE'

                await asyncio.sleep(sleep_duration)

                await websocket.send(json.dumps(payload))

                message_received = True
            else:
                print(message)  #ack
import sys
async def connect(url, timeout=60*15, sleep_duration=0.5):
    queue = Queue()
    async for websocket in websockets.connect(url, ping_timeout=timeout):
        for _ in range(5):
            queue.put_nowait(websocket)

        while True:
            try:
                await handle(websocket, sleep_duration)
            except websockets.ConnectionClosed:
                websocket = queue.get_nowait()
                if not websocket:
                    sys.exit()
                    raise Exception('No!')
                    break



# from websocket import WebSocketApp
# from websocket._app import Dispatcher


# class StormDispatcher(Dispatcher):
#     """
#     Dispatcher
#     """
#     def __init__(self, app, ping_timeout):
#         self.app = app
#         self.ping_timeout = ping_timeout

#     def read(self, sock, read_callback, check_callback):
#         while self.app.keep_running:
#             sel = selectors.DefaultSelector()
#             sel.register(self.app.sock.sock, selectors.EVENT_READ)

#             r = sel.select(self.ping_timeout)
#             if r:
#                 if not read_callback():
#                     break
#             check_callback()
#             sel.close()


# def on_message(ws, message):



if __name__ == '__main__':
    redis.delete('payloads', 'snapshots', 'initialized')
    import threading
        
    threading.Thread(target=asyncio.run, args=(websocket_pool(), )).start()

    for i in range(100):
        print(Binance.get_order_book('BTCUSDT'))
        print(Binance.get_order_book('ETHUSDT'))
        print(Binance.get_order_book('LUNAUSDT'))
     
    print('done')

    # asyncio.run(websocket_pool())