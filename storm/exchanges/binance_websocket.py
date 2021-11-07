
import time
import zmq
from threading import Thread

import simplejson as json
import websocket
from storm.clients.redis_client import get_client
from ..utils import get_logger
from collections import deque


# websocket.enableTrace(True)


redis = get_client()
logger = get_logger(__file__)


WS_URL = "wss://stream.binance.com:9443/ws"
import random

def on_message(ws, message):
    if 'result' in message:
        return
    
    if ws.symbol_updated:
        payload = {
            "method": "UNSUBSCRIBE",
            'params': [f"{ws.old_symbol.lower()}@depth10@100ms"],
            "id": 0
        }
        ws.send(json.dumps(payload))

        payload = {
            "method": "SUBSCRIBE",
            'params': [f"{ws.symbol.lower()}@depth10@100ms"],
            "id": 0
        }
        ws.send(json.dumps(payload))
        ws.symbol_updated = False
    else:
        ws.snapshot = message


    # redis.hset('snapshot', ws.symbol, message)

    # if ws.symbol:
    #     if 'result' in message:
    #         redis.hset('snapshot_ready', ws.symbol, 1)
    #         return

    #     if not redis.hget('snapshot_ready', ws.symbol):
    #         return

    #     redis.hset('snapshots', ws.symbol, message)
    #     # logger.info(message)

    #     payload = {
    #         "method": "UNSUBSCRIBE",
    #         'params': [f"{ws.symbol.lower()}@depth10@100ms"],
    #         "id": 0
    #     }
    #     ws.send(json.dumps(payload))

    #     redis.hdel('snapshot_ready', ws.symbol)
    #     ws.symbol = None


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    payload = {
        "method": "SUBSCRIBE",
        'params': [f'{ws.symbol.lower()}@depth10@100ms'],
        "id": 0
    }
    ws.send(json.dumps(payload))

    # ws.symbol = None

    # def run(*args):

    #     while True:
    #         symbol = redis.rpop('working_on_symbols')
    #         if symbol is None:
    #             continue

    #         payload = {
    #             "method": "SUBSCRIBE",
    #             'params': [f'{symbol.lower()}@depth10@100ms'],
    #             "id": 0
    #         }
    #         ws.symbol = symbol
    #         ws.send(json.dumps(payload))
    #         # time.sleep(0.1)
    #         # ws.close()

    # Thread(target=run).start()


def get_binance_websocket():
    ws = websocket.WebSocketApp(WS_URL,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.snapshot = None
    ws.symbol_updated = False

    return ws

if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://127.0.0.1:5556')

    import time
    # wss = None

    wss = get_binance_websocket()
    logger.info('Initialized')
    wss.symbol = 'BTCUSDT'
    Thread(target=wss.run_forever, kwargs={'ping_timeout': 60}).start()

    logger.info('Ready to handle order book update')
    while True:
        message = socket.recv_json()

        # if message['type'] == 'init':
        #     if not wss:
        #         wss = get_binance_websocket()
        #         logger.info('Initialized')
        #     wss.symbol = message['symbol']
        #     Thread(target=wss.run_forever).start()

        #     socket.send_string('websocket initialized')

        if message['type'] == 'update_symbol':
            wss.old_symbol = wss.symbol
            wss.symbol = message['symbol']
            wss.symbol_updated = True
            wss.snapshot = None

            socket.send_string('symbol updated')

        elif message['type'] == 'retrieve_order_book':
            while (snapshot := wss.snapshot) is None:
                time.sleep(0.1)
            snapshot = wss.snapshot
            wss.snapshot = None
            logger.info(snapshot)
            socket.send_json(snapshot)


    # Thread(target=get_binance_websocket().run_forever).start()

    # redis.lpush('working_on_symbols', 'LUNAUSDT')
    # time.sleep(0.1)

    # print(redis.hget('snapshots', 'LUNAUSDT'))

    # redis.lpush('working_on_symbols', 'ETHUSDT')
    # # symbols.put(None)

    # time.sleep(5)
    # redis.lpush('working_on_symbols', 'BTCUSDT')

    # print('done')
