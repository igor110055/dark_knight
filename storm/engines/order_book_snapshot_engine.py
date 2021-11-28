import time
from threading import Event, Thread

import simplejson as json
import websocket

from ..clients.redis_client import get_client
from ..exchanges.binance import get_order_book
from ..utils import get_logger

# websocket.enableTrace(True)

SNAPSHOTS = dict()
WEBSOCKETS = dict()
STOP_EVENTS = dict()

WS_URL = "wss://stream.binance.com:9443/ws"


def on_message(ws, message):
    if STOP_EVENTS[ws.symbol].is_set():
        SNAPSHOTS.pop(ws.symbol)
        ws.close()
    else:
        if "result" in message:
            return

        SNAPSHOTS[ws.symbol] = message


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    logger.info(f"{ws.symbol} websocket closed, {close_status_code}, {close_msg}")
    print("### closed ###")


def on_open(ws):
    payload = {
        "method": "SUBSCRIBE",
        "params": [f"{ws.symbol.lower()}@depth5@100ms"],
        "id": 0,
    }
    ws.send(json.dumps(payload))


def get_binance_websocket(symbol):
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.symbol = symbol
    return ws


def run(symbol):
    binance_ws = get_binance_websocket(symbol)
    binance_ws.run_forever(ping_timeout=60)


def cleaner():
    while True:
        time.sleep(60)
        for symbol in STOP_EVENTS:
            STOP_EVENTS[symbol].set()


if __name__ == "__main__":
    redis = get_client()
    logger = get_logger(__file__)
    subscriber = redis.pubsub()
    subscriber.subscribe("order_book_websocket")

    logger.info("Ready to handle order book update")

    Thread(target=cleaner).start()
    for message in subscriber.listen():
        if message["type"] != "message":
            continue

        data = json.loads(message["data"])
        logger.info(data)
        if data["type"] == "subscribe":
            symbol = data["symbol"]
            if symbol in WEBSOCKETS:
                if WEBSOCKETS[symbol].is_alive():
                    continue

            logger.info(f"Initialized {symbol} websocket")
            thread = Thread(target=run, args=(symbol,))
            thread.start()
            WEBSOCKETS[symbol] = thread
            STOP_EVENTS[symbol] = Event()

        elif data["type"] == "retrieve":
            start = time.time()
            snapshot = None
            while not (snapshot := SNAPSHOTS.get(data["symbol"])):
                logger.info("getting symbol...")
                time.sleep(0.1)
                if time.time() - start > 1:
                    snapshot = json.dumps(get_order_book(data["symbol"]))
                    break

            redis.hset("snapshots", symbol, snapshot)

        elif data["type"] == "unsubscribe":
            if data["symbol"] in STOP_EVENTS:
                stop_event = STOP_EVENTS[data["symbol"]]
                stop_event.set()

        elif data["type"] == "status":
            status = {
                symbol: process.is_alive() for symbol, process in WEBSOCKETS.items()
            }

            redis.set("status", json.dumps(status))
