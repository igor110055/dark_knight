import asyncio
import json
from decimal import Decimal
from pprint import pprint

import requests
import websockets

order_book = dict()
cached_responses = list()
last_update_ids = dict()
best_prices = dict()


async def main(symbols):
    for symbol in symbols:
        best_prices[symbol.upper()] = [0, 0, 0, 0]

    async with websockets.connect(
            "wss://stream.binance.com:9443/ws") as websocket:
        payload = {
            "method": "SUBSCRIBE",
            # "params": ["btcusdt@aggTrade", "btcusdt@depth"],
            # 'params': ['btcusdt@depth@100ms'],
            'params': [f"{symbol}@depth@100ms" for symbol in symbols],
            "id": 1
        }
        await websocket.send(json.dumps(payload))
        while True:
            response = await websocket.recv()
            response = json.loads(response)

            if response.get('e') != 'depthUpdate':
                continue

            symbol = response['s']

            if symbol not in last_update_ids:
                cached_responses.append(response)
                get_order_book_snapshot(symbol)
                continue

            update_order_book(response)
            # end_sequence = response['u']
            # start_sequence = response['U']
            # print(last_update_id)
            # print(response)


def update_order_book(response):
    end_sequence = response['u']
    symbol = response['s']

    if Decimal(end_sequence) < last_update_ids.get(symbol,
                                                   Decimal('Infinity')):
        return

    start_sequence = response['U']

    ob = order_book[symbol]
    for price, size in response['b']:
        price = Decimal(price)
        size = Decimal(size)
        if size:
            ob['bids'][price] = size
        else:
            ob['bids'].pop(price, None)

    for price, size in response['a']:
        price = Decimal(price)
        size = Decimal(size)
        if size:
            # update event, reconcile with trade data
            ob['asks'][price] = size
        else:
            # cancel event
            ob['asks'].pop(price, None)

    best_bid_price = max(ob['bids'])
    best_bid_size = ob['bids'][best_bid_price]
    best_ask_price = min(ob['asks'])
    best_ask_size = ob['asks'][best_ask_price]
    best_prices[symbol] = (best_bid_price, best_bid_size, best_ask_price,
                           best_ask_size)
    # print(symbol, best_bid_price, best_bid_size, best_ask_price, best_ask_size)

    sell_natural = best_prices['TROYUSDT'][0] or 1
    buy_synthetic = best_prices['TROYBNB'][2] * best_prices['BNBUSDT'][2] or 1

    buy_natural = best_prices['TROYUSDT'][2] or 1
    sell_synthetic = best_prices['TROYBNB'][0] * best_prices['BNBUSDT'][0] or 1

    # if sell_natural > buy_synthetic or sell_synthetic > buy_natural:
    print(
        sell_natural,
        buy_synthetic,
        (sell_natural - buy_synthetic) / buy_synthetic *
        100,  # which one is the base
        buy_natural,
        sell_synthetic,
        (sell_synthetic - buy_natural) / buy_natural * 100)

    # pprint(
    #     sorted(order_book['bids'].items(),
    #            key=lambda item: item[0],
    #            reverse=True)[:10])
    # print(order_book['asks'][:10])
    # update last_update_id
    # check start_sequence increment


def get_order_book_snapshot(symbol):
    global last_update_id

    response = requests.get(
        f"https://api.binance.com/api/v3/depth?symbol={symbol}")
    data = response.json()

    last_update_ids[symbol] = data['lastUpdateId']

    bids = {Decimal(price): Decimal(size) for price, size in data['bids']}
    asks = {Decimal(price): Decimal(size) for price, size in data['asks']}

    order_book[symbol] = dict(bids=bids, asks=asks)

    for response in cached_responses:
        update_order_book(response)


if __name__ == '__main__':
    symbols = ['bnbusdt', 'troyusdt', 'troybnb']
    asyncio.run(main(symbols))
