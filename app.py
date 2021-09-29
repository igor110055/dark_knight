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


async def main(natural, synthetic):
    natural = natural.upper()
    synthetic = {c.upper(): side for c, side in synthetic.items()}
    symbols = [natural] + list(synthetic.keys())
    for symbol in symbols:
        best_prices[symbol.upper()] = dict(bid=[0, 0], ask=[0, 0])

    async with websockets.connect(
            "wss://stream.binance.com:9443/ws") as websocket:
        params = [f"{symbol.lower()}@depth@100ms" for symbol in symbols]
        print(params)
        payload = {
            "method": "SUBSCRIBE",
            # "params": ["btcusdt@aggTrade", "btcusdt@depth"],
            # 'params': ['btcusdt@depth@100ms'],
            'params': [f"{symbol.lower()}@depth@100ms" for symbol in symbols],
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
                get_order_book_snapshot(symbol, natural, synthetic)
                continue

            update_order_book(response, natural, synthetic)
            # end_sequence = response['u']
            # start_sequence = response['U']
            # print(last_update_id)
            # print(response)


def update_order_book(response, natural, synthetic):
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
    best_prices[symbol] = {
        'bid': [best_bid_price, best_bid_size],
        'ask': [best_ask_price, best_ask_size]
    }
    # print(symbol, best_bid_price, best_bid_size, best_ask_price, best_ask_size)

    # print(best_prices)

    (left_curr, left_normal), (right_curr, right_normal) = synthetic.items()

    natural_bid = best_prices[natural]['bid'][0]

    if left_normal:
        left_synthetic_ask = best_prices[left_curr]['ask'][0]
    else:
        bid = best_prices[left_curr]['bid'][0]
        if not bid:
            return
        left_synthetic_ask = 1 / best_prices[left_curr]['bid'][0]

    if right_normal:
        right_synthetic_ask = best_prices[right_curr]['ask'][0]
    else:
        bid = best_prices[right_curr]['bid'][0]
        if not bid:
            return
        right_synthetic_ask = 1 / best_prices[right_curr]['bid'][0]

    synthetic_ask = left_synthetic_ask * right_synthetic_ask or 1

    natural_ask = best_prices[natural]['ask'][0] or 1

    if left_normal:
        left_synthetic_ask = best_prices[left_curr]['bid'][0]
    else:
        ask = best_prices[left_curr]['ask'][0]
        if not ask:
            return
        left_synthetic_ask = 1 / ask

    if right_normal:
        right_synthetic_ask = best_prices[right_curr]['bid'][0]
    else:
        ask = best_prices[right_curr]['ask'][0]
        if not ask:
            return
        right_synthetic_ask = 1 / ask

    synthetic_bid = left_synthetic_ask * right_synthetic_ask or 1

    if (diff := natural_bid - synthetic_ask) > 0:
        print('Buy synthetic, sell natural', natural_bid, synthetic_ask,
              diff / synthetic_ask * 100)

    if (diff := synthetic_bid - natural_ask) > 0:
        print('Buy natural, sell synthetic', synthetic_bid, natural_ask,
              diff / natural_ask * 100)

    # pprint(
    #     sorted(order_book['bids'].items(),
    #            key=lambda item: item[0],
    #            reverse=True)[:10])
    # print(order_book['asks'][:10])
    # update last_update_id
    # check start_sequence increment


def get_order_book_snapshot(symbol, natural, synthetic):
    global last_update_id

    response = requests.get(
        f"https://api.binance.com/api/v3/depth?symbol={symbol}")
    data = response.json()

    last_update_ids[symbol] = data['lastUpdateId']

    bids = {Decimal(price): Decimal(size) for price, size in data['bids']}
    asks = {Decimal(price): Decimal(size) for price, size in data['asks']}

    order_book[symbol] = dict(bids=bids, asks=asks)

    for response in cached_responses:
        update_order_book(response, natural, synthetic)


if __name__ == '__main__':
    # symbols = ['bnbusdt', 'troyusdt', 'troybnb']
    natural = 'waxpusdt'
    synthetic = {'waxpbnb': True, 'bnbusdt': True}
    asyncio.run(main(natural, synthetic))
