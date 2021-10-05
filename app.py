import pdb
import asyncio
import json
from decimal import Decimal
from pprint import pprint

import time

import requests
import websockets

import threading
import functools

from triangle_finder import get_triangles

from concurrent.futures import ProcessPoolExecutor

from ccxt import binance
from dotenv import load_dotenv
import os

order_book = dict()
cached_responses = list()
last_update_ids = dict()
best_prices = dict()
strategies = dict()
symbols = dict()

load_dotenv()
binance_client = binance({'apiKey': os.getenv('API_KEY'), 'secret': os.getenv('SECRET_KEY')})

fee_factor = Decimal('1') / Decimal('0.999') / Decimal('0.999') * Decimal('1.00001')

trade_count = 0

# last_pong = time.time()

async def main(symbols):
    # global last_pong
    global trade_count
    for symbol in symbols:
        best_prices[symbol] = dict(
            bid=[Decimal('0'), Decimal('0')], ask=[Decimal('0'), Decimal('0')])

    async with websockets.connect(
            "wss://stream.binance.com:9443/ws", ping_timeout=60*15) as websocket:
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
        while trade_count < 10:
            response = await websocket.recv()

            # if (now := time.time()) > last_pong + 15:
            #     await websocket.pong()
            #     last_pong = now

            response = json.loads(response)

            if response.get('e') != 'depthUpdate':
                print(response)
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
    best_prices[symbol] = {
        'bid': [best_bid_price, best_bid_size],
        'ask': [best_ask_price, best_ask_size]
    }

    synthetics = strategies.get(symbol)
    # not a triangle
    if not synthetics:
        return

    for synthetic in synthetics:
        check_arbitrage(symbol, synthetic, 0.4)

    # check arbitrage of symbol being a synthetic
    for natural_symbol in symbols[symbol]:
        # TODO: wrong, should only get related natural synthetics
        synthetics = strategies[natural_symbol]
        for synthetic in synthetics:
            if symbol in synthetic:
                check_arbitrage(natural_symbol, synthetic, 0.4)

    # if symbol in symbols:
    #     natural = strategies.get(symbol)
    #     if not natural:
    #         return

    #     synthetics = [strategies[s] for s in symbols[symbol]]
    #     print('symbol:', symbol)
    #     print('natural:', natural)
    #     print('synthetics:', synthetics)
    #     print('-'*30)

    # TODO: check arbitrage opportunities
    # print(symbol, best_bid_price, best_bid_size, best_ask_price, best_ask_size)

    # print(best_prices)


def check_arbitrage(natural, synthetic, target_perc=0.4, upper_bound=0.8, usdt_amount=Decimal('20.0')):
    global event
    global trade_count

    (left_curr, (left_normal, left_assets)), (right_curr, (right_normal, right_assets)) = synthetic.items()

    if natural not in best_prices:
        return
    natural_bid = best_prices[natural]['bid'][0]

    if not all(curr in best_prices for curr in [natural, left_curr, right_curr]):
        return

    if left_normal:
        left_synthetic_ask = best_prices[left_curr]['ask'][0]
    else:
        bid = best_prices[left_curr]['bid'][0]
        if not bid:
            return
        left_synthetic_ask = 1 / best_prices[left_curr]['bid'][0]
        left_synthetic_ask_size = 1 / best_prices[left_curr]['bid'][1]

    if right_normal:
        right_synthetic_ask = best_prices[right_curr]['ask'][0]
    else:
        bid = best_prices[right_curr]['bid'][0]
        if not bid:
            return
        right_synthetic_ask = 1 / best_prices[right_curr]['bid'][0]
        right_synthetic_ask_size = 1 / best_prices[right_curr]['bid'][1]

    synthetic_ask = left_synthetic_ask * right_synthetic_ask or 1
    synthetic_ask_size = Decimal('10') / synthetic_ask

    natural_ask = best_prices[natural]['ask'][0] or 1

    if left_normal:
        left_synthetic_ask = best_prices[left_curr]['bid'][0]
        left_synthetic_ask_size = best_prices[left_curr]['bid'][1]
    else:
        ask = best_prices[left_curr]['ask'][0]
        if not ask:
            return
        left_synthetic_ask = 1 / ask
        left_synthetic_ask_size = 1 / best_prices[left_curr]['ask'][1]

    if right_normal:
        right_synthetic_ask = best_prices[right_curr]['bid'][0]
        right_synthetic_ask_size = best_prices[right_curr]['bid'][1]
    else:
        ask = best_prices[right_curr]['ask'][0]
        if not ask:
            return
        right_synthetic_ask = 1 / ask
        right_synthetic_ask_size = 1 / best_prices[right_curr]['ask'][1]

    synthetic_bid = left_synthetic_ask * right_synthetic_ask or 1
    synthetic_ask_size = Decimal('10') / synthetic_bid  # TODO: extend to non USDT quote

    # TODO: add available size

    if (diff_perc := (natural_bid - synthetic_ask) / synthetic_ask * 100) > target_perc and diff_perc < upper_bound:
        print(natural, synthetic, 'buy synthetic, sell natural', natural_bid, synthetic_ask, diff_perc)

        left_order = None
        post_left_synthetic_order = None

        print(natural, natural[-4:] != 'USDT')
        if natural[-4:] != 'USDT':
            return

        # pdb.set_trace()
        base_asset, quote_asset = left_assets
        if 'USDT' in left_assets:
            if quote_asset == 'USDT':
                left_order = binance_client.create_market_buy_order(left_curr, None, params={'quoteOrderQty': usdt_amount*fee_factor})
            else:
                left_order = binance_client.create_market_sell_order(left_curr, usdt_amount*fee_factor)
        else:
            if left_normal:
                post_left_synthetic_order = lambda quote_quantity:binance_client.create_market_buy_order(left_curr, None, params={'quoteOrderQty': quote_quantity})
            else:
                bid = best_prices[left_curr]['bid'][0]
                if not bid:
                    return
                left_synthetic_ask = 1 / best_prices[left_curr]['bid'][0]

                post_left_synthetic_order = lambda quantity:binance_client.create_market_sell_order(left_curr, quantity)

        right_order = None
        post_right_synthetic_order = None


        base_asset, quote_asset = right_assets
        if 'USDT' in right_assets:
            if quote_asset == 'USDT':
                right_order = binance_client.create_market_buy_order(right_curr, None, params={'quoteOrderQty': usdt_amount*fee_factor})
            else:
                right_order = binance_client.create_market_sell_order(right_curr, usdt_amount*fee_factor)
        else:
            if right_normal:
                post_right_synthetic_order = lambda quote_quantity:binance_client.create_market_buy_order(right_curr, None, params={'quoteOrderQty': quote_quantity})
                right_synthetic_ask = best_prices[right_curr]['ask'][0]
            else:
                bid = best_prices[right_curr]['bid'][0]
                if not bid:
                    return
                right_synthetic_ask = 1 / best_prices[right_curr]['bid'][0]
                # right_synthetic_ask_amount = best_prices[right_curr]['bid'][1]
                post_right_synthetic_order = lambda quantity:binance_client.create_market_sell_order(right_curr, quantity)

        last_order = None
        if left_order and not right_order:
            amount = Decimal(left_order['amount'])
            if left_normal:
                amount *= Decimal('0.999')
            last_order = post_right_synthetic_order(amount)
        elif right_order and not left_order:
            amount = Decimal(right_order['amount'])
            if right_normal:
                amount *= Decimal('0.999')
            last_order = post_left_synthetic_order(amount)

        if last_order:
            natural_order = binance_client.create_market_sell_order(natural, Decimal(last_order['amount'])*Decimal('0.999'))

            print(left_order, right_order, natural_order)
            trade_count += 1


    if (diff_perc := (synthetic_bid - natural_ask) / natural_ask * 100) > target_perc and diff_perc < upper_bound:
        print(natural, synthetic, 'buy natural, sell synthetic',
              synthetic_bid, natural_ask, diff_perc)

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

    try:
        last_update_ids[symbol] = data['lastUpdateId']
    except Exception as e:
        print(data)
        raise e

    bids = {Decimal(price): Decimal(size) for price, size in data['bids']}
    asks = {Decimal(price): Decimal(size) for price, size in data['asks']}

    order_book[symbol] = dict(bids=bids, asks=asks)

    for response in cached_responses:
        update_order_book(response)


def dummy():
    # TODO: pickle
    triangles = get_triangles()

    i = 0
    for natural, synthetics in triangles.items():
        print(natural)
        strategies[natural] = synthetics

        i += 1

        # if i == 10:
        #     break

    # print(len(tasks))

    # print(strategies)

    for natural, synthetics in strategies.items():
        symbols[natural] = {}
        for synthetic in synthetics:
            try:
                for symbol, normal in synthetic.items():
                    if symbol not in symbols:
                        symbols[symbol] = {}
                    symbols[symbol][natural] = normal
            except:
                pdb.set_trace()

    # print(symbols)

    # for symbol in symbols.keys():
    #     tasks.append(asyncio.create_task(main(symbol)))

    # await asyncio.gather(*tasks)

    # for symbol in list(symbols.items())[::200]:

    steps = 200
    ts = []
    count = 0
    tasks = []
    for i in range(0, len(symbols), steps):
        _symbols = dict(list(symbols.items())[i:i+steps])
        # tasks.append(asyncio.create_task(main(_symbols)))
        t = threading.Thread(target=asyncio.run, args=(main(_symbols), ))
        t.start()
        count += 1
        if count == 5:
            break
        ts.append(t)

    # await asyncio.gather(*tasks)

    for t in ts:
        t.join()


if __name__ == '__main__':
    # symbols = ['bnUSDTt', 'troyusdt', 'troybnb']
# 
    # asyncio.run(dummy())
    dummy()