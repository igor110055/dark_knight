import asyncio
import json
import threading
from decimal import Decimal

import websockets

from exchanges.binance import get_client
from order_engine import OrderEngine
from redis_client import get_client as get_redis_client
from triangle_finder import get_triangles

order_book = dict()
cached_responses = list()
last_update_ids = dict()
best_prices = dict()
strategies = dict()
symbols = dict()


fee_factor = Decimal('1') / Decimal('0.999') / Decimal('0.999') * Decimal('1.00001')

trade_count = 0

TRADE_LIMIT = 3

# last_pong = time.time()

binance_client = get_client()

engines = {
    'USDT': OrderEngine(binance_client),
    # 'BUSD': OrderEngine(binance_client, currency='BUSD'),
    # 'DAI': OrderEngine(binance_client, currency='DAI')
}
# engine = OrderEngine(binance_client)

redis = get_redis_client()


async def main(symbols):
    # global last_pong
    global trade_count
    for symbol in symbols:
        best_prices[symbol] = dict(
            bid=[Decimal('0'), (Decimal('0'), 0)], ask=[Decimal('0'), (Decimal('0'), 0)])

    async with websockets.connect(
            "wss://stream.binance.com:9443/ws", ping_timeout=60*15) as websocket:
        params = [f"{symbol.lower()}@depth@100ms" for symbol in symbols]
        # print(params)
        payload = {
            "method": "SUBSCRIBE",
            # "params": ["btcusdt@aggTrade", "btcusdt@depth"],
            # 'params': ['btcusdt@depth@100ms'],
            'params': [f"{symbol.lower()}@depth@100ms" for symbol in symbols],
            "id": 1
        }
        await websocket.send(json.dumps(payload))
        while trade_count < TRADE_LIMIT:
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

    ob = order_book.get(symbol)
    if not ob:
        return

    last_best_bid_price = best_prices[symbol]['bid'][0]
    last_best_ask_price = best_prices[symbol]['ask'][0]

    epoch = response['E']

    for price, size in response['b']:
        price = Decimal(price)
        size = Decimal(size)
        if size:
            ob['bids'][price] = (size, epoch)
        else:
            ob['bids'].pop(price, None)

    for price, size in response['a']:
        price = Decimal(price)
        size = Decimal(size)
        if size:
            # update event, reconcile with trade data
            ob['asks'][price] = (size, epoch)
        else:
            # cancel event
            ob['asks'].pop(price, None)

    # finish

    best_bid_price = max(ob['bids'])
    best_bid_size = ob['bids'][best_bid_price]
    best_ask_price = min(ob['asks'])
    best_ask_size = ob['asks'][best_ask_price]
    best_prices[symbol] = {
        'bid': [best_bid_price, best_bid_size],
        'ask': [best_ask_price, best_ask_size]
    }

    if best_bid_price < last_best_bid_price and best_ask_price > last_best_ask_price:
        return

    synthetics = strategies.get(symbol)
    # not a triangle
    if not synthetics:
        return

    for synthetic in synthetics:
        check_arbitrage(symbol, synthetic, 0.3)

    # check arbitrage of symbol being a synthetic
    for natural_symbol in symbols[symbol]:
        # TODO: wrong, should only get related natural synthetics
        synthetics = strategies[natural_symbol]
        for synthetic in synthetics:
            if symbol in synthetic:
                check_arbitrage(natural_symbol, synthetic, 0.35)

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


redis.set('TRADING', 0)

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
        left_synthetic_ask_size = 1 / best_prices[left_curr]['bid'][1][0]
        left_synthetic_ask_epoch = best_prices[left_curr]['bid'][1][1]

    if right_normal:
        right_synthetic_ask = best_prices[right_curr]['ask'][0]
    else:
        bid = best_prices[right_curr]['bid'][0]
        if not bid:
            return
        right_synthetic_ask = 1 / best_prices[right_curr]['bid'][0]
        right_synthetic_ask_size = 1 / best_prices[right_curr]['bid'][1][0]
        right_synthetic_ask_epoch = best_prices[right_curr]['bid'][1][1]

    synthetic_ask = left_synthetic_ask * right_synthetic_ask or 1
    synthetic_ask_size = Decimal('10') / synthetic_ask

    natural_ask = best_prices[natural]['ask'][0] or 1

    if left_normal:
        left_synthetic_ask = best_prices[left_curr]['bid'][0]
        left_synthetic_ask_size = best_prices[left_curr]['bid'][1][0]
        left_synthetic_ask_epoch = best_prices[left_curr]['bid'][1][1]
    else:
        ask = best_prices[left_curr]['ask'][0]
        if not ask:
            return
        left_synthetic_ask = 1 / ask
        left_synthetic_ask_size = 1 / best_prices[left_curr]['ask'][1][0]
        left_synthetic_ask_epoch = best_prices[left_curr]['ask'][1][1]

    if right_normal:
        right_synthetic_ask = best_prices[right_curr]['bid'][0]
        right_synthetic_ask_size = best_prices[right_curr]['bid'][1][0]
        right_synthetic_ask_epoch = best_prices[right_curr]['bid'][1][1]
    else:
        ask = best_prices[right_curr]['ask'][0]
        if not ask:
            return
        right_synthetic_ask = 1 / ask
        right_synthetic_ask_size = 1 / best_prices[right_curr]['ask'][1][0]
        right_synthetic_ask_epoch = best_prices[right_curr]['bid'][1][1]

    synthetic_bid = left_synthetic_ask * right_synthetic_ask or 1
    synthetic_ask_size = Decimal('10') / synthetic_bid  # TODO: extend to non USDT quote

    # TODO: add available size

    # natural_bid = Decimal('0.9991')
    # synthetic_ask = Decimal('1.833e-0.5') * Decimal('54555')

    if redis.get('TRADING'):
        return

    if (diff_perc := (natural_bid - synthetic_ask) / synthetic_ask * 100) > target_perc and diff_perc < upper_bound:
        # print(natural, synthetic, 'buy synthetic, sell natural', natural_bid, synthetic_ask, diff_perc)

        # print('thread:', threading.get_native_id(), time(), natural, synthetic)

        if natural[-4:] == 'USDT':
            redis.set('TRADING', 1, 1)
            if engines['USDT'].buy_synthetic_sell_natural(natural, synthetic, best_prices, target_perc):
                trade_count += 1
            redis.set('TRADING', 0)
        # elif natural[-4:] == 'BUSD':
        #     if engines['BUSD'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)
        # elif natural[-3:] == 'DAI':
        #     if engines['DAI'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)


    # if (diff_perc := (synthetic_bid - natural_ask) / natural_ask * 100) > target_perc and diff_perc < upper_bound:
    #     print(natural, synthetic, 'buy natural, sell synthetic',
    #           synthetic_bid, natural_ask, diff_perc)

    # pprint(
    #     sorted(order_book['bids'].items(),
    #            key=lambda item: item[0],
    #            reverse=True)[:10])
    # print(order_book['asks'][:10])
    # update last_update_id
    # check start_sequence increment


def get_order_book_snapshot(symbol):
    data = binance_client.get_order_book(symbol)

    try:
        last_update_ids[symbol] = data['lastUpdateId']
    except KeyError as e:
        print(data)
        return

    bids = {Decimal(price): (Decimal(size), data['lastUpdateId']) for price, size in data['bids']}
    asks = {Decimal(price): (Decimal(size), data['lastUpdateId']) for price, size in data['asks']}

    order_book[symbol] = dict(bids=bids, asks=asks)

    for response in cached_responses:
        update_order_book(response)


def dummy():
    # TODO: pickle
    triangles = get_triangles()

    i = 0
    for natural, synthetics in triangles.items():
        # print(natural)
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

    # for t in ts:
    #     t.join()


if __name__ == '__main__':
    # symbols = ['bnUSDTt', 'troyusdt', 'troybnb']
# 
    # asyncio.run(dummy())
    dummy()
