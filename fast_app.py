import asyncio
import simplejson as json
import threading
from decimal import Decimal

import websockets

from exchanges.binance import get_client, get_order_book_snapshot
from order_engine import OrderEngine
from redis_client import get_client as get_redis_client
from triangle_finder import get_triangles

from order_book_websocket import run as connect


strategies = dict()
symbols = dict()


fee_factor = Decimal('1') / Decimal('0.999') / \
    Decimal('0.999') * Decimal('1.00001')

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
        redis.hset('best_prices', symbol, json.dumps(dict(
            bid=[Decimal('0'), (Decimal('0'), 0)], ask=[Decimal('0'), (Decimal('0'), 0)])))

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

            if not redis.hget('last_update_ids', symbol):
                redis.rpush(f"cached_responses:{symbol}", json.dumps(response))
                get_order_book_snapshot.delay(symbol)
                continue

            update_order_book(response)
            # end_sequence = response['u']
            # start_sequence = response['U']
            # print(last_update_id)
            # print(response)


def update_order_book(response):
    end_sequence = response['u']
    symbol = response['s']

    last_update_id = Decimal(redis.hget(
        'last_update_ids', symbol).decode() or 'Infinity')
    if Decimal(end_sequence) < last_update_id:
        return

    start_sequence = response['U']

    ob = redis.hget('order_books', symbol)
    if not ob:
        return

    ob = json.loads(ob)

    ob['bids'] = {Decimal(key): value for key, value in ob['bids'].items()}
    ob['asks'] = {Decimal(key): value for key, value in ob['asks'].items()}

    best_prices_symbol = json.loads(redis.hget('best_prices', symbol))

    last_best_bid_price = best_prices_symbol['bid'][0]
    last_best_ask_price = best_prices_symbol['ask'][0]

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

    redis.hset('best_prices', symbol, json.dumps({
        'bid': [best_bid_price, best_bid_size],
        'ask': [best_ask_price, best_ask_size]
    }))

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


redis.set('TRADING', 'false')


def check_arbitrage(natural, synthetic, target_perc=0.4, upper_bound=0.8, usdt_amount=Decimal('20.0')):
    global event
    global trade_count

    (left_curr, (left_normal, left_assets)), (right_curr,
                                              (right_normal, right_assets)) = synthetic.items()

    best_prices_natural = json.loads(redis.hget('best_prices', natural))

    if not best_prices_natural:
        return
    natural_bid = best_prices_natural['bid'][0]


    # TODO: need?
    # if not all(curr in best_prices for curr in [natural, left_curr, right_curr]):
    #     return

    best_prices_left = json.loads(
        redis.hget('best_prices', left_curr).decode())
    if left_normal:
        left_synthetic_ask = best_prices_left['ask'][0]
    else:
        bid = best_prices_left['bid'][0]
        if not bid:
            return
        left_synthetic_ask = 1 / best_prices_left['bid'][0]
        left_synthetic_ask_size = 1 / best_prices_left['bid'][1][0]
        left_synthetic_ask_epoch = best_prices_left['bid'][1][1]

    best_prices_right = json.loads(redis.hget('best_prices', right_curr))
    if right_normal:
        right_synthetic_ask = best_prices_right['ask'][0]
    else:
        bid = best_prices_right['bid'][0]
        if not bid:
            return
        right_synthetic_ask = 1 / best_prices_right['bid'][0]
        right_synthetic_ask_size = 1 / best_prices_right['bid'][1][0]
        right_synthetic_ask_epoch = best_prices_right['bid'][1][1]

    synthetic_ask = left_synthetic_ask * right_synthetic_ask or 1
    synthetic_ask_size = Decimal('10') / Decimal(synthetic_ask)

    natural_ask = best_prices_natural['ask'][0] or 1

    if left_normal:
        left_synthetic_ask = best_prices_left['bid'][0]
        left_synthetic_ask_size = best_prices_left['bid'][1][0]
        left_synthetic_ask_epoch = best_prices_left['bid'][1][1]
    else:
        ask = best_prices_left['ask'][0]
        if not ask:
            return
        left_synthetic_ask = 1 / ask
        left_synthetic_ask_size = 1 / best_prices_left['ask'][1][0]
        left_synthetic_ask_epoch = best_prices_left['ask'][1][1]

    if right_normal:
        right_synthetic_ask = best_prices_right['bid'][0]
        right_synthetic_ask_size = best_prices_right['bid'][1][0]
        right_synthetic_ask_epoch = best_prices_right['bid'][1][1]
    else:
        ask = best_prices_right['ask'][0]
        if not ask:
            return
        right_synthetic_ask = 1 / ask
        right_synthetic_ask_size = 1 / best_prices_right['ask'][1][0]
        right_synthetic_ask_epoch = best_prices_right['bid'][1][1]

    synthetic_bid = left_synthetic_ask * right_synthetic_ask or 1
    # TODO: extend to non USDT quote
    synthetic_ask_size = Decimal('10') / Decimal(synthetic_bid)

    # TODO: add available size

    # natural_bid = Decimal('0.9991')
    # synthetic_ask = Decimal('1.833e-0.5') * Decimal('54555')

    if redis.get('TRADING') == 'true':
        return

    if (diff_perc := (natural_bid - synthetic_ask) / synthetic_ask * 100) > target_perc and diff_perc < upper_bound:
        # print(natural, synthetic, 'buy synthetic, sell natural', natural_bid, synthetic_ask, diff_perc)

        # print('thread:', threading.get_native_id(), time(), natural, synthetic)

        if natural[-4:] == 'USDT':
            redis.set('TRADING', 'true', 1)
            if engines['USDT'].buy_synthetic_sell_natural(natural, synthetic, target_perc):
                trade_count += 1
            redis.set('TRADING', 'false')
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


async def main(symbols):
    tasks = []
    _symbols = list(symbols.keys())[:]
    for symbol in _symbols:
        tasks.append(asyncio.create_task(connect(symbol)))

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    triangles = get_triangles()

    for natural, synthetics in triangles.items():
        strategies[natural] = synthetics

    for natural, synthetics in strategies.items():
        symbols[natural] = {}
        for synthetic in synthetics:
            for symbol, normal in synthetic.items():
                if symbol not in symbols:
                    symbols[symbol] = {}
                symbols[symbol][natural] = normal

    STEPS = 50
    for i in range(0, len(symbols), STEPS):
        _symbols = dict(list(symbols.items())[i:i+STEPS])
        t = threading.Thread(target=asyncio.run, args=(main(_symbols), ))
        t.start()
