import csv
from datetime import datetime
from decimal import Decimal

from ..models.order_book import OrderBook
from ..utils import get_logger, logging
from ..clients.redis_client import get_client as get_redis
from ..services.place_order_service import OrderEngine
from ..exchanges.binance import get_client

binance_client = get_client()
binance_client.load_markets()
redis = get_redis()
file_handler = logging.FileHandler('arbitrage.log')
logger = get_logger(__file__, handler=file_handler)
trade_logger = get_logger('trading')
engine = OrderEngine(binance_client)

TRADE_COUNT = 100
TRADING = False
# import pdb
# TODO: refactor two calculate price functions

def calculate_synthetic_ask(best_prices_left, left_assets, best_prices_right, right_assets):
    if left_assets['normal']:
        left_synthetic_ask = best_prices_left['asks']
    else:
        bid = best_prices_left['bids']
        if not bid:
            return
        left_synthetic_ask = 1 / bid
        # left_synthetic_ask_size = 1 / best_prices_left['bid'][1][0]
        # left_synthetic_ask_epoch = best_prices_left['bid'][1][1]

    if right_assets['normal']:
        right_synthetic_ask = best_prices_right['asks']
    else:
        bid = best_prices_right['bids']
        if not bid:
            return
        right_synthetic_ask = 1 / bid
        # right_synthetic_ask_size = 1 / best_prices_right['bid'][1][0]
        # right_synthetic_ask_epoch = best_prices_right['bid'][1][1]

    return left_synthetic_ask * right_synthetic_ask


def calculate_synthetic_bid(best_prices_left, left_assets, best_prices_right, right_assets):
    if left_assets['normal']:
        left_synthetic_ask = best_prices_left['bids']
        # left_synthetic_ask_size = best_prices_left['bid'][1][0]
        # left_synthetic_ask_epoch = best_prices_left['bid'][1][1]
    else:
        ask = best_prices_left['asks']
        if not ask:
            return
        left_synthetic_ask = 1 / ask
        # left_synthetic_ask_size = 1 / best_prices_left['ask'][1][0]
        # left_synthetic_ask_epoch = best_prices_left['ask'][1][1]

    if right_assets['normal']:
        right_synthetic_ask = best_prices_right['bids']
        # right_synthetic_ask_size = best_prices_right['bid'][1][0]
        # right_synthetic_ask_epoch = best_prices_right['bid'][1][1]
    else:
        ask = best_prices_right['asks']
        if not ask:
            return
        right_synthetic_ask = 1 / ask
        # right_synthetic_ask_size = 1 / best_prices_right['ask'][1][0]
        # right_synthetic_ask_epoch = best_prices_right['bid'][1][1]

    return left_synthetic_ask * right_synthetic_ask


def check_arbitrage(natural_symbol, synthetic, target_perc=0.4, upper_bound=0.8, usdt_amount=Decimal('20.0')):
    global TRADING

    (left_curr, left_assets), (right_curr, right_assets) = synthetic.items()

    order_book = OrderBook.get(natural_symbol)
    if not (best_prices_natural := order_book.best_prices):
        return

    left_order_book = OrderBook.get(left_curr)
    if not (best_prices_left := left_order_book.best_prices):
        return

    right_order_book = OrderBook.get(right_curr)
    if not (best_prices_right := right_order_book.best_prices):
        return

    natural_bid = best_prices_natural['bids']
    natural_ask = best_prices_natural['asks']

    synthetic_ask = calculate_synthetic_ask(
        best_prices_left, left_assets, best_prices_right, right_assets)
    synthetic_bid = calculate_synthetic_bid(
        best_prices_left, left_assets, best_prices_right, right_assets)

    # FIXME: necessary check?
    if not synthetic_bid or not synthetic_ask:
        return

    # TODO: extend to non USDT quote
    # synthetic_ask_size = Decimal('10') / Decimal(synthetic_bid)

    # TODO: add available size

    # TODO: use redis lock like symbol_lock

    buy_synthetic_sell_natural_return_perc = (natural_bid - synthetic_ask) / synthetic_ask * 100
    logger.info(f'[Buy synthetic sell natural] Natural: {natural_symbol}, synthetic: {[left_curr, right_curr]}, natural bid {natural_bid}, synthetic ask: {synthetic_ask}, expected return: {buy_synthetic_sell_natural_return_perc}')

    if buy_synthetic_sell_natural_return_perc > target_perc:
        print(f'[Buy synthetic sell natural] Natural: {natural_symbol}, synthetic: {[left_curr, right_curr]}, natural bid {natural_bid}, synthetic ask: {synthetic_ask}, expected return: {buy_synthetic_sell_natural_return_perc}')
        data = {
            'time': datetime.utcnow(),
            'strategy': 'buy_synthetic_sell_natural',
            'natural': natural_symbol,
            'synthetic_left': left_curr,
            'synthetic_right': right_curr,
            'natural_bid': best_prices_natural['bids'],
            'natural_ask': best_prices_natural['asks'],
            'synthetic_left_bid': best_prices_left['bids'],
            'synthetic_left_ask': best_prices_left['asks'],
            'synthetic_right_bid': best_prices_right['bids'],
            'synthetic_right_ask': best_prices_right['asks'],
            'expected_return_perc': buy_synthetic_sell_natural_return_perc
        }
        write_csv(data)

        # FIXME: order execution
        # if SymbolService.get_symbol(natural_symbol)['quote'] == 'USDT':
        #     print('found')
        # redis.set('TRADING', 'true', 1)
        trade_count = int(redis.get('trade_count') or 0)
        if trade_count > TRADE_COUNT:
            return

        # FIXME: this does not block concurrent trade
        if TRADING:
            return

        TRADING = True

        # TODO: instead of trading here, retrun flag and trade (DO ONE THING principle)
        if (result := engine.buy_synthetic_sell_natural(natural_symbol, synthetic, best_prices_left, best_prices_right)):
            redis.set('trade_count', trade_count + 1)

        # elif natural[-4:] == 'BUSD':
        #     if engines['BUSD'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)
        # elif natural[-3:] == 'DAI':
        #     if engines['DAI'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)

    buy_natural_sell_synthetic_return_perc = (
        synthetic_bid - natural_ask) / natural_ask * 100
    logger.info(
        f'[Buy natural sell synthetic] Natural: {natural_symbol}, synthetic: {[left_curr, right_curr]}, natural ask {natural_ask}, synthetic bid: {synthetic_bid}, expected return: {buy_natural_sell_synthetic_return_perc}')
    if buy_natural_sell_synthetic_return_perc > target_perc:
        data = {
            'time':datetime.utcnow(),
            'strategy': 'buy_natural_sell_synthetic',
            'natural': natural_symbol,
            'synthetic_left': left_curr,
            'synthetic_right': right_curr,
            'natural_bid': best_prices_natural['bids'],
            'natural_ask': best_prices_natural['asks'],
            'synthetic_left_bid': best_prices_left['bids'],
            'synthetic_left_ask': best_prices_left['asks'],
            'synthetic_right_bid': best_prices_right['bids'],
            'synthetic_right_ask': best_prices_right['asks'],
            'expected_return_perc': buy_natural_sell_synthetic_return_perc
        }
        write_csv(data)
    # pprint(
    #     sorted(order_book['bids'].items(),
    #            key=lambda item: item[0],
    #            reverse=True)[:10])
    # print(order_book['asks'][:10])
    # update last_update_id
    # check start_sequence increment

def write_csv(data, filename='arbitrage.csv'):
    with open(filename, 'a') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=['time', 'strategy', 'natural', 'synthetic_left', 'synthetic_right', 'natural_bid', 'natural_ask', 'synthetic_left_bid', 'synthetic_left_ask', 'synthetic_right_bid', 'synthetic_right_ask', 'expected_return_perc'])
        writer.writerow(data)
