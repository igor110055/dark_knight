from datetime import datetime
from decimal import Decimal

from ..helpers.symbol_finder import load_symbols
from ..models.order_book import OrderBook
from ..utils import get_logger

logger = get_logger(__file__)


load_symbols('symbols.csv')

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
    (left_curr, left_assets), (right_curr, right_assets) = synthetic.items()

    order_book = OrderBook.get(natural_symbol)
    best_prices_natural = order_book.best_prices

    # if not best_prices_natural:
    #     return
    natural_bid = best_prices_natural['bids']
    natural_ask = best_prices_natural['asks']

    left_order_book = OrderBook.get(left_curr)
    best_prices_left = left_order_book.best_prices

    right_order_book = OrderBook.get(right_curr)
    best_prices_right = right_order_book.best_prices

    # TODO: move left_assets, right_assets to global dict
    synthetic_ask = calculate_synthetic_ask(best_prices_left, left_assets, best_prices_right, right_assets)
    synthetic_bid = calculate_synthetic_bid(best_prices_left, left_assets, best_prices_right, right_assets)

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
        with open('arbitrage', 'a') as file:
            file.write(f'{datetime.now()} - [Buy synthetic sell natural] Natural: {natural_symbol}, synthetic: {[left_curr, right_curr]}, natural bid {natural_bid}, synthetic ask: {synthetic_ask}, expected return: {buy_synthetic_sell_natural_return_perc}\n')

        # FIXME: order execution
        # if SymbolService.get_symbol(natural_symbol)['quote'] == 'USDT':
        #     print('found')
        # redis.set('TRADING', 'true', 1)
        # if engines['USDT'].buy_synthetic_sell_natural(natural_symbol, synthetic, target_perc):
        #     trade_count += 1
        # redis.set('TRADING', 'false')

        # elif natural[-4:] == 'BUSD':
        #     if engines['BUSD'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)
        # elif natural[-3:] == 'DAI':
        #     if engines['DAI'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)

    buy_natural_sell_synthetic_return_perc = (synthetic_bid - natural_ask) / natural_ask * 100
    logger.info(f'[Buy natural sell synthetic] Natural: {natural_symbol}, synthetic: {[left_curr, right_curr]}, natural bid {natural_bid}, synthetic ask: {synthetic_ask}, expected return: {buy_natural_sell_synthetic_return_perc}')
    if buy_natural_sell_synthetic_return_perc > target_perc:
        with open('arbitrage', 'a') as file:
            file.write(f'{datetime.now()} - [Buy natural sell synthetic] Natural: {natural_symbol}, synthetic: {[left_curr, right_curr]}, natural bid {natural_bid}, synthetic ask: {synthetic_ask}, expected return: {buy_synthetic_sell_natural_return_perc}\n')

    # pprint(
    #     sorted(order_book['bids'].items(),
    #            key=lambda item: item[0],
    #            reverse=True)[:10])
    # print(order_book['asks'][:10])
    # update last_update_id
    # check start_sequence increment
