from decimal import Decimal
from time import time

from .exchanges.binance import get_client
from .utils import get_logger

logger = get_logger(__file__)
client = get_client(True)

import pdb
def convert_to_base_amount(symbol, price, quote_amount):
    min_amount = Decimal(client.get_min_quantity(symbol))
    absolute_base_amount = Decimal(price) / Decimal(quote_amount)
    return absolute_base_amount // min_amount * min_amount

def first_triangle_order(trading_currency, trading_amount, symbol, assets, best_prices):
    # TODO: calulate price and amount
    base_asset, quote_asset = assets
    order = None

    if quote_asset == trading_currency:
        best_ask = Decimal(best_prices['asks'])
        # best_ask_amount = best_prices['asks'][1]
        # amount = self.trading_amount / best_ask * amount_factor
        # amount = self.trading_amount * amount_factor
        amount = trading_amount
        # print(symbol, 'buy', amount, best_ask, best_ask_amount, (diff:=now-best_ask_amount[1]))
        # order = self.client.create_limit_buy_order(symbol, amount, best_ask)
        # if diff > 100:
        #     return
        to_amount = convert_to_base_amount(symbol, trading_amount, best_ask)  # hard coded as USDT
        # order = self.client.create_limit_buy_order(symbol, to_amount, best_ask)
        order = client.create_order('BUY', 'MARKET', symbol, to_amount)
        logger.info(f'Market buy order create for {symbol} on base {to_amount}: {order}')
        # order = self.client.create_market_buy_order(symbol, None, params={'quoteOrderQty': amount})
    else:
        best_bid = Decimal(best_prices['bids'])
        # best_bid_amount = best_prices[symbol]['bid'][1]
        # amount = self.trading_amount * amount_factor
        amount = trading_amount
        # print(symbol, 'sell', amount, best_bid, best_bid_amount, (diff:=now-best_bid_amount[1]))
        # if diff > 100:
        #     return
        # amount = self.convert_to_base_amount(symbol, '20', best_bid)
        to_amount = trading_amount * best_bid  # to ensure the asset is enough
        # order = self.client.create_limit_sell_order(symbol, amount, best_bid)
        # order = self.client.create_market_sell_order(symbol, amount)
        order = client.create_order('SELL', 'MARKET', symbol, to_amount, on_quote=True)
        logger.info(f'Market sell order create for {symbol} on quote {to_amount}: {order}')

    return order, to_amount
