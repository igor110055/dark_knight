from decimal import Decimal
from time import time

from celery import Celery
from celery.utils.log import get_task_logger

from exchanges.binance import get_client
from tasks.redis_task import on_message
from tasks.order_book_task import update_order_book, get_order_book_snapshot


logger = get_task_logger(__name__)

app = Celery('hello', broker='redis://localhost:6379/1', backend='redis://localhost:6379/2')
client = get_client()


def convert_to_base_amount(symbol, price, quote_amount):
    min_amount = Decimal(client.get_min_quantity(symbol))
    absolute_base_amount = Decimal(price) / Decimal(quote_amount)
    return absolute_base_amount // min_amount * min_amount

@app.task
def first_triangle_order(trading_currency, trading_amount, symbol, assets, best_prices):
    # TODO: calulate price and amount
    base_asset, quote_asset = assets
    order = None

    now = int(time()*1000)
    print(now)
    if quote_asset == trading_currency:
        best_ask = Decimal(best_prices[symbol]['ask'][0])
        best_ask_amount = best_prices[symbol]['ask'][1]
        # amount = self.trading_amount / best_ask * amount_factor
        # amount = self.trading_amount * amount_factor
        amount = trading_amount
        print(symbol, 'buy', amount, best_ask, best_ask_amount, (diff:=now-best_ask_amount[1]))
        # order = self.client.create_limit_buy_order(symbol, amount, best_ask)
        # if diff > 100:
        #     return
        to_amount = convert_to_base_amount(symbol, trading_amount, best_ask)  # hard coded as USDT
        # order = self.client.create_limit_buy_order(symbol, to_amount, best_ask)
        try:
            order = client.create_order('BUY', 'MARKET', symbol, to_amount)
        except:
            print(symbol, to_amount)
            raise
        # order = self.client.create_market_buy_order(symbol, None, params={'quoteOrderQty': amount})
    else:
        best_bid = Decimal(best_prices[symbol]['bid'][0])
        best_bid_amount = best_prices[symbol]['bid'][1]
        # amount = self.trading_amount * amount_factor
        amount = trading_amount
        print(symbol, 'sell', amount, best_bid, best_bid_amount, (diff:=now-best_bid_amount[1]))
        # if diff > 100:
        #     return
        # amount = self.convert_to_base_amount(symbol, '20', best_bid)
        to_amount = trading_amount * best_bid # to ensure the asset is enough
        # order = self.client.create_limit_sell_order(symbol, amount, best_bid)
        # order = self.client.create_market_sell_order(symbol, amount)
        try:
            order = client.create_order('SELL', 'MARKET', symbol, to_amount, on_quote=True)
        except:
            print(symbol, to_amount)
            raise
    logger.info(str(order.id))
    return order, to_amount
