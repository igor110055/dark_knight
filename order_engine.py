import logging
from decimal import Decimal
from time import time

import simplejson as json

from execution_engine import first_triangle_order
from redis_client import get_client


logging.basicConfig(level=logging.INFO)
r = get_client()


class OrderEngine:
    def __init__(self, client, currency='USDT', amount=Decimal('20'), taker_fee=Decimal('0.001')):
        self.client = client
        self.trading_currency = currency
        self.trading_amount = amount
        self.taker_fee = taker_fee

    def can_trade(self, currency):
        return currency[-4:] == self.trading_currency or currency[-4:] == self.trading_currency

    def convert_to_base_amount(self, symbol, price, quote_amount):
        min_amount = Decimal(self.client.market(symbol)[
                             'limits']['amount']['min'])
        absolute_base_amount = Decimal(price) / Decimal(quote_amount)
        return absolute_base_amount // min_amount * min_amount

    def buy_synthetic_sell_natural(self, natural, synthetic, best_prices, target_perc, delay=300):
        (left_symbol, (left_normal, left_assets)), (right_symbol,
                                                    (right_normal, right_assets)) = synthetic.items()

        natural_bid = best_prices[natural]['bid'][0]

        if left_normal:
            left_synthetic_ask = best_prices[left_symbol]['ask'][0]
        else:
            bid = best_prices[left_symbol]['bid'][0]
            if not bid:
                return
            left_synthetic_ask = 1 / best_prices[left_symbol]['bid'][0]

        if right_normal:
            right_synthetic_ask = best_prices[right_symbol]['ask'][0]
        else:
            bid = best_prices[right_symbol]['bid'][0]
            if not bid:
                return
            right_synthetic_ask = 1 / best_prices[right_symbol]['bid'][0]

        synthetic_ask = left_synthetic_ask * right_synthetic_ask
        if not synthetic_ask:
            return
        profit_perc = (natural_bid - synthetic_ask) / synthetic_ask * 100
        logging.info(
            f'[Order Engine] natural: {natural}, synthetic: {synthetic}, expected profit {profit_perc}')
        if profit_perc < target_perc:
            return

        try:

            # print(natural, natural[-4:] != 'USDT')
            if not self.can_trade(natural):
                return

            left_order = None
            post_left_synthetic_order = None

            now = int(time()*1000)
            if self.trading_currency in left_assets:
                if left_normal:
                    if (diff := now - best_prices[left_symbol]['ask'][1][1]) > delay:
                        return
                else:
                    if (diff := now - best_prices[left_symbol]['bid'][1][1]) > delay:
                        return
                left_order, amount = first_triangle_order(
                    self.trading_currency, self.trading_amount, left_symbol, left_assets, best_prices)
            else:
                if left_normal:
                    if now - best_prices[left_symbol]['ask'][1][1] > delay:
                        return

                    # post_left_synthetic_order = lambda quote_quantity:self.client.create_market_buy_order(left_symbol, None, params={'quoteOrderQty': quote_quantity})
                    def post_left_synthetic_order(quote_quantity): return self.client.create_order(
                        'BUY', 'MARKET', left_symbol, quote_quantity, on_quote=True)
                else:
                    bid = best_prices[left_symbol]['bid'][0]
                    if now - best_prices[left_symbol]['bid'][1][1] > delay:
                        return
                    if not bid:
                        return
                    left_synthetic_ask = 1 / bid

                    def post_left_synthetic_order(quantity): return self.client.create_order(
                        'SELL', 'MARKET', left_symbol, quantity)

            right_order = None
            post_right_synthetic_order = None

            if self.trading_currency in right_assets:
                if right_normal:
                    if now - best_prices[right_symbol]['ask'][1][1] > delay:
                        return
                else:
                    if now - best_prices[right_symbol]['bid'][1][1] > delay:
                        return
                right_order, amount = first_triangle_order(
                    self.trading_currency, self.trading_amount, right_symbol, right_assets, best_prices)
            else:
                if right_normal:
                    def post_right_synthetic_order(quote_quantity): return self.client.create_order(
                        'BUY', 'MARKET', right_symbol, quote_quantity, on_quote=True)
                    right_synthetic_ask = best_prices[right_symbol]['ask'][0]
                    if now - best_prices[right_symbol]['ask'][1][1] > delay:
                        return
                else:
                    bid = best_prices[right_symbol]['bid'][0]
                    if not bid:
                        return
                    right_synthetic_ask = 1 / bid
                    # right_synthetic_ask_amount = best_prices[right_symbol]['bid'][1]

                    def post_right_synthetic_order(quantity): return self.client.create_order(
                        'SELL', 'MARKET', right_symbol, quantity)
                    if now - best_prices[right_symbol]['bid'][1][1] > delay:
                        return

            try:
                if left_order and not right_order:
                    # amount = Decimal(left_order['amount'])
                    # if left_normal:
                    #     amount *= Decimal('0.999')
                    second_order = post_right_synthetic_order(amount)
                elif right_order and not left_order:
                    # amount = Decimal(right_order['amount'])
                    # if right_normal:
                    #     amount *= Decimal('0.999')
                    second_order = post_left_synthetic_order(amount)
            except:
                logging.error(
                    f"[Order Engine] Second trade failed: natural: {natural}, synthetic: {synthetic}, amount: {amount}")
                raise

            first_order = left_order or right_order
            natural_order = self.client.create_order(
                'SELL', 'MARKET', natural, self.trading_amount*Decimal('0.99'), on_quote=True)

            logging.info(
                f"[Order Engine] Trade: natural: {natural_order}, synthetic: {left_order}, {right_order}")
            bps = {
                natural: best_prices[natural],
                left_symbol: best_prices[left_symbol],
                right_symbol: best_prices[right_symbol]
            }
            r.set(f"done_{int(time())}", json.dumps({'natural': natural, 'synthetic': synthetic, 'best_prices': bps,
                  'first_order': first_order.id, 'second_order': second_order.id, 'natural_order': natural_order.id}))
            return True
        except Exception as error:
            bps = {
                natural: best_prices[natural],
                left_symbol: best_prices[left_symbol],
                right_symbol: best_prices[right_symbol]
            }
            r.set(int(time()), json.dumps(
                {'natural': natural, 'synthetic': synthetic, 'best_prices': bps, 'error': str(error)}))

            return False
