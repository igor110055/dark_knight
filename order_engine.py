from decimal import Decimal
from time import time

class OrderEngine:
    def __init__(self, client, currency='USDT', amount=Decimal('20'), taker_fee=Decimal('0.001')):
        self.client = client
        self.trading_currency = currency
        self.trading_amount = amount
        self.taker_fee = taker_fee

    def can_trade(self, currency):
        return currency[-4:] == self.trading_currency

    def first_triangle_order(self, symbol, assets, best_prices):
        # TODO: calulate price and amount
        base_asset, quote_asset = assets
        amount_factor = Decimal('1') / (1-self.taker_fee)**2 * Decimal('1.00001')
        order = None

        now = int(time()*1000)
        print(now)
        if quote_asset == 'USDT':
            best_ask = Decimal(best_prices[symbol]['ask'][0])
            best_ask_amount = best_prices[symbol]['ask'][1]
            amount = self.trading_amount / best_ask * amount_factor
            print(symbol, 'buy', amount, best_ask, best_ask_amount, (diff:=now-best_ask_amount[1]))
            # order = self.client.create_limit_buy_order(symbol, amount, best_ask)
            # order = self.client.create_market_buy_order(symbol, None, params={'quoteOrderQty': amount})
        else:
            best_bid = Decimal(best_prices[symbol]['bid'][0])
            best_bid_amount = best_prices[symbol]['bid'][1]
            amount = self.trading_amount*amount_factor
            print(symbol, 'sell', amount, best_bid, best_bid_amount, (diff:=now-best_bid_amount[1]))
            # order = self.client.create_limit_sell_order(symbol, amount, best_bid)
        return order

    def buy_synthetic_sell_natural(self, natural, synthetic, best_prices):
        global trade_count

        (left_symbol, (left_normal, left_assets)), (right_symbol, (right_normal, right_assets)) = synthetic.items()

        # print(natural, natural[-4:] != 'USDT')
        if not self.can_trade(natural):
            return

        left_order = None
        post_left_synthetic_order = None

        if self.trading_currency in left_assets:
            left_order = self.first_triangle_order(left_symbol, left_assets, best_prices)
        else:
            pass
            # if left_normal:
            #     post_left_synthetic_order = lambda quote_quantity:self.client.create_market_buy_order(left_symbol, None, params={'quoteOrderQty': quote_quantity})
            # else:
            #     bid = best_prices[left_symbol]['bid'][0]
            #     if not bid:
            #         return
            #     left_synthetic_ask = 1 / bid

            #     post_left_synthetic_order = lambda quantity:self.client.create_market_sell_order(left_symbol, quantity)

        right_order = None
        post_right_synthetic_order = None

        if 'USDT' in right_assets:
            right_order = self.first_triangle_order(right_symbol, right_assets, best_prices)
        else:
            pass
            # if right_normal:
            #     post_right_synthetic_order = lambda quote_quantity:self.client.create_market_buy_order(right_symbol, None, params={'quoteOrderQty': quote_quantity})
            #     right_synthetic_ask = best_prices[right_symbol]['ask'][0]
            # else:
            #     bid = best_prices[right_symbol]['bid'][0]
            #     if not bid:
            #         return
            #     right_synthetic_ask = 1 / bid
            #     # right_synthetic_ask_amount = best_prices[right_symbol]['bid'][1]
            #     post_right_synthetic_order = lambda quantity:self.client.create_market_sell_order(right_symbol, quantity)

        # last_order = None
        # if left_order and not right_order:
        #     amount = Decimal(left_order['amount'])
        #     if left_normal:
        #         amount *= Decimal('0.999')
        #     last_order = post_right_synthetic_order(amount)
        # elif right_order and not left_order:
        #     amount = Decimal(right_order['amount'])
        #     if right_normal:
        #         amount *= Decimal('0.999')
        #     last_order = post_left_synthetic_order(amount)

        # if last_order:
        #     natural_order = self.client.create_market_sell_order(natural, Decimal(last_order['amount'])*Decimal('0.999'))

        #     print(left_order, right_order, natural_order)
        #     return True

        return False