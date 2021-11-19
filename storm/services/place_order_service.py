from decimal import Decimal
from time import time

from ..execution_engine import first_triangle_order

from ..clients.redis_client import get_client
from ..utils import get_logger
from ..models.session import get_session
from .save_arbitrage_orders_service import SaveArbitrageOrdersService

logger = get_logger(__file__)
redis = get_client()


class OrderEngine:
    def __init__(self, client, currency='USDT', amount=Decimal('40'), taker_fee=Decimal('0.001')):
        self.client = client
        self.trading_currency = currency
        self.trading_amount = amount
        self.taker_fee = taker_fee
        self.save_arbitrage_orders_service = SaveArbitrageOrdersService(get_session())

    def can_trade(self, currency):
        return currency[-4:] == self.trading_currency or currency[-4:] == self.trading_currency

    def convert_to_base_amount(self, symbol, price, quote_amount):
        min_amount = Decimal(self.client.market(symbol)['limits']['amount']['min'])
        absolute_base_amount = Decimal(price) / Decimal(quote_amount)
        return absolute_base_amount // min_amount * min_amount

    def buy_natural_sell_synthetic(self, natural, synthetic, best_prices_left, best_prices_right, natural_best_prices, delay=300):
        natural_order = None
        first_order = None
        second_order = None

        (left_symbol, left_items), (right_symbol, right_items) = synthetic.items()
        left_normal = left_items['normal']
        left_assets = left_items['assets']
        right_normal = right_items['normal']
        right_assets = right_items['assets']

        try:
            # print(natural, natural[-4:] != 'USDT')
            if not self.can_trade(natural):
                logger.info(f'Cannot trade {natural}, {left_symbol}, {right_symbol}')
                return False

            natural_order, natural_amount = first_triangle_order(
                self.trading_currency, self.trading_amount, natural, [natural[:-4], natural[-4:]], natural_best_prices)  # TODO: a service to get symbol assets
            
            left_order = None
            post_left_synthetic_order = None

            if self.trading_currency in left_assets:
                # last trade to convert back to USDT
                if left_normal:
                    def post_left_synthetic_order(quote_quantity):
                        return self.client.create_order('SELL', 'MARKET', left_symbol, quote_quantity, on_quote=True)
                else:
                    def post_left_synthetic_order(quantity):
                        return self.client.create_order('BUY', 'MARKET', left_symbol, quantity)
            else:
                if left_normal:
                    left_order = self.client.create_order('SELL', 'MARKET', left_symbol, natural_amount)
                else:
                    left_order = self.client.create_order('BUY', 'MARKET', left_symbol, natural_amount, on_quote=True)

            right_order = None
            post_right_synthetic_order = None

            if self.trading_currency in right_assets:
                if right_normal:
                    def post_right_synthetic_order(quote_quantity):
                        return self.client.create_order('SELL', 'MARKET', right_symbol, quote_quantity, on_quote=True)
                else:
                    def post_right_synthetic_order(quantity):
                        return self.client.create_order('BUY', 'MARKET', right_symbol, quantity)
            else:
                if right_normal:
                    right_order = self.client.create_order('SELL', 'MARKET', right_symbol, natural_amount)
                else:
                    right_order = self.client.create_order('BUY', 'MARKET', right_symbol, natural_amount, on_quote=True)

            first_order = left_order or right_order

            if 'code' in first_order:
                logger.error(
                    f'Triangular arbitrage natural: {natural}, synthetic: {synthetic} failed on first order {first_order}')
                return False

            second_order = None
            if left_order:
                # amount = Decimal(left_order['amount'])
                # if left_normal:
                #     amount *= Decimal('0.999')
                second_order = right_order = post_right_synthetic_order(self.trading_amount*Decimal('0.99'))
            else:
                # amount = Decimal(right_order['amount'])
                # if right_normal:
                #     amount *= Decimal('0.999')
                second_order = left_order = post_left_synthetic_order(self.trading_amount*Decimal('0.99'))

            if 'code' in second_order:
                logger.error(
                    f'Triangular arbitrage natural: {natural}, synthetic: {synthetic} failed on second order: {second_order}, first_order: {first_order}, natural_order: {natural_order}')
                return False

            logger.info(
                f"Triangular arbitrage natural {natural}: {natural_order}, synthetic {synthetic}: {left_order}, {right_order}")
            # bps = {
            #     natural: best_prices_natural,
            #     left_symbol: best_prices_left,
            #     right_symbol: best_prices_right
            # }
            # redis.set(f"done_{int(time())}", json.dumps({'natural': natural, 'synthetic': synthetic, 'best_prices': bps, 'first_order': first_order.id, 'second_order': second_order.id, 'natural_order': natural_order.id}))

            try:
                self.save_arbitrage_orders_service.execute(natural_order, first_order, second_order)
            except Exception as error:
                logger.error(f'Save arbitrage orders raised error {error}')

            return True
        except Exception as error:
            logger.error(
                f'Triangular arbitrage natural: {natural}, synthetic: {synthetic} failed on last order: {natural_order}, first order: {first_order}, second order: {second_order}, error: {error}')
            # bps = {
            #     natural: best_prices_natural,
            #     left_symbol: best_prices_left,
            #     right_symbol: best_prices_right
            # }
            # redis.set(int(time()), json.dumps(
            #     {'natural': natural, 'synthetic': synthetic, 'best_prices': bps, 'error': str(error)}))

            return error


    def buy_synthetic_sell_natural(self, natural, synthetic, best_prices_left, best_prices_right, delay=300):
        natural_order = None
        first_order = None
        second_order = None

        (left_symbol, left_items), (right_symbol, right_items) = synthetic.items()
        left_normal = left_items['normal']
        left_assets = left_items['assets']
        right_normal = right_items['normal']
        right_assets = right_items['assets']

        # natural_bid = best_prices_natural['bid'][0]

        # if left_normal:
        #     left_synthetic_ask = best_prices_left['ask'][0]
        # else:
        #     bid = best_prices_left['bid'][0]
        #     if not bid:
        #         return
        #     left_synthetic_ask = 1 / best_prices_left['bid'][0]

        # if right_normal:
        #     right_synthetic_ask = best_prices_right['ask'][0]
        # else:
        #     bid = best_prices_right['bid'][0]
        #     if not bid:
        #         return
        #     right_synthetic_ask = 1 / best_prices_right['bid'][0]

        # synthetic_ask = left_synthetic_ask * right_synthetic_ask
        # if not synthetic_ask:
        #     return

        # logging.info(
        #     f'[Order Engine] natural: {natural}, synthetic: {synthetic}, expected profit {profit_perc}')

        try:
            # print(natural, natural[-4:] != 'USDT')
            if not self.can_trade(natural):
                return 'cannot trade'

            left_order = None
            post_left_synthetic_order = None

            # now = int(time()*1000)  TODO: check
            if self.trading_currency in left_assets:  # check start position, left or right
                # if left_normal:
                #     diff = now - best_prices_left['ask'][1][1]
                #     logger.info(f'[Order Engine] {left_symbol} time lag: {diff}')
                #     if diff > delay:
                #         return
                # else:
                #     diff = now - best_prices_left['bid'][1][1]
                #     logger.info(f'[Order Engine] {left_symbol} time lag: {diff}')
                #     if diff > delay:
                #         return
                left_order, amount = first_triangle_order(
                    self.trading_currency, self.trading_amount, left_symbol, left_assets, best_prices_left)
            else:
                if left_normal:
                    # diff = now - best_prices_left['ask'][1][1]
                    # logging.info(
                    #     f'[Order Engine] {left_symbol} time lag: {diff}')
                    # if diff > 1000000000000:
                    #     import pdb
                    #     pdb.set_trace()
                    # if diff > delay:
                    #     return

                    # post_left_synthetic_order = lambda quote_quantity:self.client.create_market_buy_order(left_symbol, None, params={'quoteOrderQty': quote_quantity})
                    def post_left_synthetic_order(quote_quantity):
                        return self.client.create_order('BUY', 'MARKET', left_symbol, quote_quantity, on_quote=True)
                else:
                    # bid = best_prices_left['bid'][0]
                    # diff = now - best_prices_left['bid'][1][1]
                    # logging.info(
                    #     f'[Order Engine] {left_symbol} time lag: {diff}')
                    # if diff > 1000000000000:
                    #     import pdb
                    #     pdb.set_trace()
                    # if diff > delay:
                    #     return
                    # if not bid:
                    #     return
                    # left_synthetic_ask = 1 / bid

                    def post_left_synthetic_order(quantity):
                        return self.client.create_order('SELL', 'MARKET', left_symbol, quantity)

            right_order = None
            post_right_synthetic_order = None

            if self.trading_currency in right_assets:
                # if right_normal:
                #     diff = now - best_prices_right['ask'][1][1]
                #     logging.info(
                #         f'[Order Engine] {right_symbol} time lag: {diff}')
                #     if diff > 1000000000000:
                #         import pdb
                #         pdb.set_trace()
                #     if diff > delay:
                #         return
                # else:
                #     diff = now - best_prices_right['bid'][1][1]
                #     logging.info(
                #         f'[Order Engine] {right_symbol} time lag: {diff}')
                #     if diff > 1000000000000:
                #         import pdb
                #         pdb.set_trace()
                #     if diff > delay:
                #         return
                right_order, amount = first_triangle_order(
                    self.trading_currency, self.trading_amount, right_symbol, right_assets, best_prices_right)
            else:
                if right_normal:
                    def post_right_synthetic_order(quote_quantity):
                        return self.client.create_order('BUY', 'MARKET', right_symbol, quote_quantity, on_quote=True)
                    # right_synthetic_ask = best_prices_right['ask'][0]
                    # diff = now - best_prices_right['ask'][1][1]
                    # logging.info(
                    #     f'[Order Engine] {right_symbol} time lag: {diff}')
                    # if diff > 1000000000000:
                    #     import pdb
                    #     pdb.set_trace()
                    # if diff > delay:
                    #     return
                else:
                    # bid = best_prices_right['bid'][0]
                    # if not bid:
                    #     return
                    # right_synthetic_ask = 1 / bid
                    # right_synthetic_ask_amount = best_prices_right['bid'][1]

                    def post_right_synthetic_order(quantity):
                        return self.client.create_order('SELL', 'MARKET', right_symbol, quantity)
                    # diff = now - best_prices_right['bid'][1][1]
                    # logging.info(
                    #     f'[Order Engine] {right_symbol} time lag: {diff}')
                    # if diff > 1000000000000:
                    #     import pdb
                    #     pdb.set_trace()
                    # if diff > delay:
                    #     return

            first_order = left_order or right_order

            if 'code' in first_order:
                logger.error(
                    f'Triangular arbitrage natural: {natural}, synthetic: {synthetic} failed on first order {first_order}')
                return False

            second_order = None
            if left_order:
                # amount = Decimal(left_order['amount'])
                # if left_normal:
                #     amount *= Decimal('0.999')
                second_order = post_right_synthetic_order(amount)
            else:
                # amount = Decimal(right_order['amount'])
                # if right_normal:
                #     amount *= Decimal('0.999')
                second_order = post_left_synthetic_order(amount)

            if 'code' in second_order:
                logger.error(
                    f'Triangular arbitrage natural: {natural}, synthetic: {synthetic} failed on second order: {second_order}, first_order: {first_order}')
                return False

            natural_order = self.client.create_order(
                'SELL', 'MARKET', natural, self.trading_amount*Decimal('0.99'), on_quote=True)

            logger.info(
                f"Triangular arbitrage natural {natural}: {natural_order}, synthetic {synthetic}: {left_order}, {right_order}")
            # bps = {
            #     natural: best_prices_natural,
            #     left_symbol: best_prices_left,
            #     right_symbol: best_prices_right
            # }
            # redis.set(f"done_{int(time())}", json.dumps({'natural': natural, 'synthetic': synthetic, 'best_prices': bps, 'first_order': first_order.id, 'second_order': second_order.id, 'natural_order': natural_order.id}))

            try:
                self.save_arbitrage_orders_service.execute(natural_order, first_order, second_order)
            except Exception as error:
                logger.error(f'Save arbitrage orders raised error {error}')

            return True
        except Exception as error:
            logger.error(
                f'Triangular arbitrage natural: {natural}, synthetic: {synthetic} failed on last order: {natural_order}, first order: {first_order}, second order: {second_order}, error: {error}')
            # bps = {
            #     natural: best_prices_natural,
            #     left_symbol: best_prices_left,
            #     right_symbol: best_prices_right
            # }
            # redis.set(int(time()), json.dumps(
            #     {'natural': natural, 'synthetic': synthetic, 'best_prices': bps, 'error': str(error)}))

            return error
