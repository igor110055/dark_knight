from decimal import Decimal

from .exchanges.binance import get_min_quantity, create_limit_order
from .utils import get_logger

logger = get_logger(__file__)


def convert_to_base_quantity(symbol, price, quote_amount):
    min_amount = Decimal(get_min_quantity(symbol))
    base_amount = Decimal(quote_amount) / Decimal(price)
    return (base_amount // min_amount) * min_amount


def truncate_base_quantity(symbol, base_amount):
    min_amount = Decimal(get_min_quantity(symbol))
    return (base_amount // min_amount) * min_amount


# FIXME: can only start from symbol with USDT
def first_triangle_order(
    trading_currency, trading_quantity, symbol, assets, best_prices
):
    # TODO: calulate price and amount
    # TODO: use symbol helper
    base_asset, quote_asset = assets
    order = None

    # FIXME: extend from USDT quoted asset
    if quote_asset == trading_currency:
        best_ask = Decimal(best_prices["asks"])

        to_amount = convert_to_base_quantity(symbol, best_ask, trading_quantity)

        order = create_limit_order(
            "BUY", symbol, best_ask, to_amount, is_async=True
        )
        logger.info(f"Limit buy order create for {symbol} on base {to_amount}")
    else:
        best_bid = Decimal(best_prices["bids"])

        to_amount = truncate_base_quantity(symbol, trading_quantity)

        order = create_limit_order(
            "SELL", symbol, best_bid, to_amount, is_async=True
        )
        logger.info(f"Limit sell order create for {symbol} on quote {to_amount}")

    return order, to_amount
