from datetime import datetime

import simplejson as json

from ..clients.redis_client import get_client as get_redis
from ..exchanges.binance import get_client
from ..utils import get_logger, logging, redis_lock
from .place_order_service import PlaceOrderService
from .helpers import write_csv

binance_client = get_client()
binance_client.load_markets()

file_handler = logging.FileHandler("arbitrage.log")
logger = get_logger(__file__, handler=file_handler)
trade_logger = get_logger("trading")
service = PlaceOrderService(binance_client)

TRADE_COUNT = 100


def get_arbitrage_opportunity(trading_group, expected_return):
    natural = trading_group["natural"]
    synthetic = trading_group["synthetic"]

    redis_client = get_redis()
    redis_client.set("trade_count", 0)
    price_update_subscriber = redis_client.pubsub()

    symbols = [natural["symbol"], *[s["symbol"] for s in synthetic]]
    for symbol in symbols:
        price_update_subscriber.subscribe(f"updated_best_prices:{symbol}")

    # TODO: separate into different list of symbols, use brpop
    for message in price_update_subscriber.listen():
        if message["type"] == "message":
            # TODO: add TTL to best prices
            check_arbitrage(natural["symbol"], synthetic, expected_return, redis_client)


def check_arbitrage(
    natural_symbol, synthetic, target_return, redis_client, upper_bound=0.01
):

    best_prices_natural, best_prices_left, best_prices_right = _get_prices(natural_symbol, synthetic)

    natural_bid = best_prices_natural["bids"]
    natural_ask = best_prices_natural["asks"]

    synthetic_ask = _calculate_synthetic_ask(
        best_prices_left, left_synthetic['normal'], best_prices_right, right_synthetic['normal']
    )
    synthetic_bid = _calculate_synthetic_bid(
        best_prices_left, left_synthetic['normal'], best_prices_right, right_synthetic['normal']
    )

    if not synthetic_bid or not synthetic_ask:
        return

    # TODO: add available size

    buy_synthetic_sell_natural_return = (natural_bid - synthetic_ask) / synthetic_ask
    logger.info(
        f"[Buy synthetic sell natural] Natural: {natural_symbol}, synthetic: {[synthetic_left_symbol, synthetic_right_symbol]}, natural bid {natural_bid}, synthetic ask: {synthetic_ask}, expected return: {buy_synthetic_sell_natural_return}"
    )

    if target_return < buy_synthetic_sell_natural_return < upper_bound:
        data = {
            "time": datetime.utcnow(),
            "strategy": "buy_synthetic_sell_natural",
            "natural": natural_symbol,
            "synthetic_left": synthetic_left_symbol,
            "synthetic_right": synthetic_right_symbol,
            "natural_bid": best_prices_natural["bids"],
            "natural_ask": best_prices_natural["asks"],
            "synthetic_left_bid": best_prices_left["bids"],
            "synthetic_left_ask": best_prices_left["asks"],
            "synthetic_right_bid": best_prices_right["bids"],
            "synthetic_right_ask": best_prices_right["asks"],
            "expected_return_perc": buy_synthetic_sell_natural_return,
        }
        write_csv(data)

        trade_count = int(redis_client.get("trade_count") or 0)
        if trade_count > TRADE_COUNT:
            return

        # TODO: instead of trading here, retrun flag and trade (DO ONE THING principle)
        with redis_lock(
            redis_client,
            f"buy_synthetic_sell_natural__{natural_symbol}_{synthetic_left_symbol}_{synthetic_right_symbol}",
        ) as lock:
            if lock.lock_acquired:
                traded = service.buy_synthetic_sell_natural(
                    natural_symbol, synthetic, best_prices_left, best_prices_right
                )
                if traded:
                    redis_client.set("trade_count", trade_count + 1)

                lock.degrade = True

        # elif natural[-4:] == 'BUSD':
        #     if engines['BUSD'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)
        # elif natural[-3:] == 'DAI':
        #     if engines['DAI'].buy_synthetic_sell_natural(natural, synthetic, best_prices):
        #         trade_count += 1
        #         sleep(3)

    buy_natural_sell_synthetic_return = (synthetic_bid - natural_ask) / natural_ask
    logger.info(
        f"[Buy natural sell synthetic] Natural: {natural_symbol}, synthetic: {[synthetic_left_symbol, synthetic_right_symbol]}, natural ask {natural_ask}, synthetic bid: {synthetic_bid}, expected return: {buy_natural_sell_synthetic_return}"
    )
    if target_return < buy_natural_sell_synthetic_return < upper_bound:
        data = {
            "time": datetime.utcnow(),
            "strategy": "buy_natural_sell_synthetic",
            "natural": natural_symbol,
            "synthetic_left": synthetic_left_symbol,
            "synthetic_right": synthetic_right_symbol,
            "natural_bid": best_prices_natural["bids"],
            "natural_ask": best_prices_natural["asks"],
            "synthetic_left_bid": best_prices_left["bids"],
            "synthetic_left_ask": best_prices_left["asks"],
            "synthetic_right_bid": best_prices_right["bids"],
            "synthetic_right_ask": best_prices_right["asks"],
            "expected_return_perc": buy_natural_sell_synthetic_return,
        }
        write_csv(data)

        trade_count = int(redis_client.get("trade_count") or 0)
        if trade_count > TRADE_COUNT:
            return

        # TODO: instead of trading here, retrun flag and trade (DO ONE THING principle)
        with redis_lock(
            redis_client,
            f"buy_natural_sell_synthetic__{natural_symbol}_{synthetic_left_symbol}_{synthetic_right_symbol}",
        ) as lock:
            if lock.lock_acquired:
                traded = service.buy_natural_sell_synthetic(
                    natural_symbol,
                    synthetic,
                    best_prices_natural,
                )
                if traded:
                    redis_client.set("trade_count", trade_count + 1)

                lock.degrade = True


def _get_prices(natural_symbol, synthetic):
    left_synthetic, right_synthetic = synthetic
    synthetic_left_symbol = left_synthetic["symbol"]
    synthetic_right_symbol = right_synthetic["symbol"]

    prices = redis_client.mget(
        f"best_prices:{natural_symbol}",
        f"best_prices:{synthetic_left_symbol}",
        f"best_prices:{synthetic_right_symbol}",
    )

    return [
        json.loads(price) if price else price for price in prices
    ]


def _calculate_synthetic_ask(
    best_prices_left, left_normal, best_prices_right, right_normal
):
    return _calculate_synthetic_price(
        "asks", best_prices_left, left_normal, best_prices_right, right_normal
    )


def _calculate_synthetic_bid(
    best_prices_left, left_normal, best_prices_right, right_normal
):
    return _calculate_synthetic_price("bids", best_prices_right, right_normal, best_prices_left, left_normal)


# TODO: return amount available
def _calculate_synthetic_price(
    target_side,
    target_best_prices,
    target_normal,
    opposite_best_prices,
    opposite_normal,
):
    if target_side == "asks":
        opposite_side = "bids"
    else:
        opposite_side = "asks"

    if target_normal:
        left_synthetic_price = target_best_prices[target_side]
    else:
        reciprocal_price = target_best_prices[opposite_side]
        if not reciprocal_price:
            return
        left_synthetic_price = 1 / reciprocal_price
        # left_synthetic_ask_size = 1 / target_best_prices['bid'][1][0]
        # left_synthetic_ask_epoch = target_best_prices['bid'][1][1]

    if opposite_normal:
        right_synthetic_price = opposite_best_prices[target_side]
    else:
        reciprocal_price = opposite_best_prices[opposite_side]
        if not reciprocal_price:
            return
        right_synthetic_price = 1 / reciprocal_price
        # right_synthetic_ask_size = 1 / opposite_best_prices['bid'][1][0]
        # right_synthetic_ask_epoch = opposite_best_prices['bid'][1][1]

    return left_synthetic_price * right_synthetic_price
