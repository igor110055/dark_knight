import csv
from datetime import datetime

from ..models.order_book import OrderBook
from ..utils import get_logger, logging, redis_lock
from ..clients.redis_client import get_client as get_redis
from .place_order_service import OrderEngine
from ..exchanges.binance import get_client

binance_client = get_client()
binance_client.load_markets()

file_handler = logging.FileHandler("arbitrage.log")
logger = get_logger(__file__, handler=file_handler)
trade_logger = get_logger("trading")
engine = OrderEngine(binance_client)

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
            check_arbitrage(
                natural["symbol"], synthetic, expected_return, redis_client
            )  # best prices have ttl


# TODO: refactor two calculate price functions
def calculate_synthetic_ask(
    best_prices_left, left_assets, best_prices_right, right_assets
):
    if left_assets["normal"]:
        left_synthetic_ask = best_prices_left["asks"]
    else:
        bid = best_prices_left["bids"]
        if not bid:
            return
        left_synthetic_ask = 1 / bid
        # left_synthetic_ask_size = 1 / best_prices_left['bid'][1][0]
        # left_synthetic_ask_epoch = best_prices_left['bid'][1][1]

    if right_assets["normal"]:
        right_synthetic_ask = best_prices_right["asks"]
    else:
        bid = best_prices_right["bids"]
        if not bid:
            return
        right_synthetic_ask = 1 / bid
        # right_synthetic_ask_size = 1 / best_prices_right['bid'][1][0]
        # right_synthetic_ask_epoch = best_prices_right['bid'][1][1]

    return left_synthetic_ask * right_synthetic_ask


def calculate_synthetic_bid(
    best_prices_left, left_assets, best_prices_right, right_assets
):
    if left_assets["normal"]:
        left_synthetic_ask = best_prices_left["bids"]
        # left_synthetic_ask_size = best_prices_left['bid'][1][0]
        # left_synthetic_ask_epoch = best_prices_left['bid'][1][1]
    else:
        ask = best_prices_left["asks"]
        if not ask:
            return
        left_synthetic_ask = 1 / ask
        # left_synthetic_ask_size = 1 / best_prices_left['ask'][1][0]
        # left_synthetic_ask_epoch = best_prices_left['ask'][1][1]

    if right_assets["normal"]:
        right_synthetic_ask = best_prices_right["bids"]
        # right_synthetic_ask_size = best_prices_right['bid'][1][0]
        # right_synthetic_ask_epoch = best_prices_right['bid'][1][1]
    else:
        ask = best_prices_right["asks"]
        if not ask:
            return
        right_synthetic_ask = 1 / ask
        # right_synthetic_ask_size = 1 / best_prices_right['ask'][1][0]
        # right_synthetic_ask_epoch = best_prices_right['bid'][1][1]

    return left_synthetic_ask * right_synthetic_ask


def check_arbitrage(
    natural_symbol, synthetic, target_return, redis_client, upper_bound=0.01
):
    left_synthetic, right_synthetic = synthetic
    synthetic_left_symbol = left_synthetic["symbol"]
    synthetic_right_symbol = right_synthetic["symbol"]

    order_book = OrderBook.get(natural_symbol)
    if not (best_prices_natural := order_book.best_prices):
        return

    left_order_book = OrderBook.get(synthetic_left_symbol)
    if not (best_prices_left := left_order_book.best_prices):
        return

    right_order_book = OrderBook.get(synthetic_right_symbol)
    if not (best_prices_right := right_order_book.best_prices):
        return

    natural_bid = best_prices_natural["bids"]
    natural_ask = best_prices_natural["asks"]

    synthetic_ask = calculate_synthetic_ask(
        best_prices_left, left_synthetic, best_prices_right, right_synthetic
    )
    synthetic_bid = calculate_synthetic_bid(
        best_prices_left, left_synthetic, best_prices_right, right_synthetic
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
                traded = engine.buy_synthetic_sell_natural(
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
                traded = engine.buy_natural_sell_synthetic(
                    natural_symbol,
                    synthetic,
                    best_prices_left,
                    best_prices_right,
                    best_prices_natural,
                )
                if traded:
                    redis_client.set("trade_count", trade_count + 1)

                lock.degrade = True


def write_csv(data, filename="arbitrage.csv"):
    # logger.info(str(data))
    with open(filename, "a") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "time",
                "strategy",
                "natural",
                "synthetic_left",
                "synthetic_right",
                "natural_bid",
                "natural_ask",
                "synthetic_left_bid",
                "synthetic_left_ask",
                "synthetic_right_bid",
                "synthetic_right_ask",
                "expected_return_perc",
            ],
        )
        writer.writerow(data)
