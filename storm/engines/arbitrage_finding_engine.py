from multiprocessing import Process
from time import sleep

from ..clients.redis_client import get_client
from ..tasks.order_task import check_arbitrage
from ..utils import get_logger

logger = get_logger(__file__)

EXPECTED_RETURN = 0.25

# TODO: parallel computing of each strategy


def trading(symbol):
    if symbol in ["ETHUSDT", "BNBETH", "BNBUSDT"]:
        if all(redis_client.hmget("initialized", "ETHUSDT", "BNBETH", "BNBUSDT")):
            symbol = "ETHUSDT"
            synthetic = {
                "BNBETH": {"normal": False, "assets": ["BNB", "ETH"]},
                "BNBUSDT": {"normal": True, "assets": ["BNB", "USDT"]},
            }
            check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    if symbol in ["LUNAUSDT", "LUNABNB", "BNBUSDT"]:
        if all(redis_client.hmget("initialized", "LUNAUSDT", "LUNABNB", "BNBUSDT")):
            symbol = "LUNAUSDT"
            synthetic = {
                "LUNABNB": {"normal": True, "assets": ["LUNA", "BNB"]},
                "BNBUSDT": {"normal": True, "assets": ["BNB", "USDT"]},
            }
            check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    # if symbol in ['LUNAUSDT', 'EURUSDT', 'LUNAEUR']:
    #     if all(redis_client.hmget('initialized', 'LUNAUSDT', 'EURUSDT', 'LUNAEUR')):
    #         symbol = 'LUNAUSDT'
    #         synthetic = {
    #             'EURUSDT': {'normal': True, 'assets': ['EUR', 'USDT']},
    #             'LUNAEUR': {'normal': True, 'assets': ['LUNA', 'EUR']}
    #         }
    #         check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    # if symbol in ['MATICUSDT', 'MATICBNB', 'BNBUSDT']:
    #     if all(redis_client.hmget('initialized', 'MATICUSDT', 'MATICBNB', 'BNBUSDT')):
    #         symbol = 'MATICUSDT'
    #         synthetic = {
    #             'MATICBNB': {'normal': True, 'assets': ['MATIC', 'BNB']},
    #             'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']}
    #         }
    #         check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    if symbol in ["SANDUSDT", "BNBUSDT", "SANDBNB"]:
        if all(redis_client.hmget("initialized", "SANDUSDT", "BNBUSDT", "SANDBNB")):
            symbol = "SANDUSDT"
            synthetic = {
                "BNBUSDT": {"normal": True, "assets": ["BNB", "USDT"]},
                "SANDBNB": {"normal": True, "assets": ["SAND", "BNB"]},
            }
            check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    if symbol in ["MANAUSDT", "MANAETH", "ETHUSDT"]:
        if all(redis_client.hmget("initialized", "MANAUSDT", "MANAETH", "ETHUSDT")):
            symbol = "MANAUSDT"
            synthetic = {
                "MANAETH": {"normal": True, "assets": ["MANA", "ETH"]},
                "ETHUSDT": {"normal": True, "assets": ["ETH", "USDT"]},
            }
            check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    # if symbol in ['MINAUSDT', 'MINABNB', 'BNBUSDT']:
    #     if all(redis_client.hmget('initialized', 'MINAUSDT', 'MINABNB', 'BNBUSDT')):
    #         symbol = 'MINAUSDT'
    #         synthetic = {
    #             'MINABNB': {'normal': True, 'assets': ['MINA', 'BNB']},
    #             'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']}
    #         }
    #         check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    # if symbol in ['OMGUSDT', 'OMGETH', 'ETHUSDT']:
    #     if all(redis_client.hmget('initialized', 'OMGUSDT', 'OMGETH', 'ETHUSDT')):
    #         symbol = 'OMGUSDT'
    #         synthetic = {
    #             'OMGETH': {'normal': True, 'assets': ['OMG', 'ETH']},
    #             'ETHUSDT': {'normal': True, 'assets': ['ETH', 'USDT']}
    #         }
    #         check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    if symbol in ["SOLUSDT", "BNBUSDT", "SOLBNB"]:
        if all(redis_client.hmget("initialized", "SOLUSDT", "BNBUSDT", "SOLBNB")):
            symbol = "SOLUSDT"
            synthetic = {
                "BNBUSDT": {"normal": True, "assets": ["BNB", "USDT"]},
                "SOLBNB": {"normal": True, "assets": ["SOL", "BNB"]},
            }
            check_arbitrage(symbol, synthetic, EXPECTED_RETURN)

    # if symbol in ["GALAUSDT", "BNBUSDT", "GALABNB"]:
    #     if all(redis_client.hmget("initialized", "GALAUSDT", "BNBUSDT", "GALABNB")):
    #         symbol = "GALAUSDT"
    #         synthetic = {
    #             "BNBUSDT": {"normal": True, "assets": ["BNB", "USDT"]},
    #             "GALABNB": {"normal": True, "assets": ["GALA", "BNB"]},
    #         }
    #         check_arbitrage(symbol, synthetic, EXPECTED_RETURN)


def get_arbitrage_opportunity():
    redis_client = get_client()

    while True:
        # TODO: separate into different list of symbols, use brpop
        if symbol := redis_client.brpop("updated_best_prices", 0.001):
            logger.info(f"Checking arbitrage for {symbol[1]}")
            trading(symbol[1])
        else:
            sleep(0.001)


if __name__ == "__main__":
    redis_client = get_client()
    redis_client.delete("updated_best_prices")
    redis_client.set("trade_count", 0)

    logger.info("start arbitrage")

    for _ in range(4):
        Process(target=get_arbitrage_opportunity).start()

    # get_arbitrage_opportunity()

    # TODO: use psubscribe to capture symbol and updated timestamp
