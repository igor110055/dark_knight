from time import sleep
from multiprocessing import Manager, Process, current_process

from ..clients.redis_client import get_client
from ..tasks.order_task import check_arbitrage


# TODO: parallel computing of each strategy
def trading(symbol):
    if symbol in ['LUNAUSDT', 'LUNABNB', 'BNBUSDT']:
        if all(redis_client.hmget('initialized', 'LUNAUSDT', 'LUNABNB', 'BNBUSDT')):
            symbol = 'LUNAUSDT'
            synthetic = {
                'LUNABNB': {'normal': True, 'assets': ['LUNA', 'BNB']},
                'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']}
            }
            check_arbitrage(symbol, synthetic, 0.25)
    
    if symbol in ['LUNAUSDT', 'EURUSDT', 'LUNAEUR']:
        if all(redis_client.hmget('initialized', 'LUNAUSDT', 'EURUSDT', 'LUNAEUR')):
            symbol = 'LUNAUSDT'
            synthetic = {
                'EURUSDT': {'normal': True, 'assets': ['EUR', 'USDT']},
                'LUNAEUR': {'normal': True, 'assets': ['LUNA', 'EUR']}
            }
            check_arbitrage(symbol, synthetic, 0.25)

    if symbol in ['MATICUSDT', 'MATICBNB', 'BNBUSDT']:
        if all(redis_client.hmget('initialized', 'MATICUSDT', 'MATICBNB', 'BNBUSDT')):
            symbol = 'MATICUSDT'
            synthetic = {
                'MATICBNB': {'normal': True, 'assets': ['MATIC', 'BNB']},
                'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']}
            }
            check_arbitrage(symbol, synthetic, 0.25)

    # if symbol in ['MATICUSDT', 'MATICTRY', 'USDTTRY']:
    #     symbol = 'MATICUSDT'
    #     synthetic = {
    #         'MATICTRY': {'normal': True},
    #         'USDTTRY': {'normal': False}
    #     }
    #     check_arbitrage(symbol, synthetic, 0)

    if symbol in ['SANDUSDT', 'BNBUSDT', 'SANDBNB']:
        if all(redis_client.hmget('initialized', 'SANDUSDT', 'BNBUSDT', 'SANDBNB')):
            symbol = 'SANDUSDT'
            synthetic = {
                'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']},
                'SANDBNB': {'normal': True, 'assets': ['SAND', 'BNB']}
            }
            check_arbitrage(symbol, synthetic, 0.25)

    if symbol in ['MANAUSDT', 'MANAETH', 'ETHUSDT']:
        if all(redis_client.hmget('initialized', 'MANAUSDT', 'MANAETH', 'ETHUSDT')):
            symbol = 'MANAUSDT'
            synthetic = {
                'MANAETH': {'normal': True, 'assets': ['MANA', 'ETH']},
                'ETHUSDT': {'normal': True, 'assets': ['ETH', 'USDT']}
            }
            check_arbitrage(symbol, synthetic, 0.25)

    if symbol in ['MINAUSDT', 'MINABNB', 'BNBUSDT']:
        if all(redis_client.hmget('initialized', 'MINAUSDT', 'MINABNB', 'BNBUSDT')):
            symbol = 'MINAUSDT'
            synthetic = {
                'MINABNB': {'normal': True, 'assets': ['MINA', 'BNB']},
                'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']}
            }
            check_arbitrage(symbol, synthetic, 0.25)

    if symbol in ['OMGUSDT', 'OMGETH', 'ETHUSDT']:
        if all(redis_client.hmget('initialized', 'OMGUSDT', 'OMGETH', 'ETHUSDT')):
            symbol = 'OMGUSDT'
            synthetic = {
                'OMGETH': {'normal': True, 'assets': ['OMG', 'ETH']},
                'ETHUSDT': {'normal': True, 'assets': ['ETH', 'USDT']}
            }
            check_arbitrage(symbol, synthetic, 0.25)


def get_arbitrage_opportunity(symbols):
    while True:
        if (symbol := symbols.get()):
            trading(symbol)
            print(symbol, current_process())
        else:
            sleep(0.001)


if __name__ == '__main__':
    manager = Manager()
    symbols = manager.Queue()

    redis_client = get_client()
    redis_client.set('trade_count', 0)

    for _ in range(8):
        Process(target=get_arbitrage_opportunity, args=(symbols, )).start()

    print('start arbitrage')

    subscriber = redis_client.pubsub()

    # TODO: use psubscribe to capture symbol and updated timestamp
    subscriber.subscribe('updated_best_prices')

    while True:
        message = subscriber.get_message()
        if message and 'data' in message:
            symbols.put_nowait(message['data'])
        else:
            sleep(0.001)
