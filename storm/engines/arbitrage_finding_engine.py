import asyncio
from concurrent.futures import ProcessPoolExecutor

from ..clients.redis_client import get_client
from ..tasks.order_task import check_arbitrage


def trading(symbol):
    if symbol in ['LUNAUSDT', 'LUNABNB', 'BNBUSDT']:
        symbol = 'LUNAUSDT'
        synthetic = {
            'LUNABNB': {'normal': True, 'assets': ['LUNA', 'BNB']},
            'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']}
        }
        check_arbitrage(symbol, synthetic, 0.3)
    
    if symbol in ['LUNAUSDT', 'EURUSDT', 'LUNAEUR']:
        symbol = 'LUNAUSDT'
        synthetic = {
            'EURUSDT': {'normal': True, 'assets': ['EUR', 'USDT']},
            'LUNAEUR': {'normal': True, 'assets': ['LUNA', 'EUR']}
        }
        check_arbitrage(symbol, synthetic, 0.3)

    if symbol in ['MATICUSDT', 'MATICBNB', 'BNBUSDT']:
        symbol = 'MATICUSDT'
        synthetic = {
            'MATICBNB': {'normal': True, 'assets': ['MATIC', 'BNB']},
            'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']}
        }
        check_arbitrage(symbol, synthetic, 0.3)

    # if symbol in ['MATICUSDT', 'MATICTRY', 'USDTTRY']:
    #     symbol = 'MATICUSDT'
    #     synthetic = {
    #         'MATICTRY': {'normal': True},
    #         'USDTTRY': {'normal': False}
    #     }
    #     check_arbitrage(symbol, synthetic, 0)

    if symbol in ['SANDUSDT', 'BNBUSDT', 'SANDBNB']:
        symbol = 'SANDUSDT'
        synthetic = {
            'BNBUSDT': {'normal': True, 'assets': ['BNB', 'USDT']},
            'SANDBNB': {'normal': True, 'assets': ['SAND', 'BNB']}
        }
        check_arbitrage(symbol, synthetic, 0.3)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    redis_client = get_client()

    redis_client.set('trade_count', 0)

    POOL = ProcessPoolExecutor(2)
    # POOL = None

    try:
        while True:
            if not (updated_symbols := redis_client.hgetall('updated_best_prices')):
                continue
            for symbol in updated_symbols:
                redis_client.hdel('updated_best_prices', symbol)
                trading(symbol)
                # loop.run_in_executor(POOL, trading, symbol)
    except KeyboardInterrupt:
        loop.close()
