import asyncio
from concurrent.futures import ProcessPoolExecutor

from ..clients.redis_client import get_client
from ..tasks.order_task import check_arbitrage


def trading(symbol):
    if symbol in ['LUNAUSDT', 'LUNABNB', 'BNBUSDT']:
        symbol = 'LUNAUSDT'
        synthetic = {
            'LUNABNB': {'normal': True},
            'BNBUSDT': {'normal': True}
        }
        check_arbitrage(symbol, synthetic, 0)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    redis_client = get_client()

    POOL = ProcessPoolExecutor(2)
    # POOL = None

    try:
        while True:
            if not (updated_symbols := redis_client.hgetall('updated_best_prices')):
                continue
            for symbol in updated_symbols:
                redis_client.hdel('updated_best_prices', symbol)
                loop.run_in_executor(POOL, trading, symbol)
    except KeyboardInterrupt:
        loop.close()
