import asyncio
from ..tasks.order_task import check_arbitrage
from ..clients.redis_client import get_client

def trading(symbol):
    if symbol in ['LUNAUSDT', 'LUNABNB', 'BNBUSDT']:
        symbol = 'LUNAUSDT'
        synthetic = {
            'LUNABNB': {'normal': True},
            'BNBUSDT': {'normal': True}
        }
        check_arbitrage(symbol, synthetic, 0.3)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    redis_client = get_client()

    POOL = None

    try:
        while True:
            for best_order_key in redis_client.scan_iter('best_orders:*'):
                symbol = best_order_key[12:]
                loop.run_in_executor(POOL, trading, symbol)
    except KeyboardInterrupt:
        loop.close()