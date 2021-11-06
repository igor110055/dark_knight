import asyncio

from ..clients.redis_client import get_client
from ..exchanges.binance import WS_URL
from ..services.stream_symbol_service import stream_symbols
from ..services.update_order_book_service import handle_response

if __name__ == '__main__':
    redis = get_client(a_sync=False)
    redis.delete('last_sequences', 'cached_responses', 'initialized')

    LUNA = set(['LUNAUSDT', 'BNBUSDT', 'LUNABNB'])
    MATIC = set(['MATICUSDT', 'MATICBNB', 'BNBUSDT'])
    SAND = set(['SANDUSDT', 'BNBUSDT', 'SANDBNB'])

    loop = asyncio.get_event_loop()
    task = loop.create_task(stream_symbols(WS_URL, LUNA | MATIC | SAND))
    # maticusdt_task = loop.create_task(stream_symbols(
    #     WS_URL, ['MATICUSDT', 'MATICBNB', 'USDTTRY', 'MATICTRY']))
    # handle_response_task = loop.create_task(handle_response())

    try:
        loop.run_until_complete(handle_response())
    except KeyboardInterrupt:
        task.cancel()
        # maticusdt_task.cancel()
        # handle_response_task.cancel()
