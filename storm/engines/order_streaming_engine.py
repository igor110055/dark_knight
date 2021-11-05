import asyncio

from ..clients.redis_client import get_client
from ..exchanges.binance import WS_URL
from ..services.stream_symbol_service import stream_symbols
from ..services.update_order_book_service import handle_response

if __name__ == '__main__':
    redis = get_client(a_sync=False)
    redis.delete('last_update_ids', 'last_sequences',
                 'cached_responses', 'initialized')

    loop = asyncio.get_event_loop()
    stream_task = loop.create_task(stream_symbols(
        WS_URL, ['LUNAUSDT', 'BNBUSDT', 'LUNABNB']))
    handle_response_task = loop.create_task(handle_response())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        stream_task.cancel()
        handle_response_task.cancel()
