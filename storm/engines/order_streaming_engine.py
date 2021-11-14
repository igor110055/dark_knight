import asyncio

from ..clients.redis_client import get_client
from ..exchanges.binance import WS_URL
from ..services.stream_symbol_service import stream_symbols

if __name__ == '__main__':
    redis = get_client(a_sync=False)
    redis.delete('last_sequences', 'cached_responses', 'initialized', 'responses')

    LUNA = set(['LUNAUSDT', 'LUNABNB', 'BNBUSDT'])
    MATIC = set(['MATICUSDT', 'MATICBNB', 'BNBUSDT'])
    SAND = set(['SANDUSDT', 'SANDBNB', 'BNBUSDT'])
    MANA = set(['MANAUSDT', 'MANAETH', 'ETHUSDT'])
    MINA = set(['MINAUSDT', 'MINABNB', 'BNBUSDT'])
    OMG = set(['OMGUSDT', 'OMGETH', 'ETHUSDT'])

    # TODO: change ws library to remove async (buggy...)

    # loop = asyncio.get_event_loop()
    # task = loop.create_task(stream_symbols(WS_URL, LUNA | MATIC | SAND))

    # Thread(target=asyncio.run, args=(stream_symbols(WS_URL, LUNA | MATIC | SAND), )).start()
    try:
        asyncio.run(stream_symbols(WS_URL, LUNA | SAND | MANA | MINA | OMG))
    except KeyboardInterrupt:
        pass
