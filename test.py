import asyncio

from order_book_websocket import run

if __name__ == '__main__':
    asyncio.run(run(['BTCUSDT', 'ETHUSDT', 'XRPUSDT',
                'SOLUSDT', 'ADAUSDT', 'BNBUSDT', 'DOTUSDT', 'ETHBTC', 'DOTBTC', 'ENJUSDT', 'LINKUSDT'], lambda x: None))
    # asyncio.run(run(['LTCETH', 'LTCBTC', 'BNBETH'], lambda x: None))