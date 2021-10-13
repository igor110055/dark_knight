import asyncio

from order_book_websocket import run

if __name__ == '__main__':
    asyncio.run(run(['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'SOLUSDT']))
