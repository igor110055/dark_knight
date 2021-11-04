import asyncio

from order_book_websocket import run

if __name__ == '__main__':
    # asyncio.run(run(['ETHBTC', 'BNBETH', 'ETHUSDT', 'ETHTUSD', 'ETHUSDC', 'ETHBUSD', 'ETHRUB', 'ETHEUR', 'ETHGBP', 'ETHBRL', 'ETHUAH', 'LTCBTC',
    #             'LTCUSDT', 'LTCBNB', 'LTCTUSD', 'LTCUSDC', 'LTCBUSD', 'LTCEUR', 'LTCRUB', 'LTCBRL', 'LTCGBP', 'LTCUAH', 'LTCETH'], lambda x: None))
    asyncio.run(run(['LUNABNB', 'BNBUSDT', 'LUNAUSDT'], lambda x: None))
