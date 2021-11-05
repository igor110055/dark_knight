from storm.exchanges.binance import get_client

binance = get_client()

class TestBinance:
    def test_get_order_book__return_valid_order_book(self):
        symbol = 'ETHUSDT'
        order_book = binance.get_order_book(symbol)
        assert 'lastUpdateId' in order_book
        assert order_book['bids']
        assert order_book['bids']
