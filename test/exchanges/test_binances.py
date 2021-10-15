from exchanges.binance import get_order_book

class TestBinance:
    def test_get_order_book__return_valid_order_book(self):
        symbol = 'ETHUSDT'
        order_book = get_order_book(symbol, 9999)
        assert 'lastUpdateId' in order_book
        assert order_book['bids']
        assert order_book['bids']