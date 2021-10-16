from exchanges.binance import Binance



class TestBinance:
    def test_get_order_book__return_valid_order_book(self):
        symbol = 'ETHUSDT'
        order_book = Binance.get_order_book(symbol, 9999)
        assert 'lastUpdateId' in order_book
        assert order_book['bids']
        assert order_book['bids']
