import pytest
from storm.services.place_order_service import OrderEngine
from storm.exchanges.binance import get_client

@pytest.fixture
def service():
    return OrderEngine(get_client())

def test_buy_natural_sell_synthetic__cannot_book_the_first_limit_order__fail_the_arbitrage(service):
    symbol = "LUNAUSDT"
    synthetic = {
        "LUNABNB": {"normal": True, "assets": ["LUNA", "BNB"]},
        "BNBUSDT": {"normal": True, "assets": ["BNB", "USDT"]},
    }
    assert not service.buy_natural_sell_synthetic(symbol, synthetic, None, None, {'asks': '43.5'})


def test_buy_natural_sell_synthetic__can_book_the_first_limit_order__success(service):
    symbol = "LUNAUSDT"
    synthetic = {
        "LUNABNB": {"normal": True, "assets": ["LUNA", "BNB"]},
        "BNBUSDT": {"normal": True, "assets": ["BNB", "USDT"]},
    }
    assert service.buy_natural_sell_synthetic(symbol, synthetic, None, None, {'asks': '55'})
