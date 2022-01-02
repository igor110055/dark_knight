from storm.services.arbitrage_service import calculate_synthetic_bid, calculate_synthetic_ask
from decimal import Decimal

def test_calculate_synthetic_bid__left_normal_right_normal():
    best_prices_left = {
        'bids': 10,
        'asks': 12
    }

    left_assets = {
        'normal': True
    }

    best_prices_right = {
        'bids': 1.1,
        'asks': 1.2
    }

    right_assets = {
        'normal': True
    }

    synthetic_bid = calculate_synthetic_bid(best_prices_left, left_assets, best_prices_right, right_assets)
    assert synthetic_bid == 11.0  # 10 * 1.1

def test_calculate_synthetic_bid__left_normal_right_inverse():
    best_prices_left = {
        'bids': 10,
        'asks': 12
    }

    left_assets = {
        'normal': True
    }

    best_prices_right = {
        'bids': 0.4,
        'asks': 0.5
    }

    right_assets = {
        'normal': False
    }

    synthetic_bid = calculate_synthetic_bid(best_prices_left, left_assets, best_prices_right, right_assets)
    assert synthetic_bid == 20.0  # 10 * (1/0.5)

def test_calculate_synthetic_bid__left_inverse_right_normal():
    best_prices_left = {
        'bids': 4.5,
        'asks': 5
    }

    left_assets = {
        'normal': False
    }

    best_prices_right = {
        'bids': 1.1,
        'asks': 1.2
    }

    right_assets = {
        'normal': True
    }

    synthetic_bid = calculate_synthetic_bid(best_prices_left, left_assets, best_prices_right, right_assets)
    assert Decimal(f"{synthetic_bid:.8f}") == Decimal("0.22")  # (1/5) * 1.1

def test_calculate_synthetic_ask__left_normal_right_normal():
    best_prices_left = {
        'bids': 10,
        'asks': 12
    }

    left_assets = {
        'normal': True
    }

    best_prices_right = {
        'bids': 1.1,
        'asks': 1.5
    }

    right_assets = {
        'normal': True
    }

    synthetic_ask = calculate_synthetic_ask(best_prices_left, left_assets, best_prices_right, right_assets)
    assert synthetic_ask == 18  # 12 * 1.5

def test_calculate_synthetic_ask__left_normal_right_inverse():
    best_prices_left = {
        'bids': 10,
        'asks': 12
    }

    left_assets = {
        'normal': True
    }

    best_prices_right = {
        'bids': 0.4,
        'asks': 0.5
    }

    right_assets = {
        'normal': False
    }

    synthetic_ask = calculate_synthetic_ask(best_prices_left, left_assets, best_prices_right, right_assets)
    assert synthetic_ask == 30.0  # 12 * (1/0.4)

def test_calculate_synthetic_ask__left_inverse_right_normal():
    best_prices_left = {
        'bids': 4,
        'asks': 5
    }

    left_assets = {
        'normal': False
    }

    best_prices_right = {
        'bids': 1.1,
        'asks': 1.2
    }

    right_assets = {
        'normal': True
    }

    synthetic_ask = calculate_synthetic_ask(best_prices_left, left_assets, best_prices_right, right_assets)
    assert synthetic_ask == 0.3  # (1/4.5) * 1.2
