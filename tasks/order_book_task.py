from exchanges.binance import Binance
from models.order_book import OrderBook

from celery_app import app

last_update_ids = {}
last_sequences = {}
cached_responses = {}


@app.task
def update_order_book(response: dict):
    symbol = response['s']
    epoch = response['E']  # TODO: add to price
    start_sequence = response['U']
    end_sequence = response['u']

    # move to one off check
    if not has_order_book_initialized(response):
        cached_responses.setdefault(symbol, []).append(response)
        return get_order_book_snapshot.delay(symbol)

    if symbol in last_sequences:
        if start_sequence != last_sequences[symbol] + 1:
            last_update_ids[symbol] = None
    last_sequences[symbol] = end_sequence

    order_book_ob = OrderBook.get(symbol)
    order_book = order_book_ob.get_book()

    for price, amount in response['b']:
        price = float(price)
        if float(amount):
            order_book['bids'][price] = amount
        else:
            order_book['bids'].pop(price, None)

    for price, amount in response['a']:
        price = float(price)
        if float(amount):
            order_book['asks'][price] = amount
        else:
            order_book['asks'].pop(price, None)

    order_book_ob.save()

    if order_book['bids'] and order_book['asks']:
        best_bid = max(order_book['bids'])
        best_ask = min(order_book['asks'])

        if best_bid > best_ask:
            last_update_ids[symbol] = None
            order_book_ob.best_prices = {'bids': 0, 'asks': 0}
            order_book_ob.clear()
            get_order_book_snapshot.delay(symbol)

        else:
            order_book_ob.best_prices = {'bids': best_bid, 'asks': best_ask}


@app.task
def get_order_book_snapshot(symbol):
    print('Get order book for:', symbol)
    data = Binance.get_order_book(symbol, 9999)
    last_update_id = data.pop('lastUpdateId', None)

    if last_update_id is None:
        return get_order_book_snapshot.delay(symbol)

    last_update_ids[symbol] = last_update_id

    # now = int(time() * 1000)

    # bids = {float(price): [size, now] for price, size in data['bids']}
    # asks = {float(price): [size, now] for price, size in data['asks']}

    apply_cached_response(symbol)


def apply_cached_response(symbol):
    cache_applied = False
    for response in cached_responses.get(symbol, []):
        symbol = response['s']

        if not has_order_book_initialized(response):
            continue

        if not is_subsequent_response(response):
            return get_order_book_snapshot.delay(symbol)

        update_order_book(response)
        last_sequences[symbol] = response['u']
        cache_applied = True

    if cache_applied:
        cached_responses[symbol] = []


def has_order_book_initialized(response):
    symbol = response['s']
    end_sequence = response['u']

    if not (last_update_id := last_update_ids.get(symbol, None)):
        return False

    return end_sequence > last_update_id


def is_subsequent_response(response):
    symbol = response['s']
    start_sequence = response['U']
    end_sequence = response['u']

    if symbol not in last_sequences:
        return False

    return start_sequence <= last_sequences[symbol] + 1 <= end_sequence
