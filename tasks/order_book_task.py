import pdb
from decimal import Decimal
from time import time

from exchanges.binance import get_client
from models.order_book import OrderBook

from tasks.client import app

binance_client = get_client()


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
            del last_update_ids[symbol]
    last_sequences[symbol] = end_sequence

    # TODO: may need to synchronize
    flush_changes.delay(symbol, 'bids', response['b'])
    flush_changes.delay(symbol, 'asks', response['a'])


@app.task
def flush_changes(symbol, side, changes):
    order_book = OrderBook.get(symbol)
    for price, amount in changes:
        if float(amount):
            order_book.set(side, price, amount)
        else:
            order_book.delete(side, price)


@app.task
def get_order_book_snapshot(symbol):
    data = binance_client.get_order_book(symbol)
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
    for response in cached_responses[symbol]:
        symbol = response['s']

        if not has_order_book_initialized(response):
            continue

        if not is_subsequent_response(response):
            return get_order_book_snapshot.delay(symbol)

        update_order_book(response)
        last_sequences[symbol] = response['u']
        cache_applied = True

    if cache_applied:
        del cached_responses[symbol]


def has_order_book_initialized(response):
    symbol = response['s']
    end_sequence = response['u']
    last_update_id = Decimal(last_update_ids.get(symbol, 'Infinity'))

    return Decimal(end_sequence) > last_update_id


def is_subsequent_response(response):
    symbol = response['s']
    start_sequence = response['U']
    end_sequence = response['u']

    return start_sequence <= last_sequences[symbol] + 1 <= end_sequence
