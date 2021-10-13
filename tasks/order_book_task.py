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
    epoch = response['E']
    start_sequence = response['U']
    end_sequence = response['u']

    # move to one off check
    if not has_order_book_initialized(response):
        cached_responses.setdefault(symbol, []).append(response)
        return get_order_book_snapshot.delay(symbol)

    if symbol in last_sequences:
        assert start_sequence == last_sequences[symbol] + 1

    order_book = OrderBook.get(symbol)

    last_sequences[symbol] = end_sequence
    for price, amount in response['b']:
        if float(amount):
            order_book.set('bids', price, amount)
        else:
            order_book.delete('bids', price)

    for price, amount in response['a']:
        if float(amount):
            # update event, reconcile with trade data
            order_book.set('asks', price, amount)
        else:
            # cancel event
            order_book.delete('asks', price)


@app.task()
def get_order_book_snapshot(symbol):
    data = binance_client.get_order_book(symbol)
    last_update_id = data.pop('lastUpdateId', None)

    if last_update_id is None:
        return get_order_book_snapshot.delay(symbol)

    last_update_ids[symbol] = last_update_id

    # now = int(time() * 1000)

    # bids = {float(price): [size, now] for price, size in data['bids']}
    # asks = {float(price): [size, now] for price, size in data['asks']}

    # bs = {size: float(price) for price, size in data['bids']}

    order_book = OrderBook.get(symbol)
    apply_cached_response(order_book, symbol)


def apply_cached_response(order_book, symbol):
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
        cached_responses[symbol] = []
    # if cache_applied:
    #     # bids = {Decimal(price): size for price, size in order_book['bids'].items()}
    #     # asks = {Decimal(price): size for price, size in order_book['asks'].items()}
    #     # order_book['bids'] = sorted(bids, reverse=True)[5:]
    #     # order_book['asks'] = sorted(asks)[5:]
    #     redis.hset('last_update_ids', symbol, last_update_id)
    #     redis.delete(f"cached_responses:{symbol}")


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
