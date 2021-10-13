import json

from models.order_book import OrderBook

from tasks.client import app


@app.task
def on_message(symbol, message):
    if 'lastUpdateId' not in message:
        return
    response = json.loads(message)
    bids = {float(price): amount for price, amount in response['bids']}
    asks = {float(price): amount for price, amount in response['asks']}
    order_book = OrderBook.get(symbol)
    order_book.populate(bids, asks)

    order_book = OrderBook.get(symbol)
    print(order_book.get_best(5))
