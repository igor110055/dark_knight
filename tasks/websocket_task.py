from celery_app import app
from exchanges.binance import WEBSOCKETS


@app.task
def ping_server():
    while(ws := WEBSOCKETS.get_nowait()):
        ws.ping()
