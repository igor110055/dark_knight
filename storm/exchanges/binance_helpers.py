import csv
import hashlib
import hmac
import os
from urllib.parse import urlencode, urljoin

from dotenv import load_dotenv

from ..clients.rest_client import get_session
from ..clients.redis_queue import get_queue

load_dotenv()
api_key = os.getenv("API_KEY")
secret_key = os.getenv("SECRET_KEY")
headers = {"X-MBX-APIKEY": api_key}
message_hash_key = secret_key.encode("utf-8")


REST_URL = "https://api.binance.com"
session = get_session(REST_URL)


def load_symbols():
    with open("symbols.csv") as csv_file:
        return {symbol["symbol"]: symbol for symbol in csv.DictReader(csv_file)}


SYMBOLS = load_symbols()


def get(path, params=None, raw=False):
    if raw:
        url = urljoin(REST_URL, path)
        return session.get(url, params=params)
    return _request("GET", path, params)


def post(path, params=None):
    return _request("POST", path, params)


def _request(method, path, params):
    # TODO: suspect hashing key is a bit slow
    # params['timestamp'] = timestamp
    # params['recvWindow'] = 60000
    if params:
        query_string = urlencode(params)

        msg = query_string.encode("utf-8")
        params["signature"] = hmac.new(
            message_hash_key, msg, digestmod=hashlib.sha256
        ).hexdigest()

    url = urljoin(REST_URL, path)
    response = session.request(method, url, headers=headers, params=params)
    return response.json()


redis_queue = get_queue()


def allow_async(func):
    def wrapped_func(*args, is_async=False, **kwargs):
        if is_async:
            return redis_queue.enqueue(func, *args, **kwargs)
        return func(*args, **kwargs)

    return wrapped_func
