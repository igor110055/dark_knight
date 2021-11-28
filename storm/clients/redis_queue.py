from rq import Queue
from .redis_client import get_client

redis_client = get_client(decode_responses=False)

def get_queue(name='default'):
    return Queue(name=name, connection=redis_client)
