from contextlib import contextmanager


@contextmanager
def symbol_lock(redis, symbol):
    if not redis.setnx(f'working_on_{symbol}', 1):
        return
    yield
    redis.delete(f'working_on_{symbol}')


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
