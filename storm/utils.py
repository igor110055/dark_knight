import logging
import os

logging.basicConfig(level=os.getenv('LOG_LEVEL', logging.INFO))

loggers = {}

grey = "\x1b[38;21m"
yellow = "\x1b[33;21m"
red = "\x1b[31;21m"
bold_red = "\x1b[31;1m"
green = "\x1b[32;2m"
pink = "\x1b[35;21m"
reset = "\x1b[0m"


def get_logger(name, handler=None):
    if name in loggers:
        return loggers[name]

    logger = logging.getLogger(name)
    logger.propagate = False
    handler = handler or logging.StreamHandler()
    format = logging.Formatter(f'{green}%(asctime)s{reset} - %(levelname)s - {bold_red}[%(module)s]{reset} {pink}#%(funcName)s{reset}: %(message)s')
    handler.setFormatter(format)
    logger.addHandler(handler)
    loggers[name] = logger
    return logger


class symbol_lock:
    def __init__(self, redis_client, symbol):
        self.redis_client = redis_client
        self.symbol = symbol

    def __enter__(self):
        if not self.redis_client.setnx(f'working_on_{self.symbol}', 1):
            return
        yield

    def __exit__(self, exc_type, exc_value, traceback):
        self.redis_client.delete(f'working_on_{self.symbol}')


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
