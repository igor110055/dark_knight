import asyncio

from ..clients.redis_client import get_client
from ..exchanges.binance import WS_URL
from ..services.stream_symbol_service import stream_symbols

if __name__ == "__main__":
    redis = get_client()
    redis.delete("last_sequences", "cached_responses", "initialized", "responses")

    ETH = set(["ETHUSDT", "BNBETH", "BNBUSDT"])
    LUNA = set(["LUNAUSDT", "LUNABNB", "BNBUSDT"])
    # MATIC = set(["MATICUSDT", "MATICBNB", "BNBUSDT"])
    SAND = set(["SANDUSDT", "SANDBNB", "BNBUSDT"])
    # MANA = set(["MANAUSDT", "MANAETH", "ETHUSDT"])
    # MINA = set(["MINAUSDT", "MINABNB", "BNBUSDT"])
    # OMG = set(["OMGUSDT", "OMGETH", "ETHUSDT"])
    # GALA = set(["GALAUSDT", "GALABNB", "BNBUSDT"])
    # SOL = set(["SOLUSDT", "SOLBNB", "BNBUSDT"])

    # TODO: use websocket-clients
    asyncio.run(stream_symbols(WS_URL, ETH | LUNA | SAND))
