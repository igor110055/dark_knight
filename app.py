import asyncio
import simplejson as json
import threading
from decimal import Decimal

from exchanges.binance import get_client
from order_engine import OrderEngine

from services.strategy_service import StrategyService, load_triangulars
from services.symbol_service import SymbolService
from tasks.order_task import check_arbitrage


from order_book_websocket import run


SymbolService.load_symbols()

strategies = trigangulars = load_triangulars()
symbols = StrategyService.load_strategies()

fee_factor = Decimal('1') / Decimal('0.999') / \
    Decimal('0.999') * Decimal('1.00001')

trade_count = 0

TRADE_LIMIT = 3

binance_client = get_client()

engines = {
    'USDT': OrderEngine(binance_client),
    # 'BUSD': OrderEngine(binance_client, currency='BUSD'),
    # 'DAI': OrderEngine(binance_client, currency='DAI')
}
# engine = OrderEngine(binance_client)


def trading(symbol):
    synthetics = trigangulars.get(symbol)
    # not a triangle
    if not synthetics:
        return

    loop = asyncio.get_event_loop()

    tasks = []

    for synthetic in synthetics:
        # TODO: concurrent
        tasks.append(loop.create_task(check_arbitrage(symbol, synthetic, 0.3)))

    # check arbitrage of symbol being a synthetic
    for natural_symbol in symbols[symbol]:
        # TODO: wrong, should only get related natural synthetics
        synthetics = strategies[natural_symbol]
        for synthetic in synthetics:
            if symbol in synthetic:
                tasks.append(loop.create_task(check_arbitrage(natural_symbol, synthetic, 0.35)))

    asyncio.gather(*tasks)

    # if symbol in symbols:
    #     natural = strategies.get(symbol)
    #     if not natural:
    #         return

    #     synthetics = [strategies[s] for s in symbols[symbol]]
    #     print('symbol:', symbol)
    #     print('natural:', natural)
    #     print('synthetics:', synthetics)
    #     print('-'*30)

    # TODO: check arbitrage opportunities
    # print(symbol, best_bid_price, best_bid_size, best_ask_price, best_ask_size)

    # print(best_prices)


async def main():
    # print(symbols)

    # for symbol in symbols.keys():
    #     tasks.append(asyncio.create_task(main(symbol)))

    # await asyncio.gather(*tasks)

    # for symbol in list(symbols.items())[::200]:

    steps = 200
    ts = []
    count = 0
    for i in range(0, len(symbols), steps):
        s = list(symbols.items())[i:i+steps]
        _symbols = dict(s)
        # print(list(_symbols.keys()))
        ts.append(asyncio.create_task(run(_symbols, lambda _: None)))
        # t = threading.Thread(target=asyncio.run, args=(run(_symbols, trading), ))
        # t.start()
        # count += 1
        # if count == 1:
        #     break
        # ts.append(t)

    await asyncio.gather(*ts)

    # for t in ts:
    #     t.join()


if __name__ == '__main__':
    # symbols = ['bnUSDTt', 'troyusdt', 'troybnb']
    #
    asyncio.run(main())
    # dummy()
