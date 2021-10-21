from triangular_finder import load_triangulars


class StrategyService:

    @staticmethod
    def load_strategies():
        # {'ZRXUSDT': [{'BTCUSDT': {'base': 'BTC', 'normal': True, 'quote': 'USDT'},
        #               'ZRXBTC': {'base': 'ZRX', 'normal': True, 'quote': 'BTC'}},
        #              {'ETHUSDT': {'base': 'ETH', 'normal': True, 'quote': 'USDT'},
        #               'ZRXETH': {'base': 'ZRX', 'normal': True, 'quote': 'ETH'}},
        #              {'BUSDUSDT': {'base': 'BUSD', 'normal': True, 'quote': 'USDT'},
        #               'ZRXBUSD': {'base': 'ZRX', 'normal': True, 'quote': 'BUSD'}}]}
        strategies = {}

        triangulars = load_triangulars()
        for natural_symbol, synthetic_pairs in triangulars.items():
            for synthetic_pair in synthetic_pairs:
                for synthetic_symbol, synthetic_assets in synthetic_pair.items():
                    if synthetic_symbol not in strategies:
                        strategies[synthetic_symbol] = {}
                    strategies[synthetic_symbol][natural_symbol] = synthetic_assets

        return strategies
