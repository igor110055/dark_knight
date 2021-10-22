import csv


class SymbolService:
    _symbols = None

    @classmethod
    def load_symbols(cls, file_path='symbols.csv'):
        if cls._symbols is not None:
            return cls._symbols

        symbols = {}

        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if row['status'] != 'TRADING':
                    continue

                symbols[row['symbol']] = [row['baseAsset'], row['quoteAsset']]

            cls._symbols = symbols
            return symbols

    @classmethod
    def get_symbol(cls, symbol):
        s = cls._symbols.get(symbol)
        if not s:
            return dict(base=None, quote=None)
        return dict(base=s[0], quote=s[1])
