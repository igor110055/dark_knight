import csv


SYMBOLS = {}


def load_symbols(file_path="symbols.csv"):
    if SYMBOLS:
        return SYMBOLS

    with open(file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row["status"] != "TRADING":
                continue

            SYMBOLS[row["symbol"]] = [row["baseAsset"], row["quoteAsset"]]

        return SYMBOLS


def get_symbol(symbol):
    s = SYMBOLS.get(symbol)
    if not s:
        return dict(base=None, quote=None)
    return dict(base=s[0], quote=s[1])
