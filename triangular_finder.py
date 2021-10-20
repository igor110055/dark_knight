import csv
import pickle

import networkx as nx


def load_symbols(file_path='symbols.csv'):
    with open(file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        symbols = {}
        for row in reader:
            if row['status'] == 'TRADING':
                symbols[row['symbol']] = [row['baseAsset'], row['quoteAsset']]

        return symbols


def dump_triangulars(file_path='triangulars.pickle'):
    triangulars = get_triangulars()
    with open(file_path, 'wb') as file:
        pickle.dump(triangulars, file)


def load_triangulars(file_path='triangulars.pickle'):
    with open(file_path, 'rb') as file:
        return pickle.load(file)


def get_triangulars():
    symbols = load_symbols()

    G = nx.Graph()
    for symbol, [base_asset, quote_asset] in symbols.items():
        G.add_edge(base_asset, quote_asset, assets=(
            base_asset, quote_asset), symbol=symbol)

    triangular_assets = {}
    for left_currency, right_currency in G.edges:
        trading_paths = nx.algorithms.simple_paths.shortest_simple_paths(
            G, left_currency, right_currency)

        edge = G.edges[left_currency, right_currency]
        natural_symbol = edge['symbol']
        natural_base_currency, natural_quote_currency = edge['assets']

        synthetics = []
        for path in trading_paths:
            if len(path) > 3:
                break

            if len(path) == 2:
                continue

            if len(path) == 3:
                synthetic = get_synthetic(G, natural_base_currency, natural_quote_currency, path)
                synthetics.append(synthetic)

        triangular_assets[natural_symbol] = synthetics

    return triangular_assets


def get_synthetic(G, natural_base_currency, natural_quote_currency, path):
    left_edge = G.edges[path[0], path[1]]
    left_base_currency, left_quote_currency = left_edge['assets']
    left_symbol = left_edge['symbol']
    left_normal = left_base_currency == natural_base_currency or left_quote_currency == natural_quote_currency

    right_edge = G.edges[path[1], path[2]]
    right_base_currency, right_quote_currency = right_edge['assets']
    right_symbol = right_edge['symbol']
    right_normal = right_base_currency == natural_base_currency or right_quote_currency == natural_quote_currency

    return {
        left_symbol: {
            'normal': left_normal,
            'base': left_base_currency,
            'quote': left_quote_currency
        },
        right_symbol: {
            'normal': right_normal,
            'base': right_base_currency,
            'quote': right_quote_currency
        }
    }
