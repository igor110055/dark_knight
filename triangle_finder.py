import csv
import networkx as nx


def get_triangles():
    with open('symbols.csv') as csv_file:
        reader = csv.DictReader(csv_file)
        symbols = {
            row['symbol']: (row['baseAsset'], row['quoteAsset'])
            for row in reader if row['status'] == 'TRADING'
        }

    G = nx.Graph()
    for symbol, (base_asset, quote_asset) in symbols.items():
        G.add_edge(base_asset, quote_asset, assets=(base_asset, quote_asset), symbol=symbol)


    triangle_assets = {}
    for edge in G.edges:
        paths = nx.algorithms.simple_paths.shortest_simple_paths(G, *edge)
        edge = G.edges[edge[0], edge[1]]
        original = edge['symbol']
        assets = edge['assets']
        synthetics = []
        for path in paths:
            if len(path) > 3:
                break

            if len(path) == 2:
                continue

            if len(path) == 3:
                left_edge = G.edges[path[0], path[1]]
                left_assets = left_edge['assets']
                left_symbol = left_edge['symbol']
                left_normal = left_assets[0] == assets[0] or left_assets[1] == assets[1]

                right_edge = G.edges[path[1], path[2]]
                right_assets = right_edge['assets']
                right_symbol = right_edge['symbol']
                right_normal = right_assets[0] == assets[0] or right_assets[1] == assets[1]

                synthetics.append({left_symbol: (left_normal, left_assets), right_symbol: (right_normal, right_assets)})

        triangle_assets[original] = synthetics

    return triangle_assets
