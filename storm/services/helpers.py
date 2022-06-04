import csv

def write_csv(data, filename="arbitrage.csv"):
    # logger.info(str(data))
    with open(filename, "a") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "time",
                "strategy",
                "natural",
                "synthetic_left",
                "synthetic_right",
                "natural_bid",
                "natural_ask",
                "synthetic_left_bid",
                "synthetic_left_ask",
                "synthetic_right_bid",
                "synthetic_right_ask",
                "expected_return_perc",
            ],
        )
        writer.writerow(data)