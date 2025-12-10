import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

import time
from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

if __name__ == "__main__":
    from xno.data2 import DataProvider

    data = DataProvider._on_consume_order_book({
        "time": 1765264521.586,
        "symbol": "MSB",
        "std_symbol": "MSB",
        "bp": [
            12.85,
            12.8,
            12.75,
        ],
        "bq": [
            4390,
            21790,
            7250,
        ],
        "ap": [
            12.9,
            12.95,
            13,
        ],
        "aq": [
            19150,
            22430,
            47430,
        ],
        "total_bid": 0,
        "total_ask": 0,
        "data_type": "TP",
        "source": "dnse"
    })

    order_book = DataProvider.get_order_book_depth("MSB")
    print(order_book)