import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from dotenv import load_dotenv

load_dotenv("../xalpha/.env")


def test_order_book_depth():
    from xno.data2 import DataProvider

    # DataProvider.start()
    DataProvider._on_consume_order_book(
        {
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
            "source": "dnse",
        }
    )

    order_book = DataProvider.get_order_book_depth("MSB")
    assert order_book.symbol == "MSB"
    assert order_book.bp == [12.85, 12.8, 12.75]
    assert order_book.bq == [4390, 21790, 7250]
    assert order_book.ap == [12.9, 12.95, 13]
    assert order_book.aq == [19150, 22430, 47430]
    assert order_book.total_bid == 0
    assert order_book.total_ask == 0

    print("Order book depth test passed.", order_book)


if __name__ == "__main__":
    test_order_book_depth()
