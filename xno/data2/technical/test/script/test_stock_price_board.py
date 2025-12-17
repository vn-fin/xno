import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))


from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

logging.basicConfig(level=logging.DEBUG)

from xno.data2.technical import TechnicalDataProvider

DataProvider = TechnicalDataProvider.singleton()


def test_stock_price_board():
    DataProvider.start()

    DataProvider._on_consume_stock_price_board(
        {
            "time": 1765264218.983,
            "symbol": "PAN",
            "price": 27.75,
            "vol": 10,
            "total_vol": 63280,
            "total_val": 17.511695,
            "change": 0.5,
            "change_pct": 1.83,
            "data_type": "PB",
            "source": "dnse",
        }
    )

    data = DataProvider.get_stock_price_board("PAN")
    print(data)


def test_history_stock_price_board():
    histories = DataProvider.get_history_stock_price_board(
        symbol="PAN",
        from_time=datetime.now() - timedelta(days=3),
        to_time=datetime.now(),
    )
    print(histories)


if __name__ == "__main__":

    # test_stock_price_board()
    test_history_stock_price_board()
