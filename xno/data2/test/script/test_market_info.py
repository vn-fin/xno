import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))


from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

logging.basicConfig(level=logging.DEBUG)

from xno.data2 import DataProvider


def test_market_info():
    DataProvider.start()

    DataProvider._on_consume_market_info(
        {
            "time": 1765264215.014,
            "symbol": "VNINDEX",
            "name": "VNINDEX",
            "prior": 1753.74,
            "value": 1749.9,
            "total_vol": 796127304,
            "total_val": 23198.30224288,
            "advance": 56,
            "decline": 260,
            "nochange": 43,
            "ceil": 4,
            "floor": 3,
            "change": -3.839999999999918,
            "change_pct": -0.22,
            "data_type": "MI",
            "source": "dnse",
        }
    )

    data = DataProvider.get_market_info("VNINDEX")
    print(data)


def test_history_market_info():
    histories = DataProvider.get_history_market_info(
        symbol="VN30", from_time=datetime.now() - timedelta(days=3), to_time=datetime.now()
    )
    print(histories)


if __name__ == "__main__":

    test_market_info()
    # test_history_market_info()
