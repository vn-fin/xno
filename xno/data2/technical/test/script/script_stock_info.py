import logging
from datetime import datetime, timedelta


logging.basicConfig(level=logging.DEBUG)

from xno.data2.technical import TechnicalDataProvider

DataProvider = TechnicalDataProvider.singleton()


def test_stock_info():
    DataProvider.start()

    DataProvider._on_consume_stock_info(
        {
            "time": 1765264218.983,
            "symbol": "PAN",
            "open": 27.4,
            "high": 27.9,
            "low": 27.4,
            "close": 0,
            "avg": 27.673,
            "ceil": 29.15,
            "floor": 25.35,
            "prior": 27.25,
            "data_type": "SI",
            "source": "dnse",
        }
    )

    data = DataProvider.get_stock_info("PAN")
    print(data)


def test_history_stock_info():
    histories = DataProvider.get_history_stock_info(
        symbol="PAN",
        from_time=datetime.now() - timedelta(days=3),
        to_time=datetime.now(),
    )
    print(histories)


if __name__ == "__main__":

    # test_stock_info()
    test_history_stock_info()
