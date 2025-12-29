import logging
from datetime import datetime, timedelta


logging.basicConfig(level=logging.DEBUG)

from xno.data2.technical import TechnicalDataProvider

DataProvider = TechnicalDataProvider.singleton()


def test_trade_tick():
    DataProvider.start()

    DataProvider._on_consume_trade_tick(
        {
            "time": 1765264214.565,
            "symbol": "MCH",
            "price": 214.8,
            "vol": 10,
            "side": "S",
            "total_vol": 9430,
            "data_type": "ST",
            "source": "dnse",
        }
    )

    data = DataProvider.get_trade_tick("MCH")
    print(data)


def test_history_trade_tick():
    histories = DataProvider.get_history_trade_tick(
        symbol="MCH",
        from_time=datetime.now() - timedelta(days=3),
        to_time=datetime.now(),
    )
    print(histories)


if __name__ == "__main__":

    test_trade_tick()
    test_history_trade_tick()
