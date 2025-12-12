import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))


from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    from xno.data2 import DataProvider

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
