import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

import time

from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    from xno.data2 import DataProvider

    DataProvider.start()

    data = DataProvider.get_ohlcv(
        symbol="ACB",
        resolution="MIN",
        from_time=datetime.now() - timedelta(days=60),
        to_time=datetime.now(),
    )
    print(data)

    for _ in range(20):
        time.sleep(10)
        data = DataProvider.get_ohlcv(
            symbol="ACB",
            resolution="MIN",
            from_time=datetime.now() - timedelta(days=60),
            to_time=datetime.now(),
        )
        print(data)
