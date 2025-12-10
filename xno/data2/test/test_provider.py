import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

import time
from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

if __name__ == "__main__":
    from xno.data2 import DataProvider

    DataProvider.start()

    data = DataProvider.get_ohlcv(
        symbol="ACB",
        resolution="MIN",
        from_time="2025-06-01 00:00:00",
        to_time="2025-12-31 00:00:00"
    )
    print(data)

    for _ in range(10):
        time.sleep(30)
        data = DataProvider.get_ohlcv(
            symbol="ACB",
            resolution="MIN",
            from_time="2025-06-01 00:00:00",
            to_time="2025-12-31 00:00:00"
        )
        print(data)
