import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

import time

from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

logging.basicConfig(level=logging.DEBUG)

from xno.data2.technical import TechnicalDataProvider

DataProvider = TechnicalDataProvider.singleton()


if __name__ == "__main__":

    DataProvider.start()

    time.sleep(600)
    DataProvider.stop()
    print("Data provider stopped.")
