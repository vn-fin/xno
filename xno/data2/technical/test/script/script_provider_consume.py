import logging
import time


logging.basicConfig(level=logging.DEBUG)

from xno.data2.technical import TechnicalDataProvider

DataProvider = TechnicalDataProvider.singleton()


if __name__ == "__main__":
    DataProvider.start()

    time.sleep(600)
    DataProvider.stop()
    print("Data provider stopped.")
