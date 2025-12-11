import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from dotenv import load_dotenv

load_dotenv("../xalpha/.env")

if __name__ == "__main__":
    from xno.data2 import DataProvider

    to_time = datetime.now()
    from_time = to_time - timedelta(days=1)

    order_books = DataProvider.get_history_order_book_depths(symbol="MSB", from_time=from_time, to_time=to_time)
    print("Fetched", len(order_books), "order book depth entries.")
    print("First and last:", order_books[0], order_books[-1])
