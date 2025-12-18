from datetime import datetime, timedelta

from xno.data2.technical import TechnicalDataProvider

DataProvider = TechnicalDataProvider.singleton()


if __name__ == "__main__":
    to_time = datetime.now()
    from_time = to_time - timedelta(days=1)

    order_books = DataProvider.get_history_order_book_depth(symbol="MSB", from_time=from_time, to_time=to_time)
    print("Fetched", len(order_books), "order book depth entries.")
    print("First and last:", order_books[0], order_books[-1])
