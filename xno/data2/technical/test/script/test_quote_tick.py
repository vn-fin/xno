import logging

from xno.data2.technical import TechnicalDataProvider

DataProvider = TechnicalDataProvider.singleton()

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":

    DataProvider.start()

    DataProvider._on_consume_quote_tick(
        {
            "time": 1765264213.588,
            "symbol": "HPG",
            "total_room": 376097826,
            "current_room": 225755857,
            "buy_vol": 854570,
            "sell_vol": 228060,
            "buy_val": 227.273465,
            "sell_val": 60.54754,
            "data_type": "SF",
            "source": "dnse",
        }
    )

    data = DataProvider.get_quote_tick("HPG")
    print(data)
