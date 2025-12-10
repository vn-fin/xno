import logging

logger = logging.getLogger(__name__)
_ORDER_BOOKS = dict()


def get(symbol: str, depth: int = 10) -> list[dict] | None:
    if symbol not in _ORDER_BOOKS:
        return None
    order_book = _ORDER_BOOKS[symbol]
    return order_book

def push(order_book):
    key = order_book['symbol']
    _ORDER_BOOKS[key] = order_book


def parse_from_external_kafka(raw):
    # {
    #     "time": 1765264521.586,
    #     "symbol": "MSB",
    #     "std_symbol": "MSB",
    #     "bp": [
    #         12.85,
    #         12.8,
    #         12.75,
    #     ],
    #     "bq": [
    #         4390,
    #         21790,
    #         7250,
    #     ],
    #     "ap": [
    #         12.9,
    #         12.95,
    #         13,
    #     ],
    #     "aq": [
    #         19150,
    #         22430,
    #         47430,
    #     ],
    #     "total_bid": 0,
    #     "total_ask": 0,
    #     "data_type": "TP",
    #     "source": "dnse"
    # }
    return {
        "symbol": raw['symbol'],
        "bp": raw['bp'],
        "bq": raw['bq'],
        "ap": raw['ap'],
        "aq": raw['aq'],
        "time": raw['time'],
        "total_bid": raw['total_bid'],
        "total_ask": raw['total_ask'],
    }
