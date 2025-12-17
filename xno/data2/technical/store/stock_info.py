from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xno.data2.technical.entity.stock_info import StockInfo

_STOCK_INFO_STORE = {}


def get(symbol: str) -> "StockInfo | None":
    return _STOCK_INFO_STORE.get(symbol, None)


def push(stock_info: "StockInfo"):
    _STOCK_INFO_STORE[stock_info.symbol] = stock_info
