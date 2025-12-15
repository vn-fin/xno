from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xno.data2.entity.market_info import MarketInfo

_MARKET_INFO_STORE = {}


def get(symbol: str) -> "MarketInfo | None":
    return _MARKET_INFO_STORE.get(symbol, None)


def push(market_info: "MarketInfo"):
    _MARKET_INFO_STORE[market_info.symbol] = market_info
