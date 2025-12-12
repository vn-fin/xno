from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xno.data2.entity.trade_tick import TradeTick

_TRADE_TICK_STORE = {}


def get(symbol: str) -> "TradeTick | None":
    return _TRADE_TICK_STORE.get(symbol, None)


def push(trade_tick: "TradeTick"):
    _TRADE_TICK_STORE[trade_tick.symbol] = trade_tick
