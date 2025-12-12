from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xno.data2.entity.quote_tick import QuoteTick

_QUOTE_TICK_STORE = {}


def get(symbol: str) -> "QuoteTick | None":
    return _QUOTE_TICK_STORE.get(symbol, None)


def push(quote_tick: "QuoteTick"):
    _QUOTE_TICK_STORE[quote_tick.symbol] = quote_tick
