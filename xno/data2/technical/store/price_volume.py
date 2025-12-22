from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xno.data2.technical.entity.price_volume import PriceVolume

_STORE = {}


def get(symbol: str) -> "PriceVolume | None":
    return _STORE.get(symbol, None)


def push(data: "PriceVolume"):
    _STORE[data.ticker] = data
