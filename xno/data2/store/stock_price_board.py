from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xno.data2.entity.stock_price_board import StockPriceBoard

_STOCK_PRICE_BOARD_STORE = {}


def get(symbol: str) -> "StockPriceBoard | None":
    return _STOCK_PRICE_BOARD_STORE.get(symbol, None)


def push(stock_price_board: "StockPriceBoard"):
    _STOCK_PRICE_BOARD_STORE[stock_price_board.symbol] = stock_price_board
