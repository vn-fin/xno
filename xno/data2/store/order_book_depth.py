import logging
from  typing import  TYPE_CHECKING

if TYPE_CHECKING:
    from xno.data2 import OrderBookDepth

logger = logging.getLogger(__name__)
_ORDER_BOOKS: "dict[str, OrderBookDepth]" = dict()


def get(symbol: str, depth: int = 10) -> "OrderBookDepth | None":
    """
    Get order book depth for a given symbol
    Returns None if symbol not found
    """
    if symbol not in _ORDER_BOOKS:
        return None
    order_book = _ORDER_BOOKS[symbol]
    return order_book

def push(order_book: "OrderBookDepth") -> None:
    """
    Set order book depth for a given symbol
    """
    key = order_book.symbol
    _ORDER_BOOKS[key] = order_book
