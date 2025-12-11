import os
import sys

import xno.data2.store.order_book_depth as ob_store
from xno.data2.entity.order_book_depth import OrderBookDepth

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))


from dotenv import load_dotenv

load_dotenv("../xalpha/.env")


def _sample_raw():
    return {
        "time": "2025-12-11T10:00:00",
        "symbol": "ABC",
        "bp": [1.1, 1.0, 0.9],
        "bq": [100, 50, 25],
        "ap": [1.2, 1.25, 1.3],
        "aq": [80, 40, 20],
        "total_bid": 175,
        "total_ask": 140,
        "data_type": "TP",
        "source": "test",
    }


def test_order_book_depth_from_external_kafka():
    raw = _sample_raw()

    ob = OrderBookDepth.from_external_kafka(raw)
    assert ob is not None
    assert ob.symbol == "ABC"
    # bp/ap lists preserved
    assert list(ob.bp) == [1.1, 1.0, 0.9]
    assert list(ob.ap) == [1.2, 1.25, 1.3]
    assert ob.total_bid == 175
    assert ob.total_ask == 140
    # time should parse to a timestamp-like value
    assert "2025-12-11" in str(ob.time)


def test_order_book_depth_store_push_get_and_missing():
    # ensure store is clean
    ob_store._ORDER_BOOKS.clear()

    raw = _sample_raw()
    ob = OrderBookDepth.from_external_kafka(raw)

    ob_store.push(ob)

    fetched = ob_store.get("ABC")
    assert fetched is not None
    # push stores the same instance
    assert fetched is ob

    # missing symbol returns None
    assert ob_store.get("MISSING_SYMBOL") is None
