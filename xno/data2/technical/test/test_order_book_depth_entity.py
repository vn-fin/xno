import unittest
from datetime import datetime
from unittest.mock import MagicMock

from sqlalchemy.engine.row import Row

import xno.data2.technical.store.order_book_depth as ob_store
from xno.data2.technical.entity.order_book_depth import OrderBookDepth


class TestOrderBookDepthEntity(unittest.TestCase):
    """Unit tests for OrderBookDepth entity"""

    def setUp(self):
        """Set up test fixtures"""
        self.valid_raw_kafka = {
            "time": 1734518400.0,  # 2024-12-18 10:00:00 UTC
            "symbol": "MSB",
            "bp": [12.85, 12.8, 12.75],
            "bq": [4390, 21790, 7250],
            "ap": [12.9, 12.95, 13.0],
            "aq": [19150, 22430, 47430],
            "total_bid": 33430,
            "total_ask": 89010,
        }

    def test_create_valid_order_book_depth(self):
        """Test creating a valid OrderBookDepth instance"""
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="MSB",
            bp=[12.85, 12.8, 12.75],
            bq=[4390, 21790, 7250],
            ap=[12.9, 12.95, 13.0],
            aq=[19150, 22430, 47430],
            total_bid=33430,
            total_ask=89010,
        )

        self.assertEqual(ob.symbol, "MSB")
        self.assertEqual(ob.bp, [12.85, 12.8, 12.75])
        self.assertEqual(ob.bq, [4390, 21790, 7250])
        self.assertEqual(ob.ap, [12.9, 12.95, 13.0])
        self.assertEqual(ob.aq, [19150, 22430, 47430])
        self.assertEqual(ob.total_bid, 33430)
        self.assertEqual(ob.total_ask, 89010)
        self.assertIsInstance(ob.time, datetime)

    def test_from_external_kafka(self):
        """Test creating OrderBookDepth from external Kafka message"""
        ob = OrderBookDepth.from_external_kafka(self.valid_raw_kafka)

        self.assertIsNotNone(ob)
        self.assertEqual(ob.symbol, "MSB")
        self.assertEqual(ob.bp, [12.85, 12.8, 12.75])
        self.assertEqual(ob.bq, [4390, 21790, 7250])
        self.assertEqual(ob.ap, [12.9, 12.95, 13.0])
        self.assertEqual(ob.aq, [19150, 22430, 47430])
        self.assertEqual(ob.total_bid, 33430)
        self.assertEqual(ob.total_ask, 89010)
        self.assertIsInstance(ob.time, datetime)

    def test_from_external_db(self):
        """Test creating OrderBookDepth from external DB row"""
        # Mock a SQLAlchemy Row
        raw_dict = {
            "time": datetime(2024, 12, 18, 10, 0, 0),
            "symbol": "VNM",
            "bp": [80.5, 80.4, 80.3],
            "bq": [1000, 2000, 3000],
            "ap": [80.6, 80.7, 80.8],
            "aq": [1500, 2500, 3500],
            "total_bid": 6000,
            "total_ask": 7500,
        }

        # Test with dict
        ob = OrderBookDepth.from_external_db(raw_dict, depth=10)

        self.assertEqual(ob.symbol, "VNM")
        self.assertEqual(ob.bp, [80.5, 80.4, 80.3])
        self.assertEqual(ob.bq, [1000, 2000, 3000])
        self.assertEqual(ob.ap, [80.6, 80.7, 80.8])
        self.assertEqual(ob.aq, [1500, 2500, 3500])

    def test_from_external_db_with_depth_limit(self):
        """Test depth limiting when creating from DB"""
        raw_dict = {
            "time": datetime(2024, 12, 18, 10, 0, 0),
            "symbol": "VNM",
            "bp": [80.5, 80.4, 80.3, 80.2, 80.1],
            "bq": [1000, 2000, 3000, 4000, 5000],
            "ap": [80.6, 80.7, 80.8, 80.9, 81.0],
            "aq": [1500, 2500, 3500, 4500, 5500],
            "total_bid": 15000,
            "total_ask": 17500,
        }

        ob = OrderBookDepth.from_external_db(raw_dict, depth=3)

        self.assertEqual(len(ob.bp), 3)
        self.assertEqual(len(ob.bq), 3)
        self.assertEqual(len(ob.ap), 3)
        self.assertEqual(len(ob.aq), 3)
        self.assertEqual(ob.bp, [80.5, 80.4, 80.3])

    def test_from_external_db_with_time_resampled(self):
        """Test creating from DB with time_resampled field"""
        raw_dict = {
            "time_resampled": datetime(2024, 12, 18, 10, 0, 0),
            "symbol": "VNM",
            "bp": [80.5],
            "bq": [1000],
            "ap": [80.6],
            "aq": [1500],
            "total_bid": 1000,
            "total_ask": 1500,
        }

        ob = OrderBookDepth.from_external_db(raw_dict)
        self.assertEqual(ob.time, datetime(2024, 12, 18, 10, 0, 0))

    def test_validation_invalid_time_type(self):
        """Test validation raises error for invalid time type"""
        with self.assertRaises(TypeError):
            OrderBookDepth(
                time="2024-12-18",  # Should be datetime
                symbol="MSB",
                bp=[12.85],
                bq=[4390],
                ap=[12.9],
                aq=[19150],
                total_bid=4390,
                total_ask=19150,
            )

    def test_validation_invalid_symbol_type(self):
        """Test validation raises error for invalid symbol type"""
        with self.assertRaises(TypeError):
            OrderBookDepth(
                time=datetime(2024, 12, 18, 10, 0, 0),
                symbol=123,  # Should be string
                bp=[12.85],
                bq=[4390],
                ap=[12.9],
                aq=[19150],
                total_bid=4390,
                total_ask=19150,
            )

    def test_validation_bp_bq_length_mismatch(self):
        """Test validation raises error when bp and bq have different lengths"""
        with self.assertRaises(ValueError):
            OrderBookDepth(
                time=datetime(2024, 12, 18, 10, 0, 0),
                symbol="MSB",
                bp=[12.85, 12.8],  # 2 items
                bq=[4390],  # 1 item - mismatch
                ap=[12.9],
                aq=[19150],
                total_bid=4390,
                total_ask=19150,
            )

    def test_validation_ap_aq_length_mismatch(self):
        """Test validation raises error when ap and aq have different lengths"""
        with self.assertRaises(ValueError):
            OrderBookDepth(
                time=datetime(2024, 12, 18, 10, 0, 0),
                symbol="MSB",
                bp=[12.85],
                bq=[4390],
                ap=[12.9, 12.95],  # 2 items
                aq=[19150],  # 1 item - mismatch
                total_bid=4390,
                total_ask=19150,
            )

    def test_validation_invalid_bp_type(self):
        """Test validation raises error for invalid bp element type"""
        with self.assertRaises(TypeError):
            OrderBookDepth(
                time=datetime(2024, 12, 18, 10, 0, 0),
                symbol="MSB",
                bp=["12.85"],  # Should be float/int
                bq=[4390],
                ap=[12.9],
                aq=[19150],
                total_bid=4390,
                total_ask=19150,
            )

    def test_validation_invalid_bq_type(self):
        """Test validation raises error for invalid bq element type"""
        with self.assertRaises(TypeError):
            OrderBookDepth(
                time=datetime(2024, 12, 18, 10, 0, 0),
                symbol="MSB",
                bp=[12.85],
                bq=[4390.5],  # Should be int
                ap=[12.9],
                aq=[19150],
                total_bid=4390,
                total_ask=19150,
            )

    def test_validation_invalid_total_bid_type(self):
        """Test validation raises error for invalid total_bid type"""
        with self.assertRaises(TypeError):
            OrderBookDepth(
                time=datetime(2024, 12, 18, 10, 0, 0),
                symbol="MSB",
                bp=[12.85],
                bq=[4390],
                ap=[12.9],
                aq=[19150],
                total_bid="4390",  # Should be int
                total_ask=19150,
            )

    def test_to_dict(self):
        """Test converting OrderBookDepth to dictionary"""
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="MSB",
            bp=[12.85, 12.8],
            bq=[4390, 21790],
            ap=[12.9, 12.95],
            aq=[19150, 22430],
            total_bid=26180,
            total_ask=41580,
        )

        result = ob.to_dict()

        self.assertIsInstance(result, dict)
        self.assertEqual(result["symbol"], "MSB")
        self.assertEqual(result["bp"], [12.85, 12.8])
        self.assertEqual(result["bq"], [4390, 21790])
        self.assertEqual(result["ap"], [12.9, 12.95])
        self.assertEqual(result["aq"], [19150, 22430])
        self.assertEqual(result["total_bid"], 26180)
        self.assertEqual(result["total_ask"], 41580)

    def test_empty_order_book(self):
        """Test creating OrderBookDepth with empty bid/ask lists"""
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="MSB",
            bp=[],
            bq=[],
            ap=[],
            aq=[],
            total_bid=0,
            total_ask=0,
        )

        self.assertEqual(len(ob.bp), 0)
        self.assertEqual(len(ob.bq), 0)
        self.assertEqual(len(ob.ap), 0)
        self.assertEqual(len(ob.aq), 0)


class TestOrderBookDepthStore(unittest.TestCase):
    """Unit tests for order_book_depth store"""

    def setUp(self):
        """Clear the store before each test"""
        ob_store._ORDER_BOOKS.clear()

    def tearDown(self):
        """Clean up after each test"""
        ob_store._ORDER_BOOKS.clear()

    def test_push_and_get(self):
        """Test pushing and retrieving an order book"""
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="MSB",
            bp=[12.85, 12.8],
            bq=[4390, 21790],
            ap=[12.9, 12.95],
            aq=[19150, 22430],
            total_bid=26180,
            total_ask=41580,
        )

        ob_store.push(ob)
        retrieved = ob_store.get("MSB")

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.symbol, "MSB")
        self.assertEqual(retrieved.bp, [12.85, 12.8])
        self.assertIs(retrieved, ob)  # Should be the same instance

    def test_get_nonexistent_symbol(self):
        """Test retrieving a non-existent symbol returns None"""
        result = ob_store.get("NONEXISTENT")
        self.assertIsNone(result)

    def test_push_overwrites_existing(self):
        """Test that pushing a new order book for same symbol overwrites"""
        ob1 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="VNM",
            bp=[80.5],
            bq=[1000],
            ap=[80.6],
            aq=[1500],
            total_bid=1000,
            total_ask=1500,
        )

        ob2 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 1, 0),
            symbol="VNM",
            bp=[80.6],
            bq=[2000],
            ap=[80.7],
            aq=[2500],
            total_bid=2000,
            total_ask=2500,
        )

        ob_store.push(ob1)
        ob_store.push(ob2)

        retrieved = ob_store.get("VNM")
        self.assertIs(retrieved, ob2)
        self.assertEqual(retrieved.bp, [80.6])

    def test_multiple_symbols(self):
        """Test storing and retrieving multiple symbols"""
        ob1 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="MSB",
            bp=[12.85],
            bq=[4390],
            ap=[12.9],
            aq=[19150],
            total_bid=4390,
            total_ask=19150,
        )

        ob2 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="VNM",
            bp=[80.5],
            bq=[1000],
            ap=[80.6],
            aq=[1500],
            total_bid=1000,
            total_ask=1500,
        )

        ob_store.push(ob1)
        ob_store.push(ob2)

        retrieved_msb = ob_store.get("MSB")
        retrieved_vnm = ob_store.get("VNM")

        self.assertEqual(retrieved_msb.symbol, "MSB")
        self.assertEqual(retrieved_vnm.symbol, "VNM")
        self.assertIsNot(retrieved_msb, retrieved_vnm)

    def test_get_with_depth_parameter(self):
        """Test that depth parameter is accepted (though currently unused)"""
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="MSB",
            bp=[12.85, 12.8, 12.75],
            bq=[4390, 21790, 7250],
            ap=[12.9, 12.95, 13.0],
            aq=[19150, 22430, 47430],
            total_bid=33430,
            total_ask=89010,
        )

        ob_store.push(ob)
        retrieved = ob_store.get("MSB", depth=5)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.symbol, "MSB")


if __name__ == "__main__":
    unittest.main()
