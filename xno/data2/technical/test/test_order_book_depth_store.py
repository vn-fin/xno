import unittest
from datetime import datetime

import xno.data2.technical.store.order_book_depth as ob_store
from xno.data2.technical.entity.order_book_depth import OrderBookDepth


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

    def test_store_state_isolation(self):
        """Test that store state is properly isolated between tests"""
        # Verify store is empty after setUp
        self.assertEqual(len(ob_store._ORDER_BOOKS), 0)

        # Add some data
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="TEST",
            bp=[100.0],
            bq=[1000],
            ap=[101.0],
            aq=[1500],
            total_bid=1000,
            total_ask=1500,
        )
        ob_store.push(ob)

        # Verify it's there
        self.assertEqual(len(ob_store._ORDER_BOOKS), 1)

    def test_push_updates_timestamp(self):
        """Test that pushing a new order book updates with latest timestamp"""
        ob1 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="HPG",
            bp=[50.0],
            bq=[5000],
            ap=[50.5],
            aq=[6000],
            total_bid=5000,
            total_ask=6000,
        )

        ob2 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 5, 0),  # 5 minutes later
            symbol="HPG",
            bp=[51.0],
            bq=[5500],
            ap=[51.5],
            aq=[6500],
            total_bid=5500,
            total_ask=6500,
        )

        ob_store.push(ob1)
        retrieved1 = ob_store.get("HPG")
        self.assertEqual(retrieved1.time, datetime(2024, 12, 18, 10, 0, 0))

        ob_store.push(ob2)
        retrieved2 = ob_store.get("HPG")
        self.assertEqual(retrieved2.time, datetime(2024, 12, 18, 10, 5, 0))
        self.assertEqual(retrieved2.bp, [51.0])

    def test_store_with_special_symbols(self):
        """Test storing order books with special characters in symbols"""
        symbols = ["VN30F2412", "ABC-WT", "DEF_GHI", "JKL.123"]

        for symbol in symbols:
            ob = OrderBookDepth(
                time=datetime(2024, 12, 18, 10, 0, 0),
                symbol=symbol,
                bp=[100.0],
                bq=[1000],
                ap=[101.0],
                aq=[1500],
                total_bid=1000,
                total_ask=1500,
            )
            ob_store.push(ob)
            retrieved = ob_store.get(symbol)
            self.assertIsNotNone(retrieved)
            self.assertEqual(retrieved.symbol, symbol)

    def test_get_returns_none_for_empty_string(self):
        """Test that get returns None for empty string symbol"""
        result = ob_store.get("")
        self.assertIsNone(result)

    def test_concurrent_symbol_updates(self):
        """Test updating multiple symbols in sequence"""
        symbols = ["MSB", "VNM", "HPG", "VCB", "TCB"]

        # Push order books for all symbols
        for i, symbol in enumerate(symbols):
            ob = OrderBookDepth(
                time=datetime(2024, 12, 18, 10, i, 0),
                symbol=symbol,
                bp=[100.0 + i],
                bq=[1000 * (i + 1)],
                ap=[101.0 + i],
                aq=[1500 * (i + 1)],
                total_bid=1000 * (i + 1),
                total_ask=1500 * (i + 1),
            )
            ob_store.push(ob)

        # Verify all are stored correctly
        self.assertEqual(len(ob_store._ORDER_BOOKS), 5)

        for i, symbol in enumerate(symbols):
            retrieved = ob_store.get(symbol)
            self.assertIsNotNone(retrieved)
            self.assertEqual(retrieved.symbol, symbol)
            self.assertEqual(retrieved.bp, [100.0 + i])

    def test_store_large_order_book(self):
        """Test storing order book with maximum depth"""
        # Create order book with 10 levels
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="LARGE",
            bp=[100.0 - i * 0.1 for i in range(10)],
            bq=[1000 * (i + 1) for i in range(10)],
            ap=[100.1 + i * 0.1 for i in range(10)],
            aq=[1500 * (i + 1) for i in range(10)],
            total_bid=55000,
            total_ask=82500,
        )

        ob_store.push(ob)
        retrieved = ob_store.get("LARGE")

        self.assertIsNotNone(retrieved)
        self.assertEqual(len(retrieved.bp), 10)
        self.assertEqual(len(retrieved.bq), 10)
        self.assertEqual(len(retrieved.ap), 10)
        self.assertEqual(len(retrieved.aq), 10)

    def test_store_empty_order_book(self):
        """Test storing order book with no bid/ask levels"""
        ob = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="EMPTY",
            bp=[],
            bq=[],
            ap=[],
            aq=[],
            total_bid=0,
            total_ask=0,
        )

        ob_store.push(ob)
        retrieved = ob_store.get("EMPTY")

        self.assertIsNotNone(retrieved)
        self.assertEqual(len(retrieved.bp), 0)
        self.assertEqual(len(retrieved.ap), 0)

    def test_store_persistence_across_operations(self):
        """Test that store maintains data across multiple operations"""
        # Add first symbol
        ob1 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="SYMBOL1",
            bp=[100.0],
            bq=[1000],
            ap=[101.0],
            aq=[1500],
            total_bid=1000,
            total_ask=1500,
        )
        ob_store.push(ob1)

        # Add second symbol
        ob2 = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 1, 0),
            symbol="SYMBOL2",
            bp=[200.0],
            bq=[2000],
            ap=[201.0],
            aq=[2500],
            total_bid=2000,
            total_ask=2500,
        )
        ob_store.push(ob2)

        # Verify first symbol still exists
        retrieved1 = ob_store.get("SYMBOL1")
        self.assertIsNotNone(retrieved1)
        self.assertEqual(retrieved1.bp, [100.0])

        # Update first symbol
        ob1_updated = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 2, 0),
            symbol="SYMBOL1",
            bp=[105.0],
            bq=[1100],
            ap=[106.0],
            aq=[1600],
            total_bid=1100,
            total_ask=1600,
        )
        ob_store.push(ob1_updated)

        # Verify second symbol is unchanged
        retrieved2 = ob_store.get("SYMBOL2")
        self.assertEqual(retrieved2.bp, [200.0])

        # Verify first symbol is updated
        retrieved1_updated = ob_store.get("SYMBOL1")
        self.assertEqual(retrieved1_updated.bp, [105.0])

    def test_store_handles_case_sensitive_symbols(self):
        """Test that symbol lookup is case-sensitive"""
        ob_lower = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="msb",
            bp=[100.0],
            bq=[1000],
            ap=[101.0],
            aq=[1500],
            total_bid=1000,
            total_ask=1500,
        )

        ob_upper = OrderBookDepth(
            time=datetime(2024, 12, 18, 10, 0, 0),
            symbol="MSB",
            bp=[200.0],
            bq=[2000],
            ap=[201.0],
            aq=[2500],
            total_bid=2000,
            total_ask=2500,
        )

        ob_store.push(ob_lower)
        ob_store.push(ob_upper)

        retrieved_lower = ob_store.get("msb")
        retrieved_upper = ob_store.get("MSB")

        self.assertIsNotNone(retrieved_lower)
        self.assertIsNotNone(retrieved_upper)
        self.assertEqual(retrieved_lower.bp, [100.0])
        self.assertEqual(retrieved_upper.bp, [200.0])
        self.assertEqual(len(ob_store._ORDER_BOOKS), 2)


if __name__ == "__main__":
    unittest.main()
