import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow as pa

import xno.data2.technical.store.ohlcv as ohlcv_store
from xno.data2.technical.entity.ohlcv import OHLCV, OHLCVs
from xno.data2.technical.entity.resolution import Resolution


class TestOHLCVStore(unittest.TestCase):
    """Unit tests for OHLCV store"""

    def setUp(self):
        """Clear the store before each test"""
        ohlcv_store._LAST_OHLCV.clear()
        ohlcv_store._OHLCV_HISTORY_DATA.clear()
        ohlcv_store._OHLCV_BUFFER_DATA.clear()

        self.ohlcv1 = OHLCV(
            time=datetime(2024, 12, 18, 10, 0, 0),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
        )
        self.ohlcv2 = OHLCV(
            time=datetime(2024, 12, 18, 11, 0, 0),
            open=103.0,
            high=108.0,
            low=102.0,
            close=107.0,
            volume=1200000.0,
        )
        self.resolution = Resolution(unit="H", value=1)

    def tearDown(self):
        """Clean up after each test"""
        ohlcv_store._LAST_OHLCV.clear()
        ohlcv_store._OHLCV_HISTORY_DATA.clear()
        ohlcv_store._OHLCV_BUFFER_DATA.clear()

    def test_push_single_ohlcv(self):
        """Test pushing a single OHLCV to buffer"""
        ohlcv_store.push("MSB", "1H", self.ohlcv1)

        buffer = ohlcv_store._OHLCV_BUFFER_DATA.get(("MSB", "1H"))
        self.assertIsNotNone(buffer)
        self.assertIn(self.ohlcv1.time, buffer)

    def test_push_multiple_ohlcvs(self):
        """Test pushing multiple OHLCVs"""
        ohlcv_store.push("MSB", "1H", self.ohlcv1)
        ohlcv_store.push("MSB", "1H", self.ohlcv2)

        buffer = ohlcv_store._OHLCV_BUFFER_DATA.get(("MSB", "1H"))
        self.assertEqual(len(buffer), 2)

    def test_pushes_collection(self):
        """Test pushing OHLCVs collection to history"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        ohlcv_store.pushes(ohlcvs)

        history = ohlcv_store._OHLCV_HISTORY_DATA.get(("MSB", self.resolution))
        self.assertIsNotNone(history)
        self.assertIsInstance(history, pl.DataFrame)
        self.assertEqual(len(history), 2)

    def test_is_come_late_ohlcv(self):
        """Test detection of late/duplicate OHLCV data"""
        # First push
        ohlcv_store.push("MSB", "1H", self.ohlcv1)

        # Try to push same time with lower volume (should be rejected)
        late_ohlcv = OHLCV(
            time=datetime(2024, 12, 18, 10, 0, 0),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=500000.0,  # Lower volume
        )

        is_late = ohlcv_store._is_come_late_ohlcv("MSB", "1H", late_ohlcv)
        self.assertTrue(is_late)

    def test_is_not_come_late_ohlcv(self):
        """Test that new data with higher volume is not rejected"""
        # First push
        ohlcv_store.push("MSB", "1H", self.ohlcv1)

        # New data with higher volume should not be late
        new_ohlcv = OHLCV(
            time=datetime(2024, 12, 18, 10, 0, 0),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=2000000.0,  # Higher volume
        )

        is_late = ohlcv_store._is_come_late_ohlcv("MSB", "1H", new_ohlcv)
        self.assertFalse(is_late)

    def test_flush_buffers(self):
        """Test flushing buffers to history"""
        # Push to buffer
        ohlcv_store.push("MSB", "1H", self.ohlcv1)
        ohlcv_store.push("MSB", "1H", self.ohlcv2)

        # Initialize history first
        # ohlcvs = OHLCVs(
        #     symbol="MSB",
        #     resolution=self.resolution,
        #     ohlcvs=[],
        # )
        # Create empty history DataFrame
        ohlcv_store._OHLCV_HISTORY_DATA[("MSB", "1H")] = pl.DataFrame([], schema=ohlcv_store.OHLCV_POLARS_SCHEMA)

        # Flush
        ohlcv_store.flush_buffers()

        # Buffer should be cleared
        buffer = ohlcv_store._OHLCV_BUFFER_DATA.get(("MSB", "1H"))
        self.assertIsNone(buffer)

    @patch("xno.data2.technical.store.ohlcv.connect")
    def test_get_numpy(self, mock_connect):
        """Test getting OHLCV data as numpy array"""
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_result = MagicMock()
        mock_result.fetchnumpy.return_value = {
            "time": [datetime(2024, 12, 18, 10, 0, 0)],
            "open": [100.0],
            "high": [105.0],
            "low": [99.0],
            "close": [103.0],
            "volume": [1000000.0],
        }
        mock_conn.execute.return_value = mock_result

        # Push data to history
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1],
        )
        ohlcv_store.pushes(ohlcvs)

        # Get data
        result = ohlcv_store.get_numpy(
            symbol="MSB",
            resolution="1H",
            from_time=datetime(2024, 12, 18, 9, 0, 0),
            to_time=datetime(2024, 12, 18, 12, 0, 0),
        )

        self.assertIsNotNone(result)

    def test_push_with_late_data_rejection(self):
        """Test that late data is rejected in push"""
        # Push original
        ohlcv_store.push("MSB", "1H", self.ohlcv1)

        buffer_size_before = len(ohlcv_store._OHLCV_BUFFER_DATA.get(("MSB", "1H"), {}))

        # Try to push late data (same time, lower volume)
        late_ohlcv = OHLCV(
            time=datetime(2024, 12, 18, 10, 0, 0),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=500000.0,
        )
        ohlcv_store.push("MSB", "1H", late_ohlcv)

        buffer_size_after = len(ohlcv_store._OHLCV_BUFFER_DATA.get(("MSB", "1H"), {}))

        # Buffer size should not increase
        self.assertEqual(buffer_size_before, buffer_size_after)


if __name__ == "__main__":
    unittest.main()
