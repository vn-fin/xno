import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow as pa

import xno.data2.technical.store.ohlcv as ohlcv_store
from xno.data2.technical.entity.ohlcv import OHLCV, OHLCVs
from xno.data2.technical.entity.resolution import Resolution


class TestOHLCVEntity(unittest.TestCase):
    """Unit tests for OHLCV entity"""

    def setUp(self):
        """Set up test fixtures"""
        self.valid_ohlcv = OHLCV(
            time=datetime(2024, 12, 18, 10, 0, 0),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
        )

        self.kafka_raw = {
            "time": 1734518400,  # 2024-12-18 10:00:00 UTC
            "symbol": "MSB",
            "resolution": "DAY",
            "open": 12.85,
            "high": 13.0,
            "low": 12.75,
            "close": 12.95,
            "volume": 1234567.0,
            "updated": 1734518500,
            "data_type": "OH",
            "source": "dnse",
        }

    def test_create_valid_ohlcv(self):
        """Test creating a valid OHLCV instance"""
        ohlcv = OHLCV(
            time=datetime(2024, 12, 18, 10, 0, 0),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
        )

        self.assertEqual(ohlcv.open, 100.0)
        self.assertEqual(ohlcv.high, 105.0)
        self.assertEqual(ohlcv.low, 99.0)
        self.assertEqual(ohlcv.close, 103.0)
        self.assertEqual(ohlcv.volume, 1000000.0)
        self.assertIsInstance(ohlcv.time, datetime)

    def test_ohlcv_frozen_dataclass(self):
        """Test that OHLCV is immutable (frozen dataclass)"""
        ohlcv = self.valid_ohlcv
        with self.assertRaises(Exception):  # FrozenInstanceError
            ohlcv.open = 200.0

    def test_validation_invalid_time_type(self):
        """Test validation raises error for invalid time type"""
        with self.assertRaises(TypeError):
            OHLCV(
                time="2024-12-18",  # Should be datetime
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
            )

    def test_validation_invalid_open_type(self):
        """Test validation raises error for invalid open type"""
        with self.assertRaises(TypeError):
            OHLCV(
                time=datetime(2024, 12, 18, 10, 0, 0),
                open="100.0",  # Should be float
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
            )

    def test_validation_invalid_high_type(self):
        """Test validation raises error for invalid high type"""
        with self.assertRaises(TypeError):
            OHLCV(
                time=datetime(2024, 12, 18, 10, 0, 0),
                open=100.0,
                high="105.0",  # Should be float
                low=99.0,
                close=103.0,
                volume=1000000.0,
            )

    def test_validation_invalid_low_type(self):
        """Test validation raises error for invalid low type"""
        with self.assertRaises(TypeError):
            OHLCV(
                time=datetime(2024, 12, 18, 10, 0, 0),
                open=100.0,
                high=105.0,
                low="99.0",  # Should be float
                close=103.0,
                volume=1000000.0,
            )

    def test_validation_invalid_close_type(self):
        """Test validation raises error for invalid close type"""
        with self.assertRaises(TypeError):
            OHLCV(
                time=datetime(2024, 12, 18, 10, 0, 0),
                open=100.0,
                high=105.0,
                low=99.0,
                close="103.0",  # Should be float
                volume=1000000.0,
            )

    def test_validation_invalid_volume_type(self):
        """Test validation raises error for invalid volume type"""
        with self.assertRaises(TypeError):
            OHLCV(
                time=datetime(2024, 12, 18, 10, 0, 0),
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume="1000000.0",  # Should be float
            )

    def test_validation_accepts_int_values(self):
        """Test that validation accepts int values for prices"""
        ohlcv = OHLCV(
            time=datetime(2024, 12, 18, 10, 0, 0),
            open=100,  # int
            high=105,  # int
            low=99,  # int
            close=103,  # int
            volume=1000000,  # int
        )
        self.assertEqual(ohlcv.open, 100)
        self.assertEqual(ohlcv.volume, 1000000)

    def test_to_dict(self):
        """Test converting OHLCV to dictionary"""
        ohlcv = self.valid_ohlcv
        result = ohlcv.to_dict()

        self.assertIsInstance(result, dict)
        self.assertEqual(result["open"], 100.0)
        self.assertEqual(result["high"], 105.0)
        self.assertEqual(result["low"], 99.0)
        self.assertEqual(result["close"], 103.0)
        self.assertEqual(result["volume"], 1000000.0)
        self.assertIn("time", result)

    def test_to_duckdb_row(self):
        """Test converting OHLCV to DuckDB row format"""
        ohlcv = self.valid_ohlcv
        row = ohlcv.to_duckdb_row("MSB", "1D")

        self.assertIsInstance(row, list)
        self.assertEqual(len(row), 6)
        self.assertEqual(row[1], 100.0)  # open
        self.assertEqual(row[2], 105.0)  # high
        self.assertEqual(row[3], 99.0)  # low
        self.assertEqual(row[4], 103.0)  # close
        self.assertEqual(row[5], 1000000.0)  # volume

    def test_from_external_kafka(self):
        """Test creating OHLCV from external Kafka message"""
        symbol, resolution, ohlcv = OHLCV.from_external_kafka(self.kafka_raw)

        self.assertEqual(symbol, "MSB")
        self.assertEqual(resolution, "DAY")
        self.assertIsInstance(ohlcv, OHLCV)
        self.assertEqual(ohlcv.open, 12.85)
        self.assertEqual(ohlcv.high, 13.0)
        self.assertEqual(ohlcv.low, 12.75)
        self.assertEqual(ohlcv.close, 12.95)
        self.assertEqual(ohlcv.volume, 1234567.0)
        self.assertIsInstance(ohlcv.time, datetime)

    def test_from_external_db_list(self):
        """Test creating OHLCV from external DB list"""
        raw = [
            datetime(2024, 12, 18, 10, 0, 0),
            100.5,
            105.5,
            99.5,
            103.5,
            1500000,
        ]

        ohlcv = OHLCV.from_external_db_list(raw)

        self.assertIsInstance(ohlcv, OHLCV)
        self.assertEqual(ohlcv.time, datetime(2024, 12, 18, 10, 0, 0))
        self.assertEqual(ohlcv.open, 100.5)
        self.assertEqual(ohlcv.high, 105.5)
        self.assertEqual(ohlcv.low, 99.5)
        self.assertEqual(ohlcv.close, 103.5)
        self.assertEqual(ohlcv.volume, 1500000.0)


class TestOHLCVsCollection(unittest.TestCase):
    """Unit tests for OHLCVs collection entity"""

    def setUp(self):
        """Set up test fixtures"""
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

    def test_create_valid_ohlcvs(self):
        """Test creating a valid OHLCVs collection"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        self.assertEqual(ohlcvs.symbol, "MSB")
        self.assertEqual(ohlcvs.resolution, self.resolution)
        self.assertEqual(len(ohlcvs.ohlcvs), 2)
        self.assertEqual(len(ohlcvs), 2)

    def test_validation_empty_symbol(self):
        """Test validation raises error for empty symbol"""
        with self.assertRaises(ValueError):
            OHLCVs(
                symbol="",
                resolution=self.resolution,
                ohlcvs=[self.ohlcv1],
            )

    def test_validation_invalid_symbol_type(self):
        """Test validation raises error for invalid symbol type"""
        with self.assertRaises(TypeError):
            OHLCVs(
                symbol=123,  # Should be string
                resolution=self.resolution,
                ohlcvs=[self.ohlcv1],
            )

    def test_validation_empty_resolution(self):
        """Test validation raises error for empty resolution"""
        with self.assertRaises(ValueError):
            OHLCVs(
                symbol="MSB",
                resolution=None,
                ohlcvs=[self.ohlcv1],
            )

    def test_validation_invalid_resolution_type(self):
        """Test validation raises error for invalid resolution type"""
        with self.assertRaises(TypeError):
            OHLCVs(
                symbol="MSB",
                resolution="1H",  # Should be Resolution object
                ohlcvs=[self.ohlcv1],
            )

    def test_validation_empty_ohlcvs_list(self):
        """Test validation raises error for empty ohlcvs list"""
        with self.assertRaises(ValueError):
            OHLCVs(
                symbol="MSB",
                resolution=self.resolution,
                ohlcvs=[],
            )

    def test_validation_invalid_ohlcvs_type(self):
        """Test validation raises error for invalid ohlcvs type"""
        with self.assertRaises(TypeError):
            OHLCVs(
                symbol="MSB",
                resolution=self.resolution,
                ohlcvs="not a list",
            )

    def test_validation_invalid_ohlcv_element(self):
        """Test validation raises error for invalid ohlcv element in list"""
        with self.assertRaises(TypeError):
            OHLCVs(
                symbol="MSB",
                resolution=self.resolution,
                ohlcvs=[self.ohlcv1, "not an ohlcv"],
            )

    def test_to_duckdb_rows(self):
        """Test converting OHLCVs to DuckDB rows"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        rows = ohlcvs.to_duckdb_rows()

        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(rows[0]), 6)

    def test_to_pyarrow(self):
        """Test converting OHLCVs to PyArrow Table"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        table = ohlcvs.to_pyarrow()

        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.num_rows, 2)
        self.assertIn("time", table.column_names)
        self.assertIn("open", table.column_names)
        self.assertIn("high", table.column_names)
        self.assertIn("low", table.column_names)
        self.assertIn("close", table.column_names)
        self.assertIn("volume", table.column_names)

    def test_to_pyarrow_batch(self):
        """Test converting OHLCVs to PyArrow RecordBatch"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        batch = ohlcvs.to_pyarrow_batch()

        self.assertIsInstance(batch, pa.RecordBatch)
        self.assertEqual(batch.num_rows, 2)

    def test_to_df(self):
        """Test converting OHLCVs to pandas DataFrame"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        df = ohlcvs.to_df()

        self.assertEqual(len(df), 2)
        self.assertIn("time", df.columns)
        self.assertIn("open", df.columns)
        self.assertIn("high", df.columns)
        self.assertIn("low", df.columns)
        self.assertIn("close", df.columns)
        self.assertIn("volume", df.columns)

    def test_to_polars_df(self):
        """Test converting OHLCVs to Polars DataFrame"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        df = ohlcvs.to_polars_df()

        self.assertIsInstance(df, pl.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn("time", df.columns)
        self.assertIn("open", df.columns)
        self.assertIn("high", df.columns)
        self.assertIn("low", df.columns)
        self.assertIn("close", df.columns)
        self.assertIn("volume", df.columns)

    def test_from_external_db(self):
        """Test creating OHLCVs from external DB data"""
        raws = [
            [datetime(2024, 12, 18, 10, 0, 0), 100.0, 105.0, 99.0, 103.0, 1000000.0],
            [datetime(2024, 12, 18, 11, 0, 0), 103.0, 108.0, 102.0, 107.0, 1200000.0],
        ]

        ohlcvs = OHLCVs.from_external_db("MSB", self.resolution, raws)

        self.assertEqual(ohlcvs.symbol, "MSB")
        self.assertEqual(ohlcvs.resolution, self.resolution)
        self.assertEqual(len(ohlcvs), 2)
        self.assertEqual(ohlcvs.ohlcvs[0].open, 100.0)
        self.assertEqual(ohlcvs.ohlcvs[1].open, 103.0)

    def test_len_method(self):
        """Test __len__ method"""
        ohlcvs = OHLCVs(
            symbol="MSB",
            resolution=self.resolution,
            ohlcvs=[self.ohlcv1, self.ohlcv2],
        )

        self.assertEqual(len(ohlcvs), 2)


if __name__ == "__main__":
    unittest.main()
