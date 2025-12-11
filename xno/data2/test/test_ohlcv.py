import threading
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
from dotenv import load_dotenv

from xno.data2.provider import DataProvider

load_dotenv("../xalpha/.env")


class TestDataProvider(unittest.TestCase):
    def setUp(self):
        self.consumer_config = {"bootstrap.servers": "dummy:9092"}

    @patch("xno.data2.provider.OHLCV_store")
    @patch("xno.data2.provider.ExternalDataService")
    def test_start_calls_init_and_external_start(self, MockExternalService, MockOHLCVStore):
        # Arrange
        mock_service_instance = MockExternalService.return_value
        mock_service_instance.start = MagicMock()

        provider = DataProvider(consumer_config=self.consumer_config)

        # Act
        provider.start()

        # Assert
        MockOHLCVStore.init.assert_called_once()
        mock_service_instance.start.assert_called_once()
        # ensure start was called with the two callbacks
        kwargs = mock_service_instance.start.call_args.kwargs
        self.assertIn("on_consume_ohlcv", kwargs)
        self.assertIn("on_consume_order_book", kwargs)

    @patch("xno.data2.provider.OHLCV_store")
    @patch("xno.data2.provider.OHLCVs")
    @patch("xno.data2.provider.DistributedSemaphore")
    def test_get_ohlcv_syncs_from_db_and_returns_numpy(self, MockSemaphore, MockOHLCVs, MockOHLCVStore):
        # Arrange
        # Make DistributedSemaphore a no-op context manager
        cm = MagicMock()
        cm.__enter__.return_value = None
        cm.__exit__.return_value = None
        MockSemaphore.return_value = cm

        provider = DataProvider(consumer_config=self.consumer_config)
        # replace real external service with a mock that returns history raws
        provider._external_data_service = MagicMock()
        provider._external_data_service.get_history_ohlcv.return_value = [{"raw": 1}]

        # OHLCVs.from_external_db returns a list/object to be pushed
        MockOHLCVs.from_external_db.return_value = ["ohlcv1"]
        # pushes should be called with the result
        MockOHLCVStore.pushes = MagicMock()

        expected_array = np.array([[1, 2, 3]])
        MockOHLCVStore.get_numpy.return_value = expected_array

        # Act
        result = provider.get_ohlcv(
            symbol="ABC", resolution="MIN", from_time="2025-01-01 00:00:00", to_time="2025-01-02 00:00:00"
        )

        # Assert
        provider._external_data_service.get_history_ohlcv.assert_called_once_with(symbol="ABC", resolution="MIN")
        MockOHLCVs.from_external_db.assert_called_once()
        MockOHLCVStore.pushes.assert_called_once_with(["ohlcv1"])
        MockOHLCVStore.get_numpy.assert_called_once()
        np.testing.assert_array_equal(result, expected_array)

    @patch("xno.data2.provider.OHLCV_store")
    @patch("xno.data2.provider.ExternalDataService")
    def test_get_ohlcv_returns_when_already_synced(self, MockExternalService, MockOHLCVStore):
        # Arrange
        provider = DataProvider(consumer_config=self.consumer_config)
        # Put the lock key into _ohlcv_sync_locks as None (means already synced)
        lock_key = f"ohlcv_sync_ABC_MIN"
        provider._ohlcv_sync_locks[lock_key] = None

        # Ensure external service would raise if called (it should not be called)
        MockExternalService.return_value.get_history_ohlcv.side_effect = AssertionError("Should not be called")

        expected = np.array([[9]])
        MockOHLCVStore.get_numpy.return_value = expected

        # Act
        result = provider.get_ohlcv(symbol="ABC", resolution="MIN", from_time=None, to_time=None)

        # Assert
        MockExternalService.return_value.get_history_ohlcv.assert_not_called()
        MockOHLCVStore.get_numpy.assert_called_once()
        np.testing.assert_array_equal(result, expected)


if __name__ == "__main__":
    unittest.main()
