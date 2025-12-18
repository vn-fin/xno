import unittest
import time


class TestConnection(unittest.TestCase):
    """Unit tests for All Connector initializations"""

    def test_semaphore_connect_default_env(self):
        """Test DistributedSemaphore initialization with default env"""
        from xno.connectors.semaphore import DistributedSemaphore

        with DistributedSemaphore(lock_key="test_lock"):
            print("Semaphore initialized successfully with default env.")

    def test_database_connection_default_env(self):
        """Test DistributedSemaphore initialization with database connection env"""
        from xno.data2.technical.provider import TechnicalDataProvider

        provider = TechnicalDataProvider.singleton()
        provider.start()

        time.sleep(3)  # Allow some time for connection establishment

        provider.stop()


if __name__ == "__main__":
    unittest.main()
