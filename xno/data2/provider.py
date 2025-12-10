import logging
import threading
from datetime import datetime
from openpyxl.chart.chartspace import ExternalData

from xno.connectors.semaphore import DistributedSemaphore
from xno.data2.external import ExternalDataService
import xno.data2.data.ohlcv as OHLCV_db
import xno.data2.data.order_book as OrderBook_db
from xno.utils.dc import timing

logger = logging.getLogger(__name__)


class DataProvider:
    def __init__(self, consumer_config: dict, ohlcv_db: str = "ohlcv_db"):
        self._external_data_service = ExternalDataService(consumer_config=consumer_config, db_name=ohlcv_db)
        self._ohlcv_sync_locks = {}

    def start(self):
        if __debug__:
            logger.debug("Starting data provider")
        self._external_data_service.start(on_consume_ohlcv=self._on_consume_ohlcv,
                                          on_consume_order_book=self._on_consume_order_book)

    def _on_consume_ohlcv(self, raw):
        """
        Received OHLCV data from external kafka
        """
        symbol, resolution, ohlcv = OHLCV_db.parse_from_external_kafka(raw)
        OHLCV_db.push(symbol, resolution, [ohlcv])

    def _on_consume_order_book(self, raw):
        """"
        Received Order Book data from external kafka
        """
        order_book = OrderBook_db.parse_from_external_kafka(raw)
        OrderBook_db.push(order_book)

    def stop(self):
        self._external_data_service.stop()

    @timing
    def get_ohlcv(self, symbol: str, resolution: str, from_time=None, to_time=None):
        """
        Get OHLCV data for a given symbol and resolution
        """
        if __debug__:
            logger.debug(f"Getting OHLCV for {symbol} {resolution} from {from_time} to {to_time}")

        self._sync_ohlcv_from_db(symbol=symbol, resolution=resolution, from_time=from_time, to_time=to_time)

        ohlcv_data = OHLCV_db.get(symbol=symbol, resolution=resolution, from_time=from_time, to_time=to_time)
        return ohlcv_data

    def _sync_ohlcv_from_db(self, symbol: str, resolution: str, from_time=None, to_time=None):
        """
        Sync OHLCV data from external DB to local DB
        1. Check if there is an ongoing sync for the same symbol and resolution
        2. If not, create a lock and start syncing
        3. If yes, wait for the ongoing sync to complete
        4. Once sync is complete, release the lock
        5. Return
        """
        lock_key = f"ohlcv_sync_{symbol}_{resolution}"
        if lock_key not in self._ohlcv_sync_locks:
            # First time sync, create a lock for this symbol and resolution
            lock = threading.Event()
            self._ohlcv_sync_locks[lock_key] = lock

            with DistributedSemaphore(lock_key=lock_key):
                logger.debug(f"Syncing OHLCV data for {symbol} at {resolution} from DB")
                raws = self._external_data_service.get_ohlcv(symbol=symbol, resolution=resolution)

            # Save to local DB
            OHLCV_db.sync_from_external_db(symbol=symbol, resolution=resolution, raws=raws)

            # Release the lock
            self._ohlcv_sync_locks[lock_key] = None
            lock.set()
            return

        if self._ohlcv_sync_locks[lock_key] is None:
            return

        # Wait for the ongoing sync to complete
        self._ohlcv_sync_locks[lock_key].wait(timeout=5)

    def get_order_book_depth(self, symbol: str, depth: int = 10):
        """
        Get Order Book depth for a given symbol
        """
        return OrderBook_db.get(symbol, depth)

    def get_history_order_book_depths(self, symbol: str, from_time: str | datetime | None = None,
                                      to_time: str | datetime | None = None) -> list:
        """
        Get Order Book depth history for a given symbol from external DB
        1. Fetch data from external DB
        2. Parse and return the data
        """
        if isinstance(from_time, str):
            from_time = datetime.fromisoformat(from_time)
        if isinstance(to_time, str):
            to_time = datetime.fromisoformat(to_time)

        lock_key = f"order_book_sync_{symbol}"
        with DistributedSemaphore(lock_key=lock_key):
            logger.debug(f"Syncing OrderBook history data for {symbol} from DB")
            raws = self._external_data_service.get_order_book_depths(symbol=symbol, from_time=from_time,
                                                                     to_time=to_time)

        return [OrderBook_db.parse_from_external_db(raw) for raw in raws]

    def get_trade_ticks(self, symbol: str, from_time=None, to_time=None):
        return f"Trades data fetched"

    def get_quote_ticks(self, symbol: str, from_time=None, to_time=None):
        # Placeholder method to fetch quote ticks data
        return f"Quote ticks data fetched"
