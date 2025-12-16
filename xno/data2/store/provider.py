import logging
import threading
from datetime import datetime

import xno.data2.store.ohlcv as OHLCV_store
import xno.data2.store.order_book_depth as OrderBookDepth_store
import xno.data2.store.trade_tick as TradeTick_store
from xno.connectors.semaphore import DistributedSemaphore
from xno.data2.entity import OHLCV, OHLCVs, OrderBookDepth, TradeTick
from xno.data2.external import ExternalDataService
from xno.utils.dc import timing

logger = logging.getLogger(__name__)


class DataProvider:
    def __init__(self, consumer_config: dict, external_db: str):
        self._external_data_service = ExternalDataService(consumer_config=consumer_config, db_name=external_db)
        self._ohlcv_sync_locks = {}

    def start(self):
        if __debug__:
            logger.debug("Starting data provider")

        OHLCV_store.init()

        self._external_data_service.start(
            on_consume_ohlcv=self._on_consume_ohlcv,
            on_consume_order_book=self._on_consume_order_book,
            on_consume_trade_tick=self._on_consume_trade_tick,
            on_consume_quote_tick=self._on_consume_quote_tick,
        )

    def _on_consume_ohlcv(self, raw: dict):
        """
        Received OHLCV data from external kafka
        """
        symbol, resolution, ohlcv = OHLCV.from_external_kafka(raw)
        OHLCV_store.push(symbol=symbol, resolution=resolution, ohlcv=ohlcv)

    def _on_consume_order_book(self, raw: dict):
        """ "
        Received Order Book data from external kafka
        """
        order_book_depth = OrderBookDepth.from_external_kafka(raw)
        OrderBookDepth_store.push(order_book_depth)

    def _on_consume_trade_tick(self, raw: dict):
        """ "
        Received Trade Tick data from external kafka
        """
        trade_tick = TradeTick.from_external_kafka(raw)
        TradeTick_store.push(trade_tick)

    def _on_consume_quote_tick(self, raw: dict):
        """ "
        Received Trade Tick data from external kafka
        """
        trade_tick = TradeTick.from_external_kafka(raw)
        TradeTick_store.push(trade_tick)

    def stop(self):
        self._external_data_service.stop()

    @timing
    def get_ohlcv(
        self,
        symbol: str,
        resolution: str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
    ):
        """
        Get OHLCV data for a given symbol and resolution
        """
        if __debug__:
            logger.debug(f"Getting OHLCV for {symbol} {resolution} from {from_time} to {to_time}")

        if isinstance(from_time, str):
            from_time = datetime.fromisoformat(from_time)
        if isinstance(to_time, str):
            to_time = datetime.fromisoformat(to_time)

        if from_time >= to_time:
            raise ValueError("from_time must be less than to_time")

        self._sync_ohlcv_from_db(symbol=symbol, resolution=resolution)

        ohlcv_data = OHLCV_store.get_numpy(symbol=symbol, resolution=resolution, from_time=from_time, to_time=to_time)
        return ohlcv_data

    def _sync_ohlcv_from_db(self, symbol: str, resolution: str):
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
                if __debug__:
                    logger.debug(f"Syncing OHLCV data for {symbol} at {resolution} from DB")
                raws = self._external_data_service.get_history_ohlcv(symbol=symbol, resolution=resolution)

            # Save to local DB
            ohlcvs = OHLCVs.from_external_db(symbol=symbol, resolution=resolution, raws=raws)
            OHLCV_store.pushes(ohlcvs)

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
        return OrderBookDepth_store.get(symbol, depth)

    @timing
    def get_history_order_book_depths(
        self,
        symbol: str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
    ) -> list:
        """
        Get Order Book depth history for a given symbol from external DB
        1. Fetch data from external DB
        2. Parse and return the data
        """
        if isinstance(from_time, str):
            from_time = datetime.fromisoformat(from_time)
        if isinstance(to_time, str):
            to_time = datetime.fromisoformat(to_time)

        if from_time >= to_time:
            raise ValueError("from_time must be less than to_time")

        with DistributedSemaphore(lock_key=f"order_book_sync_{symbol}"):
            if __debug__:
                logger.debug(f"Syncing OrderBook history data for {symbol} from DB")
            raws = self._external_data_service.get_history_order_book_depth(
                symbol=symbol, from_time=from_time, to_time=to_time
            )

        return [OrderBookDepth.from_external_db(raw) for raw in raws]

    def get_trade_tick(self, symbol: str):
        return TradeTick_store.get(symbol)

    def get_quote_ticks(self, symbol: str, from_time=None, to_time=None):
        # Placeholder method to fetch quote ticks data
        return f"Quote ticks data fetched"
