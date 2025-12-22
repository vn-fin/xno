import logging
from math import inf
import threading
from datetime import datetime

import numpy as np
import pandas as pd

import xno.data2.technical.store.market_info as MarketInfo_store
import xno.data2.technical.store.ohlcv as OHLCV_store
import xno.data2.technical.store.order_book_depth as OrderBookDepth_store
import xno.data2.technical.store.quote_tick as QuoteTick_store
import xno.data2.technical.store.stock_info as StockInfo_store
import xno.data2.technical.store.stock_price_board as StockPriceBoard_store
import xno.data2.technical.store.trade_tick as TradeTick_store
from xno.data2.technical.entity import (
    OHLCV,
    MarketInfo,
    OHLCVs,
    OrderBookDepth,
    QuoteTick,
    Resolution,
    StockInfo,
    StockPriceBoard,
    TradeTick,
)
from xno.data2.technical.external import ExternalDataService
from xno.utils.dc import timing

logger = logging.getLogger(__name__)


class TechnicalDataProvider:
    @classmethod
    def singleton(cls) -> "TechnicalDataProvider":
        if not hasattr(cls, "_instance"):
            from xno import settings

            cls._instance = cls(
                external_db="xno_data",
                consumer_config=dict(
                    topic=settings.kafka_market_data_topic,
                    **{
                        "bootstrap.servers": settings.kafka_bootstrap_servers,
                        "enable.auto.commit": False,
                        "group.id": "xno-data-consumer-group",
                        "auto.offset.reset": "latest",
                    },
                ),
            )
        return cls._instance

    def __init__(self, consumer_config: dict, external_db: str):
        self._external_data_service = ExternalDataService(consumer_config=consumer_config, db_name=external_db)
        self._ohlcv_sync_locks = {}

    def start(self):
        if __debug__:
            logger.debug("Starting data provider")

        OHLCV_store.start()

        self._external_data_service.start(
            on_consume_ohlcv=self._on_consume_ohlcv,
            on_consume_order_book=self._on_consume_order_book,
            on_consume_trade_tick=self._on_consume_trade_tick,
            on_consume_quote_tick=self._on_consume_quote_tick,
            on_consume_market_info=self._on_consume_market_info,
            on_consume_stock_info=self._on_consume_stock_info,
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
        Received Quote Tick data from external kafka
        """
        quote_tick = QuoteTick.from_external_kafka(raw)
        QuoteTick_store.push(quote_tick)

    def _on_consume_market_info(self, raw: dict):
        market_info = MarketInfo.from_external_kafka(raw)
        MarketInfo_store.push(market_info)

    def _on_consume_stock_info(self, raw: dict):
        stock_info = StockInfo.from_external_kafka(raw)
        StockInfo_store.push(stock_info)

    def _on_consume_stock_price_board(self, raw: dict):
        stock_price_board = StockPriceBoard.from_external_kafka(raw)
        StockPriceBoard_store.push(stock_price_board)

    def stop(self):
        OHLCV_store.stop()
        self._external_data_service.stop()

    # --- PRICE VOLUME ---
    def get_price_volume(
        self,
        symbol: str,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get Price Volume data for a given symbol
        """

        from xno.data2.fundamental.provider import FundamentalDataProvider

        fundamental_provider = FundamentalDataProvider.singleton()

        info = fundamental_provider.get_basic_info(symbol=symbol)
        if not info:
            raise ValueError(f"Cannot find basic info for symbol {symbol}")

        df = self.get_ohlcv_dataframe(
            symbol=symbol,
            resolution=Resolution.from_string("1D"),
            from_time=from_time,
            to_time=to_time,
        )
        df["ticker"] = info.ticker
        df["exchange"] = info.exchange
        df["currency"] = getattr(info, "currency", "VND")

        return df

    # --- OHLCV ---
    @timing
    def get_ohlcv(
        self,
        symbol: str,
        resolution: Resolution | str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
        factor: int = 1,
        **kwargs,
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

        if isinstance(resolution, str):
            resolution = Resolution.from_string(resolution)

        self._sync_ohlcv_from_db(symbol=symbol, resolution=resolution)

        ohlcv_data = OHLCV_store.get_numpy(symbol=symbol, resolution=resolution, from_time=from_time, to_time=to_time)

        if factor != 1:
            ohlcv_data["open"] = np.multiply(ohlcv_data["open"], factor)
            ohlcv_data["high"] = np.multiply(ohlcv_data["high"], factor)
            ohlcv_data["low"] = np.multiply(ohlcv_data["low"], factor)
            ohlcv_data["close"] = np.multiply(ohlcv_data["close"], factor)

        return ohlcv_data

    def get_ohlcv_dataframe(
        self,
        symbol: str,
        resolution: Resolution | str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
        factor: int = 1,
        column_as_upper: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        ohlcvs_np = self.get_ohlcv(
            symbol=symbol,
            resolution=resolution,
            from_time=from_time,
            to_time=to_time,
            factor=factor,
            **kwargs,
        )

        index = ohlcvs_np["time"]
        del ohlcvs_np["time"]

        df = pd.DataFrame(ohlcvs_np, index=index)
        df.index.set_names("time", inplace=True)

        df.columns = ["open", "high", "low", "close", "volume"]

        if column_as_upper:
            df = df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})

        return df

    def _sync_ohlcv_from_db(self, symbol: str, resolution: Resolution):
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

            from xno.connectors.semaphore import DistributedSemaphore

            with DistributedSemaphore(lock_key=lock_key):
                if __debug__:
                    logger.debug(f"Syncing OHLCV history data for {symbol} at {resolution} from DB")
                raws = self._external_data_service.get_history_ohlcv(symbol=symbol, resolution=resolution.to_external())

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
        self._ohlcv_sync_locks[lock_key].wait(timeout=20)

    def get_order_book_depth(self, symbol: str, depth: int = 10) -> OrderBookDepth | None:
        """
        Get Order Book depth for a given symbol
        """
        return OrderBookDepth_store.get(symbol, depth)

    get_stock_top_price = get_order_book_depth

    def get_history_order_book_depth(
        self,
        symbol: str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
        limit: int | None = None,
        resolution: str | Resolution | None = None,
        depth: int = 10,
    ) -> list[OrderBookDepth]:
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
        if limit and isinstance(limit, int) and limit <= 0:
            raise ValueError("limit must be a positive integer")
        if isinstance(resolution, str):
            resolution = Resolution.from_string(resolution)

        from xno.connectors.semaphore import DistributedSemaphore

        with DistributedSemaphore(lock_key=f"order_book_sync_{symbol}"):
            if __debug__:
                logger.debug(f"Syncing OrderBook history data for {symbol} from DB")
            if resolution is not None:
                resolution = resolution.to_external_postgre()
            raws = self._external_data_service.get_history_order_book_depth(
                symbol=symbol,
                from_time=from_time,
                to_time=to_time,
                limit=limit,
                resolution=resolution,
            )

        return [OrderBookDepth.from_external_db(raw, depth=depth) for raw in raws]

    get_history_stock_top_price = get_history_order_book_depth

    def get_trade_tick(self, symbol: str) -> TradeTick:
        """
        Get Trade Tick for a given symbol
        """
        return TradeTick_store.get(symbol)

    get_stock_tick = get_trade_tick

    def get_history_trade_tick(
        self,
        symbol: str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
        limit: int | None = None,
    ) -> list[TradeTick]:
        """
        Get Trade Tick history for a given symbol from external DB
        1. Fetch data from external DB
        2. Parse and return the data
        """
        if isinstance(from_time, str):
            from_time = datetime.fromisoformat(from_time)
        if isinstance(to_time, str):
            to_time = datetime.fromisoformat(to_time)
        if from_time >= to_time:
            raise ValueError("from_time must be less than to_time")
        if limit and isinstance(limit, int) and limit <= 0:
            raise ValueError("limit must be a positive integer")

        raws = self._external_data_service.get_history_trade_tick(
            symbol=symbol,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )
        return [TradeTick.from_external_db(raw) for raw in raws]

    get_history_stock_tick = get_history_trade_tick

    def get_quote_tick(self, symbol: str):
        """
        Get Quote Tick for a given symbol
        """
        return QuoteTick_store.get(symbol)

    def get_market_info(self, symbol: str) -> MarketInfo:
        return MarketInfo_store.get(symbol)

    def get_history_market_info(
        self,
        symbol: str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
        limit: int | None = None,
    ) -> list[MarketInfo]:
        if isinstance(from_time, str):
            from_time = datetime.fromisoformat(from_time)
        if isinstance(to_time, str):
            to_time = datetime.fromisoformat(to_time)
        if from_time >= to_time:
            raise ValueError("from_time must be less than to_time")
        if limit and isinstance(limit, int) and limit <= 0:
            raise ValueError("limit must be a positive integer")

        raws = self._external_data_service.get_history_market_info(
            symbol=symbol,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )
        return [MarketInfo.from_external_db(raw) for raw in raws]

    def get_stock_info(self, symbol: str):
        """
        Get Stock Info for a given symbol
        """
        return StockInfo_store.get(symbol)

    def get_history_stock_info(
        self,
        symbol: str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
        limit: int | None = None,
    ) -> list[StockInfo]:
        if isinstance(from_time, str):
            from_time = datetime.fromisoformat(from_time)
        if isinstance(to_time, str):
            to_time = datetime.fromisoformat(to_time)
        if from_time >= to_time:
            raise ValueError("from_time must be less than to_time")
        if limit and isinstance(limit, int) and limit <= 0:
            raise ValueError("limit must be a positive integer")

        raws = self._external_data_service.get_history_stock_info(
            symbol=symbol,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )
        return [StockInfo.from_external_db(raw) for raw in raws]

    def get_stock_price_board(self, symbol: str):
        """
        Get Stock Price Board for a given symbol
        """
        return StockPriceBoard_store.get(symbol)

    def get_history_stock_price_board(
        self,
        symbol: str,
        from_time: str | datetime | None = None,
        to_time: str | datetime | None = None,
        limit: int | None = None,
    ) -> list[StockPriceBoard]:
        if isinstance(from_time, str):
            from_time = datetime.fromisoformat(from_time)
        if isinstance(to_time, str):
            to_time = datetime.fromisoformat(to_time)
        if from_time >= to_time:
            raise ValueError("from_time must be less than to_time")
        if limit and isinstance(limit, int) and limit <= 0:
            raise ValueError("limit must be a positive integer")

        raws = self._external_data_service.get_history_stock_price_board(
            symbol=symbol,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )
        return [StockPriceBoard.from_external_db(raw) for raw in raws]
