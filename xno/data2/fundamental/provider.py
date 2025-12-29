from datetime import datetime

from xno.data2.fundamental.entity import Period, PriceVolume, IncomeStatement, BalanceSheet, CashFlow
from xno.data2.fundamental.store import DataStoreService
from xno.data2.fundamental.entity.data_quality import DataQualityCategory, DataQualityDataset
import threading
import logging

logger = logging.getLogger(__name__)


_LOCK_TASKS = {}


class LockTask:
    def __init__(self, name: str):
        self._name = name
        self._obj = _LOCK_TASKS
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        with self._lock:
            lock: threading.Event = self._obj.get(self._name, None)
            if lock is True:
                return False
            if lock is None:
                lock = threading.Event()
                self._obj[self._name] = lock
                logger.debug(f"Lock {self._name} acquired.")
                lock.clear()
                return True
            lock.wait()

    def release(self):
        with self._lock:
            lock: threading.Event = self._obj.get(self._name, None)
            if isinstance(lock, threading.Event):
                lock.set()
                self._obj[self._name] = True
            logger.debug(f"Lock {self._name} released.")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class FundamentalDataProvider:
    @classmethod
    def singleton(cls) -> "FundamentalDataProvider":
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._data_store = DataStoreService.singleton()

    def get_price_volume(
        self,
        symbol: str,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> list[PriceVolume]:
        raws = self._data_store.get_price_volume(symbol=symbol, from_time=from_time, to_time=to_time)
        pvs = [PriceVolume.from_db(raw) for raw in raws]
        return pvs

    def get_income_statement(
        self,
        symbol: str,
        period: Period,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        **kwargs,
    ) -> list[IncomeStatement]:
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        raws = self._data_store.get_income_statement(symbol=symbol, period=period, **kwargs)
        return [IncomeStatement.from_db(raw=raw) for raw in raws]

    def get_cash_flow(
        self,
        symbol: str,
        period: Period,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        **kwargs,
    ) -> list[CashFlow]:
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        raws = self._data_store.get_cash_flow(symbol=symbol, period=period, **kwargs)
        return [CashFlow.from_db(raw=raw) for raw in raws]

    def get_balance_sheet(
        self,
        symbol: str,
        period: Period,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        **kwargs,
    ) -> list[BalanceSheet]:
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        raws = self._data_store.get_balance_sheet(symbol=symbol, period=period, **kwargs)
        return [BalanceSheet.from_db(raw=raw) for raw in raws]

    # --- Data Quality Methods ---
    def get_data_quality_categories(self) -> list[DataQualityCategory]:
        rows = self._data_store.get_data_quality_categories()
        categories = [DataQualityCategory.from_db(row) for row in rows]
        return categories

    def get_data_quality_datasets(self, data_category: str) -> list[DataQualityDataset]:
        rows = self._data_store.get_data_quality_datasets(data_category=data_category)
        datasets = [DataQualityDataset.from_db(row) for row in rows]
        return datasets