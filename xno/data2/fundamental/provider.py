import pandas as pd
from datetime import datetime

from xno.data2.fundamental.entity import Period, PriceVolume, IncomeStatement, BalanceSheet
from xno.data2.fundamental.external import WiGroupExternalDataService
from xno.data2.fundamental.store import (
    BalanceSheetStore,
)
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
        self._external_data_service = WiGroupExternalDataService.singleton()
        self._balance_sheet_store = BalanceSheetStore.singleton()

    def get_price_volume(
        self,
        symbol: str,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> list[PriceVolume]:
        raws = self._external_data_service.get_price_volume(symbol=symbol, from_time=from_time, to_time=to_time)
        pvs = [PriceVolume.from_db(raw) for raw in raws]
        return pvs

    # def get_balance_sheet(
    #     self,
    #     symbol: str,
    #     period: str | Period,
    #     from_time: datetime | None = None,
    #     to_time: datetime | None = None,
    #     **kwargs,
    # ) -> pd.DataFrame:
    #     if isinstance(period, str):
    #         period = Period(period)
    #     if from_time is not None and to_time is not None and from_time >= to_time:
    #         raise ValueError("from_time must be earlier than to_time")

    #     if not self._balance_sheet_store.has(symbol=symbol, period=period):
    #         raw_df = self._external_data_service.get_balance_sheet(symbol=symbol, period=period.to_wigroup(), **kwargs)
    #         balance_sheet = BalanceSheet.from_wigroup(symbol=symbol, period=period, dataframe=raw_df)
    #         self._balance_sheet_store.add(symbol=symbol, period=period, value=balance_sheet)
    #     else:
    #         balance_sheet = self._balance_sheet_store.get(symbol=symbol, period=period)

    #     df = balance_sheet.dataframe

    #     # Filter by from_time and to_time
    #     if from_time is not None:
    #         df = df[df.index >= from_time]
    #     if to_time is not None:
    #         df = df[df.index < to_time]
    #     return df

    def get_income_statement(
        self,
        symbol: str,
        period: str | Period,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        **kwargs,
    ) -> list[IncomeStatement]:
        if isinstance(period, str):
            period = Period(period)
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        raws = self._external_data_service.get_income_statement(symbol=symbol, period=period, **kwargs)
        income_statements = [IncomeStatement.from_db(raw=raw) for raw in raws]

        return income_statements

    # def get_cash_flow(self, symbol: str, period: str | Period, from_time: datetime | None = None,
    #                   to_time: datetime | None = None, **kwargs) -> pd.DataFrame:
    #     if isinstance(period, str):
    #         period = Period(period)
    #     if from_time is not None and to_time is not None and from_time >= to_time:
    #         raise ValueError("from_time must be earlier than to_time")

    #     if not self._cash_flow_store.has(symbol=symbol, period=period):
    #         raw_df = self._external_data_service.get_cash_flow(symbol=symbol, period=period.to_vnstock(), **kwargs)
    #         cash_flow = CashFlow.from_vnstock(symbol=symbol, period=period, dataframe=raw_df)
    #         self._cash_flow_store.add(symbol=symbol, period=period, value=cash_flow)
    #     else:
    #         cash_flow = self._cash_flow_store.get(symbol=symbol, period=period)

    #     df = cash_flow.dataframe

    #     # Filter by from_time and to_time
    #     if from_time is not None:
    #         df = df[df.index >= from_time]
    #     if to_time is not None:
    #         df = df[df.index < to_time]
    #     return df

    # def get_ratio(self, symbol: str, period: str | Period, from_time: datetime | None = None,
    #               to_time: datetime | None = None, **kwargs) -> pd.DataFrame:
    #     if isinstance(period, str):
    #         period = Period(period)
    #     if from_time is not None and to_time is not None and from_time >= to_time:
    #         raise ValueError("from_time must be earlier than to_time")

    #     if not self._ratio_store.has(symbol=symbol, period=period):
    #         raw_df = self._external_data_service.get_ratio(symbol=symbol, period=period.to_vnstock(), **kwargs)
    #         ratio = Ratio.from_vnstock(symbol=symbol, period=period, dataframe=raw_df)
    #         self._ratio_store.add(symbol=symbol, period=period, value=ratio)
    #     else:
    #         ratio = self._ratio_store.get(symbol=symbol, period=period)

    #     df = ratio.dataframe

    #     # Filter by from_time and to_time
    #     if from_time is not None:
    #         df = df[df.index >= from_time]
    #     if to_time is not None:
    #         df = df[df.index < to_time]

    #     return df
