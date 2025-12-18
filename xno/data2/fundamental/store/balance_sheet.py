import pandas as pd
from datetime import datetime, timedelta

from xno.data2.fundamental.entity.period import Period
from xno.data2.fundamental.entity.balance_sheet import BalanceSheet


class BalanceSheetStore:
    @classmethod
    def singleton(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._store: dict[str, BalanceSheet] = dict()

    def add(self, symbol: str, period: Period, value: BalanceSheet):
        key = f"{symbol}_{period}"
        self._store[key] = value

    def get(self, symbol: str, period: Period) -> pd.DataFrame | None:
        key = f"{symbol}_{period}"
        return self._store.get(key, None)

    def has(self, symbol: str, period: Period) -> bool:
        key = f"{symbol}_{period}"

        if key in self._store:
            ratio = self._store[key]
            if ratio.at > datetime.now() - timedelta(days=1):
                return True

        return False

    def remove(self, name):
        if name in self._store:
            del self._store[name]
