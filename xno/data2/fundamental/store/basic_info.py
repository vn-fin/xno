import pandas as pd
from datetime import datetime, timedelta

from xno.data2.fundamental.entity.period import Period
from xno.data2.fundamental.entity.basic_info import BasicInfo


class BasicInfoStore:
    @classmethod
    def singleton(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._store: dict[str, BasicInfo] = dict()

    def add(self, symbol: str, value: BasicInfo):
        self._store[symbol] = value

    def get(self, symbol: str) -> BasicInfo | None:
        return self._store.get(symbol, None)

    def has(self, symbol: str) -> bool:
        return symbol in self._store

    def remove(self, symbol: str):
        if symbol in self._store:
            del self._store[symbol]
