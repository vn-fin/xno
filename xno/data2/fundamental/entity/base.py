from typing import Self
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from xno.data2.fundamental.entity.period import Period


@dataclass
class StockEntity:
    symbol: str
    period: Period
    dataframe: pd.DataFrame
    at: datetime = None

    _DATAFRAME_MAPPING = {}

    @classmethod
    def from_vnstock(cls, symbol: str, period: Period, dataframe: pd.DataFrame) -> Self:
        dataframe.rename(columns=cls._DATAFRAME_MAPPING, inplace=True)
        return cls(
            symbol=symbol,
            period=period,
            dataframe=dataframe,
            at=datetime.now()
        )
