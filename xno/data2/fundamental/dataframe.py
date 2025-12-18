import numpy as np
import numpy.typing as npt
import pandas as pd

from xno.data2.docs import document, DOCUMENT_GROUP_FUNDAMENTAL
from xno.data2.fundamental import FundamentalDataProvider
from xno.data2.fundamental.entity import Period


class FundamentalDataFrame:
    def __init__(self, symbol: str, from_time: str | None = None, to_time: str | None = None) -> None:
        self._symbol = symbol
        self._from_time = from_time
        self._to_time = to_time
        self._provider = FundamentalDataProvider()

    # --- Balance sheet
    def balance_sheet(self, period: Period, **kwargs) -> pd.DataFrame:
        return self._provider.get_balance_sheet(symbol=self._symbol, period=period, from_time=self._from_time,
                                                to_time=self._to_time, **kwargs)

    @document(
        group=DOCUMENT_GROUP_FUNDAMENTAL,
        name="goodwill_net",
        prototype="self.fun.goodwill_net(period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]",
        docs="Goodwill (net)",
    )
    def goodwill_net(self, period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]:
        """Goodwill (net)"""
        if isinstance(period, str):
            period = Period(period)

        df = self.balance_sheet(period=period)

        if not "goodwill" in df.columns:
            return np.array([], dtype=np.float64)
        return df["goodwill"].to_numpy(dtype=np.float64)

    # --- Ratio
    def ratio(self, period: Period, **kwargs) -> pd.DataFrame:
        return self._provider.get_ratio(symbol=self._symbol, period=period, from_time=self._from_time,
                                        to_time=self._to_time, **kwargs)

    @document(
        group=DOCUMENT_GROUP_FUNDAMENTAL,
        name="pe_ratio",
        prototype="self.fun.pe_ratio(period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]",
        docs="Price to Earnings (P/E) ratio",
    )
    def pe_ratio(self, period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]:
        """Price to Earnings (P/E) ratio"""
        if isinstance(period, str):
            period = Period(period)

        df = self.ratio(period=period)

        if not "p_e" in df.columns:
            return np.array([], dtype=np.float64)
        return df["p_e"].to_numpy(dtype=np.float64)

    @document(
        group=DOCUMENT_GROUP_FUNDAMENTAL,
        name="pb_ratio",
        prototype="self.fun.pb_ratio(period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]",
        docs="Price to Book (P/B) ratio",
    )
    def pb_ratio(self, period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]:
        """Price to Book (P/B) ratio"""
        if isinstance(period, str):
            period = Period(period)

        df = self.ratio(period=period)

        if not "p_b" in df.columns:
            return np.array([], dtype=np.float64)
        return df["p_b"].to_numpy(dtype=np.float64)

    @document(
        group=DOCUMENT_GROUP_FUNDAMENTAL,
        name="ps_ratio",
        prototype="self.fun.ps_ratio(period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]",
        docs="Price to Sales (P/S) ratio",
    )
    def ps_ratio(self, period: Period | str = Period.YEAR) -> npt.NDArray[np.float64]:
        """Price to Sales (P/S) ratio"""
        if isinstance(period, str):
            period = Period(period)

        df = self.ratio(period=period)

        if not "p_s" in df.columns:
            return np.array([], dtype=np.float64)
        return df["p_s"].to_numpy(dtype=np.float64)
