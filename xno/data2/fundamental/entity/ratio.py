from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from xno.data2.fundamental.entity.period import Period
from xno.data2.fundamental.entity.base import StockEntity


@dataclass
class Ratio(StockEntity):
    _DATAFRAME_MAPPING = {
            "yearReport": "year_report",
            "lengthReport": "length_report",
            "Fixed Asset-To-Equity": "fixed_asset_to_equity",
            "Owners' Equity/Charter Capital": "owners_equity_charter_capital",
            "Financial Leverage": "financial_leverage",
            "Market Capital (Bn. VND)": "market_capital",
            "Outstanding Share (Mil. Shares)": "outstanding_share_mil",
            "P/E": "p_e",
            "P/B": "p_b",
            "P/S": "p_s",
            "Net Profit Margin (%)": "net_profit_margin_pct",
            "ROE (%)": "roe_pct",
            "ROA (%)": "roa_pct",
            "Dividend yield (%)": "dividend_yield_pct",
            "P/Cash Flow": "p_cash_flow",
            "EPS (VND)": "eps",
            "BVPS (VND)": "bvps"
        }

    @classmethod
    def from_vnstock(cls, symbol: str, period: Period, dataframe: pd.DataFrame) -> "Ratio":
        dataframe.columns = dataframe.columns.droplevel(0)
        dataframe.rename(columns=cls._DATAFRAME_MAPPING, inplace=True)
        return cls(
            symbol=symbol,
            period=period,
            dataframe=dataframe,
            at=datetime.now()
        )
