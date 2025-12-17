import numpy as np
import pandas as pd
from datetime import datetime

from xno.data2.fundamental.entity import Period, Ratio, BalanceSheet, IncomeStatement, CashFlow
from xno.data2.fundamental.exernal import ExternalVnStockService
from xno.data2.fundamental.store import RatioStore, BalanceSheetStore, IncomeStatementStore, CashFlowStore


class FundamentalDataProvider:
    @classmethod
    def singleton(cls) -> "FundamentalDataProvider":
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def __init__(self, external_source="vnstock"):
        if external_source == "vnstock":
            self._external_data_service = ExternalVnStockService()
        else:
            raise NotImplementedError(f"External source '{external_source}' is not supported.")

        self._ratio_store = RatioStore.singleton()
        self._balance_sheet_store = BalanceSheetStore.singleton()
        self._income_statement_store = IncomeStatementStore.singleton()
        self._cash_flow_store = CashFlowStore.singleton()

    def get_ratio(self, symbol: str, period: str | Period, from_time: datetime | None = None,
                  to_time: datetime | None = None, **kwargs) -> pd.DataFrame:
        if isinstance(period, str):
            period = Period(period)
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        if not self._ratio_store.has(symbol=symbol, period=period):
            raw_df = self._external_data_service.get_ratio(symbol=symbol, period=period.to_vnstock(), **kwargs)
            ratio = Ratio.from_vnstock(symbol=symbol, period=period, dataframe=raw_df)
            self._ratio_store.add(symbol=symbol, period=period, value=ratio)
        else:
            ratio = self._ratio_store.get(symbol=symbol, period=period)

        df = ratio.dataframe

        # Filter by from_time and to_time
        if from_time is not None:
            df = df[df.index >= from_time]
        if to_time is not None:
            df = df[df.index < to_time]

        return df

    def get_balance_sheet(self, symbol: str, period: str | Period, from_time: datetime | None = None,
                          to_time: datetime | None = None, **kwargs) -> pd.DataFrame:
        if isinstance(period, str):
            period = Period(period)
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        if not self._balance_sheet_store.has(symbol=symbol, period=period):
            raw_df = self._external_data_service.get_balance_sheet(symbol=symbol, period=period.to_vnstock(), **kwargs)
            balance_sheet = BalanceSheet.from_vnstock(symbol=symbol, period=period, dataframe=raw_df)
            self._balance_sheet_store.add(symbol=symbol, period=period, value=balance_sheet)
        else:
            balance_sheet = self._balance_sheet_store.get(symbol=symbol, period=period)

        df = balance_sheet.dataframe

        # Filter by from_time and to_time
        if from_time is not None:
            df = df[df.index >= from_time]
        if to_time is not None:
            df = df[df.index < to_time]
        return df

    def get_income_statement(self, symbol: str, period: str | Period, from_time: datetime | None = None,
                             to_time: datetime | None = None, **kwargs) -> pd.DataFrame:
        if isinstance(period, str):
            period = Period(period)
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        if not self._income_statement_store.has(symbol=symbol, period=period):
            raw_df = self._external_data_service.get_income_statement(symbol=symbol, period=period.to_vnstock(),
                                                                      **kwargs)
            income_statement = IncomeStatement.from_vnstock(symbol=symbol, period=period, dataframe=raw_df)
            self._income_statement_store.add(symbol=symbol, period=period, value=income_statement)
        else:
            income_statement = self._income_statement_store.get(symbol=symbol, period=period)

        df = income_statement.dataframe

        # Filter by from_time and to_time
        if from_time is not None:
            df = df[df.index >= from_time]
        if to_time is not None:
            df = df[df.index < to_time]
        return df

    def get_cash_flow(self, symbol: str, period: str | Period, from_time: datetime | None = None,
                      to_time: datetime | None = None, **kwargs) -> pd.DataFrame:
        if isinstance(period, str):
            period = Period(period)
        if from_time is not None and to_time is not None and from_time >= to_time:
            raise ValueError("from_time must be earlier than to_time")

        if not self._cash_flow_store.has(symbol=symbol, period=period):
            raw_df = self._external_data_service.get_cash_flow(symbol=symbol, period=period.to_vnstock(), **kwargs)
            cash_flow = CashFlow.from_vnstock(symbol=symbol, period=period, dataframe=raw_df)
            self._cash_flow_store.add(symbol=symbol, period=period, value=cash_flow)
        else:
            cash_flow = self._cash_flow_store.get(symbol=symbol, period=period)

        df = cash_flow.dataframe

        # Filter by from_time and to_time
        if from_time is not None:
            df = df[df.index >= from_time]
        if to_time is not None:
            df = df[df.index < to_time]
        return df
