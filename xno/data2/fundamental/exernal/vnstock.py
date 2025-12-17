import pandas as pd
from vnstock import Vnstock
from .base import ExternalFundamentalDataService


class ExternalVnStockService(ExternalFundamentalDataService):
    def __init__(self):
        self._vnstock = Vnstock()
        self._stock_components_cache = {}

    def start(self):
        """Stop service"""

    def stop(self):
        """Stop service"""

    def _get_component(self, symbol: str, source: str):
        """Get stock components for a stock symbol from a specific source."""
        cache_key = f"{source}:{symbol}"
        if cache_key not in self._stock_components_cache:
            stock = self._vnstock.stock(symbol=symbol, source=source)
            self._stock_components_cache[cache_key] = stock
        return self._stock_components_cache[cache_key]

    def get_balance_sheet(self, symbol: str, period: str = "year", lang="en", dropna=True, source: str = "VCI") -> pd.DataFrame:
        """Get balance sheet data for a stock symbol from a specific source."""
        component = self._get_component(symbol=symbol, source=source)
        return component.finance.balance_sheet(period=period, lang=lang, dropna=dropna)

    def get_ratio(self, symbol: str, period: str = "year", lang="en", dropna=True, source: str = "VCI") -> pd.DataFrame:
        """Get financial ratio data for a stock symbol from a specific source."""
        component = self._get_component(symbol=symbol, source=source)
        return component.finance.ratio(period=period, lang=lang, dropna=dropna)

    def get_income_statement(self, symbol: str, period: str = "year", lang="en", dropna=True, source: str = "VCI") -> pd.DataFrame:
        """Get income statement data for a stock symbol from a specific source."""
        component = self._get_component(symbol=symbol, source=source)
        return component.finance.income_statement(period=period, lang=lang, dropna=dropna)

    def get_cash_flow(self, symbol: str, period: str = "year", lang="en", dropna=True, source: str = "VCI") -> pd.DataFrame:
        """Get cash flow data for a stock symbol from a specific source."""
        component = self._get_component(symbol=symbol, source=source)
        return component.finance.cash_flow(period=period, lang=lang, dropna=dropna)
