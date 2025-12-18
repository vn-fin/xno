from abc import  ABC, abstractmethod

import pandas as pd

class ExternalFundamentalDataService(ABC):
    @abstractmethod
    def get_ratio(self, symbol: str, period: str = "year", **kwargs) -> pd.DataFrame:
        """Fetch fundamental data for a given symbol."""
        pass

    @abstractmethod
    def get_balance_sheet(self, symbol: str, period: str = "year", **kwargs) -> pd.DataFrame:
        """Fetch balance sheet data for a given symbol."""
        pass

    @abstractmethod
    def get_income_statement(self, symbol: str, period: str = "year", **kwargs) -> pd.DataFrame:
        """Fetch income statement data for a given symbol."""
        pass

    @abstractmethod
    def get_cash_flow(self, symbol: str, period: str = "year", **kwargs) -> pd.DataFrame:
        """Fetch cash flow data for a given symbol."""
        pass