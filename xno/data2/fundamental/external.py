import logging
from datetime import datetime
from typing import Literal

from sqlalchemy import Row, text

from xno.connectors.sql import SqlSession
from xno.data2.fundamental.entity.period import Period

logger = logging.getLogger(__name__)


class WiGroupExternalDataService:
    @classmethod
    def singleton(cls, **kwargs) -> "WiGroupExternalDataService":
        if not hasattr(cls, "_instance"):
            if not kwargs:
                kwargs = {
                    "db_name": "xno_data",
                }
            cls._instance = cls(**kwargs)
        return cls._instance

    def __init__(self, db_name: str = "xno_data"):
        self._db_name = db_name

    def get_price_volume(self, symbol: str, from_time: datetime | None = None, to_time: datetime | None = None) -> dict:
        """Get price volume information by stock symbol."""
        if __debug__:
            logger.debug("Fetching price volume info for symbol: %s", symbol)

        with SqlSession(self._db_name) as session:
            sql = """
                SELECT
                    date,
                    symbol,
                    exchange,
                    currency,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    adv20,
                    returns,
                    sharesout,
                    cap,
                    cash_dividend_payout_ratio,
                    stock_dividend_ratio,
                    market,
                    industry,
                    sector,
                    sub_industry
                FROM reference.v_trading_universe
                WHERE symbol = :symbol
                """

            if from_time is not None:
                sql += " AND date >= :from_time"
            if to_time is not None:
                sql += " AND date < :to_time"
            sql += " ORDER BY symbol, date ASC"

            result = session.execute(
                text(sql),
                dict(symbol=symbol, from_time=from_time, to_time=to_time),
            )
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for price volume symbol: %s", len(rows), symbol)
        return rows

    def get_income_statement(
        self, symbol: str, period: str, from_time: datetime = None, to_time: datetime = None
    ) -> dict:
        """Get income statement information by stock symbol and period."""
        if __debug__:
            logger.debug("Fetching income statement info for symbol: %s", symbol)

        sql = """
            SELECT
                symbol,
                fiscal_year,
                fiscal_quarter,
                revenue,
                revenue_deductions,
                net_sales,
                cost_of_revenue,
                gross_profit,
                financial_income,
                financial_expense,
                interest_expense,
                equity_affiliate_income,
                selling_expense,
                general_admin_expense,
                operating_income,
                other_income,
                other_expense,
                other_profit,
                pretax_income,
                current_tax_expense,
                deferred_tax_expense,
                net_income_after_tax,
                minority_interest,
                net_income,
                basic_eps,
                diluted_eps,
                audit_firm,
                audit_opinion
            FROM reference.v_income_statement
            WHERE symbol = :symbol
            """

        params = dict(symbol=symbol)

        if period == Period.ANNUALLY:
            sql += " AND fiscal_quarter = 0"

            if from_time is not None:
                sql += " AND fiscal_year >= :from_time"
                params["from_time"] = from_time.year
            if to_time is not None:
                sql += " AND fiscal_year <= :to_time"
                params["to_time"] = to_time.year

        elif period == Period.QUARTERLY:
            if from_time is not None:
                quarter = (from_time.month - 1) // 3 + 1
                sql += " AND fiscal_year >= :from_time and fiscal_quarter >= :quarter"

                params["from_time"] = from_time.year
                params["quarter"] = quarter

            if to_time is not None:
                quarter = (to_time.month - 1) // 3 + 1
                sql += " AND fiscal_year <= :to_time and fiscal_quarter <= :quarter"

                params["to_time"] = to_time.year
                params["quarter"] = quarter

        sql += " ORDER BY symbol, fiscal_year ASC, fiscal_quarter ASC"

        with SqlSession(self._db_name) as session:
            result = session.execute(text(sql), params)
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for symbol: %s", len(rows), symbol)
        return rows

    def get_balance_sheet(self, symbol: str, period: str, from_time: datetime = None, to_time: datetime = None) -> dict:
        """Get balance sheet information by stock symbol and period."""
        if __debug__:
            logger.debug("Fetching balance sheet info for symbol: %s", symbol)

        with SqlSession(self._db_name) as session:
            sql = """
                
                """
            result = session.execute(
                text(sql),
                dict(mack=symbol),
            )
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for symbol: %s", len(rows), symbol)
        return rows
