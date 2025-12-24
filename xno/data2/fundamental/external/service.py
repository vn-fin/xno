import logging
from typing import Literal
from datetime import datetime

from sqlalchemy import Row, text

from xno.connectors.sql import SqlSession

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

    # --- Price Volume --- #
    def get_basic_info(self, symbol: str) -> list[Row]:
        """Get basic stock information by stock symbol."""
        if __debug__:
            logger.debug("Fetching basic info for code: %s", symbol)

        with SqlSession("wi_replica") as session:
            sql = """
                SELECT
                    t.mack              as ticker,
                    t.ten_vi            as name,
                    t.san               as exchange,
                    t.soluongniemyet    as sharesout,
                    t.nganhcap1_en_new  as sector,
                    t.nganhcap2_en_new  as industry,
                    t.nganhcap3_en_new  as subindustry
                FROM public.tblthongtinniemyet_news t
                WHERE t.mack = :symbol
                """

            result = session.execute(
                text(sql),
                dict(symbol=symbol),
            )
            rows = result.fetchone()

            if __debug__:
                logger.debug("Retrieved %d rows for basic info code: %s", len(rows), symbol)
        return rows

    def get_price_volume(self, symbol: str, from_time: datetime | None = None, to_time: datetime | None = None) -> dict:
        """Get price volume information by stock symbol."""
        if __debug__:
            logger.debug("Fetching price volume info for code: %s", symbol)

        with SqlSession(self._db_name) as session:
            sql = """
                SELECT * FROM reference.v_trading_universe
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
                logger.debug("Retrieved %d rows for price volume code: %s", len(rows), symbol)
        return rows

    # --- Balance Sheet --- #
    def get_balance_sheet(self, symbol: str, period: str, from_time: datetime = None, to_time: datetime = None) -> dict:
        """Get balance sheet information by stock symbol and period."""
        if __debug__:
            logger.debug("Fetching balance sheet info for code: %s", symbol)

        with SqlSession(self._db_name) as session:
            sql = """
                
                """
            result = session.execute(
                text(sql),
                dict(mack=symbol),
            )
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for code: %s", len(rows), symbol)
        return rows
