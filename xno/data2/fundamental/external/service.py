import logging
from typing import Literal
from datetime import datetime

from sqlalchemy import Row, text

from xno.connectors.sql import SqlSession
from xno.data2.fundamental.external.tai_chinh_doanh_nghiep import WiGroupTaiChinhDoanhNghiepAPI
from xno.data2.fundamental.external.thong_tin_co_phieu import WiGroupThongTinCoPhieuAPI
from xno.data2.fundamental.external.trai_phieu import WiGroupDuLieuTraiPhieuAPI

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
        self._thong_tin_co_phieu_api = WiGroupThongTinCoPhieuAPI.singleton(db_name=db_name)
        self._tai_chinh_doanh_nghiep_api = WiGroupTaiChinhDoanhNghiepAPI.singleton(db_name=db_name)
        self._du_lieu_trai_phieu_api = WiGroupDuLieuTraiPhieuAPI.singleton(db_name=db_name)

    # --- Thong tin co phieu --- #
    def get_thong_tin_co_ban(self, symbol: str) -> dict:
        """Get basic stock information by stock symbol."""
        return self._thong_tin_co_phieu_api.get_thong_tin_co_ban(mack=symbol)

    # --- Tai chinh doanh nghiep --- #
    def get_can_doi_ke_toan(self, symbol: str) -> dict:
        """Get balance sheet information by stock symbol."""
        return self._tai_chinh_doanh_nghiep_api.get_can_doi_ke_toan(mack=symbol)

    # --- Du lieu trai phieu --- #
    def get_loi_suat_trai_phieu_chinh_phu(
        self,
        page: int = None,
        limit: int = None,
        from_date: datetime = None,
        to_date: datetime = None,
        from_time: datetime = None,
        to_time: datetime = None,
        by_time: Literal["created_at", "updated_at", None] = None,
    ) -> dict:
        """Get financial information by stock code and year."""
        return self._du_lieu_trai_phieu_api.get_loi_suat_trai_phieu_chinh_phu(
            page=page,
            limit=limit,
            from_date=from_date.strftime("%Y-%m-%d") if from_date else None,
            from_time=from_time.strftime("%Y-%m-%d") if from_time else None,
            to_date=to_date.strftime("%Y-%m-%d") if to_date else None,
            to_time=to_time.strftime("%Y-%m-%d") if to_time else None,
            by_time=by_time,
        )

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

        with SqlSession("wi_replica") as session:
            sql = """
                SELECT
                    daily_trade.ngay              as date,
                    listed_info.mack              as ticker,
                    listed_info.san               as exchange,
                    'VND'                         as currency,
                    market_price.vol_tb_15ngay    as adv15,     --latest info
                    listed_info.soluongniemyet    as sharesout, -- latest info
                    daily_trade.vonhoa            as cap,
                    NULL                          as dividend,
                    daily_trade.tile_chiacotuc_cp as split,
                    case
                        when listed_info.vn30 then 'VN30'
                        when listed_info.vn100 then 'VN100'
                        when listed_info.vnall then 'VNALL'
                        when listed_info.vnmid then 'VNMID'
                        when listed_info.vnsi then 'VNSI'
                        when listed_info.vnsml then 'VNSML'
                        when listed_info.vnx50 then 'VNX50'
                        when listed_info.vnxall then 'VNXALL'
                        when listed_info.vndiamond then 'VNDIAMOND'
                        when listed_info.vnfinlead then 'VNFINLEAD'
                        when listed_info.vnfinselect then 'VNFINSELECT'
                        when listed_info.hnxindex then 'HNXINDEX'
                        when listed_info.upcomindex then 'UPCOMINDEX'
                        when listed_info.hnx30 then 'HNX30'
                        else NULL
                        end                       as market,
                    listed_info.nganhcap2_en_new  as industry,
                    listed_info.nganhcap1_en_new  as sector,
                    listed_info.nganhcap3_en_new  as subindustry
                FROM public.tblchisotaichinh_daily daily_trade
                        LEFT JOIN public.tblthongtinniemyet_news listed_info USING (mack)
                        LEFT JOIN public.tblchisotaichinh_giathitruong market_price USING (mack)
                WHERE daily_trade.mack = :symbol
                """

            if from_time is not None:
                sql += " AND daily_trade.ngay >= :from_time"
            if to_time is not None:
                sql += " AND daily_trade.ngay < :to_time"
            sql += " ORDER BY daily_trade.ngay ASC"

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
