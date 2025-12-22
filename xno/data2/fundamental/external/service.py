from typing import Literal
from datetime import datetime

from xno.data2.fundamental.external.tai_chinh_doanh_nghiep import WiGroupTaiChinhDoanhNghiepAPI
from xno.data2.fundamental.external.thong_tin_co_phieu import WiGroupThongTinCoPhieuAPI
from xno.data2.fundamental.external.trai_phieu import WiGroupDuLieuTraiPhieuAPI


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
