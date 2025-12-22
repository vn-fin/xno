from typing import Literal

from xno.data2.fundamental.external.base import WiGroupBaseAPI


class WiGroupDuLieuTraiPhieuAPI(WiGroupBaseAPI):
    def get_loi_suat_trai_phieu_chinh_phu(
        self,
        page: int = None,
        limit: int = None,
        from_time: str = None,
        from_date: str = None,
        to_time: str = None,
        to_date: str = None,
        by_time: Literal["created_at", "updated_at", None] = None,
    ) -> dict:
        pass
