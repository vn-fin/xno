import logging

from sqlalchemy import text

from xno.connectors.sql import SqlSession
from xno.data2.fundamental.external.base import WiGroupBaseAPI

logger = logging.getLogger(__name__)


class WiGroupThongTinCoPhieuAPI(WiGroupBaseAPI):
    def get_thong_tin_co_ban(self, mack: str, fields: list[str] = None) -> dict:
        if __debug__:
            logger.debug("Fetching basic stock information for code: %s", mack)

        if fields is None:
            fields = [
                "mack",
                "ten",
                "name",
                "loai_hinh_cong_ty",
                "san_niem_yet",
                "gioithieu",
                "donvikiemtoan",
                "ghichu",
                "diachi",
                "website",
                "nganhcap1",
                "nganhcap2",
                "nganhcap3",
                "nganhcap4",
                "ngayniemyet",
                "smg",
                "volume_daily",
                "vol_tb_15ngay",
                "vonhoa",
                "dif",
                "dif_percent",
                "tong_tai_san",
                "soluongluuhanh",
                "soluongniemyet",
                "cophieuquy",
                "logo",
                "logo_resize",
                "created_at",
                "updated_at",
            ]

        with SqlSession(self._database_name) as session:
            sql = f"""
                  SELECT
                    {', '.join(fields)}
                  FROM wigroup_api.Thong_tin_co_phieu_Thong_tin_co_ban
                  WHERE
                    mack = :mack
                  """
            result = session.execute(
                text(sql),
                dict(mack=mack),
            )
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for code: %s", len(rows), mack)
        return rows
