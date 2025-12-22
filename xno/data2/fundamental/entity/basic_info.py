from dataclasses import dataclass

import logging
from typing import Self
from sqlalchemy.engine.row import Row

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BasicInfo:
    ticker: str
    name: str
    name_en: str | None = None
    exchange: str | None = None
    type: str | None = None
    description: str | None = None
    volume_daily: float | None = None
    total_assets: float | None = None
    address: str | None = None
    website: str | None = None
    logo: str | None = None
    logo_resize: str | None = None

    donvikiemtoan: str | None = None
    ghichu: str | None = None
    nganhcap1: str | None = None
    nganhcap2: str | None = None
    nganhcap3: str | None = None
    nganhcap4: str | None = None
    ngayniemyet: str | None = None
    smg: float | None = None
    vol_tb_15ngay: float | None = None
    vonhoa: float | None = None
    dif: float | None = None
    dif_percent: float | None = None
    soluongluuhanh: float | None = None
    soluongniemyet: float | None = None
    cophieuquy: float | None = None

    _WIGROUP_MAP = {
        "mack": "ticker",
        "ten": "name",
        "name": "name_en",
        "loai_hinh_cong_ty": "type",
        "san_niem_yet": "exchange",
        "gioithieu": "description",
        "volume_daily": "volume_daily",
        "tong_tai_san": "total_assets",
        "diachi": "address",
        "website": "website",
        "logo": "logo",
        "logo_resize": "logo_resize",
        "donvikiemtoan": "donvikiemtoan",
        "ghichu": "ghichu",
        "nganhcap1": "nganhcap1",
        "nganhcap2": "nganhcap2",
        "nganhcap3": "nganhcap3",
        "nganhcap4": "nganhcap4",
        "ngayniemyet": "ngayniemyet",
        "smg": "smg",
        "vol_tb_15ngay": "vol_tb_15ngay",
        "vonhoa": "vonhoa",
        "dif": "dif",
        "dif_percent": "dif_percent",
        "soluongluuhanh": "soluongluuhanh",
        "soluongniemyet": "soluongniemyet",
        "cophieuquy": "cophieuquy",
    }

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in MarketInfo: %s", self, exc_info=True)
            raise e

    def validate(self) -> bool:
        if not self.ticker:
            raise ValueError("Stock ticker cannot be empty.")
        if not self.name:
            raise ValueError("Stock name cannot be empty.")
        return True

    @classmethod
    def from_wigroup(cls, raw: dict) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        mapped_data = {}
        for key, value in raw.items():
            if key in cls._WIGROUP_MAP:
                mapped_key = cls._WIGROUP_MAP[key]
                mapped_data[mapped_key] = value
        return cls(**mapped_data)
