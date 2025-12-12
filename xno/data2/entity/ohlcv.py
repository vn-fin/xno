import logging
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa

from xno.data2.entity.base import BaseEntity

logger = logging.getLogger(__name__)

_PA_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("resolution", pa.string()),
        ("time", pa.timestamp("ms")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
    ]
)


@dataclass(frozen=True)
class OHLCV(BaseEntity):
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in OHLCV: %s", self, exc_info=True)
            raise e

    def validate(self) -> None:
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.open, (float, int)):
            raise TypeError("open must be a float")
        if not isinstance(self.high, (float, int)):
            raise TypeError("high must be a float")
        if not isinstance(self.low, (float, int)):
            raise TypeError("low must be a float")
        if not isinstance(self.close, (float, int)):
            raise TypeError("close must be a float")
        if not isinstance(self.volume, (float, int)):
            raise TypeError("volume must be a float")

    def to_duckdb_row(self, symbol: str, resolution: str):
        return [
            symbol,
            resolution,
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        ]

    @classmethod
    def from_external_kafka(cls, raw: object) -> "OHLCV":
        # {'time': 1765332000, 'symbol': 'C4G', 'resolution': 'DAY', 'open': 8.5, 'high': 8.8, 'low': 8.4, 'close': 8.6, 'volume': 3156600.0, 'updated': 1765352009, 'data_type': 'OH', 'source': 'dnse'}

        obj = cls(
            time=datetime.fromtimestamp(raw["time"]),
            open=raw["open"],
            high=raw["high"],
            low=raw["low"],
            close=raw["close"],
            volume=raw["volume"],
        )
        return raw["symbol"], raw["resolution"], obj

    @classmethod
    def from_external_db_list(cls, raw: list) -> "OHLCV":
        obj = cls(
            time=raw[0],
            open=float(raw[1]),
            high=float(raw[2]),
            low=float(raw[3]),
            close=float(raw[4]),
            volume=float(raw[5]),
        )
        return obj


@dataclass
class OHLCVs(BaseEntity):
    symbol: str
    resolution: str
    ohlcvs: list[OHLCV]

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in OHLCVs: %s", self, exc_info=True)

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol cannot be empty")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a string")
        if not self.resolution:
            raise ValueError("resolution cannot be empty")
        if not isinstance(self.resolution, str):
            raise TypeError("resolution must be a string")
        if not self.ohlcvs:
            raise ValueError("ohlcvs cannot be empty")
        if not isinstance(self.ohlcvs, list):
            raise TypeError("ohlcvs must be a list")

        for ohlcv in self.ohlcvs:
            if not isinstance(ohlcv, OHLCV):
                raise TypeError("ohlcvs must be a list of OHLCV")

    def to_duckdb_rows(self):
        rows = []
        for ohlcv in self.ohlcvs:
            rows.append(ohlcv.to_duckdb_row(self.symbol, self.resolution))
        return rows

    def __len__(self):
        return len(self.ohlcvs)

    def to_pyarrow_batch(self) -> pa.RecordBatch:
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([self.symbol for _ in range(0, len(self.ohlcvs))], pa.string()),
                pa.array([self.resolution for _ in range(0, len(self.ohlcvs))], pa.string()),
                pa.array([ohlcv.time for ohlcv in self.ohlcvs], pa.timestamp("ms")),
                pa.array([float(ohlcv.open) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.high) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.low) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.close) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.volume) for ohlcv in self.ohlcvs], pa.float64()),
            ],
            schema=_PA_SCHEMA,
        )
        return batch

    @classmethod
    def from_external_db(cls, symbol: str, resolution: str, raws: list[list]) -> "OHLCVs":
        ohlcvs = []
        for raw in raws:
            ohlcv = OHLCV.from_external_db_list(raw)
            ohlcvs.append(ohlcv)

        return cls(symbol=symbol, resolution=resolution, ohlcvs=ohlcvs)
