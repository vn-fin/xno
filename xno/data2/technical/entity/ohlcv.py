import logging
from dataclasses import dataclass
from datetime import datetime

import polars as pl
import pyarrow as pa

from xno.data2.technical.entity.base import BaseEntity
from xno.data2.technical.entity.resolution import Resolution

logger = logging.getLogger(__name__)

PA_SCHEMA = pa.schema(
    [
        # ("symbol", pa.string()),
        # ("resolution", pa.string()),
        ("time", pa.timestamp("ms")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
    ]
)

POLARS_SCHEMA = {
    # "symbol": pl.String,
    # "resolution": pl.String,
    "time": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}


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
            # symbol,
            # resolution,
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        ]

    def to_dict(self) -> dict:
        return {
            "time": self.time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }

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
    resolution: Resolution
    ohlcvs: list[OHLCV]

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in OHLCVs: %s", self, exc_info=True)
            raise e

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol cannot be empty")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a string")
        if not self.resolution:
            raise ValueError("resolution cannot be empty")
        if not isinstance(self.resolution, Resolution):
            raise TypeError("resolution must be a Resolution object")
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

    def to_pyarrow(self) -> pa.Table:
        batch = pa.Table.from_arrays(
            [
                # pa.array([self.symbol for _ in range(0, len(self.ohlcvs))], pa.string()),
                # pa.array([self.resolution for _ in range(0, len(self.ohlcvs))], pa.string()),
                pa.array([ohlcv.time for ohlcv in self.ohlcvs], pa.timestamp("ms")),
                pa.array([float(ohlcv.open) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.high) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.low) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.close) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.volume) for ohlcv in self.ohlcvs], pa.float64()),
            ],
            schema=PA_SCHEMA,
        )
        return batch

    def to_pyarrow_batch(self) -> pa.RecordBatch:
        batch = pa.RecordBatch.from_arrays(
            [
                # pa.array([self.symbol for _ in range(0, len(self.ohlcvs))], pa.string()),
                # pa.array([self.resolution for _ in range(0, len(self.ohlcvs))], pa.string()),
                pa.array([ohlcv.time for ohlcv in self.ohlcvs], pa.timestamp("ms")),
                pa.array([float(ohlcv.open) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.high) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.low) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.close) for ohlcv in self.ohlcvs], pa.float64()),
                pa.array([float(ohlcv.volume) for ohlcv in self.ohlcvs], pa.float64()),
            ],
            schema=PA_SCHEMA,
        )
        return batch

    def to_df(self):
        import pandas as pd

        data = {
            # "symbol": [self.symbol for _ in range(0, len(self.ohlcvs))],
            # "resolution": [self.resolution for _ in range(0, len(self.ohlcvs))],
            "time": [ohlcv.time for ohlcv in self.ohlcvs],
            "open": [float(ohlcv.open) for ohlcv in self.ohlcvs],
            "high": [float(ohlcv.high) for ohlcv in self.ohlcvs],
            "low": [float(ohlcv.low) for ohlcv in self.ohlcvs],
            "close": [float(ohlcv.close) for ohlcv in self.ohlcvs],
            "volume": [float(ohlcv.volume) for ohlcv in self.ohlcvs],
        }
        df = pd.DataFrame(data)
        return df

    def to_polars_df(self):
        data = {
            # "symbol": [self.symbol for _ in range(0, len(self.ohlcvs))],
            # "resolution": [self.resolution for _ in range(0, len(self.ohlcvs))],
            "time": [ohlcv.time for ohlcv in self.ohlcvs],
            "open": [float(ohlcv.open) for ohlcv in self.ohlcvs],
            "high": [float(ohlcv.high) for ohlcv in self.ohlcvs],
            "low": [float(ohlcv.low) for ohlcv in self.ohlcvs],
            "close": [float(ohlcv.close) for ohlcv in self.ohlcvs],
            "volume": [float(ohlcv.volume) for ohlcv in self.ohlcvs],
        }
        df = pl.DataFrame(data, schema=POLARS_SCHEMA)
        return df

    @classmethod
    def from_external_db(cls, symbol: str, resolution: Resolution, raws: list[list]) -> "OHLCVs":
        ohlcvs = []
        for raw in raws:
            ohlcv = OHLCV.from_external_db_list(raw)
            ohlcvs.append(ohlcv)

        return cls(symbol=symbol, resolution=resolution, ohlcvs=ohlcvs)
