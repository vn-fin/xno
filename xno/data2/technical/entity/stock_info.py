import logging
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.engine.row import Row

logger = logging.getLogger(__name__)


@dataclass
class StockInfo:
    time: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    avg: float
    ceil: float
    floor: float
    prior: float

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in StockInfo: %s", self, exc_info=True)
            raise e

    def validate(self):
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a str")
        if not isinstance(self.open, float):
            raise TypeError("open must be a float")
        if not isinstance(self.high, float):
            raise TypeError("high must be a float")
        if not isinstance(self.low, float):
            raise TypeError("low must be a float")
        if not isinstance(self.close, float):
            raise TypeError("close must be a float")
        if not isinstance(self.avg, float):
            raise TypeError("avg must be a float")
        if not isinstance(self.ceil, float):
            raise TypeError("ceil must be a float")
        if not isinstance(self.floor, float):
            raise TypeError("floor must be a float")
        if not isinstance(self.prior, float):
            raise TypeError("prior must be a float")

    @classmethod
    def from_external_kafka(cls, data: dict) -> "StockInfo":
        return cls(
            time=datetime.fromtimestamp(data["time"]),
            symbol=data["symbol"],
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=float(data["close"]),
            avg=float(data["avg"]),
            ceil=float(data["ceil"]),
            floor=float(data["floor"]),
            prior=float(data["prior"]),
        )

    @classmethod
    def from_external_db(cls, raw: Row) -> "StockInfo":
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(
            time=raw["time"],
            symbol=raw["symbol"],
            open=float(raw["open"]),
            high=float(raw["high"]),
            low=float(raw["low"]),
            close=float(raw["close"]),
            avg=float(raw["avg"]),
            ceil=float(raw["ceil"]),
            floor=float(raw["floor"]),
            prior=float(raw["prior"]),
        )
