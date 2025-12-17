import logging
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.engine.row import Row

from xno.data2.technical.entity.base import BaseEntity

logger = logging.getLogger(__name__)


@dataclass
class OrderBookDepth(BaseEntity):
    time: datetime
    symbol: str
    bp: list[float]
    bq: list[int]
    ap: list[float]
    aq: list[int]
    total_bid: int
    total_ask: int

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in OrderBookDepth: %s", self, exc_info=True)
            raise e

    def validate(self) -> None:
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a str")

        if not isinstance(self.bp, list):
            raise TypeError("bp must be a list")
        for b in self.bp:
            if not isinstance(b, int | float):
                raise TypeError("bp must be a list of int or float")
        if not isinstance(self.bq, list):
            raise TypeError("bq must be a list")
        for b in self.bq:
            if not isinstance(b, int):
                raise TypeError("bq must be a list of int")
        if len(self.bp) != len(self.bq):
            raise ValueError("bp and bq must have the same length")

        if not isinstance(self.ap, list):
            raise TypeError("ap must be a list")
        for a in self.ap:
            if not isinstance(a, int | float):
                raise TypeError("ap must be a list of int or float")
        if not isinstance(self.aq, list):
            raise TypeError("aq must be a list")
        for a in self.aq:
            if not isinstance(a, int):
                raise TypeError("aq must be a list of int")
        if len(self.ap) != len(self.aq):
            raise ValueError("ap and aq must have the same length")

        if not isinstance(self.total_bid, int):
            raise TypeError("total_bid must be an int")
        if not isinstance(self.total_ask, int):
            raise TypeError("total_ask must be an int")
    
    def to_dict(self) -> dict:
        return {
            "time": self.time,
            "symbol": self.symbol,
            "bp": self.bp,
            "bq": self.bq,
            "ap": self.ap,
            "aq": self.aq,
            "total_bid": self.total_bid,
            "total_ask": self.total_ask,
        }

    @classmethod
    def from_external_kafka(cls, raw: dict) -> "OrderBookDepth":
        # {
        #     "time": 1765264521.586,
        #     "symbol": "MSB",
        #     "std_symbol": "MSB",
        #     "bp": [
        #         12.85,
        #         12.8,
        #         12.75,
        #     ],
        #     "bq": [
        #         4390,
        #         21790,
        #         7250,
        #     ],
        #     "ap": [
        #         12.9,
        #         12.95,
        #         13,
        #     ],
        #     "aq": [
        #         19150,
        #         22430,
        #         47430,
        #     ],
        #     "total_bid": 0,
        #     "total_ask": 0,
        #     "data_type": "TP",
        #     "source": "dnse"
        # }
        return cls(
            time=datetime.fromtimestamp(raw["time"]),
            symbol=raw["symbol"],
            bp=raw["bp"],
            bq=[int(q) for q in raw["bq"]],
            ap=raw["ap"],
            aq=[int(q) for q in raw["aq"]],
            total_bid=int(raw["total_bid"]),
            total_ask=int(raw["total_ask"]),
        )

    @classmethod
    def from_external_db(cls, raw: Row, depth: int = 10) -> "OrderBookDepth":
        if isinstance(raw, Row):
            raw = raw._asdict()

        if not "time" in raw:
            if not "time_resampled" in raw:
                raise KeyError("time or time_resampled must be in raw data")
            raw["time"] = raw["time_resampled"]

        if depth != 10:
            raw["bp"] = raw["bp"][:depth]
            raw["bq"] = raw["bq"][:depth]
            raw["ap"] = raw["ap"][:depth]
            raw["aq"] = raw["aq"][:depth]

        return cls(
            time=raw["time"],
            symbol=raw["symbol"],
            bp=raw["bp"],
            bq=[int(q) for q in raw["bq"]],
            ap=raw["ap"],
            aq=[int(q) for q in raw["aq"]],
            total_bid=int(raw["total_bid"]),
            total_ask=int(raw["total_ask"]),
        )
