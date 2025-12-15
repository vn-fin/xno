import logging
from dataclasses import dataclass
from datetime import datetime

from xno.data2.entity.base import BaseEntity

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
            logger.exception("Validation error in OrderBookDepth", exc_info=e)

    def validate(self) -> None:
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a str")

        if not isinstance(self.bp, list):
            raise TypeError("bp must be a list")
        for b in self.bp:
            if not isinstance(b, float):
                raise TypeError("bp must be a list of float")

        if not isinstance(self.bq, list):
            raise TypeError("bq must be a list")
        for b in self.bq:
            if not isinstance(b, int):
                raise TypeError("bq must be a list of int")

        if not isinstance(self.ap, list):
            raise TypeError("ap must be a list")
        for a in self.ap:
            if not isinstance(a, float):
                raise TypeError("ap must be a list of float")

        if not isinstance(self.aq, list):
            raise TypeError("aq must be a list")
        for a in self.aq:
            if not isinstance(a, int):
                raise TypeError("aq must be a list of int")

        if not isinstance(self.total_bid, int):
            raise TypeError("total_bid must be an int")
        if not isinstance(self.total_ask, int):
            raise TypeError("total_ask must be an int")

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

        return cls.from_json(raw)

    @classmethod
    def from_external_db(cls, raw) -> "OrderBookDepth":
        if hasattr(raw, "_mapping"):
            raw = raw._mapping

        return cls.from_json(raw)
