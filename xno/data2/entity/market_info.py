import logging
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.engine.row import Row

logger = logging.getLogger(__name__)


@dataclass
class MarketInfo:
    time: datetime
    symbol: str
    name: str
    prior: float
    value: float
    total_vol: int
    total_val: float
    advance: int
    decline: int
    nochange: int
    ceil: int
    floor: int
    change: float
    change_pct: float

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in MarketInfo: %s", self, exc_info=True)
            raise e

    def validate(self):
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a str")
        if not isinstance(self.name, str):
            raise TypeError("name must be a str")
        if not isinstance(self.prior, float):
            raise TypeError("prior must be a float")
        if not isinstance(self.value, float):
            raise TypeError("value must be a float")
        if not isinstance(self.total_vol, int):
            raise TypeError("total_vol must be an int")
        if not isinstance(self.total_val, float):
            raise TypeError("total_val must be a float")
        if not isinstance(self.advance, int):
            raise TypeError("advance must be an int")
        if not isinstance(self.decline, int):
            raise TypeError("decline must be an int")
        if not isinstance(self.nochange, int):
            raise TypeError("nochange must be an int")
        if not isinstance(self.ceil, int):
            raise TypeError("ceil must be an int")
        if not isinstance(self.floor, int):
            raise TypeError("floor must be an int")
        if not isinstance(self.change, float):
            raise TypeError("change must be a float")
        if not isinstance(self.change_pct, float):
            raise TypeError("change_pct must be a float")

    @classmethod
    def from_external_kafka(cls, raw):
        return cls(
            time=datetime.fromtimestamp(raw["time"]),
            symbol=str(raw["symbol"]),
            name=str(raw["name"]),
            prior=float(raw["prior"]),
            value=float(raw["value"]),
            total_vol=int(raw["total_vol"]),
            total_val=float(raw["total_val"]),
            advance=int(raw["advance"]),
            decline=int(raw["decline"]),
            nochange=int(raw["nochange"]),
            ceil=int(raw["ceil"]),
            floor=int(raw["floor"]),
            change=float(raw["change"]),
            change_pct=float(raw["change_pct"]),
        )

    @classmethod
    def from_external_db(cls, raw: Row) -> "MarketInfo":
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(
            time=raw["time"],
            symbol=raw["symbol"],
            name=raw["name"],
            prior=float(raw["prior"]),
            value=float(raw["value"]),
            total_vol=int(raw["total_vol"]),
            total_val=float(raw["total_val"]),
            advance=int(raw["advance"]),
            decline=int(raw["decline"]),
            nochange=int(raw["nochange"]),
            ceil=int(raw["ceil"]),
            floor=int(raw["floor"]),
            change=float(raw["change"]),
            change_pct=float(raw["change_pct"]),
        )

    def to_dict(self):
        return {
            "time": self.time,
            "symbol": self.symbol,
            "name": self.name,
            "prior": self.prior,
            "value": self.value,
            "total_vol": self.total_vol,
            "total_val": self.total_val,
            "advance": self.advance,
            "decline": self.decline,
            "nochange": self.nochange,
            "ceil": self.ceil,
            "floor": self.floor,
            "change": self.change,
            "change_pct": self.change_pct,
        }
