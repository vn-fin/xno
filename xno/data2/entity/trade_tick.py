import logging
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.engine.row import Row

from .type import Side

logger = logging.getLogger(__name__)


@dataclass
class TradeTick:
    time: datetime
    symbol: str
    price: float
    vol: int
    side: str
    # total_volume: int

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in TradeTick: %s", self, exc_info=True)
            raise e

    def validate(self) -> None:
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a str")
        if not isinstance(self.price, (float, int)):
            raise TypeError("price must be a float")
        if not isinstance(self.vol, int):
            raise TypeError("vol must be an int")
        # if not isinstance(self.side, Side):
        #     raise TypeError("side must be a Side")
        if not isinstance(self.side, str):
            raise TypeError("side must be a str")
        # if not isinstance(self.total_volume, int):
        #     raise TypeError("total_volume must be an int")

    @classmethod
    def from_external_kafka(cls, raw: dict):
        # {
        #     "time":1765264214.565
        #     "symbol":"MCH"
        #     "price":214.8
        #     "vol":10
        #     "side":"S"
        #     "total_vol":9430
        #     "data_type":"ST"
        #     "source":"dnse"
        # }
        return cls(
            time=datetime.fromtimestamp(raw["time"]),
            symbol=str(raw["symbol"]),
            price=float(raw["price"]),
            vol=int(raw["vol"]),
            # side=Side.from_external(raw["side"]),
            side=raw["side"],
            # total_volume=int(raw["total_vol"]),
        )

    @classmethod
    def from_external_db(cls, raw: Row) -> "TradeTick":
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(
            time=raw["time"],
            symbol=raw["symbol"],
            price=float(raw["price"]),
            vol=int(raw["vol"]),
            side=raw["side"],
            # total_volume=raw["total_volume"],
        )
