import logging
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.engine.row import Row

logger = logging.getLogger(__name__)


@dataclass
class StockPriceBoard:
    time: datetime
    symbol: str
    price: float
    vol: int
    total_vol: int
    change: float
    change_pct: float

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in StockPriceBoard: %s", self, exc_info=True)
            raise e

    def validate(self):
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a str")
        if not isinstance(self.price, float):
            raise TypeError("price must be a float")
        if not isinstance(self.vol, int):
            raise TypeError("vol must be an int")
        if not isinstance(self.total_vol, int):
            raise TypeError("total_vol must be an int")
        if not isinstance(self.change, float):
            raise TypeError("change must be a float")
        if not isinstance(self.change_pct, float):
            raise TypeError("change_pct must be a float")

    @classmethod
    def from_external_kafka(cls, data: dict) -> "StockPriceBoard":
        return cls(
            time=datetime.fromtimestamp(data["time"]),
            symbol=data["symbol"],
            price=float(data["price"]),
            vol=int(data["vol"]),
            total_vol=int(data["total_vol"]),
            change=float(data["change"]),
            change_pct=float(data["change_pct"]),
        )

    @classmethod
    def from_external_db(cls, row: Row) -> "StockPriceBoard":
        if isinstance(row, Row):
            row = row._asdict()

        return cls(
            time=row["time"],
            symbol=row["symbol"],
            price=float(row["price"]),
            vol=int(row["vol"]),
            total_vol=int(row["total_vol"]),
            change=float(row["change"]),
            change_pct=float(row["change_pct"]),
        )
