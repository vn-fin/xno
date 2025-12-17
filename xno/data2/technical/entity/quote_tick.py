import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class QuoteTick:
    time: datetime
    symbol: str
    buy_val: float
    sell_val: float
    buy_vol: float
    sell_vol: float
    total_room: int
    current_room: int

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in QuoteTick: %s", self, exc_info=True)
            raise e

    def validate(self) -> None:
        if not isinstance(self.time, datetime):
            raise TypeError("time must be a datetime")
        if not isinstance(self.symbol, str):
            raise TypeError("symbol must be a str")
        if not isinstance(self.buy_val, float):
            raise TypeError("buy_val must be a float")
        if not isinstance(self.sell_val, float):
            raise TypeError("sell_val must be a float")
        if not isinstance(self.buy_vol, float):
            raise TypeError("buy_vol must be a float")
        if not isinstance(self.sell_vol, float):
            raise TypeError("sell_vol must be a float")
        if not isinstance(self.total_room, int):
            raise TypeError("total_room must be an int")
        if not isinstance(self.current_room, int):
            raise TypeError("current_room must be an int")

    @classmethod
    def from_external_kafka(cls, raw: dict):
        # {
        #     "time":1765264214.565
        #     "symbol":"MCH"
        #     "total_room":53381037
        #     "current_room":36975719
        #     "buy_vol":1780
        #     "sell_vol":560
        #     "buy_val":3.82711
        #     "sell_val":1.204
        #     "data_type":"SF"
        #     "source":"dnse"
        # }
        return cls(
            time=datetime.fromtimestamp(raw["time"]),
            symbol=str(raw["symbol"]),
            buy_val=float(raw["buy_val"]),
            sell_val=float(raw["sell_val"]),
            buy_vol=float(raw["buy_vol"]),
            sell_vol=float(raw["sell_vol"]),
            total_room=int(raw["total_room"]),
            current_room=int(raw["current_room"]),
        )
