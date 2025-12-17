from enum import Enum


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"

    @classmethod
    def from_external(cls, raw: str):
        if raw == "B":
            return cls.BUY
        if raw == "S":
            return cls.SELL

        raise ValueError(f"Unknown side: {raw}")
