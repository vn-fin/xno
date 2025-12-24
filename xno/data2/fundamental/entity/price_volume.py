from dataclasses import dataclass
from datetime import date

import logging
from typing import Self
from sqlalchemy import Row

logger = logging.getLogger(__name__)


@dataclass
class PriceVolume:
    date: date
    symbol: str
    exchange: str
    currency: str = "VND"
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    vwap: float | None = None
    volume: float | None = None
    adv20: float | None = None
    returns: float | None = None
    sharesout: float | None = None
    cap: float | None = None
    cash_dividend_payout_ratio: float | None = None
    stock_dividend_ratio: float | None = None
    market: str | None = None
    industry: str | None = None
    sector: str | None = None
    sub_industry: str | None = None

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in MarketInfo: %s", self, exc_info=True)
            raise e

    def validate(self) -> bool:
        if not self.symbol:
            raise ValueError("Stock symbol cannot be empty.")
        if not self.exchange:
            raise ValueError("Stock exchange cannot be empty.")
        if self.volume is not None and self.volume < 0:
            raise ValueError("Volume cannot be negative.")
        return True

    def to_dict(self, keys: list[str] = None) -> dict:
        if keys is None:
            keys = [
                "date",
                "symbol",
                "exchange",
                "currency",
                "open",
                "high",
                "low",
                "close",
                "vwap",
                "volume",
                "adv20",
                "returns",
                "sharesout",
                "cap",
                "cash_dividend_payout_ratio",
                "stock_dividend_ratio",
                "market",
                "industry",
                "sector",
                "sub_industry",
            ]
        return {key: getattr(self, key) for key in keys}

    @classmethod
    def from_db(cls, raw: dict) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(**raw)
