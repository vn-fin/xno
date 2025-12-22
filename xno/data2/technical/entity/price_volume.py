import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PriceVolume:
    ticker: str
    exchange: str
    currency: str
    open: float
    high: float
    low: float
    close: float
    vwap: float
    volume: float
    adv20: float
    returns: float
    sharesout: float
    cap: float
    dividend: float
    split: float
    market: float
    industry: float
    sector: float
    subindustry: float

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error: %s", self, exc_info=True)
            raise e

    def validate(self) -> None:
        if not isinstance(self.ticker, str):
            raise TypeError("ticker must be a str")
        if not isinstance(self.exchange, str):
            raise TypeError("exchange must be a str")
        if not isinstance(self.currency, str):
            raise TypeError("currency must be a str")
        if not isinstance(self.open, float):
            raise TypeError("open must be a float")
        if not isinstance(self.high, float):
            raise TypeError("high must be a float")
        if not isinstance(self.low, float):
            raise TypeError("low must be a float")
        if not isinstance(self.close, float):
            raise TypeError("close must be a float")
        if not isinstance(self.vwap, float):
            raise TypeError("vwap must be a float")
        if not isinstance(self.volume, float):
            raise TypeError("volume must be a float")
        if not isinstance(self.adv20, float):
            raise TypeError("adv20 must be a float")
        if not isinstance(self.returns, float):
            raise TypeError("returns must be a float")
        if not isinstance(self.sharesout, float):
            raise TypeError("sharesout must be a float")
        if not isinstance(self.cap, float):
            raise TypeError("cap must be a float")
        if not isinstance(self.dividend, float):
            raise TypeError("dividend must be a float")
        if not isinstance(self.split, float):
            raise TypeError("split must be a float")
        if not isinstance(self.market, float):
            raise TypeError("market must be a float")
        if not isinstance(self.industry, float):
            raise TypeError("industry must be a float")
        if not isinstance(self.sector, float):
            raise TypeError("sector must be a float")
        if not isinstance(self.subindustry, float):
            raise TypeError("subindustry must be a float")

    @classmethod
    def from_wigroup_api(cls, raw: dict):
        return cls(
            ticker=str(raw["ticker"]),
            exchange=str(raw["exchange"]),
            currency=str(raw["currency"]),
            open=float(raw["open"]),
            high=float(raw["high"]),
            low=float(raw["low"]),
            close=float(raw["close"]),
            vwap=float(raw["vwap"]),
            volume=float(raw["volume"]),
            adv20=float(raw["adv20"]),
            returns=float(raw["returns"]),
            sharesout=float(raw["sharesout"]),
            cap=float(raw["cap"]),
            dividend=float(raw["dividend"]),
            split=float(raw["split"]),
            market=float(raw["market"]),
            industry=float(raw["industry"]),
            sector=float(raw["sector"]),
            subindustry=float(raw["subindustry"]),
        )
