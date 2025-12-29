from dataclasses import dataclass, asdict
from sqlalchemy import Row

from typing import Self


@dataclass(frozen=True)
class DataQualityCategory:
    data_category: str
    datasets: int
    total_fields: int

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_db(cls, raw: dict | Row) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(**raw)


@dataclass(frozen=True)
class DataQualityDataset:
    dataset: str
    data_category: str
    total_fields: int
    symbols_with_data: int
    data_coverage_pct: float
    date_coverage_pct: float

    @classmethod
    def from_db(cls, raw: dict | Row) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(**raw)
