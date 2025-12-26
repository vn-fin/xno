from dataclasses import asdict, dataclass
from typing import Self
import logging
from sqlalchemy import Row

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IncomeStatement:

    symbol: str
    fiscal_quarter: int
    fiscal_year: int
    revenue: int | None = None
    revenue_deductions: int | None = None
    net_sales: int | None = None
    cost_of_revenue: int | None = None
    gross_profit: int | None = None
    financial_income: int | None = None
    financial_expense: int | None = None
    interest_expense: int | None = None
    equity_affiliate_income: int | None = None
    selling_expense: int | None = None
    general_admin_expense: int | None = None
    operating_income: int | None = None
    other_income: int | None = None
    other_expense: int | None = None
    other_profit: int | None = None
    pretax_income: int | None = None
    current_tax_expense: int | None = None
    deferred_tax_expense: int | None = None
    net_income_after_tax: int | None = None
    minority_interest: int | None = None
    net_income: int | None = None
    basic_eps: int | None = None
    diluted_eps: int | None = None
    audit_firm: str | None = None
    audit_opinion: str | None = None

    def __post_init__(self):
        try:
            self.validate()
        except Exception as e:
            logger.error("Validation error in MarketInfo: %s", self, exc_info=True)
            raise e

    def validate(self) -> bool:
        if not self.symbol:
            raise ValueError("Stock symbol cannot be empty.")
        if self.fiscal_quarter not in {0, 1, 2, 3, 4}:
            raise ValueError("Fiscal quarter must be one of {0, 1, 2, 3, 4}.")
        return True

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_db(cls, raw: Row) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(**raw)
