from dataclasses import dataclass
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

    def to_dict(self, keys: list[str] = None) -> dict:
        if keys is None:
            keys = [
                "symbol",
                "fiscal_quarter",
                "fiscal_year",
                "revenue",
                "revenue_deductions",
                "net_sales",
                "cost_of_revenue",
                "gross_profit",
                "financial_income",
                "financial_expense",
                "interest_expense",
                "equity_affiliate_income",
                "selling_expense",
                "general_admin_expense",
                "operating_income",
                "other_income",
                "other_expense",
                "other_profit",
                "pretax_income",
                "current_tax_expense",
                "deferred_tax_expense",
                "net_income_after_tax",
                "minority_interest",
                "net_income",
                "basic_eps",
                "diluted_eps",
                "audit_firm",
                "audit_opinion",
            ]
        return {key: getattr(self, key) for key in keys}

    @classmethod
    def from_db(cls, raw: Row) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(
            symbol=raw.get("symbol", ""),
            fiscal_quarter=raw.get("fiscal_quarter", 0),
            fiscal_year=raw.get("fiscal_year", 0),
            revenue=raw.get("revenue"),
            revenue_deductions=raw.get("revenue_deductions"),
            net_sales=raw.get("net_sales"),
            cost_of_revenue=raw.get("cost_of_revenue"),
            gross_profit=raw.get("gross_profit"),
            financial_income=raw.get("financial_income"),
            financial_expense=raw.get("financial_expense"),
            interest_expense=raw.get("interest_expense"),
            equity_affiliate_income=raw.get("equity_affiliate_income"),
            selling_expense=raw.get("selling_expense"),
            general_admin_expense=raw.get("general_admin_expense"),
            operating_income=raw.get("operating_income"),
            other_income=raw.get("other_income"),
            other_expense=raw.get("other_expense"),
            other_profit=raw.get("other_profit"),
            pretax_income=raw.get("pretax_income"),
            current_tax_expense=raw.get("current_tax_expense"),
            deferred_tax_expense=raw.get("deferred_tax_expense"),
            net_income_after_tax=raw.get("net_income_after_tax"),
            minority_interest=raw.get("minority_interest"),
            net_income=raw.get("net_income"),
            basic_eps=raw.get("basic_eps"),
            diluted_eps=raw.get("diluted_eps"),
            audit_firm=raw.get("audit_firm"),
            audit_opinion=raw.get("audit_opinion"),
        )
