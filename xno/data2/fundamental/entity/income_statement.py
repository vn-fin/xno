from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True)
class IncomeStatement:

    ticker: str
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
    basic_eps: float | None = None
    diluted_eps: float | None = None
    audit_firm: str | None = None
    audit_opinion: str | None = None

    _WIGROUP_MAP = {
        "code": "ticker",
        "quy": "fiscal_quarter",
        "nam": "fiscal_year",
        "doanhthubanhangvacungcapdichvu": "revenue",
        "cackhoangiamtrudoanhthu": "revenue_deductions",
        "doanhthuthuanvebanhangvacungcapdichvu": "net_sales",
        "giavonhangban": "cost_of_revenue",
        "loinhuangopvebanhangvacungcapdichvu": "gross_profit",
        "doanhthuhoatdongtaichinh": "financial_income",
        "chiphitaichinh": "financial_expense",
        "trongdochiphilaivay": "interest_expense",
        "phanlailohoaclotrongcongtyliendoanhlienket": "equity_affiliate_income",
        "chiphibanhang": "selling_expense",
        "chiphiquanlydoanhnghiep": "general_admin_expense",
        "loinhuanthuantuhoatdongkinhdoanh": "operating_income",
        "thunhapkhac": "other_income",
        "chiphikhac": "other_expense",
        "loinhuankhac": "other_profit",
        "tongloinhuanketoantruocthue": "pretax_income",
        "chiphithuetndnhienhanh": "current_tax_expense",
        "chiphithuetndnhoanlai": "deferred_tax_expense",
        "loinhuansauthuethunhapdoanhnghiep": "net_income_after_tax",
        "loiichcuacodongthieuso_bctn": "minority_interest",
        "loinhuansauthuecuacongtyme": "net_income",
        "laicobantrencophieu": "basic_eps",
        "laisuygiamtrencophieu": "diluted_eps",
        "donvikiemtoan": "audit_firm",
        "ykienkiemtoan": "audit_opinion",
    }

    @classmethod
    def from_wigroup(cls, data: dict) -> Self:
        mapped_data = {}
        for key, value in data.items():
            if key in cls._WIGROUP_MAP:
                mapped_key = cls._WIGROUP_MAP[key]
                mapped_data[mapped_key] = value
        return cls(**mapped_data)
