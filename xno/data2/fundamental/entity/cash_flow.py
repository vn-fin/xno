from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True)
class CashFlow:

    ticker: str
    fiscal_quarter: int
    fiscal_year: int
    cashflow_operating_section: int | None = None
    pretax_income: int | None = None
    adjustments_section: int | None = None
    depreciation_amortization: int | None = None
    provisions: int | None = None
    investment_gain_loss: int | None = None
    unrealized_fx_gain_loss: int | None = None
    asset_writeoff_gain_loss: int | None = None
    interest_expense_adjustment: int | None = None
    other_non_cash_adjustments: int | None = None
    interest_income: int | None = None
    goodwill_amortization: int | None = None
    asset_disposal_gain_loss: int | None = None
    operating_profit_before_wc_changes: int | None = None
    change_in_receivables: int | None = None
    change_in_trading_securities: int | None = None
    change_in_inventory: int | None = None
    change_in_payables: int | None = None
    change_in_prepaid_expenses: int | None = None
    interest_paid: int | None = None
    income_tax_paid: int | None = None
    other_operating_cash_inflows: int | None = None
    other_operating_cash_outflows: int | None = None
    net_cashflow_operating: int | None = None

    _WIGROUP_MAP = {
        "code": "ticker",
        "quy": "fiscal_quarter",
        "nam": "fiscal_year",
        "luuchuyentientuhoatdongkinhdoanh": "cashflow_operating_section",
        "loinhuanlotruocthue": "pretax_income",
        "dieuchinhchocackhoan": "adjustments_section",
        "khauhaotaisancodinh": "depreciation_amortization",
        "cackhoanduphong": "provisions",
        "lailotudautuvaocongtylienket": "investment_gain_loss",
        "lailochenhlechtygiahoidoaichuathuchien": "unrealized_fx_gain_loss",
        "lailotuhoatdongdaututhanhlytaisancodinh": "asset_writeoff_gain_loss",
        "chiphilaivay": "interest_expense_adjustment",
        "cackhoangiamtrukhac": "other_non_cash_adjustments",
        "thunhaptulaitiengui": "interest_income",
        "phanboloithethuongmai": "goodwill_amortization",
        "lailothanhlytaisancodinh": "asset_disposal_gain_loss",
        "loinhuanlotuhoatdongkinhdoanhtruocthaydoivonluudong": "operating_profit_before_wc_changes",
        "tanggiamcackhoanphaithu": "change_in_receivables",
        "tanggiamchungkhoantudoanh": "change_in_trading_securities",
        "tanggiamhangtonkho": "change_in_inventory",
        "tanggiamcackhoanphaitrakhonggomlaivaythuetndnphaitra": "change_in_payables",
        "tanggiamchiphitratruoc": "change_in_prepaid_expenses",
        "tienlaivaydatra": "interest_paid",
        "thuethunhapdoanhnghiepdanop": "income_tax_paid",
        "tienthukhactuhoatdongkinhdoanh": "other_operating_cash_inflows",
        "tienchikhacchohoatdongkinhdoanh": "other_operating_cash_outflows",
        "luuchuyentienthuantuhoatdongkinhdoanh": "net_cashflow_operating",
    }

    @classmethod
    def from_wigroup(cls, data: dict) -> Self:
        mapped_data = {}
        for key, value in data.items():
            if key in cls._WIGROUP_MAP:
                mapped_key = cls._WIGROUP_MAP[key]
                mapped_data[mapped_key] = value
        return cls(**mapped_data)
