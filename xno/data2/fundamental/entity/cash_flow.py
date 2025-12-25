from dataclasses import dataclass
from sqlalchemy import Row
from typing import Self


@dataclass(frozen=True)
class CashFlow:
    symbol: str
    fiscal_year: int
    fiscal_quarter: int
    cashflow_operating_section: float
    pretax_income: float
    adjustments_section: float
    depreciation_amortization: float
    provisions: float
    investment_gain_loss: float
    unrealized_fx_gain_loss: float
    asset_writeoff_gain_loss: float
    interest_expense_adjustment: float
    other_non_cash_adjustments: float
    interest_income: float
    goodwill_amortization: float
    asset_disposal_gain_loss: float
    operating_profit_before_wc_changes: float
    change_in_receivables: float
    change_in_trading_securities: float
    change_in_inventory: float
    change_in_payables: float
    change_in_prepaid_expenses: float
    interest_paid: float
    income_tax_paid: float
    other_operating_cash_inflows: float
    other_operating_cash_outflows: float
    net_cashflow_operating: float
    cashflow_investing_section: float
    capex: float
    proceeds_from_asset_disposal: float
    loans_granted: float
    loans_collected: float
    equity_investment_purchase: float
    equity_investment_sale_proceeds: float
    interest_dividends_received: float
    other_investing_cashflows: float
    net_cashflow_investing: float
    cashflow_financing_section: float
    capital_raised: float
    capital_repayment_buyback: float
    borrowings_received: float
    loan_principal_repaid: float
    lease_principal_paid: float
    dividends_paid: float
    other_financing_cashflows: float
    net_cashflow_financing: float
    net_change_in_cash: float
    cash_beginning: float
    fx_effect_on_cash: float
    cash_ending: float
    audit_firm: str
    audit_opinion: str

    def to_dict(self) -> dict:
        """Convert CashFlow to dictionary."""
        return {
            "symbol": self.symbol,
            "fiscal_year": self.fiscal_year,
            "fiscal_quarter": self.fiscal_quarter,
            "cashflow_operating_section": self.cashflow_operating_section,
            "pretax_income": self.pretax_income,
            "adjustments_section": self.adjustments_section,
            "depreciation_amortization": self.depreciation_amortization,
            "provisions": self.provisions,
            "investment_gain_loss": self.investment_gain_loss,
            "unrealized_fx_gain_loss": self.unrealized_fx_gain_loss,
            "asset_writeoff_gain_loss": self.asset_writeoff_gain_loss,
            "interest_expense_adjustment": self.interest_expense_adjustment,
            "other_non_cash_adjustments": self.other_non_cash_adjustments,
            "interest_income": self.interest_income,
            "goodwill_amortization": self.goodwill_amortization,
            "asset_disposal_gain_loss": self.asset_disposal_gain_loss,
            "operating_profit_before_wc_changes": self.operating_profit_before_wc_changes,
            "change_in_receivables": self.change_in_receivables,
            "change_in_trading_securities": self.change_in_trading_securities,
            "change_in_inventory": self.change_in_inventory,
            "change_in_payables": self.change_in_payables,
            "change_in_prepaid_expenses": self.change_in_prepaid_expenses,
            "interest_paid": self.interest_paid,
            "income_tax_paid": self.income_tax_paid,
            "other_operating_cash_inflows": self.other_operating_cash_inflows,
            "other_operating_cash_outflows": self.other_operating_cash_outflows,
            "net_cashflow_operating": self.net_cashflow_operating,
            "cashflow_investing_section": self.cashflow_investing_section,
            "capex": self.capex,
            "proceeds_from_asset_disposal": self.proceeds_from_asset_disposal,
            "loans_granted": self.loans_granted,
            "loans_collected": self.loans_collected,
            "equity_investment_purchase": self.equity_investment_purchase,
            "equity_investment_sale_proceeds": self.equity_investment_sale_proceeds,
            "interest_dividends_received": self.interest_dividends_received,
            "other_investing_cashflows": self.other_investing_cashflows,
            "net_cashflow_investing": self.net_cashflow_investing,
            "cashflow_financing_section": self.cashflow_financing_section,
            "capital_raised": self.capital_raised,
            "capital_repayment_buyback": self.capital_repayment_buyback,
            "borrowings_received": self.borrowings_received,
            "loan_principal_repaid": self.loan_principal_repaid,
            "lease_principal_paid": self.lease_principal_paid,
            "dividends_paid": self.dividends_paid,
            "other_financing_cashflows": self.other_financing_cashflows,
            "net_cashflow_financing": self.net_cashflow_financing,
            "net_change_in_cash": self.net_change_in_cash,
            "cash_beginning": self.cash_beginning,
            "fx_effect_on_cash": self.fx_effect_on_cash,
            "cash_ending": self.cash_ending,
            "audit_firm": self.audit_firm,
            "audit_opinion": self.audit_opinion,
        }

    @classmethod
    def from_db(cls, raw: Row) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()

        return cls(**raw)
