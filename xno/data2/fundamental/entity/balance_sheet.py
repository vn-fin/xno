from dataclasses import dataclass, asdict
from typing import Self

from sqlalchemy import Row


@dataclass(frozen=True)
class BalanceSheet:
    symbol: str
    fiscal_year: int | None = None
    fiscal_quarter: int | None = None
    current_assets_section: float | None = None
    cash_and_equivalents_section: float | None = None
    cash: float | None = None
    cash_equivalents: float | None = None
    short_term_financial_investments_section: float | None = None
    trading_securities: float | None = None
    trading_securities_allowance: float | None = None
    held_to_maturity_st: float | None = None
    receivables_short_term_section: float | None = None
    accounts_receivable_st: float | None = None
    advances_to_suppliers_st: float | None = None
    intercompany_receivables_st: float | None = None
    construction_receivables: float | None = None
    loans_receivable_st: float | None = None
    other_receivables_st: float | None = None
    receivables_allowance_st: float | None = None
    pending_asset_loss: float | None = None
    inventory_section: float | None = None
    inventory: float | None = None
    inventory_allowance: float | None = None
    other_current_assets_section: float | None = None
    prepaid_expenses_st: float | None = None
    vat_credit: float | None = None
    tax_receivables: float | None = None
    repo_gov_bonds_st: float | None = None
    other_current_assets: float | None = None
    non_current_assets_section: float | None = None
    receivables_long_term_section: float | None = None
    accounts_receivable_lt: float | None = None
    advances_to_suppliers_lt: float | None = None
    capital_in_subunits: float | None = None
    intercompany_receivables_lt: float | None = None
    loans_receivable_lt: float | None = None
    other_receivables_lt: float | None = None
    receivables_allowance_lt: float | None = None
    fixed_assets_section: float | None = None
    ppe_tangible: float | None = None
    ppe_tangible_cost: float | None = None
    ppe_tangible_accum_depr: float | None = None
    ppe_finance_leased: float | None = None
    ppe_finance_cost: float | None = None
    ppe_finance_accum_depr: float | None = None
    intangible_assets: float | None = None
    intangible_cost: float | None = None
    intangible_accum_amort: float | None = None
    investment_property: float | None = None
    investment_property_cost: float | None = None
    investment_property_accum_depr: float | None = None
    cip_section: float | None = None
    cip_production: float | None = None
    cip_construction: float | None = None
    investments_long_term_section: float | None = None
    investment_in_subsidiaries: float | None = None
    investment_in_associates: float | None = None
    investment_in_other_entities: float | None = None
    long_term_investment_allowance: float | None = None
    held_to_maturity_lt: float | None = None
    other_non_current_assets_section: float | None = None
    prepaid_expenses_lt: float | None = None
    deferred_tax_assets: float | None = None
    spare_parts_lt: float | None = None
    other_non_current_assets: float | None = None
    goodwill: float | None = None
    total_assets: float | None = None
    liabilities_section: float | None = None
    current_liabilities_section: float | None = None
    payables_to_suppliers_st: float | None = None
    advances_from_customers_st: float | None = None
    taxes_payable: float | None = None
    employee_payables: float | None = None
    accrued_expenses_st: float | None = None
    intercompany_payables_st: float | None = None
    construction_contract_payables: float | None = None
    unearned_revenue_st: float | None = None
    other_payables_st: float | None = None
    borrowings_st: float | None = None
    provisions_st: float | None = None
    welfare_bonus_fund: float | None = None
    price_stabilization_fund: float | None = None
    repo_gov_bonds_liability_st: float | None = None
    long_term_liabilities_section: float | None = None
    payables_to_suppliers_lt: float | None = None
    advances_from_customers_lt: float | None = None
    accrued_expenses_lt: float | None = None
    intercompany_capital_payable: float | None = None
    intercompany_payables_lt: float | None = None
    unearned_revenue_lt: float | None = None
    other_payables_lt: float | None = None
    borrowings_lt: float | None = None
    convertible_bonds: float | None = None
    preferred_shares_liability: float | None = None
    deferred_tax_liabilities: float | None = None
    provisions_lt: float | None = None
    science_tech_fund: float | None = None
    equity_section: float | None = None
    owners_equity_subsection: float | None = None
    contributed_capital: float | None = None
    common_shares: float | None = None
    preferred_shares_equity: float | None = None
    share_premium: float | None = None
    convertible_bond_option_reserve: float | None = None
    other_owner_equity: float | None = None
    treasury_shares: float | None = None
    asset_revaluation_reserve: float | None = None
    fx_translation_reserve_equity: float | None = None
    development_fund: float | None = None
    restructuring_support_fund: float | None = None
    other_equity_funds: float | None = None
    retained_earnings: float | None = None
    retained_earnings_prior: float | None = None
    retained_earnings_current: float | None = None
    construction_investment_capital: float | None = None
    non_controlling_interest_equity: float | None = None
    special_funds_section: float | None = None
    special_funds: float | None = None
    funds_forming_fixed_assets: float | None = None
    total_liabilities_equity: float | None = None
    audit_firm: str | None = None
    audit_opinion: str | None = None

    def to_dict(self) -> dict:
        """Convert BalanceSheet to dictionary."""
        return asdict(self)

    @classmethod
    def from_db(cls, raw: Row | dict) -> Self:
        if isinstance(raw, Row):
            raw = raw._asdict()
        return cls(**raw)
