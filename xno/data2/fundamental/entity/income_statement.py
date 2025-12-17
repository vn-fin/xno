from dataclasses import dataclass

from xno.data2.fundamental.entity.base import StockEntity


@dataclass
class IncomeStatement(StockEntity):
    _DATAFRAME_MAPPING = {
        "ticker": "ticker",
        "yearReport": "year_report",
        "Revenue (Bn. VND)": "revenue",
        "Revenue YoY (%)": "revenue_yoy",
        "Attribute to parent company (Bn. VND)": "attribute_to_parent_company",
        "Attribute to parent company YoY (%)": "attribute_to_parent_company_yoy",
        "Interest and Similar Income": "interest_and_similar_income",
        "Interest and Similar Expenses": "interest_and_similar_expenses",
        "Net Interest Income": "net_interest_income",
        "Fees and Comission Income": "fees_and_comission_income",
        "Fees and Comission Expenses": "fees_and_comission_expenses",
        "Net Fee and Commission Income": "net_fee_and_commission_income",
        "Net gain (loss) from foreign currency and gold dealings": "net_gain_loss_from_foreign_currency_and_gold_dealings",
        "Net gain (loss) from trading of trading securities": "net_gain_loss_from_trading_of_trading_securities",
        "Net gain (loss) from disposal of investment securities": "net_gain_loss_from_disposal_of_investment_securities",
        "Net Other income/(expenses)": "net_other_income_expenses",
        "Other expenses": "other_expenses",
        "Net Other income/expenses": "net_other_income_expenses",
        "Dividends received": "dividends_received",
        "Total operating revenue": "total_operating_revenue",
        "General & Admin Expenses": "general_admin_expenses",
        "Operating Profit before Provision": "operating_profit_before_provision",
        "Provision for credit losses": "provision_for_credit_losses",
        "Profit before tax": "profit_before_tax",
        "Tax For the Year": "tax_for_the_year",
        "Business income tax - current": "business_income_tax_current",
        "Business income tax - deferred": "business_income_tax_deferred",
        "Net Profit For the Year": "net_profit_for_the_year",
        "Attributable to parent company": "attributable_to_parent_company",
        "EPS_basis": "eps_basis"
    }
