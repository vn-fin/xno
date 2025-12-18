from dataclasses import dataclass

from xno.data2.fundamental.entity.base import StockEntity


@dataclass
class CashFlow(StockEntity):
    _DATAFRAME_MAPPING = {
        "ticker": "ticker",
        "yearReport": "year_report",
        "Profits from other activities": "profits_from_other_activities",
        "Operating profit before changes in working capital": "operating_profit_before_changes_in_working_capital",
        "Net Cash Flows from Operating Activities before BIT": "net_cash_flows_from_operating_activities_before_bit",
        "Payment from reserves": "payment_from_reserves",
        "Purchase of fixed assets": "purchase_of_fixed_assets",
        "Gain on Dividend": "gain_on_dividend",
        "Net Cash Flows from Investing Activities": "net_cash_flows_from_investing_activities",
        "Increase in charter captial": "increase_in_charter_captial",
        "Cash flows from financial activities": "cash_flows_from_financial_activities",
        "Net increase/decrease in cash and cash equivalents": "net_increase_decrease_in_cash_and_cash_equivalents",
        "Cash and cash equivalents": "cash_and_cash_equivalents",
        "Foreign exchange differences Adjustment": "foreign_exchange_differences_adjustment",
        "Cash and Cash Equivalents at the end of period": "cash_and_cash_equivalents_at_the_end_of_period",
        "Net cash inflows/outflows from operating activities": "net_cash_inflows_outflows_from_operating_activities",
        "Proceeds from disposal of fixed assets": "proceeds_from_disposal_of_fixed_assets",
        "Investment in other entities": "investment_in_other_entities",
        "Proceeds from divestment in other entities": "proceeds_from_divestment_in_other_entities",
        "Dividends paid": "dividends_paid"
    }
