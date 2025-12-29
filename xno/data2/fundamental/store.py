import logging
from datetime import datetime
from sqlalchemy import text

from xno.connectors.sql import SqlSession
from xno.data2.fundamental.entity.period import Period

logger = logging.getLogger(__name__)


class DataStoreService:
    @classmethod
    def singleton(cls, **kwargs) -> "DataStoreService":
        if not hasattr(cls, "_instance"):
            if not kwargs:
                kwargs = {
                    "db_name": "xno_data",
                }
            cls._instance = cls(**kwargs)
        return cls._instance

    def __init__(self, db_name: str = "xno_data"):
        self._db_name = db_name

    def get_price_volume(self, symbol: str, from_time: datetime | None = None, to_time: datetime | None = None) -> dict:
        """Get price volume information by stock symbol."""
        if __debug__:
            logger.debug("Fetching price volume info for symbol: %s", symbol)

        with SqlSession(self._db_name) as session:
            sql = """
                SELECT
                    date,
                    symbol,
                    exchange,
                    currency,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    adv20,
                    returns,
                    sharesout,
                    cap,
                    cash_dividend_payout_ratio,
                    stock_dividend_ratio,
                    market,
                    industry,
                    sector,
                    sub_industry
                FROM reference.v_trading_universe
                WHERE symbol = :symbol
                """

            if from_time is not None:
                sql += " AND date >= :from_time"
            if to_time is not None:
                sql += " AND date < :to_time"
            sql += " ORDER BY symbol, date ASC"

            result = session.execute(
                text(sql),
                dict(symbol=symbol, from_time=from_time, to_time=to_time),
            )
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for price volume symbol: %s", len(rows), symbol)
        return rows

    def get_income_statement(
        self, symbol: str, period: str, from_time: datetime = None, to_time: datetime = None
    ) -> dict:
        """Get income statement information by stock symbol and period."""
        if __debug__:
            logger.debug("Fetching income statement info for symbol: %s", symbol)

        sql = """
            SELECT
                symbol,
                fiscal_year,
                fiscal_quarter,
                revenue,
                revenue_deductions,
                net_sales,
                cost_of_revenue,
                gross_profit,
                financial_income,
                financial_expense,
                interest_expense,
                equity_affiliate_income,
                selling_expense,
                general_admin_expense,
                operating_income,
                other_income,
                other_expense,
                other_profit,
                pretax_income,
                current_tax_expense,
                deferred_tax_expense,
                net_income_after_tax,
                minority_interest,
                net_income,
                basic_eps,
                diluted_eps,
                audit_firm,
                audit_opinion
            FROM reference.v_income_statement
            WHERE symbol = :symbol
            """

        params = dict(symbol=symbol)

        if period == Period.ANNUALLY:
            sql += " AND fiscal_quarter = 0"

            if from_time is not None:
                sql += " AND fiscal_year >= :from_year"
                params["from_year"] = from_time.year
            if to_time is not None:
                sql += " AND fiscal_year <= :to_year"
                params["to_year"] = to_time.year

        elif period == Period.QUARTERLY:
            sql += " AND fiscal_quarter != 0"
            if from_time is not None:
                from_quarter = (from_time.month - 1) // 3 + 1
                sql += (
                    " AND (fiscal_year > :from_year OR (fiscal_year = :from_year AND fiscal_quarter >= :from_quarter))"
                )
                params["from_year"] = from_time.year
                params["from_quarter"] = from_quarter
            if to_time is not None:
                to_quarter = (to_time.month - 1) // 3 + 1
                sql += " AND (fiscal_year < :to_year OR (fiscal_year = :to_year AND fiscal_quarter <= :to_quarter))"
                params["to_year"] = to_time.year
                params["to_quarter"] = to_quarter

        sql += " ORDER BY symbol, fiscal_year ASC, fiscal_quarter ASC"

        with SqlSession(self._db_name) as session:
            result = session.execute(text(sql), params)
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for symbol: %s", len(rows), symbol)
        return rows

    def get_cash_flow(self, symbol: str, period: str, from_time: datetime = None, to_time: datetime = None) -> dict:
        """Get cash flow information by stock symbol and period."""
        if __debug__:
            logger.debug("Fetching cash flow info for symbol: %s", symbol)

        sql = """
            SELECT
                symbol,
                fiscal_year,
                fiscal_quarter,
                cashflow_operating_section,
                pretax_income,
                adjustments_section,
                depreciation_amortization,
                provisions,
                investment_gain_loss,
                unrealized_fx_gain_loss,
                asset_writeoff_gain_loss,
                interest_expense_adjustment,
                other_non_cash_adjustments,
                interest_income,
                goodwill_amortization,
                asset_disposal_gain_loss,
                operating_profit_before_wc_changes,
                change_in_receivables,
                change_in_trading_securities,
                change_in_inventory,
                change_in_payables,
                change_in_prepaid_expenses,
                interest_paid,
                income_tax_paid,
                other_operating_cash_inflows,
                other_operating_cash_outflows,
                net_cashflow_operating,
                cashflow_investing_section,
                capex,
                proceeds_from_asset_disposal,
                loans_granted,
                loans_collected,
                equity_investment_purchase,
                equity_investment_sale_proceeds,
                interest_dividends_received,
                other_investing_cashflows,
                net_cashflow_investing,
                cashflow_financing_section,
                capital_raised,
                capital_repayment_buyback,
                borrowings_received,
                loan_principal_repaid,
                lease_principal_paid,
                dividends_paid,
                other_financing_cashflows,
                net_cashflow_financing,
                net_change_in_cash,
                cash_beginning,
                fx_effect_on_cash,
                cash_ending,
                audit_firm,
                audit_opinion
            FROM reference.v_cash_flow_statement
            WHERE symbol = :symbol
            """

        params = dict(symbol=symbol)

        if period == Period.ANNUALLY:
            sql += " AND fiscal_quarter = 0"

            if from_time is not None:
                sql += " AND fiscal_year >= :from_year"
                params["from_year"] = from_time.year
            if to_time is not None:
                sql += " AND fiscal_year <= :to_year"
                params["to_year"] = to_time.year

        elif period == Period.QUARTERLY:
            sql += " AND fiscal_quarter != 0"
            if from_time is not None:
                from_quarter = (from_time.month - 1) // 3 + 1
                sql += (
                    " AND (fiscal_year > :from_year OR (fiscal_year = :from_year AND fiscal_quarter >= :from_quarter))"
                )
                params["from_year"] = from_time.year
                params["from_quarter"] = from_quarter
            if to_time is not None:
                to_quarter = (to_time.month - 1) // 3 + 1
                sql += " AND (fiscal_year < :to_year OR (fiscal_year = :to_year AND fiscal_quarter <= :to_quarter))"
                params["to_year"] = to_time.year
                params["to_quarter"] = to_quarter

        sql += " ORDER BY symbol, fiscal_year ASC, fiscal_quarter ASC"

        with SqlSession(self._db_name) as session:
            result = session.execute(text(sql), params)
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for symbol: %s", len(rows), symbol)
        return rows

    def get_balance_sheet(self, symbol: str, period: str, from_time: datetime = None, to_time: datetime = None) -> dict:
        """Get balance sheet information by stock symbol and period."""
        if __debug__:
            logger.debug("Fetching balance sheet info for symbol: %s", symbol)

        sql = """
            SELECT
                symbol,
                fiscal_year,
                fiscal_quarter,
                current_assets_section,
                cash_and_equivalents_section,
                cash,
                cash_equivalents,
                short_term_financial_investments_section,
                trading_securities,
                trading_securities_allowance,
                held_to_maturity_st,
                receivables_short_term_section,
                accounts_receivable_st,
                advances_to_suppliers_st,
                intercompany_receivables_st,
                construction_receivables,
                loans_receivable_st,
                other_receivables_st,
                receivables_allowance_st,
                pending_asset_loss,
                inventory_section,
                inventory,
                inventory_allowance,
                other_current_assets_section,
                prepaid_expenses_st,
                vat_credit,
                tax_receivables,
                repo_gov_bonds_st,
                other_current_assets,
                non_current_assets_section,
                receivables_long_term_section,
                accounts_receivable_lt,
                advances_to_suppliers_lt,
                capital_in_subunits,
                intercompany_receivables_lt,
                loans_receivable_lt,
                other_receivables_lt,
                receivables_allowance_lt,
                fixed_assets_section,
                ppe_tangible,
                ppe_tangible_cost,
                ppe_tangible_accum_depr,
                ppe_finance_leased,
                ppe_finance_cost,
                ppe_finance_accum_depr,
                intangible_assets,
                intangible_cost,
                intangible_accum_amort,
                investment_property,
                investment_property_cost,
                investment_property_accum_depr,
                cip_section,
                cip_production,
                cip_construction,
                investments_long_term_section,
                investment_in_subsidiaries,
                investment_in_associates,
                investment_in_other_entities,
                long_term_investment_allowance,
                held_to_maturity_lt,
                other_non_current_assets_section,
                prepaid_expenses_lt,
                deferred_tax_assets,
                spare_parts_lt,
                other_non_current_assets,
                goodwill,
                total_assets,
                liabilities_section,
                current_liabilities_section,
                payables_to_suppliers_st,
                advances_from_customers_st,
                taxes_payable,
                employee_payables,
                accrued_expenses_st,
                intercompany_payables_st,
                construction_contract_payables,
                unearned_revenue_st,
                other_payables_st,
                borrowings_st,
                provisions_st,
                welfare_bonus_fund,
                price_stabilization_fund,
                repo_gov_bonds_liability_st,
                long_term_liabilities_section,
                payables_to_suppliers_lt,
                advances_from_customers_lt,
                accrued_expenses_lt,
                intercompany_capital_payable,
                intercompany_payables_lt,
                unearned_revenue_lt,
                other_payables_lt,
                borrowings_lt,
                convertible_bonds,
                preferred_shares_liability,
                deferred_tax_liabilities,
                provisions_lt,
                science_tech_fund,
                equity_section,
                owners_equity_subsection,
                contributed_capital,
                common_shares,
                preferred_shares_equity,
                share_premium,
                convertible_bond_option_reserve,
                other_owner_equity,
                treasury_shares,
                asset_revaluation_reserve,
                fx_translation_reserve_equity,
                development_fund,
                restructuring_support_fund,
                other_equity_funds,
                retained_earnings,
                retained_earnings_prior,
                retained_earnings_current,
                construction_investment_capital,
                non_controlling_interest_equity,
                special_funds_section,
                special_funds,
                funds_forming_fixed_assets,
                total_liabilities_equity,
                audit_firm,
                audit_opinion
            FROM reference.v_balance_sheet
            WHERE symbol = :symbol
            """

        params = dict(symbol=symbol)

        if period == Period.ANNUALLY:
            sql += " AND fiscal_quarter = 0"

            if from_time is not None:
                sql += " AND fiscal_year >= :from_year"
                params["from_year"] = from_time.year
            if to_time is not None:
                sql += " AND fiscal_year <= :to_year"
                params["to_year"] = to_time.year
        elif period == Period.QUARTERLY:
            sql += " AND fiscal_quarter != 0"
            if from_time is not None:
                from_quarter = (from_time.month - 1) // 3 + 1
                sql += (
                    " AND (fiscal_year > :from_year OR (fiscal_year = :from_year AND fiscal_quarter >= :from_quarter))"
                )
                params["from_year"] = from_time.year
                params["from_quarter"] = from_quarter
            if to_time is not None:
                to_quarter = (to_time.month - 1) // 3 + 1
                sql += " AND (fiscal_year < :to_year OR (fiscal_year = :to_year AND fiscal_quarter <= :to_quarter))"
                params["to_year"] = to_time.year
                params["to_quarter"] = to_quarter

        sql += " ORDER BY symbol, fiscal_year ASC, fiscal_quarter ASC"

        with SqlSession(self._db_name) as session:
            result = session.execute(text(sql), params)
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for symbol: %s", len(rows), symbol)
        return rows

    # --- Data Quality ---
    def get_data_quality_categories(self) -> dict:
        """Get data quality categories."""
        if __debug__:
            logger.debug("Fetching data quality summary")

        with SqlSession(self._db_name) as session:
            sql = """
                SELECT
                    data_category,
                    count(dataset) as datasets,
                    sum(total_fields) as total_fields
                FROM reference_data_quality.v_data_quality
                GROUP BY data_category
                """

            result = session.execute(text(sql))
            rows = result.fetchall()

            if __debug__:
                logger.debug("Retrieved %d rows for data quality summary", len(rows))
        return rows

    def get_data_quality_datasets(self, data_category: str) -> dict:
        """Get data quality datasets by category."""
        if __debug__:
            logger.debug(
                "Fetching datasets for category: %s",
                data_category,
            )

        with SqlSession(self._db_name) as session:
            sql = """
                SELECT
                    dataset,
                    data_category,
                    total_fields,
                    symbols_with_data,
                    data_coverage_pct,
                    date_coverage_pct
                FROM reference_data_quality.v_data_quality
                """

            if data_category:
                sql += " WHERE data_category = :data_category"

            result = session.execute(text(sql), dict(data_category=data_category))
            rows = result.fetchall()

            if __debug__:
                logger.debug(
                    "Retrieved %d rows for data quality datasets in category: %s",
                    len(rows),
                    data_category,
                )
        return rows
