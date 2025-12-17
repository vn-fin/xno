from xno.data2.fundamental.db import WiSqlSession


class ExternalWiDataService:
    def __init__(self, database: str = "fundamental"):
        self._database = database

    def get_history(self, symbol: str, start_time: str, end_time: str):
        with WiSqlSession(self._database) as session:
            result = session.execute(
                "SELECT * FROM fundamental_data WHERE symbol = :symbol AND date BETWEEN :start_time AND :end_time",
                {"symbol": symbol, "start_time": start_time, "end_time": end_time},
            )
            return result.fetchall()

    def get_history_revenue(
        self,
        symbol: str,
        from_time: str | None = None,
        to_time: str | None = None,
        limit: int | None = None,
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT revenue FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time
            if limit is not None:
                sql += " LIMIT :limit"
                params["limit"] = limit

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_net_sales(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_gross_profit(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_cost_of_revenue(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_sgna_expense(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_research_development_exp(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_operating_income(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ebit(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ebitda(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_depreciation(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_amortization(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_depreciation_amortization(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_operating_expense(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_interest_expense(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_interest_income(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_pretax_income(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_net_income(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_net_income_after_tax(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_basic_eps(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_diluted_eps(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_gross_dividends(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_assets(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_current_assets(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_cash_and_equivalents(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_cash_and_short_term_investments(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_short_term_investments(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_accounts_receivable_gross(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_accounts_receivable_net(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_inventory(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_inventory(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ppe_gross(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ppe_net(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_accumulated_depreciation(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_intangibles_net(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_goodwill_net(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_other_current_assets(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_non_current_assets(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_other_long_term_assets(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_accounts_payable(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_accrued_expenses(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_prepaid_expenses(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_current_liabilities(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_short_term_debt(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_long_term_debt(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_long_term_debt(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_other_current_liabilities(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_other_long_term_liabilities(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_liabilities(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_shareholder_equity(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_liabilities_equity(
        self, symbol: str, from_time: str | None = None, to_time: str | None = None
    ):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_retained_earnings(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_treasury_stock(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_cashflow_operating(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_cashflow_investing(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_cashflow_financing(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_capital_expenditures(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_free_cashflow(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_shares_outstanding(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_market_cap(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_dividend_cash(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_split_ratio(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_gross_margin(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_operating_margin(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_pretax_margin(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_net_margin(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ebit_margin(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ebitda_margin(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_total_debt(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_net_debt(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_debt_to_equity(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_current_ratio(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_quick_ratio(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_asset_turnover(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_inventory_turnover(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_receivable_turnover(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_pe_ratio(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_pb_ratio(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ps_ratio(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_ev_ebitda(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_capxy2q(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_dltry2q(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_fiscalquarter(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_fiscalyear(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_q2ycktsrp(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_q2yfcnao(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_q2yktss(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_q2ysitld(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_q2yvd(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_qta(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_rtqf(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()

    def get_history_fnd2_ta(self, symbol: str, from_time: str | None = None, to_time: str | None = None):
        with WiSqlSession(self._database) as session:
            sql = "SELECT net_sales FROM revenue_data WHERE symbol = :symbol"
            params = {"symbol": symbol}

            if from_time is not None:
                sql += " AND year >= :from_time"
                params["from_time"] = from_time
            if to_time is not None:
                sql += " AND year <= :to_time"
                params["to_time"] = to_time

            result = session.execute(sql, params)
            return result.fetchall()
