"""
QuestDB data fetcher for stock screening and analysis.

Provides high-performance data access for financial metrics using QuestDB.
Works with normalized concept names (e.g., 'revenue' instead of raw XBRL tags).

Supports streaming via cursor-based pagination for progressive stock screening.
"""
import polars as pl
import psycopg2
from typing import Generator, List, Optional

from edgar.standardization import SynonymGroups


class QuestDBFetcher:
    """Fetches financial data from QuestDB for stock screening and analysis."""

    def __init__(self, host="localhost", port=8812, user="admin", password="quest", database="qdb"):
        """Initialize connection parameters for QuestDB."""
        self.conn_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

    def _query_to_polars(self, query: str) -> pl.DataFrame:
        """Execute a SQL query and return results as a Polars DataFrame."""
        conn = psycopg2.connect(**self.conn_params)
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
        finally:
            conn.close()

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame(rows, schema=columns, orient="row")

    def fetch_financial_data(
        self,
        ciks: List[int],
        concepts: List[str],
        fiscal_periods: List[str] | None = None,
        forms: List[str] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch financial data for multiple CIKs and concepts.

        Since concepts are normalized at import time, you can use canonical names
        directly (e.g., 'revenue', 'net_income') instead of raw XBRL tags.

        Args:
            ciks: List of CIK numbers
            concepts: List of canonical concept names (e.g., ['revenue', 'net_income'])
            fiscal_periods: Filter by fiscal period (default: ['FY'])
            forms: Filter by form type (default: ['10-K', '10-K/A'])

        Returns:
            Polars DataFrame
        """
        if not ciks or not concepts:
            return pl.DataFrame()

        if fiscal_periods is None:
            fiscal_periods = ["FY"]
        if forms is None:
            forms = ["10-K", "10-K/A"]

        cik_list = ",".join(str(c) for c in ciks)
        concept_list = ",".join(f"'{c}'" for c in concepts)
        period_list = ",".join(f"'{p}'" for p in fiscal_periods)
        form_list = ",".join(f"'{f}'" for f in forms)

        query = f"""
        SELECT
            cik,
            ticker,
            entity_name,
            concept,
            xbrl_tag,
            fiscal_year,
            fiscal_period,
            form,
            val,
            filed_date,
            end_date
        FROM financial
        WHERE cik IN ({cik_list})
          AND concept IN ({concept_list})
          AND fiscal_period IN ({period_list})
          AND form IN ({form_list})
        ORDER BY cik, concept, fiscal_year DESC
        """

        return self._query_to_polars(query)

    def fetch_annual_facts(self, ciks: List[int], concepts: List[str]) -> pl.DataFrame:
        """
        Fetch annual facts for multiple CIKs and concepts.

        Compatibility wrapper - uses the same interface as before but
        concepts are now canonical names.
        """
        return self.fetch_financial_data(ciks, concepts)

    def stream_financial_data(
        self,
        ciks: List[int],
        concepts: List[str],
        batch_size: int = 500,
        fiscal_periods: List[str] | None = None,
        forms: List[str] | None = None,
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Stream financial data in batches using cursor-based pagination on CIK.

        Yields Polars DataFrames, one per batch of CIKs. This allows the
        caller to process and display results progressively.

        Args:
            ciks: List of CIK numbers
            concepts: List of canonical concept names
            batch_size: Number of CIKs per batch
            fiscal_periods: Filter by fiscal period (default: ['FY'])
            forms: Filter by form type (default: ['10-K', '10-K/A'])

        Yields:
            Polars DataFrame for each batch of CIKs
        """
        if not ciks or not concepts:
            return

        for i in range(0, len(ciks), batch_size):
            batch_ciks = ciks[i : i + batch_size]
            df = self.fetch_financial_data(
                batch_ciks, concepts, fiscal_periods, forms
            )
            if not df.is_empty():
                yield df

    def fetch_daily_prices(
        self,
        tickers: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Fetch daily price data for tickers.

        Args:
            tickers: List of ticker symbols
            start_date: Start date as 'YYYY-MM-DD' string (optional)
            end_date: End date as 'YYYY-MM-DD' string (optional)

        Returns:
            Polars DataFrame with OHLCV data
        """
        if not tickers:
            return pl.DataFrame()

        ticker_list = ",".join(f"'{t}'" for t in tickers)

        date_filters = []
        if start_date:
            date_filters.append(f"ts >= '{start_date}'")
        if end_date:
            date_filters.append(f"ts <= '{end_date}'")

        date_clause = f" AND {' AND '.join(date_filters)}" if date_filters else ""

        query = f"""
        SELECT
            ticker,
            open,
            high,
            low,
            close,
            volume,
            adj_close,
            ts
        FROM daily_price
        WHERE ticker IN ({ticker_list}){date_clause}
        ORDER BY ticker, ts DESC
        """

        return self._query_to_polars(query)

    def get_screening_concepts(self, synonym_groups: SynonymGroups | None = None) -> List[str]:
        """
        Get canonical concept names needed for stock screening.

        Since concepts are normalized at import time, this returns simple
        canonical names instead of lists of XBRL tag synonyms.
        """
        return [
            "revenue",
            "net_income",
            "stockholders_equity",
            "operating_income",
            "income_before_tax",
            "income_tax_expense",
            "total_debt",
            "long_term_debt",
            "short_term_debt",
            "operating_cash_flow",
            "capex",
            "common_shares_outstanding",
            "total_assets",
            "accounts_payable",
            "accrued_liabilities",
            "cash_and_equivalents",
            "short_term_investments",
            "total_current_assets",
            "total_current_liabilities",
        ]

    def calculate_historical_pe(self, ticker: str, cik: int) -> Optional[float]:
        """
        Calculate historical P/E stats using QuestDB financial facts and price data.
        Returns the median historical P/E.
        """
        # 1. Fetch Net Income and Shares to calculate EPS
        facts_df = self.fetch_financial_data(
            [cik], ["net_income", "common_shares_outstanding"]
        )
        if facts_df.is_empty():
            return None

        # Pivot to get Net Income and Shares per year
        eps_df = facts_df.pivot(
            values="val", index=["fiscal_year", "end_date"], on="concept"
        )
        
        if "net_income" not in eps_df.columns or "common_shares_outstanding" not in eps_df.columns:
            return None
            
        eps_df = eps_df.with_columns(
            (pl.col("net_income") / pl.col("common_shares_outstanding")).alias("eps")
        ).sort("end_date")

        # 2. Fetch Daily Prices
        price_df = self.fetch_daily_prices([ticker])
        if price_df.is_empty():
            return None

        # 3. Join on dates to map EPS to Price
        # We use a join_asof to match each price with the latest available EPS
        # Both must be sorted by date
        price_df = price_df.sort("ts")
        eps_df = eps_df.sort("end_date").rename({"end_date": "ts"})
        
        merged = price_df.join_asof(
            eps_df.select(["ts", "eps"]),
            on="ts",
            strategy="backward"
        )
        
        # Calculate PE where EPS > 0
        pe_series = merged.filter(pl.col("eps") > 0).select(
            (pl.col("adj_close") / pl.col("eps")).alias("pe")
        ).to_series()
        
        if pe_series.is_empty():
            return None
            
        # Return median to avoid outliers
        return pe_series.median()

    # Keep old method name for backwards compatibility
    def get_all_concepts_for_screening(self, synonym_groups: SynonymGroups) -> List[str]:
        """Backwards-compatible alias for get_screening_concepts."""
        return self.get_screening_concepts(synonym_groups)
