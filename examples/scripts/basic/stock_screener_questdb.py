"""
Stock screener using QuestDB for high-performance data access.

Screens stocks based on growth metrics (Revenue, Net Income, Equity) and ROIC.
Uses QuestDB with normalized concept names and Polars for fast analysis.

Supports streaming mode: shows matching stocks immediately as data is processed,
rather than waiting for the full dataset to load first.
"""
import sys
import os
import polars as pl
from typing import Dict, List, Optional
from pathlib import Path

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from questdb_fetcher import QuestDBFetcher
    from edgar import Company, get_company_tickers
    import yfinance as yf
    import asyncio
    import httpx
    from financial_calcs import FinancialCalcs as fc
except ImportError as e:
    # Try one more path if running not from root
    sys.path.append(os.path.join(os.path.dirname(current_dir), '..', '..'))
    try:
         from questdb_fetcher import QuestDBFetcher
         from edgar import Company, get_company_tickers
         import yfinance as yf
         import asyncio
         import httpx
    except:
         print(f"Error: Could not import required modules (questdb_fetcher, yfinance, or httpx): {e}")
         sys.exit(1)


def calculate_growth_metrics(df: pl.DataFrame, concept: str, current_year: int) -> Dict:
    """
    Calculate growth metrics for a normalized concept using Polars.

    Since concepts are normalized at import time, we filter by canonical name
    directly (e.g., 'revenue') instead of a list of XBRL tag synonyms.

    Returns dict with: ttm, 1y, 5y, 10y growth rates
    """
    concept_df = df.filter(pl.col("concept") == concept)

    if concept_df.is_empty():
        return {"ttm": None, "1y": None, "5y": None, "10y": None}

    # Get annual values sorted by year
    annual = concept_df.filter(pl.col("fiscal_period") == "FY").sort(
        "fiscal_year", descending=True
    )

    if annual.is_empty():
        return {"ttm": None, "1y": None, "5y": None, "10y": None}

    def calc_cagr(years_back: int) -> Optional[float]:
        target_year = current_year - years_back
        current_val = annual.filter(pl.col("fiscal_year") == current_year).select("val").to_series()
        past_val = annual.filter(pl.col("fiscal_year") == target_year).select("val").to_series()

        if len(current_val) == 0 or len(past_val) == 0:
            return None

        cagr = fc.cagr(past_val[0], current_val[0], years_back)
        return cagr

    return {
        "ttm": calc_cagr(1),
        "1y": calc_cagr(1),
        "5y": calc_cagr(5),
        "10y": calc_cagr(10),
    }


def calculate_roic(df: pl.DataFrame, current_year: int) -> Dict:
    """Calculate ROIC metrics precisely using NOPAT / Invested Capital."""
    
    def get_val(concept: str, year: int) -> Optional[float]:
        val = df.filter((pl.col("concept") == concept) & (pl.col("fiscal_year") == year)).select("val").to_series()
        return val[0] if len(val) > 0 else None

    def calc_roic_year(year: int) -> Optional[float]:
        op_inc = get_val("operating_income", year)
        restr = get_val("restructuring_charges", year) or 0
        tax_exp = get_val("income_tax_expense", year)
        pre_tax = get_val("income_before_tax", year)
        reported_tax_rate = get_val("effective_tax_rate", year)
        
        adj_op = op_inc + restr if op_inc is not None else None
        
        # GuruFocus IC Components
        tot_assets = get_val("total_assets", year)
        ap = get_val("accounts_payable", year) or 0
        accrued = get_val("accrued_liabilities", year) or 0
        cash_val = get_val("cash_and_equivalents", year) or 0
        st_inv = get_val("short_term_investments", year) or 0
        curr_assets = get_val("total_current_assets", year) or 0
        curr_liab = get_val("total_current_liabilities", year) or 0

        if op_inc is None or tot_assets is None:
            return None

        # Determine tax rate
        tax_rate = 0.21
        if pre_tax and pre_tax != 0 and tax_exp is not None:
             tax_rate = max(0.0, min(0.5, tax_exp / pre_tax))
        elif reported_tax_rate is not None:
            tax_rate = reported_tax_rate
            if tax_rate > 1.0: tax_rate /= 100.0

        def calc_ic(y):
            v_assets = get_val("total_assets", y)
            if v_assets is None: return None
            v_ap = get_val("accounts_payable", y) or 0
            v_accrued = get_val("accrued_liabilities", y) or 0
            v_cash = get_val("cash_and_equivalents", y) or 0
            v_st_inv = get_val("short_term_investments", y) or 0
            v_curr_assets = get_val("total_current_assets", y) or 0
            v_curr_liab = get_val("total_current_liabilities", y) or 0
            
            return fc.invested_capital_gurufocus(
                v_assets, v_ap, v_accrued, v_cash + v_st_inv, 
                v_curr_assets, v_curr_liab
            )

        invested_capital = calc_ic(year)
        if invested_capital is None: return None
        
        # Average with prior
        ic_prior = calc_ic(year - 1)
        avg_ic = (invested_capital + ic_prior) / 2 if ic_prior is not None else invested_capital

        return fc.roic(adj_op, tax_rate, avg_ic)

    roics = []
    # Fetch ROIC for up to 11 years to ensure we can get 10Y results
    for year in range(current_year, current_year - 11, -1):
        roic = calc_roic_year(year)
        if roic is not None:
            roics.append(roic)

    if not roics:
        return {"ttm": None, "1y": None, "5y": None, "10y": None}

    return {
        "ttm": roics[0],
        "1y": roics[0],
        "5y": sum(roics[:5]) / len(roics[:5]) if len(roics) >= 3 else None,
        "10y": sum(roics[:10]) / len(roics[:10]) if len(roics) >= 5 else None,
    }


def check_stock(
    cik: int,
    ticker: str,
    entity_name: str,
    df: pl.DataFrame,
    threshold: float = 9.0,
) -> Optional[Dict]:
    """
    Check if a stock meets screening criteria using pre-fetched data.
    """
    if df.is_empty():
        return None

    current_year = (
        df.filter(pl.col("fiscal_period") == "FY")
        .select("fiscal_year")
        .max()
        .to_series()[0]
    )
    if current_year is None:
        return None

    # Calculate metrics using canonical concept names directly
    results = {}
    for label, concept in [
        ("Revenue", "revenue"),
        ("Net Income", "net_income"),
        ("Equity", "stockholders_equity"),
    ]:
        results[label] = calculate_growth_metrics(df, concept, current_year)

    results["ROIC"] = calculate_roic(df, current_year)

    # Apply screening logic
    for metric, vals in results.items():
        if vals["ttm"] is None or vals["ttm"] < threshold:
            return None
        if vals["1y"] is None or vals["1y"] < threshold:
            return None
        if vals["5y"] is None or vals["5y"] < threshold:
            return None
        if vals["10y"] is not None and vals["10y"] < threshold:
            return None

    return results


def calculate_sticker_price(ticker_symbol: str, cik: int, hist_equity_growth: Optional[float], fetcher: QuestDBFetcher) -> Dict:
    """
    Calculates Phil Town's Sticker Price and MOS.
    
    - Growth: Lower of hist_equity_growth and Synthetic (PEG) growth.
    - Future P/E: min(2 * Growth, Historical Median PE)
    - MARR: 15%
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        
        current_price = info.get("currentPrice")
        ttm_eps = info.get("trailingEps")
        
        # 1. Growth Rate
        # We calculate a "Synthetic 5Y Growth" from PEG ratio: Growth = PE / PEG
        synthetic_growth = None
        pe = info.get("trailingPE")
        peg = info.get("trailingPegRatio")
        
        if pe and peg and peg > 0:
            synthetic_growth = pe / peg
            
        # Use lower of historical equity growth and synthetic analyst growth
        growth_source = "Equity CAGR"
        growth_rate = hist_equity_growth or 0
        
        if synthetic_growth is not None:
             if synthetic_growth < growth_rate:
                 growth_rate = synthetic_growth
                 growth_source = "Synthetic (PEG)"
        
        # Cap growth at 25% for safety as Town suggests
        growth_rate = min(growth_rate, 25.0)

        # 2. Future P/E
        # Rule: min(Double the Growth Rate, Historical Median PE)
        hist_pe = fetcher.calculate_historical_pe(ticker_symbol, cik)
        future_pe = growth_rate * 2.0
        
        if hist_pe is not None and hist_pe > 0:
            if hist_pe < future_pe:
                future_pe = hist_pe
                pe_source = "Historical Median"
            else:
                pe_source = "2x Growth"
        else:
            pe_source = "2x Growth (No Hist PE)"
        
        # 3. Future EPS (10 years)
        future_eps = ttm_eps * ((1 + (growth_rate / 100)) ** 10)
        
        # 4. Future Value
        future_value = future_eps * future_pe
        
        # 5. Sticker Price (Discount back at 15%)
        sticker_results = fc.sticker_price(ttm_eps, growth_rate, future_pe)
        sticker_price = sticker_results["sticker_price"]
        mos_price = sticker_results["mos_price"]
        
        discount = ((sticker_price - current_price) / sticker_price) * 100

        return {
            "current_price": current_price,
            "eps": ttm_eps,
            "growth_rate": growth_rate,
            "growth_source": growth_source,
            "hist_equity_growth": hist_equity_growth,
            "synthetic_growth": synthetic_growth,
            "trailing_pe": pe,
            "peg_ratio": peg,
            "future_pe": future_pe,
            "pe_source": pe_source,
            "hist_pe": hist_pe,
            "sticker_price": sticker_price,
            "mos_price": mos_price,
            "discount": discount
        }
    except Exception as e:
        # print(f"Valuation error for {ticker_symbol}: {e}")
        return None


def format_match(ticker, name, industry, results, valuation=None):
    """Format a matching stock for display."""
    lines = []
    lines.append(f"\n✨ MATCH: {name} ({ticker})")
    lines.append(f"   Industry: {industry or 'N/A'}")
    lines.append(
        f"{'Metric':<20} {'TTM':<15} {'1Y (CAGR/Avg)':<15} {'5Y (CAGR/Avg)':<15} {'10Y (CAGR/Avg)':<15}"
    )
    lines.append("-" * 85)

    order = ["ROIC", "Equity", "Net Income", "Revenue"]

    for metric in order:
        vals = results.get(metric, {})
        is_growth = metric != "ROIC"

        def fmt(v):
            return fc.format_pct(v, show_sign=is_growth)

        ttm = fmt(vals.get("ttm"))
        v1 = fmt(vals.get("1y"))
        v5 = fmt(vals.get("5y"))
        v10 = fmt(vals.get("10y"))

        lines.append(f"{metric:<20} {ttm:<15} {v1:<15} {v5:<15} {v10:<15}")

    lines.append("-" * 85)
    
    if valuation:
        lines.append(f"📈 Rule #1 Valuation (Phil Town Sticker Price):")
        lines.append(f"   Current Price:  {fc.format_usd(valuation['current_price']):>8}")
        lines.append(f"   EPS (TTM):      {fc.format_usd(valuation['eps']):>8}")
        lines.append(f"   Growth Rate:    {fc.format_pct(valuation['growth_rate'], False):>8} ({valuation['growth_source']})")
        
        # Show components of the estimate
        syn = valuation.get('synthetic_growth')
        pe_val = valuation.get('trailing_pe')
        peg_val = valuation.get('peg_ratio')
        
        lines.append(f"   Historical CAGR: {fc.format_pct(valuation.get('hist_equity_growth'), False) if valuation.get('hist_equity_growth') else '     N/A'}")
        lines.append(f"   Synthetic (PEG): {fc.format_pct(syn, False) if syn is not None else '     N/A'} (PE: {pe_val:.1f} / PEG: {peg_val:.2f})")
             
        lines.append(f"   Future P/E:     {valuation['future_pe']:>8.2f} ({valuation['pe_source']})")
        lines.append(f"   Historical PE:  {f'{valuation['hist_pe']:>8.2f}' if valuation['hist_pe'] else '     N/A'}")
        lines.append(f"   STICKER PRICE:  {fc.format_usd(valuation['sticker_price']):>8}")
        lines.append(f"   MOS BUY PRICE:  {fc.format_usd(valuation['mos_price']):>8} (50% Margin of Safety)")
        
        disc = valuation['discount']
        status = "UNDERVALUED" if disc > 0 else "OVERVALUED"
        color = "🟢" if disc > 0 else "🔴"
        lines.append(f"   {color} Result:    {abs(disc):.1f}% {status} relative to Sticker Price")
        lines.append("-" * 85)
        
    return "\n".join(lines)


def main():
    print("🔎 Starting QuestDB-Powered Stock Screener (streaming mode)...")
    print("   Criteria: ROIC, Equity, Net Income, Revenue > 10% (TTM, 1Y, 5Y)")

    # Parse arguments
    limit = None
    stream_batch = 200

    args = sys.argv[1:]
    if args:
        try:
            limit = int(args[0])
        except ValueError:
            pass
    if len(args) > 1:
        try:
            stream_batch = int(args[1])
        except ValueError:
            pass

    # Load tickers
    try:
        df_tickers = get_company_tickers()
    except Exception as e:
        print(f"Failed to load tickers: {e}")
        return

    # Prepare stock list
    stocks_to_screen = []
    if "cik" in df_tickers.columns and "ticker" in df_tickers.columns:
        for _, row in df_tickers.iterrows():
            stocks_to_screen.append((row["ticker"], row["cik"], row.get("company", "")))

    print(f"📋 Found {len(stocks_to_screen)} total stocks available.")
    if limit:
        stocks_to_screen = stocks_to_screen[:limit]
        print(f"   Screening the first {limit} stocks (batch size: {stream_batch}).")
    else:
        print(f"   Screening ALL available stocks (batch size: {stream_batch}).")

    # Initialize
    fetcher = QuestDBFetcher()
    concepts = fetcher.get_screening_concepts()

    # Get all CIKs
    ciks = [cik for _, cik, _ in stocks_to_screen]
    cik_to_info = {cik: (ticker, name) for ticker, cik, name in stocks_to_screen}

    print(f"\n📊 Streaming data in batches of {stream_batch} companies...")
    print(f"   Concepts: {', '.join(concepts[:5])}...")

    # Screen stocks using streaming
    matches = []
    stocks_processed = 0

    for batch_df in fetcher.stream_financial_data(ciks, concepts, batch_size=stream_batch):
        # Get unique CIKs in this batch
        batch_ciks = batch_df.select("cik").unique().to_series().to_list()

        for cik in batch_ciks:
            if cik not in cik_to_info:
                continue

            ticker, entity_name = cik_to_info[cik]
            cik_df = batch_df.filter(pl.col("cik") == cik)

            results = check_stock(cik, ticker, entity_name, cik_df)

            if results:
                # Fetch industry and valuation only for matches to save time/API calls
                try:
                    comp = Company(cik)
                    industry = comp.industry
                except Exception:
                    industry = "N/A"
                
                # Rule #1 Valuation
                # Use 5Y Equity CAGR as historical growth baseline
                eq_growth = results.get("Equity", {}).get("5y")
                valuation = calculate_sticker_price(ticker, cik, eq_growth, fetcher)
                
                match_display = format_match(ticker, entity_name, industry, results, valuation)
                matches.append(ticker)
                print(match_display)  # Print immediately - streaming!

            stocks_processed += 1

        # Progress update
        sys.stdout.write(
            f"\r   Processed: {stocks_processed}/{len(ciks)} stocks, "
            f"Matches: {len(matches)}  "
        )
        sys.stdout.flush()

    print(f"\n\n✅ Screening Complete. Found {len(matches)} matches.")
    if matches:
        print(f"Tickers: {', '.join(matches)}")


if __name__ == "__main__":
    main()
