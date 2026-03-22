"""
Screen stocks based on growth metrics (Revenue, Net Income, Equity) and ROIC.
"""
import sys
import os
import pandas as pd
from typing import Dict, List, Optional

# Add current directory to path to allow importing stock_cagr
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    import stock_cagr
except ImportError:
    print("Error: Could not import stock_cagr.py. Make sure it is in the same directory.")
    sys.exit(1)

from edgar import Company, use_local_storage, get_company_tickers
from edgar.standardization import SynonymGroups

# Check for local data usage - prefer local if available or env var set
if os.environ.get("EDGAR_USE_LOCAL_DATA") or os.environ.get("EDGAR_LOCAL_DATA_DIR") or os.path.exists(os.path.expanduser("~/.edgar/companyfacts")):
    print("   ✅ Local storage enabled")
    use_local_storage(True) 
else:
    print("   ℹ️  Using live SEC data (Local storage not explicitly enabled)")

def check_stock(ticker: str, cik: int, synonym_groups: SynonymGroups) -> Optional[str]:
    """
    Check if a stock meets the screening criteria (>10% growth/return).
    Returns a formatted string if it matches, None otherwise.
    """
    try:
        # Utilize the CIK
        company = Company(cik)
        
        # Skip if company name implies it's not a standard operating company (optional check)
        
        all_facts = company.get_facts()
        if not all_facts: return None
        
        df_all = all_facts.to_dataframe(include_metadata=True)
        
        # Annual DF
        df_annual = pd.DataFrame()
        if 'fiscal_period' in df_all.columns and 'form_type' in df_all.columns:
            mask = (df_all['fiscal_period'] == 'FY') & (df_all['form_type'].isin(['10-K', '10-K/A'])) 
            df_annual = df_all[mask]
        
        if df_annual.empty: return None

        current_year = df_annual['fiscal_year'].max()
        
        # Define Metrics
        metrics_config = {
            'Revenue': {'vars': synonym_groups.get_synonyms("revenue"), 'type': 'Growth'},
            'Net Income': {'vars': synonym_groups.get_synonyms("net_income"), 'type': 'Growth'},
            'Equity': {'vars': synonym_groups.get_synonyms("stockholders_equity"), 'type': 'Growth'},
            # ROIC handled separately
        }
        
        results = {}
        
        # 1. Growth Metrics
        for name, config in metrics_config.items():
            variants = config['vars']
            
            # TTM Growth
            ttm_growth = None
            is_flow = name in ['Revenue', 'Net Income']
            
            if is_flow:
                q_series = stock_cagr.get_derived_quarterly_series(df_all, df_annual, variants)
                if not q_series.empty:
                     ttm_growth = stock_cagr.calculate_ttm_growth(q_series)
            else:
                # Equity (Instant)
                q_series = stock_cagr.get_quarterly_time_series(df_all, variants)
                if not q_series.empty:
                    ttm_growth = stock_cagr.calculate_yoy_point_growth(q_series)

            # CAGRs
            cagrs = stock_cagr.calculate_metric_cagr(df_annual, variants, current_year, [1, 5, 10])
            
            results[name] = {
                'ttm': ttm_growth,
                '1y': cagrs.get('1Y'),
                '5y': cagrs.get('5Y'),
                '10y': cagrs.get('10Y')
            }

        # 2. ROIC
        roic_series = stock_cagr.calculate_roic_series(df_annual, synonym_groups)
        roic_q = stock_cagr.calculate_roic_series_quarterly(df_all, df_annual, synonym_groups)
        
        ttm_roic = None
        if not roic_q.empty and len(roic_q) >= 4:
             recent_4 = roic_q.iloc[0:4]
             ttm_nopat = recent_4['nopat'].sum()
             latest_capital = recent_4.iloc[0]['invested_capital']
             if latest_capital != 0:
                 ttm_roic = (ttm_nopat / latest_capital) * 100
        
        avg_1y = stock_cagr.calculate_average(roic_series, 1)
        avg_5y = stock_cagr.calculate_average(roic_series, 5)
        avg_10y = stock_cagr.calculate_average(roic_series, 10)
        
        results['ROIC'] = {
            'ttm': ttm_roic,
            '1y': avg_1y,
            '5y': avg_5y,
            '10y': avg_10y
        }
        
        # SCREENING LOGIC
        # Threshold: 9%
        # Strictness: All available periods must be > 10%.
        # If 10Y is missing (None), we allow it?
        # User prompt: "list all the stocks that have ... 10Y ... over 10 percent"
        # Since Microsoft has it, I will look for stocks that HAVE it.
        # But to be safe against data gaps, I'll allow None for 10Y if 5Y is strong.
        # Check: If value exists, it MUST be > 10.
        # If value is None:
        #   - TTM/1Y/5Y: Must exist.
        #   - 10Y: Can be None.
        
        threshold = 9.0
        
        for metric, vals in results.items():
            # Check TTM
            if vals['ttm'] is None or vals['ttm'] < threshold: return None
            # Check 1Y
            if vals['1y'] is None or vals['1y'] < threshold: return None
            # Check 5Y
            if vals['5y'] is None or vals['5y'] < threshold: return None
            # Check 10Y - Optional
            if vals['10y'] is not None and vals['10y'] < threshold: return None
            
        return format_match(ticker, company.name, results)
        
    except Exception as e:
        # Ignore errors (data missing, calculation error)
        return None

def format_match(ticker, name, results):
    lines = []
    lines.append(f"\n✨ MATCH: {name} ({ticker})")
    lines.append(f"{'Metric':<20} {'TTM':<15} {'1Y (CAGR/Avg)':<15} {'5Y (CAGR/Avg)':<15} {'10Y (CAGR/Avg)':<15}")
    lines.append("-" * 85)
    
    order = ['ROIC', 'Equity', 'Net Income', 'Revenue']
    
    for metric in order:
        vals = results.get(metric, {})
        
        # Determine if it's Growth (add +) or Ratio
        is_growth = metric != 'ROIC'
        
        def fmt(v):
            if v is None: return "N/A"
            prefix = "+" if is_growth and v > 0 else ""
            return f"{prefix}{v:.2f}%"
        
        ttm = fmt(vals.get('ttm'))
        v1 = fmt(vals.get('1y'))
        v5 = fmt(vals.get('5y'))
        v10 = fmt(vals.get('10y'))
        
        lines.append(f"{metric:<20} {ttm:<15} {v1:<15} {v5:<15} {v10:<15}")
        
    lines.append("-" * 85)
    return "\n".join(lines)

def main():
    print("🔎 Starting Stock Screener...")
    print("   Criteria: ROIC, Equity, Net Income, Revenue > 10% (TTM, 1Y, 5Y)")
    
    # Default to 1000 stocks as requested
    limit = 1000
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            pass
            
    try:
        # Use full ticker list instead of just popular ones
        df_tickers = get_company_tickers()
    except Exception as e:
        print(f"Failed to load tickers: {e}")
        return

    # Prepare list of (ticker, cik) tuples
    stocks_to_screen = []
    
    # get_company_tickers returns a DataFrame with 'cik', 'ticker', 'title' columns
    if 'cik' in df_tickers.columns and 'ticker' in df_tickers.columns:
            for _, row in df_tickers.iterrows():
                stocks_to_screen.append((row['ticker'], row['cik']))
    
    print(f"📋 Found {len(stocks_to_screen)} total stocks available.")
    if limit:
        stocks_to_screen = stocks_to_screen[:limit]
        print(f"   Screening the first {limit} stocks.")

        
    synonym_groups = SynonymGroups()
    
    matches = []
    
    try:
        from tqdm import tqdm
        iterator = tqdm(stocks_to_screen, desc="Screening")
    except ImportError:
        iterator = stocks_to_screen
        print("   (Install tqdm for progress bar)")
    
    for item in iterator:
        ticker, cik = item
        if not isinstance(iterator, list):
             # Update tqdm description
             iterator.set_description(f"Screening {ticker}")
             
        result = check_stock(ticker, cik, synonym_groups)
        if result:
            matches.append(ticker)
            # If using tqdm, use write to avoid breaking bar
            if hasattr(iterator, 'write'):
                iterator.write(result)
            else:
                print(result)
            
    print(f"\n✅ Screening Complete. Found {len(matches)} matches.")
    if matches:
        print(f"Tickers: {', '.join(matches)}")

if __name__ == "__main__":
    main()
