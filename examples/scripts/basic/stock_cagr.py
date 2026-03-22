"""
Calculate Compound Annual Growth Rate (CAGR) for key financial metrics using Edgar Facts API.

This script demonstrates how to calculate 10-year, 5-year, and 1-year CAGR for:
- Revenue
- Net Income
- Total Assets
- Shareholders Equity

CAGR Formula: ((Ending Value / Beginning Value) ^ (1 / Number of Years)) - 1
"""

import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from edgar import Company, set_identity
from edgar.enums import PeriodType
from edgar.standardization import SynonymGroups
from financial_calcs import FinancialCalcs as fc

# Set identity as required by SEC
set_identity("Mikko Honkanen mikko.honkanen@example.com")

def get_metric_by_year(df: pd.DataFrame, concept: str, fiscal_year: int) -> Optional[float]:
    """
    Extract a specific metric value for a given fiscal year.
    
    Args:
        df: DataFrame with financial facts
        concept: Concept name to search for
        fiscal_year: Fiscal year to filter
        
    Returns:
        Numeric value or None if not found
    """
    # Filter by concept (case-insensitive partial match)
    mask = df['concept'].str.contains(concept, case=False, na=False)
    filtered = df[mask & (df['fiscal_year'] == fiscal_year)]
    
    if filtered.empty:
        return None
    
    # Get the most recent value for that year (in case of amendments)
    latest = filtered.sort_values('filing_date', ascending=False).iloc[0]
    return latest['numeric_value']

def get_annual_time_series(df: pd.DataFrame, concept_variants: List[str]) -> pd.DataFrame:
    """Helper to get time series for a list of concept variants."""
    all_results = []
    
    for concept in concept_variants:
        # Strict match
        mask = df['concept'].apply(lambda x: x.split(':')[-1] == concept if ':' in x else x == concept)
        filtered = df[mask].copy()
        
        if not filtered.empty:
            # Filter zero/null
            filtered = filtered[
                (filtered['numeric_value'].notna()) & 
                (filtered['numeric_value'] != 0)
            ]
            if filtered.empty: continue
            
            result_df = None
            
            # Ensure date columns are datetime
            if 'period_end' in filtered.columns:
                # print(f"DEBUG: Found period_end in columns: {filtered.columns}")
                pass
            
            if 'period_end' in filtered.columns:
                filtered['period_end'] = pd.to_datetime(filtered['period_end'])
                
                if 'filing_date' in filtered.columns:
                     filtered['filing_date'] = pd.to_datetime(filtered['filing_date'])
                     filtered = filtered.sort_values('filing_date', ascending=True)
                elif 'filed' in filtered.columns:
                     filtered['filed'] = pd.to_datetime(filtered['filed'])
                     filtered['filing_date'] = filtered['filed']
                     filtered = filtered.sort_values('filing_date', ascending=True)

                is_duration = False
                if 'period_type' in filtered.columns: 
                    # Check if the concept is duration or instant
                    # Usually homogenous for a single concept
                    if 'duration' in filtered['period_type'].values:
                        is_duration = True
                
                if is_duration and 'period_start' in filtered.columns:
                    filtered['period_start'] = pd.to_datetime(filtered['period_start'])
                    # Duration filter: Annual ~ 365 days
                    # This removes quarterly data tagged as FY
                    filtered['duration_days'] = (filtered['period_end'] - filtered['period_start']).dt.days
                    filtered = filtered[(filtered['duration_days'] >= 350) & (filtered['duration_days'] <= 375)]
                    
                    if filtered.empty: continue
                    
                    # Deduplicate by unique period (start, end), keeping the last (latest filing)
                    result_df = filtered.drop_duplicates(subset=['period_start', 'period_end'], keep='last').copy()
                else:
                    # Instant facts (Balance Sheet)
                    # Deduplicate by period_end, keeping the last (latest filing)
                    result_df = filtered.drop_duplicates(subset=['period_end'], keep='last').copy()

                if result_df is not None and not result_df.empty:
                    # Derived Year Alignment:
                    # For consistency across different concepts (some tagged fiscal, some calendar),
                    # derive year from the period_end date for annual alignment.
                    result_df['fiscal_year'] = result_df['period_end'].dt.year

            # Normalize and collect
            if result_df is not None and not result_df.empty:
                # Keep period_end for Q4 derivation
                result_df = result_df[['fiscal_year', 'period_end', 'numeric_value']].rename(
                    columns={'numeric_value': 'value'}
                )
                all_results.append(result_df)
                
    if not all_results:
        return pd.DataFrame()
        
    combined = pd.concat(all_results)
    # Deduplicate by fiscal_year, keeping the first (priority concept)
    combined = combined.drop_duplicates(subset=['fiscal_year'], keep='first')
    
    return combined.set_index('fiscal_year').sort_index(ascending=False)

def get_time_series_by_duration(df: pd.DataFrame, concept_variants: List[str], min_days: int, max_days: int) -> pd.DataFrame:
    """Helper to get time series for specific duration range."""
    all_results = []
    
    for concept in concept_variants:
        mask = df['concept'].apply(lambda x: x.split(':')[-1] == concept if ':' in x else x == concept)
        filtered = df[mask].copy()
        
        if not filtered.empty:
            filtered = filtered[
                (filtered['numeric_value'].notna()) & 
                (filtered['numeric_value'] != 0)
            ]
            if filtered.empty: continue
            
            result_df = None
            
            if 'period_end' in filtered.columns:
                filtered['period_end'] = pd.to_datetime(filtered['period_end'])
                
                # Deduplicate by filing date if available
                if 'filing_date' in filtered.columns:
                     filtered['filing_date'] = pd.to_datetime(filtered['filing_date'])
                     filtered = filtered.sort_values('filing_date', ascending=True)
                elif 'filed' in filtered.columns:
                     filtered['filed'] = pd.to_datetime(filtered['filed'])
                     filtered['filing_date'] = filtered['filed']
                     filtered = filtered.sort_values('filing_date', ascending=True)
                else:
                     # Create dummy filing date to avoid sorting errors if needed
                     filtered['filing_date'] = filtered['period_end']

                is_duration = False
                if 'period_type' in filtered.columns: 
                    if 'duration' in filtered['period_type'].values:
                        is_duration = True
                
                if is_duration and 'period_start' in filtered.columns:
                    filtered['period_start'] = pd.to_datetime(filtered['period_start'])
                    filtered['duration_days'] = (filtered['period_end'] - filtered['period_start']).dt.days
                    filtered = filtered[(filtered['duration_days'] >= min_days) & (filtered['duration_days'] <= max_days)]
                    
                    if filtered.empty: continue
                    
                    result_df = filtered.drop_duplicates(subset=['period_start', 'period_end'], keep='last').copy()
                else: 
                     # Allow 'instants' (point-in-time) facts. 
                     # These shouldn't be blocked by min_days if they are not durations.
                     result_df = filtered.drop_duplicates(subset=['period_end'], keep='last').copy()

                if result_df is not None and not result_df.empty:
                    cols = {'numeric_value': 'value'}
                    if 'period_end' not in result_df.columns:
                         result_df['period_end'] = pd.NaT
                    result_df = result_df[['fiscal_year', 'period_end', 'numeric_value']].rename(columns=cols)
                    all_results.append(result_df)
                
    if not all_results:
        return pd.DataFrame()
        
    combined = pd.concat(all_results)
    combined = combined.drop_duplicates(subset=['period_end'], keep='first')
    return combined.set_index('period_end').sort_index(ascending=False)  

def get_quarterly_time_series(df: pd.DataFrame, concept_variants: List[str]) -> pd.DataFrame:
    """Helper to get quarterly (3-month) time series."""
    # Use the consolidated duration helper with standard quarterly range (approx 90 days)
    return get_time_series_by_duration(df, concept_variants, 70, 105)

def get_summed_series(df_ts: pd.DataFrame, index_col: str) -> pd.DataFrame:
    """Sum multiple concepts for the same period (e.g. adding AP and Accrued)."""
    if df_ts.empty: return df_ts
    # Group by period and sum the 'value' column
    # Note: df_ts already has duplicates dropped PER CONCEPT in get_time_series, 
    # but here we want to sum ACROSS concepts for the same date.
    return df_ts.reset_index().groupby(index_col)['value'].sum().to_frame().sort_index(ascending=False)

def get_summed_annual_series(df: pd.DataFrame, concept_variants: List[str]) -> pd.DataFrame:
    """Fetch and sum multiple annual concepts for each year."""
    all_rows = []
    for concept in concept_variants:
        series = get_annual_time_series(df, [concept])
        if not series.empty:
            all_rows.append(series)
    if not all_rows: return pd.DataFrame()
    combined = pd.concat(all_rows)
    return get_summed_series(combined, 'fiscal_year')

def get_summed_quarterly_series(df: pd.DataFrame, concept_variants: List[str]) -> pd.DataFrame:
    """Fetch and sum multiple quarterly concepts for each period end."""
    all_rows = []
    for concept in concept_variants:
        series = get_quarterly_time_series(df, [concept])
        if not series.empty:
            all_rows.append(series)
    if not all_rows: return pd.DataFrame()
    combined = pd.concat(all_rows)
    return get_summed_series(combined, 'period_end')

def derive_quarters_from_ytd(q_df: pd.DataFrame, df_all: pd.DataFrame, concepts: List[str]) -> pd.DataFrame:
    """Derive missing Q2 and Q3 discrete values from YTD data (Q2 YTD, Q3 YTD)."""
    # Get YTD Series
    ytd_q2 = get_time_series_by_duration(df_all, concepts, 170, 190) # ~180 days
    ytd_q3 = get_time_series_by_duration(df_all, concepts, 260, 290) # ~270 days
    
    derived_rows = []
    
    # 1. Derive Q2 = YTD_Q2 - Q1
    for date, row in ytd_q2.iterrows():
        # Check if we already have Q2
        mask = (q_df.index >= (date - pd.Timedelta(days=10))) & (q_df.index <= (date + pd.Timedelta(days=10)))
        if not q_df[mask].empty: continue
        
        # Need matching Q1 (Date - 3 months approx)
        # Q1 ends ~90 days before Q2 end
        q1_target = date - pd.Timedelta(days=90)
        # Look for Q1 in existing q_df
        # Tolerant search
        found_q1 = None
        for q_date in q_df.index:
            if abs((q_date - q1_target).days) < 20: 
                found_q1 = q_df.loc[q_date, 'value']
                break
        
        if found_q1 is not None:
             q2_val = row['value'] - found_q1
             derived_rows.append({'period_end': date, 'value': q2_val})
             
    # 2. Derive Q3 = YTD_Q3 - YTD_Q2
    for date, row in ytd_q3.iterrows():
         # Check if we already have Q3
        mask = (q_df.index >= (date - pd.Timedelta(days=10))) & (q_df.index <= (date + pd.Timedelta(days=10)))
        if not q_df[mask].empty: continue
        
        # Need matching YTD_Q2 (Date - 3 months approx)
        ytd_q2_target = date - pd.Timedelta(days=90)
        
        found_ytd_q2 = None
        for q2_date in ytd_q2.index:
             if abs((q2_date - ytd_q2_target).days) < 20:
                 found_ytd_q2 = ytd_q2.loc[q2_date, 'value']
                 break
        
        if found_ytd_q2 is not None:
            q3_val = row['value'] - found_ytd_q2
            derived_rows.append({'period_end': date, 'value': q3_val})
            
    if derived_rows:
        derived_df = pd.DataFrame(derived_rows).set_index('period_end')
        q_df = pd.concat([q_df, derived_df])
        q_df = q_df.sort_index(ascending=False)
        
    return q_df


    return q_df

def get_derived_quarterly_series(df_all: pd.DataFrame, df_annual: pd.DataFrame, variants: List[str]) -> pd.DataFrame:
    """
    Get quarterly series with full derivation chain:
    1. Fetch Discrete Quarterly (Q1, Q2, Q3, Q4)
    2. Derive discrete Q2/Q3 from YTD data if missing
    3. Derive Q4 from Annual Total - (Q1+Q2+Q3) if missing
    """
    # Fetch Annual Series for derivation
    annual = get_annual_time_series(df_annual, variants)
    
    # Fetch Quarterly Series (Discrete 3m)
    quarterly = get_quarterly_time_series(df_all, variants)
    
    # 1. Derive from YTD
    quarterly = derive_quarters_from_ytd(quarterly, df_all, variants)
    
    # 2. Derive Q4s
    quarterly = derive_quarterly_q4(quarterly, annual)
    
    return quarterly

def derive_quarterly_q4(quarterly_df: pd.DataFrame, annual_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive Q4 data for Flow metrics (Revenue, Income) by subtracting Q1-Q3 from Annual Total.
    Q4 = Annual - (Q1 + Q2 + Q3)
    """
    if annual_df.empty:
        return quarterly_df
        
    # Ensure quarterly_df has a copy we can modify
    q_df = quarterly_df.copy()
    if q_df.empty:
         q_df = pd.DataFrame(columns=['value']) # Index is period_end
    
    # List to collect derived quarters
    derived_rows = []
    
    for year, row in annual_df.iterrows():
        fy_end = row.get('period_end')
        annual_val = row['value']
        
        if pd.isna(fy_end): 
            continue
            
        # Check if we already have a quarter roughly at fy_end (allow +/- 10 days margin)
        # Typically 10-K date matches Q4 date exactly or very close
        # If explicitly present, we TRUST the explicit Q4 over derivation
        
        # Look for existing data point within 20 days of fy_end
        mask = (q_df.index >= (fy_end - pd.Timedelta(days=20))) & (q_df.index <= (fy_end + pd.Timedelta(days=20)))
        if not q_df[mask].empty:
            continue
            
        # Look for Q1, Q2, Q3 (Period ends within [fy_end - 300 days, fy_end - 60 days])
        # A quarter is ~90 days. 3 quarters ~ 270 days.
        search_start = fy_end - pd.Timedelta(days=300)
        search_end = fy_end - pd.Timedelta(days=60)
         
        # Find quarters in this window
        mask_quarters = (q_df.index >= search_start) & (q_df.index <= search_end)
        found_quarters = q_df[mask_quarters]
        
        # We expect 3 quarters. 
        # Sometimes short fiscal years happen, but for robust standard calc, require 3.
        # Note: If we just derived Q2/Q3 from YTD, we should find them here!
        if len(found_quarters) == 3:
            sum_q1_q3 = found_quarters['value'].sum()
            q4_val = annual_val - sum_q1_q3
            
            # Sanity check: Q4 shouldn't be wildly negative if Annual is positive (unless seasonal loss)
            # But we accept math is math.
            derived_rows.append({'period_end': fy_end, 'value': q4_val})
            
    if derived_rows:
        derived_df = pd.DataFrame(derived_rows).set_index('period_end')
        # Combine and sort
        q_df = pd.concat([q_df, derived_df])
        q_df = q_df.sort_index(ascending=False)
        
    return q_df

def calculate_ttm_sum(series: pd.DataFrame) -> Optional[float]:
    """Calculate Sum of Last 4 Quarters."""
    if len(series) < 4:
        return None
    return series.iloc[0:4]['value'].sum()


def calculate_metric_cagr_from_series(time_series: pd.DataFrame, 
                                      current_year: int, 
                                      periods: List[int] = [1, 5, 10]) -> Dict[str, Optional[float]]:
    """Calculate CAGR for different time periods using a provided time series."""
    if time_series.empty:
        return {f"{p}Y": None for p in periods}
    
    results = {}
    
    for period in periods:
        start_year = current_year - period
        
        # Check if years exist in index
        if current_year in time_series.index and start_year in time_series.index:
            end_value = time_series.loc[current_year, 'value']
            start_value = time_series.loc[start_year, 'value']
            
            # For CAGR, start value must be positive (and non-zero)
            # If start value is massive negative and end is positive, CAGR formula fails or is misleading.
            # Standard CAGR formula technically requires positive start/end.
            if start_value > 0 and end_value > 0:
                cagr = fc.cagr(start_value, end_value, period)
                results[f"{period}Y"] = cagr
            else:
                results[f"{period}Y"] = None
        else:
            results[f"{period}Y"] = None
    
    return results

def calculate_metric_cagr(df: pd.DataFrame, 
                         concept_variants: List[str],
                         current_year: int,
                         periods: List[int] = [1, 5, 10]) -> Dict[str, Optional[float]]:
    """Calculate CAGR for different time periods."""
    time_series = get_annual_time_series(df, concept_variants)
    return calculate_metric_cagr_from_series(time_series, current_year, periods)
    
def calculate_free_cash_flow_series(df: pd.DataFrame, synonym_groups) -> pd.DataFrame:
    """Calculate Free Cash Flow (OCF - CapEx) time series."""
    ocf = get_annual_time_series(df, synonym_groups.get_synonyms("operating_cash_flow"))
    capex = get_annual_time_series(df, synonym_groups.get_synonyms("capex"))
    
    if ocf.empty:
        return pd.DataFrame()
        
    # Align series
    # Using OCF as the base
    years = ocf.index
    
    fcf_data = []
    for year in years:
        ocf_val = ocf.loc[year, 'value']
        
        # Get CapEx for the year, defaulting to 0 if not found
        capex_val = 0.0
        if not capex.empty and year in capex.index:
             capex_val = capex.loc[year, 'value']
        
        # FCF = OCF - CapEx
        fcf_val = ocf_val - capex_val
        fcf_data.append({'fiscal_year': year, 'value': fcf_val})
        
    return pd.DataFrame(fcf_data).set_index('fiscal_year').sort_index(ascending=False)


def calculate_free_cash_flow_series_quarterly(df_all: pd.DataFrame, df_annual: pd.DataFrame, synonym_groups) -> pd.DataFrame:
    """Calculate Free Cash Flow (OCF - CapEx) quarterly time series."""
    ocf_vars = synonym_groups.get_synonyms("operating_cash_flow")
    capex_vars = synonym_groups.get_synonyms("capex")
    
    # Fetch Annual Series for derivation
    ocf_annual = get_annual_time_series(df_annual, ocf_vars)
    capex_annual = get_annual_time_series(df_annual, capex_vars)
    
    # Fetch Derived Quarterly Series
    ocf = get_derived_quarterly_series(df_all, df_annual, ocf_vars)
    capex = get_derived_quarterly_series(df_all, df_annual, capex_vars)
    
    # Derive Q2/Q3 from YTD, then Q4 from Annual
    # 1. Derive from YTD
    ocf = derive_quarters_from_ytd(ocf, df_all, ocf_vars)
    capex = derive_quarters_from_ytd(capex, df_all, capex_vars)
    
    # 2. Derive Q4s
    ocf = derive_quarterly_q4(ocf, ocf_annual)
    capex = derive_quarterly_q4(capex, capex_annual)
    
    if ocf.empty:
        return pd.DataFrame()
        
    # Align series
    dates = ocf.index
    
    fcf_data = []
    for date in dates:
        ocf_val = ocf.loc[date, 'value']
        
        capex_val = 0.0
        if not capex.empty and date in capex.index:
             capex_val = capex.loc[date, 'value']
        
        fcf_val = ocf_val - capex_val
        fcf_data.append({'period_end': date, 'value': fcf_val})
        
    return pd.DataFrame(fcf_data).set_index('period_end').sort_index(ascending=False)
def get_val(series: pd.DataFrame, key) -> float:
    """Safely get numeric value from a DataFrame series."""
    if key in series.index:
        val = series.loc[key, 'value']
        # Handle duplicate indices or Series results
        if isinstance(val, pd.Series):
             return float(val.iloc[0])
        return float(val)
    return 0.0

def calculate_roic_series(df: pd.DataFrame, synonym_groups) -> pd.DataFrame:
    """Calculate ROIC (Return on Invested Capital) time series."""
    # Fetch components
    op_inc = get_annual_time_series(df, synonym_groups.get_synonyms("operating_income"))
    restructuring = get_summed_annual_series(df, synonym_groups.get_synonyms("restructuring_charges"))
    
    tax_exp = get_annual_time_series(df, synonym_groups.get_synonyms("income_tax_expense"))
    inc_pre_tax = get_annual_time_series(df, synonym_groups.get_synonyms("income_before_tax"))
    reported_tax_rate = get_annual_time_series(df, synonym_groups.get_synonyms("effective_tax_rate"))
    
    # Components for simple Invested Capital
    assets = get_annual_time_series(df, synonym_groups.get_synonyms("total_assets"))
    ap_accrued = get_summed_annual_series(df, synonym_groups.get_synonyms("accounts_payable") + synonym_groups.get_synonyms("accrued_liabilities"))
    
    # Align by years
    years = sorted(op_inc.index.intersection(assets.index), reverse=True)
    
    roic_data = []
    tax_source_info = {'found': 0, 'total': 0, 'details': {}}
    for year in years:
        try:
            # 1. Tax Rate
            v_tax = tax_exp.loc[year, 'value'] if year in tax_exp.index else 0
            v_pre = inc_pre_tax.loc[year, 'value'] if year in inc_pre_tax.index else 0
            
            tax_rate = 0.21
            found = False
            source = "Statutory"
            
            if v_pre != 0 and v_tax != 0:
                tax_rate = v_tax / v_pre
                tax_source_info['found'] += 1
                found = True
                source = "Calculated"
            elif year in reported_tax_rate.index:
                tax_rate = reported_tax_rate.loc[year, 'value']
                if tax_rate > 1.0: tax_rate /= 100.0
                tax_source_info['found'] += 1
                found = True
                source = "Reported"
                
            tax_rate = max(0.0, min(0.5, tax_rate))
            tax_source_info['total'] += 1
            tax_source_info['details'][year] = {
                'tax': v_tax if v_tax != 0 else None, 
                'pre_tax': v_pre if v_pre != 0 else None, 
                'rate': tax_rate, 'fallback': not found, 'source': source
            }
            
            # 2. NOPAT
            v_op = get_val(op_inc, year)
            v_restr = get_val(restructuring, year)
            adj_op = v_op + v_restr
            
            # --- Simple Invested Capital ---
            def calc_ic(y):
                v_ass = get_val(assets, y)
                if v_ass == 0: return 0.0
                v_apa = get_val(ap_accrued, y)
                return max(0, v_ass - v_apa)

            invested_capital = calc_ic(year)
            avg_invested_capital = invested_capital
            prior_year = year - 1
            if prior_year in assets.index:
                prior_ic = calc_ic(prior_year)
                if prior_ic > 0:
                    avg_invested_capital = (invested_capital + prior_ic) / 2

            if avg_invested_capital != 0:
                roic = fc.roic(adj_op, tax_rate, avg_invested_capital)
                roic_data.append({'fiscal_year': year, 'period_end': assets.loc[year, 'period_end'], 'value': roic})
                
        except Exception:
            continue
            
    if not roic_data:
        return pd.DataFrame(), tax_source_info
        
    return pd.DataFrame(roic_data).sort_values('fiscal_year', ascending=False), tax_source_info

def calculate_roic_series_quarterly(df_all: pd.DataFrame, df_annual: pd.DataFrame, synonym_groups) -> pd.DataFrame:
    """Calculate Quarterly ROIC time series."""
    # Fetch components
    op_inc = get_derived_quarterly_series(df_all, df_annual, synonym_groups.get_synonyms("operating_income"))
    restructuring = get_summed_quarterly_series(df_all, synonym_groups.get_synonyms("restructuring_charges"))
    
    tax_exp = get_derived_quarterly_series(df_all, df_annual, synonym_groups.get_synonyms("income_tax_expense"))
    inc_pre_tax = get_derived_quarterly_series(df_all, df_annual, synonym_groups.get_synonyms("income_before_tax"))
    reported_tax_rate = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("effective_tax_rate"))
    
    # GuruFocus Components
    assets = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("total_assets"))
    ap_accrued = get_summed_quarterly_series(df_all, synonym_groups.get_synonyms("accounts_payable") + synonym_groups.get_synonyms("accrued_liabilities"))
    
    cash = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("cash_and_equivalents"))
    st_inv = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("short_term_investments"))
    curr_assets = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("total_current_assets"))
    curr_liab = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("total_current_liabilities"))
    
    st_debt_df = get_summed_quarterly_series(df_all, synonym_groups.get_synonyms("short_term_debt"))
    
    # Ensure unique indices
    op_inc = op_inc[~op_inc.index.duplicated(keep='first')]
    assets = assets[~assets.index.duplicated(keep='first')]
    
    # Align
    dates = op_inc.index.intersection(assets.index)
    
    roic_data = []
    tax_source_info = {'found': 0, 'total': 0, 'details': {}}
    for date in dates:
        try:
            tax = get_val(tax_exp, date)
            pre_tax = get_val(inc_pre_tax, date)
            
            tax_rate = 0.21
            found = False
            source = "Statutory"
            
            if pre_tax != 0 and tax != 0:
                tax_rate = tax / pre_tax
                tax_source_info['found'] += 1
                found = True
                source = "Calculated"
            elif date in reported_tax_rate.index:
                tax_rate = get_val(reported_tax_rate, date)
                if tax_rate > 1.0: tax_rate = tax_rate / 100.0
                tax_source_info['found'] += 1
                found = True
                source = "Reported"
                
            tax_rate = max(0.0, min(0.5, tax_rate))
            
            # --- GuruFocus Invested Capital ---
            def calc_ic(d):
                v_assets = get_val(assets, d)
                if v_assets == 0: return 0.0
                
                v_curr_liab = get_val(curr_liab, d)
                v_st_debt = get_val(st_debt_df, d)
                nibcl = max(0, v_curr_liab - v_st_debt)
                
                v_apa = get_val(ap_accrued, d)
                # Hybrid NIBCL logic
                if v_apa < (0.5 * nibcl) and nibcl > 0:
                    effective_apa = nibcl
                else:
                    effective_apa = v_apa if v_apa > 0 else nibcl
                
                v_cash = get_val(cash, d)
                v_st_inv = get_val(st_inv, d)
                v_curr_assets = get_val(curr_assets, d)
                
                return fc.invested_capital_gurufocus(
                    total_assets=v_assets,
                    accounts_payable=effective_apa,
                    accrued_expense=0, 
                    cash_and_marketable_securities=v_cash + v_st_inv,
                    total_current_assets=v_curr_assets,
                    total_current_liabilities=v_curr_liab
                )

            invested_capital = calc_ic(date)
            
            # --- Average Invested Capital ---
            avg_invested_capital = invested_capital
            
            # Find date exactly 1 year ago
            prior_date = None
            try:
                target = date - pd.DateOffset(years=1)
                diffs = (assets.index - target).abs().days
                if diffs.min() < 10:
                    prior_date = assets.index[diffs.argmin()]
            except: pass

            if prior_date is not None:
                prior_ic = calc_ic(prior_date)
                avg_invested_capital = (invested_capital + prior_ic) / 2

            if avg_invested_capital != 0:
                v_op = get_val(op_inc, date)
                v_restr = get_val(restructuring, date)
                adj_op = v_op + v_restr
                
                nopat = adj_op * (1 - tax_rate)
                roic = fc.roic(adj_op, tax_rate, avg_invested_capital)
                
                roic_data.append({
                    'period_end': date, 
                    'value': roic,
                    'nopat': nopat,
                    'invested_capital': invested_capital,
                    'avg_invested_capital': avg_invested_capital
                })
            
        except Exception:
            continue
            
    if not roic_data:
        return pd.DataFrame(), tax_source_info
        
    return pd.DataFrame(roic_data).set_index('period_end').sort_index(ascending=False), tax_source_info


def main():
    print("=" * 80)
    print("Stock Financial Metrics: CAGR & ROIC Calculator")
    print("=" * 80)
    
    print("=" * 80)
    
    ticker = "AAPL"
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        
    company = Company(ticker)
    print(f"\n📊 Company: {company.name} ({ticker})")
    
    print("\n⏳ Fetching all financial facts from SEC...")
    all_facts = company.get_facts()
    if not all_facts: return
    
    df_all = all_facts.to_dataframe(include_metadata=True)
    
    # Create annual filtered DF for existing logic
    df_annual = pd.DataFrame()
    if 'fiscal_period' in df_all.columns and 'form_type' in df_all.columns:
        mask = (df_all['fiscal_period'] == 'FY') & (df_all['form_type'].isin(['10-K', '10-K/A'])) 
        df_annual = df_all[mask]
        
    current_year = df_annual['fiscal_year'].max() if not df_annual.empty else datetime.now().year
    print(f"📅 Most recent fiscal year: {current_year}")
    
    synonym_groups = SynonymGroups()
    
    # Growth Metrics (Expect CAGR)
    growth_metrics = {
        'Revenue': synonym_groups.get_synonyms("revenue"),
        'Net Income': synonym_groups.get_synonyms("net_income"),
        'Equity': synonym_groups.get_synonyms("stockholders_equity"),
        # 'Operating Cash Flow': synonym_groups.get_synonyms("operating_cash_flow") - Replaced by FCF
    }
    
    results = []
    
    print("\n" + "=" * 80)
    print("Growth Analysis (CAGR)")
    print("=" * 80)
    
    # Process Growth Metrics
    for name, variants in growth_metrics.items():
        print(f"\n📈 {name}")
        series = get_annual_time_series(df_annual, variants)
        
        ttm_growth = None
        # Calculate TTM Growth using full df (quarterly)
        
        # Derive Q4/YTD if it's a flow metric (Revenue, Net Income)
        is_flow = name in ['Revenue', 'Net Income'] 
        if is_flow:
             q_series = get_derived_quarterly_series(df_all, df_annual, variants)
        else:
             q_series = get_quarterly_time_series(df_all, variants)
            
        if not q_series.empty:
             if name == 'Equity':
                 ttm_growth = fc.yoy_growth(q_series.iloc[0]['value'], q_series.iloc[4]['value']) if len(q_series) >= 5 else None
             else:
                 ttm_growth = fc.ttm_growth(q_series.iloc[0:4]['value'], q_series.iloc[4:8]['value']) if len(q_series) >= 8 else None
        
        if not series.empty:
            # Show values
            print(f"   Recent values:")
            for year, row in series.head(11).iterrows():
                print(f"      {int(year)}: {fc.format_usd(row['value'])}")
                
            if not q_series.empty:
                print(f"   Recent Quarterly values:")
                for date, row in q_series.head(8).iterrows():
                    print(f"      {date.strftime('%Y-%m-%d')}: {fc.format_usd(row['value'])}")
                
            # Calc CAGR
            cagrs = calculate_metric_cagr(df_annual, variants, current_year, [1, 5, 10])
            results.append({
                'Metric': name,
                'Type': 'Growth',
                '1Y': cagrs.get('1Y'),
                '5Y': cagrs.get('5Y'),
                '10Y': cagrs.get('10Y'),
                'TTM': ttm_growth,
                'Latest': series.iloc[0]['value']
            })
            print(f"   CAGR: 1Y: {fc.format_pct(cagrs.get('1Y'))}, 5Y: {fc.format_pct(cagrs.get('5Y'))}, 10Y: {fc.format_pct(cagrs.get('10Y'))}")
            print(f"   TTM Growth: {fc.format_pct(ttm_growth)}")
        else:
            print("   ⚠️ No data")

    # Process Free Cash Flow
    print(f"\n📈 Free Cash Flow")
    fcf_series = calculate_free_cash_flow_series(df_annual, synonym_groups)
    
    # Calculate TTM FCF Growth
    ttm_fcf_growth = None
    fcf_q_series = calculate_free_cash_flow_series_quarterly(df_all, df_annual, synonym_groups)
    if not fcf_q_series.empty and len(fcf_q_series) >= 8:
        ttm_fcf_growth = fc.ttm_growth(fcf_q_series.iloc[0:4]['value'], fcf_q_series.iloc[4:8]['value'])
    
    if not fcf_series.empty:
        # Show values
        print(f"   Recent values:")
        for year, row in fcf_series.head(11).iterrows():
            print(f"      {int(year)}: {fc.format_usd(row['value'])}")
            
        if not fcf_q_series.empty:
            print(f"   Recent Quarterly values:")
            for date, row in fcf_q_series.head(8).iterrows():
                 print(f"      {date.strftime('%Y-%m-%d')}: {fc.format_usd(row['value'])}")
            
        # Calc CAGR
        cagrs = calculate_metric_cagr_from_series(fcf_series, current_year, [1, 5, 10])
        results.append({
            'Metric': 'Free Cash Flow',
            'Type': 'Growth',
            '1Y': cagrs.get('1Y'),
            '5Y': cagrs.get('5Y'),
            '10Y': cagrs.get('10Y'),
            'TTM': ttm_fcf_growth,
            'Latest': fcf_series.iloc[0]['value']
        })
        print(f"   CAGR: 1Y: {fc.format_pct(cagrs.get('1Y'))}, 5Y: {fc.format_pct(cagrs.get('5Y'))}, 10Y: {fc.format_pct(cagrs.get('10Y'))}")
        print(f"   TTM Growth: {fc.format_pct(ttm_fcf_growth)}")
    else:
        print("   ⚠️ No data")
    
    # Store FCF metrics for DCF
    fcf_5y_cagr = cagrs.get('5Y') if not fcf_series.empty else None
    ttm_fcf_value = None
    if not fcf_q_series.empty and len(fcf_q_series) >= 4:
        ttm_fcf_value = fcf_q_series.iloc[0:4]['value'].sum()

    print("\n" + "=" * 80)
    print("Profitability Analysis (ROIC)")
    print("=" * 80)
    
    # Process ROIC
    print(f"\n💰 Return on Invested Capital (ROIC)")
    roic_series, tax_info_annual = calculate_roic_series(df_annual, synonym_groups)
    
    # Calculate TTM ROIC Value (Annualized)
    ttm_roic_val = None
    roic_q_series, tax_info_q = calculate_roic_series_quarterly(df_all, df_annual, synonym_groups)
    
    # Report Tax Data Source Status
    total_tax_points = tax_info_annual['total'] + tax_info_q['total']
    found_tax_points = tax_info_annual['found'] + tax_info_q['found']
    
    if total_tax_points > 0:
        if found_tax_points == total_tax_points:
            print("   ✅ Explicit tax expense data found for all periods.")
        elif found_tax_points > 0:
            pct = (found_tax_points / total_tax_points) * 100
            print(f"   ⚠️ Mixed tax data: Found explicit values for {found_tax_points}/{total_tax_points} ({pct:.0f}%) periods.")
            print(f"      Others using fallback 21.00% rate.")
        else:
            print("   ℹ️ No explicit tax expense found. Using fallback statutory rate (21.00%) for all periods.")
            
        print(f"\n   Tax Data Audit (Annual):")
        print(f"      {'Year':<6} {'Tax Expense':<15} {'Pre-Tax Income':<15} {'Rate':<10} {'Source'}")
        for year in sorted(tax_info_annual['details'].keys(), reverse=True):
            d = tax_info_annual['details'][year]
            s_tax = fc.format_usd(d['tax']) if d['tax'] is not None else "N/A"
            s_pre = fc.format_usd(d['pre_tax']) if d['pre_tax'] is not None else "N/A"
            s_rate = f"{d['rate']*100:.2f}%"
            s_src = d['source']
            print(f"      {int(year):<6} {s_tax:<15} {s_pre:<15} {s_rate:<10} {s_src}")
    
    if not roic_q_series.empty and len(roic_q_series) >= 4:
        # Match Annual Method: Sum(Last 4 Q NOPAT) / Average Invested Capital (Latest vs 1 Year Ago)
        recent_4 = roic_q_series.iloc[0:4]
        ttm_nopat = recent_4['nopat'].sum()
        
        latest_cap = recent_4.iloc[0]['invested_capital']
        # Latest quarter - 4 is approx 1 year ago
        prior_cap = roic_q_series.iloc[4]['invested_capital'] if len(roic_q_series) > 4 else latest_cap
        
        avg_cap = (latest_cap + prior_cap) / 2
        if avg_cap != 0:
            ttm_roic_val = (ttm_nopat / avg_cap) * 100

    print(f"DEBUG: roic_q_series length: {len(roic_q_series)}")
    
    if not roic_series.empty:
        print(f"   Recent values:")
        for _, row in roic_series.head(11).iterrows():
            print(f"      {int(row['fiscal_year'])}: {row['value']:.2f}%")
            
        if not roic_q_series.empty:
            print(f"   Recent Quarterly values:")
            for date, row in roic_q_series.head(8).iterrows():
                print(f"      {date.strftime('%Y-%m-%d')}: {row['value']:.2f}%")
            
        avg_1y = roic_series.iloc[0]['value'] if len(roic_series) >= 1 else None
        avg_5y = roic_series.head(5)['value'].mean() if len(roic_series) >= 3 else None
        avg_10y = roic_series.head(10)['value'].mean() if len(roic_series) >= 5 else None
        
        results.append({
            'Metric': 'ROIC',
            'Type': 'Ratio',
            '1Y': avg_1y, # 1Y Average is just the value
            '5Y': avg_5y,
            '10Y': avg_10y,
            'TTM': ttm_roic_val,
            'Latest': roic_series.iloc[0]['value']
        })
        print(f"   Averages: 1Y: {avg_1y:.2f}%, 5Y: {avg_5y:.2f}%, 10Y: {avg_10y:.2f}%")
        if ttm_roic_val:
            print(f"   TTM Average: {ttm_roic_val:.2f}%")
    else:
        print("   ⚠️ Insufficient data to calculate ROIC")

    # Estimated Future Growth (SGR)
    print("\n" + "=" * 80)
    print("Estimated Future Growth (Sustainable Growth Rate)")
    print("=" * 80)
    print("SGR = (Net Income / Equity) * (1 - Dividends / Net Income)")
    
    df_divs = get_derived_quarterly_series(df_all, df_annual, synonym_groups.get_synonyms("dividends_paid"))
    df_ni = get_derived_quarterly_series(df_all, df_annual, synonym_groups.get_synonyms("net_income"))
    df_equity = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("stockholders_equity"))
    
    display_sgr = None
    if not df_divs.empty and not df_ni.empty and not df_equity.empty:
        # Require 4 quarters for TTM
        if len(df_divs) >= 4 and len(df_ni) >= 4:
            ttm_div = df_divs.iloc[0:4]['value'].sum()
            ttm_ni = df_ni.iloc[0:4]['value'].sum()
            
            # Determine Equity Denominator
            # Ideally: Beginning Equity (1 Year Ago) ensures SGR = Growth of Capital Base
            # Fallback: Average Equity
            equity_denom = df_equity.iloc[0]['value']
            denom_label = "Latest Equity"
            
            if len(df_equity) >= 5:
                beg_equity = df_equity.iloc[4]['value'] # 4 quarters ago
                equity_denom = beg_equity
                denom_label = "Beginning Equity"
            elif len(df_equity) >= 2:
                 # Fallback to average of what we have (e.g. latest and prev)
                 equity_denom = df_equity.iloc[0:2]['value'].mean()
                 denom_label = "Average Equity"
            
            if ttm_ni <= 0:
                display_sgr = "   TTM Sustainable Growth Rate: 0.00% (Net Income is zero or negative)"
            elif equity_denom > 0:
                # SGR = ROE * Retention Ratio
                roe = ttm_ni / equity_denom
                payout_ratio = ttm_div / ttm_ni
                retention_ratio = 1 - payout_ratio
                
                ttm_sgr = (roe * retention_ratio) * 100
                
                # Format components for display
                # Billions
                ni_b = ttm_ni / 1e9
                div_b = ttm_div / 1e9
                eq_b = equity_denom / 1e9
                
                display_sgr = (
                    f"   TTM Sustainable Growth Rate: {ttm_sgr:.2f}%\n"
                    f"   Formula: ROE * Retention Ratio\n"
                    f"   Components: ROE: {(roe*100):.2f}% (NI ${ni_b:.2f}B / {denom_label} ${eq_b:.2f}B) \n"
                    f"               * Retention: {(retention_ratio*100):.2f}% (1 - Div ${div_b:.2f}B / NI ${ni_b:.2f}B)"
                )

    if display_sgr:
         print(display_sgr)
    else:
         print("   ⚠️ Insufficient data to calculate SGR")
         
    # Intrinsic Value Analysis (DCF)
    print("\n" + "=" * 80)
    print("Intrinsic Value Analysis (Discounted Cash Flow)")
    print("=" * 80)
    
    # Inputs
    df_cash = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("cash_and_equivalents"))
    df_shares = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("common_shares_outstanding"))
    # Debt (re-using concept lookup)
    df_lt_debt = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("long_term_debt"))
    df_st_debt = get_quarterly_time_series(df_all, synonym_groups.get_synonyms("short_term_debt"))
    
    latest_cash = df_cash.iloc[0]['value'] if not df_cash.empty else 0
    latest_shares = df_shares.iloc[0]['value'] if not df_shares.empty else 0
    
    latest_debt = 0
    if not df_lt_debt.empty: latest_debt += df_lt_debt.iloc[0]['value']
    if not df_st_debt.empty: latest_debt += df_st_debt.iloc[0]['value']
    
    # DCF Parameters
    discount_rate = 0.15
    terminal_growth = 0.03
    growth_rate_5y = fcf_5y_cagr if fcf_5y_cagr is not None else 0
    
    # Assumptions Check
    if ttm_fcf_value and latest_shares > 0:
        dcf_results = fc.dcf_valuation(
            base_fcf=ttm_fcf_value,
            growth_rate_pct=growth_rate_5y,
            discount_rate=discount_rate,
            terminal_growth=terminal_growth,
            shares=latest_shares,
            cash=latest_cash,
            debt=latest_debt
        )
        
        print(f"DCF Assumptions:")
        print(f"   Baseline FCF (TTM): {fc.format_usd(ttm_fcf_value)}")
        print(f"   Growth Rate (Years 1-10): {growth_rate_5y:.2f}% (Based on 5Y FCF CAGR)")
        print(f"   Discount Rate: {discount_rate*100:.2f}%")
        print(f"   Terminal Growth Rate: {terminal_growth*100:.2f}%")
        print(f"   Shares Outstanding: {latest_shares/1e9:.3f}B")
        print(f"   Net Debt: {fc.format_usd(latest_debt - latest_cash)} (Debt: {fc.format_usd(latest_debt)} - Cash: {fc.format_usd(latest_cash)})")
        
        print(f"\nDCF Results:")
        print(f"   Intrinsic Value (Per Share): ${dcf_results['value_per_share']:.2f}")
        print(f"   Enterprise Value: {fc.format_usd(dcf_results['enterprise_value'])}")
        print(f"   Equity Value:     {fc.format_usd(dcf_results['equity_value'])}")
        
        # Projection
        future_fcf = []
        growth_multiplier = 1 + (growth_rate_5y / 100)
        current_fcf = ttm_fcf_value
        
        pv_fcf_sum = 0
        
        # Years 1-10 (Constant Growth as requested)
        for year in range(1, 11):
            current_fcf = current_fcf * growth_multiplier
            discount_factor = (1 + discount_rate) ** year
            pv_fcf = current_fcf / discount_factor
            pv_fcf_sum += pv_fcf

        # Method 1: Perpetuity Growth (Gordon Growth)
        terminal_growth = 0.03
        fcf_10 = current_fcf
        
        # TV = FCF_11 / (r - g) = (FCF_10 * (1+g)) / (r-g)
        tv_growth_val = fcf_10 * (1 + terminal_growth) / (discount_rate - terminal_growth)
        pv_terminal_growth = tv_growth_val / ((1 + discount_rate) ** 10)
        
        ev_growth = pv_fcf_sum + pv_terminal_growth
        equity_growth = ev_growth + latest_cash - latest_debt
        val_growth = equity_growth / latest_shares
        
        # Method 2: Exit Multiple (from User Screenshot default ~20x)
        exit_multiple = 20.0
        tv_multiple_val = fcf_10 * exit_multiple
        pv_terminal_multiple = tv_multiple_val / ((1 + discount_rate) ** 10)
        
        ev_multiple = pv_fcf_sum + pv_terminal_multiple
        equity_multiple = ev_multiple + latest_cash - latest_debt
        val_multiple = equity_multiple / latest_shares
        
        print(f"\nResults (Method 1: Perpetuity Growth {terminal_growth:.1%}):")
        print(f"   PV of Terminal Value: {fc.format_usd(pv_terminal_growth)} (Implied Multiple: {1/(discount_rate-terminal_growth):.2f}x)")
        print(f"   Intrinsic Value per Share: ${val_growth:.2f}")
        
        print(f"\nResults (Method 2: Exit Multiple {exit_multiple}x):")
        print(f"   PV of Terminal Value: {fc.format_usd(pv_terminal_multiple)}")
        print(f"   Intrinsic Value per Share: ${val_multiple:.2f}")
        print("-" * 40)

    else:
        print("   ⚠️ Insufficient data for DCF (Missing FCF or Shares)")


    # Summary table
    print("\n" + "=" * 80)
    
    # Header Info
    header_parts = [f"{company.name}"]
    if hasattr(company, 'industry') and company.industry:
        header_parts.append(f"Industry: {company.industry}")
    
    if hasattr(company, 'fiscal_year_end') and company.fiscal_year_end:
        # Format MMDD
        fy_end = company.fiscal_year_end
        if len(fy_end) == 4:
            try:
                # Use a leap year (2000) to handle Feb 29 if present
                dt = datetime.strptime(f"2000{fy_end}", "%Y%m%d")
                header_parts.append(f"FY End: {dt.strftime('%B %d')}")
            except ValueError:
                header_parts.append(f"FY End: {fy_end}")
        else:
             header_parts.append(f"FY End: {fy_end}")
             
    print(" | ".join(header_parts))
    print("=" * 80)
    
    # Reorder results
    desired_order = ["ROIC", "Equity", "Net Income", "Revenue", "Free Cash Flow"]
    # Sort results list based on index in desired_order, putting unknown metrics at the end
    results.sort(key=lambda x: desired_order.index(x['Metric']) if x['Metric'] in desired_order else 999)
    
    summary_df = pd.DataFrame(results)
    print(f"\n{'Metric':<20} {'TTM':<15} {'1Y (CAGR/Avg)':<15} {'5Y (CAGR/Avg)':<15} {'10Y (CAGR/Avg)':<15}")
    print("-" * 85)
    
    # ANSI Color Codes
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    
    def color_text(text: str, value: Optional[float]) -> str:
        """Color text green if value >= 10, red otherwise. Maintains padding."""
        padded_text = f"{text:<15}"
        if value is None:
            return padded_text
        if value >= 10:
            return f"{GREEN}{padded_text}{RESET}"
        else:
            return f"{RED}{padded_text}{RESET}"
    
    for row in results:
        name = row['Metric']
        
        # Get string representation
        if row['Type'] == 'Growth':
            s_ttm = fc.format_pct(row.get('TTM'))
            s1 = fc.format_pct(row['1Y'])
            s5 = fc.format_pct(row['5Y'])
            s10 = fc.format_pct(row['10Y'])
        else:
            s_ttm = f"{row['TTM']:.2f}%" if row.get('TTM') is not None else "N/A"
            s1 = f"{row['1Y']:.2f}%" if row['1Y'] is not None else "N/A"
            s5 = f"{row['5Y']:.2f}%" if row['5Y'] is not None else "N/A"
            s10 = f"{row['10Y']:.2f}%" if row['10Y'] is not None else "N/A"
        
        # Apply color based on raw value
        m_ttm = color_text(s_ttm, row.get('TTM'))
        m1 = color_text(s1, row['1Y'])
        m5 = color_text(s5, row['5Y'])
        m10 = color_text(s10, row['10Y'])
            
        print(f"{name:<20} {m_ttm} {m1} {m5} {m10}")

    print("=" * 80)


if __name__ == "__main__":
    main()
