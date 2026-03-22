
import pandas as pd
from edgar import Company
from edgar.standardization import SynonymGroups

def investigate_cash_flow_durations():
    ticker = "AAPL"
    company = Company(ticker)
    print(f"Fetching facts for {ticker}...")
    
    all_facts = company.get_facts()
    df = all_facts.to_dataframe(include_metadata=True)
    
    synonyms = SynonymGroups()
    ocf_concepts = synonyms.get_synonyms("operating_cash_flow")
    
    print(f"\nChecking Operating Cash Flow concepts: {ocf_concepts}")
    
    # Filter for OCF
    # Strict match check similar to stock_cagr.py
    mask = df['concept'].apply(lambda x: x.split(':')[-1] in ocf_concepts if ':' in x else x in ocf_concepts)
    filtered = df[mask].copy()
    
    if filtered.empty:
        print("No OCF data found.")
        return

    # Convert dates
    filtered['period_end'] = pd.to_datetime(filtered['period_end'])
    filtered['period_start'] = pd.to_datetime(filtered['period_start'])
    
    # Calculate duration
    filtered['duration_days'] = (filtered['period_end'] - filtered['period_start']).dt.days
    
    # Filter for recent years (2024, 2025)
    filtered['fy'] = pd.to_numeric(filtered['fiscal_year'], errors='coerce')
    recent = filtered[filtered['fy'] >= 2024].sort_values(['period_end', 'duration_days'])
    
    print("\nRecent OCF Data Points (FY >= 2024):")
    print(f"{'End Date':<15} {'Start Date':<15} {'Duration':<10} {'Form':<10} {'FY':<5} {'FP':<5} {'Value (B)':<10}")
    print("-" * 80)
    
    for _, row in recent.iterrows():
        val_b = row['numeric_value'] / 1e9
        print(f"{row['period_end'].strftime('%Y-%m-%d'):<15} {row['period_start'].strftime('%Y-%m-%d'):<15} {row['duration_days']:<10} {row['form_type']:<10} {row['fiscal_year']:<5} {row['fiscal_period']:<5} ${val_b:.2f}")

if __name__ == "__main__":
    investigate_cash_flow_durations()
