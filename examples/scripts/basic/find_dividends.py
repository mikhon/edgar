
from edgar import Company, set_identity
import pandas as pd

def find_dividend_concepts():
    ticker = "AAPL"
    company = Company(ticker)
    print(f"Fetching facts for {ticker}...")
    
    all_facts = company.get_facts()
    df = all_facts.to_dataframe(include_metadata=True)
    
    # Search for dividend related concepts
    print("\nSearching for 'Dividend' in concepts:")
    dividend_mask = df['concept'].str.contains('Dividend', case=False, na=False)
    dividend_concepts = df[dividend_mask]['concept'].unique()
    
    for concept in dividend_concepts:
        print(f" - {concept}")
        
    print("\nChecking specific common tags:")
    common_tags = [
        'PaymentsOfDividends',
        'PaymentsOfDividendsCommonStock',
        'Dividends',
        'DividendsCash'
    ]
    
    for tag in common_tags:
        # Check if exists (strict or namespaced)
        mask = df['concept'].apply(lambda x: x.split(':')[-1] == tag if ':' in x else x == tag)
        if not df[mask].empty:
            count = len(df[mask])
            latest = df[mask].sort_values('period_end', ascending=False).iloc[0]
            val = latest['numeric_value']
            date = latest['period_end']
            print(f"✅ Found {tag}: {count} records. Latest: {val} on {date}")
        else:
            print(f"❌ {tag} not found")

if __name__ == "__main__":
    find_dividend_concepts()
