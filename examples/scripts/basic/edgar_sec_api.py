import requests
import pandas as pd
import json

def fetch_apple_revenue():
    # SEC Configuration
    # Important: SEC requires a User-Agent in the format: "Company Name admin@company.com"
    headers = {
        "User-Agent": "mthonkan@hotmail.com",
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov"
    }
    
    # Apple CIK (Central Index Key) - padded to 10 digits
    cik = "0000320193"
    
    # URL for Company Facts (XBRL data)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    
    print(f"🚀 Fetching data from: {url}")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Save the raw data to a file
        filename = "company_facts.json"
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
        print(f"💾 Saved raw company facts to {filename}")
        
        company_name = data['entityName']
        print(f"✅ Successfully fetched data for: {company_name}")
        
        # Navigate to Revenue data
        # Apple typically uses 'RevenueFromContractWithCustomerExcludingAssessedTax' for recent years
        # and 'SalesRevenueNet' or 'Revenues' for older years.
        
        us_gaap = data.get('facts', {}).get('us-gaap', {})
        
        # List of tags to check (in order of preference/modern usage)
        revenue_tags = [
            'RevenueFromContractWithCustomerExcludingAssessedTax',
            'Revenues',
            'SalesRevenueNet',
            'SalesRevenueGoodsNet'
        ]
        
        all_revenue_data = []
        
        for tag in revenue_tags:
            if tag in us_gaap:
                units = us_gaap[tag]['units']
                if 'USD' in units:
                    df = pd.DataFrame(units['USD'])
                    
                    # User Filter Requirement: form="10-K" and fp="FY"
                    # We strictly filter for these as they represent the annual report data
                    mask = (df['form'] == '10-K') & (df['fp'] == 'FY')
                    annual = df[mask].copy()
                    
                    if not annual.empty:
                        annual['concept'] = tag
                        
                        # Ensure we have start/end dates (Revenue is a duration, so these should exist)
                        if 'start' in annual.columns and 'end' in annual.columns:
                            all_revenue_data.append(annual)
        
        if all_revenue_data:
            combined = pd.concat(all_revenue_data)
            
            # Logic:
            # 1. Identify unique periods by 'start' and 'end' columns
            # 2. For each unique period, pick the value from the LATEST filing (correction logic)
            # 3. Derive Fiscal Year from 'end' date (usually the year of the end date)
            
            # Sort by filing date (ascending) so 'keep=last' keeps the most recent filing
            combined = combined.sort_values('filed', ascending=True)
            
            # Deduplicate by unique period (start, end)
            # We assume user wants the most recent *filing* for that specific period.
            final_df = combined.drop_duplicates(subset=['start', 'end'], keep='last').copy()
            
            # Derive Fiscal Year from 'end' date
            final_df['derived_fy'] = pd.to_datetime(final_df['end']).dt.year
            
            # Filter for approx 1 year duration (350-375 days) to exclude quarterly items in 10-K
            final_df['duration_days'] = (pd.to_datetime(final_df['end']) - pd.to_datetime(final_df['start'])).dt.days
            final_df = final_df[(final_df['duration_days'] >= 350) & (final_df['duration_days'] <= 375)]
            
            # Sort by Derived Fiscal Year descending for display
            final_df = final_df.sort_values('derived_fy', ascending=False)
            
            print(f"\n📈 Annual Revenue for {company_name} (Consolidated):")
            print("-" * 105)
            print(f"{'Fiscal Year':<15} {'Revenue':<15} {'Filing Date':<15} {'Period Start':<15} {'Period End':<15} {'Concept Used':<30}")
            print("-" * 105)
            
            for _, row in final_df.head(15).iterrows():
                val = row['val']
                val_str = f"${val/1_000_000_000:.2f}B"
                print(f"{row['derived_fy']:<15} {val_str:<15} {row['filed']:<15} {row['start']:<15} {row['end']:<15} {row['concept']:<30}")
                
        else:
            print("❌ Could not find Revenue data in us-gaap facts.")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")

if __name__ == "__main__":
    fetch_apple_revenue()
