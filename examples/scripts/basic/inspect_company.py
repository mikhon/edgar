
from edgar import Company
import sys

def inspect_company():
    ticker = "AAPL"
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
    
    company = Company(ticker)
    print(f"Name: {company.name}")
    print(f"Tickers: {company.tickers}")
    print(f"CIK: {company.cik}")
    
    # Check for industry or sic
    if hasattr(company, 'sic'):
        print(f"SIC: {company.sic}")
    if hasattr(company, 'sic_description'):
        print(f"SIC Description: {company.sic_description}")
    if hasattr(company, 'industry'):
        print(f"Industry: {company.industry}")
    
    # Check for fiscal year end
    if hasattr(company, 'fiscal_year_end'):
        print(f"Fiscal Year End: {company.fiscal_year_end}")
        
    # Check facts or other attributes
    print("\nDir(company):")
    print(dir(company))

if __name__ == "__main__":
    inspect_company()
