import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
import sys
import os

async def fetch_finviz_growth(ticker):
    """Backup: Fast and reliable scraping from Finviz."""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    try:
        resp = curl_requests.get(url, headers=headers, impersonate="chrome110", timeout=15)
        if resp.status_code != 200:
            return None
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        tag = soup.find('td', string="EPS next 5Y")
        if tag:
            val_td = tag.find_next_sibling('td')
            if val_td:
                val_text = val_td.get_text(strip=True).replace('%', '')
                try:
                    return float(val_text) if val_text != "-" else None
                except ValueError:
                    return None
        return None
    except Exception:
        return None

async def fetch_zacks_growth_rate(ticker):
    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            url = f"https://www.zacks.com/stock/quote/{ticker}/detailed-earning-estimates"
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(10)
            
            content = await page.content()
            
            # Diagnostic
            with open(f"/tmp/zacks_{ticker}_diag.txt", "w") as f:
                 f.write(content[:10000])

            if "Pardon Our Interruption" in content or "Request unsuccessful" in content:
                print(f"[{ticker}] Zacks bot detected. Falling back to Finviz...")
                return await fetch_finviz_growth(ticker)
            
            soup = BeautifulSoup(content, 'html.parser')
            td = soup.find('td', string=lambda s: s and "Next 5 Years" in s)
            if td:
                val_td = td.find_next('td')
                if val_td:
                    val = val_td.get_text(strip=True)
                    try:
                        return float(val) if val != "NA" else None
                    except ValueError:
                        return None
            
            # If search fails on Zacks, try Finviz as a general fallback
            return await fetch_finviz_growth(ticker)
        except Exception:
            return await fetch_finviz_growth(ticker)
        finally:
            if browser:
                await browser.close()

if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else 'AAPL'
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(fetch_zacks_growth_rate(ticker))
    print(f"Growth Estimate for {ticker}: {result}")
