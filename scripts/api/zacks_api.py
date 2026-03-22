from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from contextlib import asynccontextmanager
import uvicorn
import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
import sys
import os
import json

# Persistence for cache
CACHE_FILE = "/tmp/zacks_cache.json"

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache):
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f)
    except Exception:
        pass

_cache = load_cache()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start playwright and browser once
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    
    # Store in app state
    app.state.playwright = playwright
    app.state.browser = browser
    
    print("🚀 Zacks/Finviz Browser Service Started.")
    
    yield
    
    # Shutdown: Clean up resources
    await browser.close()
    await playwright.stop()
    print("🛑 Zacks/Finviz Browser Service Stopped.")

app = FastAPI(title="Stock Growth Estimate API", lifespan=lifespan)

class Response(BaseModel):
    ticker: str
    growth_estimate_5y: float = None
    source: str = "Zacks"
    success: bool = True

async def fetch_finviz_growth(ticker):
    """Fallback: Fast and reliable scraping from Finviz."""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    try:
        # Use curl_cffi to match browser TLS fingerprint
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
    except Exception as e:
        print(f"[Finviz] Error for {ticker}: {e}")
        return None

async def scrape_zacks(ticker_symbol: str, request: Request):
    """Creates a new page to scrape Zacks, using the shared browser but fresh context."""
    browser = request.app.state.browser
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        viewport={'width': 1280, 'height': 800}
    )
    page = await context.new_page()
    try:
        await Stealth().apply_stealth_async(page)
        url = f"https://www.zacks.com/stock/quote/{ticker_symbol}/detailed-earning-estimates"
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(10)
        content = await page.content()
        
        # Diagnostic
        with open(f"/tmp/zacks_api_{ticker_symbol}_diag.txt", "w") as f:
             f.write(content[:10000])

        if "Pardon Our Interruption" in content or "Please stand by" in content:
            return None
            
        soup = BeautifulSoup(content, 'html.parser')
        td = soup.find('td', string=lambda s: s and "Next 5 Years" in s)
        if td:
            val_td = td.find_next('td')
            if val_td:
                val_text = val_td.get_text(strip=True)
                try:
                    return float(val_text) if val_text != "NA" else None
                except ValueError:
                    return None
        return None
    except Exception as e:
        print(f"[Zacks] Error: {e}")
        return None
    finally:
        await page.close()
        await context.close()

@app.get("/estimate/{ticker}", response_model=Response)
async def get_estimate(ticker: str, request: Request):
    ticker = ticker.upper()
    
    # 1. Check cache
    if ticker in _cache:
        val = _cache[ticker]
        return Response(ticker=ticker, growth_estimate_5y=val)
    
    # 2. Try Zacks (Original Source)
    result = await scrape_zacks(ticker, request)
    source = "Zacks"
    
    # 3. Fallback to Finviz if Zacks fails (likely IP block)
    if result is None:
        print(f"[{ticker}] Zacks failed (likely IP block), falling back to Finviz...")
        result = await fetch_finviz_growth(ticker)
        source = "Finviz"
    
    if result is None:
        return Response(ticker=ticker, success=False)
        
    # 4. Save to cache
    _cache[ticker] = result
    save_cache(_cache)
    
    return Response(ticker=ticker, growth_estimate_5y=result, source=source)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
