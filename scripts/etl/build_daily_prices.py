"""
ETL Script to load daily stock price data into QuestDB.

This script:
1. Reads tickers from company_tickers.json
2. Downloads daily OHLCV data from Yahoo Finance (yfinance)
3. Loads data into QuestDB via ILP (Influx Line Protocol)

The `daily_price` table stores daily OHLCV + adjusted close for each ticker.

Usage:
    python scripts/etl/build_daily_prices.py [--limit N] [--period PERIOD] [--tickers AAPL,MSFT]

Prerequisites:
    pip install yfinance questdb
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

import orjson
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_tickers(limit: int | None = None) -> list[str]:
    """Load tickers from company_tickers.json."""
    tickers_path = Path.home() / ".edgar" / "company_tickers.json"
    if not tickers_path.exists():
        tickers_path = project_root / "data" / "company_tickers.json"

    if not tickers_path.exists():
        raise FileNotFoundError(
            "company_tickers.json not found. Run `edgar.get_company_tickers()` first."
        )

    with open(tickers_path, "rb") as f:
        data = orjson.loads(f.read())

    tickers = []
    for entry in data.values():
        ticker = entry.get("ticker", "")
        if ticker:
            tickers.append(ticker)

    tickers = sorted(set(tickers))
    if limit:
        tickers = tickers[:limit]

    return tickers


def create_daily_price_table():
    """Create the daily_price table in QuestDB if it doesn't exist."""
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=8812, user="admin", password="quest", database="qdb"
    )
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_price (
        ticker SYMBOL CAPACITY 8192 CACHE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume LONG,
        adj_close DOUBLE,
        ts TIMESTAMP
    ) TIMESTAMP(ts) PARTITION BY YEAR
      DEDUP UPSERT KEYS(ticker, ts);
    """

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Table 'daily_price' created/verified in QuestDB")


def download_and_load_prices(
    tickers: list[str],
    period: str = "max",
    batch_size: int = 20,
):
    """
    Download price data from Yahoo Finance and load into QuestDB.

    Args:
        tickers: List of stock ticker symbols
        period: yfinance period string ('1y', '5y', '10y', 'max')
        batch_size: Number of tickers to download at once
    """
    try:
        import yfinance as yf
    except ImportError:
        print("❌ yfinance not installed. Run: pip install yfinance")
        sys.exit(1)

    try:
        from questdb.ingress import Sender
    except ImportError:
        print("❌ questdb package not installed. Run: pip install questdb")
        sys.exit(1)

    total_rows = 0
    tickers_processed = 0
    tickers_failed = 0

    with Sender.from_conf("http::addr=localhost:9000;") as sender:
        # Process in batches
        for i in tqdm(range(0, len(tickers), batch_size), desc="Downloading batches"):
            batch = tickers[i : i + batch_size]

            try:
                # Download batch (yfinance supports multi-ticker download)
                data = yf.download(
                    batch,
                    period=period,
                    group_by="ticker",
                    auto_adjust=False,
                    progress=False,
                    threads=True,
                )

                if data.empty:
                    tickers_failed += len(batch)
                    continue

                # Process each ticker in the batch
                for ticker in batch:
                    try:
                        if len(batch) == 1:
                            ticker_data = data
                        else:
                            if ticker not in data.columns.get_level_values(0):
                                tickers_failed += 1
                                continue
                            ticker_data = data[ticker]

                        ticker_data = ticker_data.dropna(subset=["Close"])

                        for ts, row in ticker_data.iterrows():
                            try:
                                sender.row(
                                    "daily_price",
                                    symbols={"ticker": ticker},
                                    columns={
                                        "open": float(row.get("Open", 0)) if row.get("Open") is not None else 0.0,
                                        "high": float(row.get("High", 0)) if row.get("High") is not None else 0.0,
                                        "low": float(row.get("Low", 0)) if row.get("Low") is not None else 0.0,
                                        "close": float(row.get("Close", 0)),
                                        "volume": int(row.get("Volume", 0)) if row.get("Volume") is not None else 0,
                                        "adj_close": float(row.get("Adj Close", 0)) if row.get("Adj Close") is not None else 0.0,
                                    },
                                    at=ts.to_pydatetime(),
                                )
                                total_rows += 1
                            except Exception:
                                continue

                        tickers_processed += 1

                    except Exception as e:
                        tickers_failed += 1
                        continue

                # Flush after each batch
                sender.flush()

            except Exception as e:
                tqdm.write(f"⚠️  Batch download failed: {e}")
                tickers_failed += len(batch)
                continue

    return total_rows, tickers_processed, tickers_failed


def main():
    """Main ETL process for daily prices."""
    parser = argparse.ArgumentParser(description="Load daily stock prices into QuestDB")
    parser.add_argument("--limit", type=int, help="Limit number of tickers to process")
    parser.add_argument(
        "--period",
        type=str,
        default="max",
        help="yfinance period: 1y, 5y, 10y, max (default: max)",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        help="Comma-separated list of specific tickers (overrides company_tickers.json)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Number of tickers to download per batch (default: 20)",
    )
    parser.add_argument("--skip-table-creation", action="store_true")
    args = parser.parse_args()

    print("🚀 Starting Daily Price → QuestDB ETL")

    # Create table
    if not args.skip_table_creation:
        try:
            create_daily_price_table()
        except Exception as e:
            print(f"⚠️  Could not create table (may already exist): {e}")

    # Get tickers
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    else:
        tickers = load_tickers(limit=args.limit)

    print(f"📊 Processing {len(tickers)} tickers (period: {args.period})")

    # Download and load
    total_rows, processed, failed = download_and_load_prices(
        tickers,
        period=args.period,
        batch_size=args.batch_size,
    )

    print(f"\n✅ Daily Price ETL Complete!")
    print(f"   Tickers processed: {processed}")
    print(f"   Tickers failed:    {failed}")
    print(f"   Total rows loaded: {total_rows:,}")
    print(f"\n🌐 View data at: http://localhost:9000")
    print(f"\n   Example query:")
    print(f"   SELECT * FROM daily_price WHERE ticker = 'AAPL' ORDER BY ts DESC LIMIT 10;")


if __name__ == "__main__":
    main()
