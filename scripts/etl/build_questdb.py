"""
ETL Script to load Edgar Company Facts JSON files into QuestDB.

This script:
1. Reads all JSON files from ~/.edgar/companyfacts/
2. Flattens the nested structure using Polars
3. Normalizes XBRL concept names to canonical forms using SynonymGroups
4. Loads data into QuestDB via ILP (Influx Line Protocol)

The `financial` table uses normalized concept names (e.g., 'revenue' instead of
'RevenueFromContractWithCustomerExcludingAssessedTax') for simpler queries.
Original XBRL tags are preserved in the `xbrl_tag` column.

Usage:
    python scripts/etl/build_questdb.py [--limit N] [--batch-size N] [--dry-run]
"""
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

import orjson
import polars as pl
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from edgar.standardization.synonym_groups import SynonymGroups


def get_companyfacts_dir() -> Path:
    """Get the companyfacts directory path."""
    companyfacts_dir = Path.home() / ".edgar" / "companyfacts"
    if not companyfacts_dir.exists():
        raise FileNotFoundError(f"Company facts directory not found: {companyfacts_dir}")
    return companyfacts_dir


def build_concept_lookup(synonym_groups: SynonymGroups) -> dict[str, str]:
    """
    Pre-build a dictionary mapping every known XBRL tag -> canonical name.

    This is much faster than calling identify_concept() per-record since we
    do the lookup once per unique tag rather than once per row.

    Note: SynonymGroups.__iter__ yields group name strings, so we call
    get_group() to retrieve the actual SynonymGroup object.
    """
    lookup = {}
    for group_name in synonym_groups:
        group = synonym_groups.get_group(group_name)
        if group is None:
            continue
        for tag in group.synonyms:
            tag_lower = tag.lower()
            if tag_lower not in lookup:
                lookup[tag_lower] = group.name
    return lookup


def build_allowed_concepts(synonym_groups: SynonymGroups) -> set[str]:
    """
    Return the set of all canonical concept names defined in SynonymGroups.

    Only records whose XBRL tag maps to one of these names will be imported.
    Tags that don't resolve to any canonical name are skipped entirely.
    """
    return {name for name in synonym_groups}


def parse_company_facts_json(
    json_path: Path,
    concept_lookup: dict[str, str],
    allowed_concepts: set[str] | None = None,
) -> pl.DataFrame:
    """
    Parse a single company facts JSON file into a Polars DataFrame.

    Normalizes XBRL concept names to canonical forms using the pre-built lookup.
    If `allowed_concepts` is provided, only records whose canonical name is in
    that set are included — unknown/unmapped tags are skipped entirely.

    Returns a flattened DataFrame with columns:
    - cik, entity_name, taxonomy, concept (normalized), xbrl_tag (original),
    - units, val, fiscal_year, fiscal_period, form, filed_date, start_date,
    - end_date, frame
    """
    try:
        with open(json_path, "rb") as f:
            data = orjson.loads(f.read())

        cik = data.get("cik")
        entity_name = data.get("entityName", "")

        records = []

        facts = data.get("facts", {})
        for taxonomy, concepts in facts.items():
            for xbrl_tag, concept_data in concepts.items():
                # Normalize concept name
                tag_lower = xbrl_tag.lower()
                canonical_name = concept_lookup.get(tag_lower)

                # Skip if: (a) tag has no canonical mapping, or
                #          (b) canonical name not in allowed set
                if canonical_name is None:
                    continue
                if allowed_concepts is not None and canonical_name not in allowed_concepts:
                    continue

                units_data = concept_data.get("units", {})
                for unit, values in units_data.items():
                    for value_record in values:
                        record = {
                            "cik": cik,
                            "entity_name": entity_name,
                            "taxonomy": taxonomy,
                            "concept": canonical_name,
                            "xbrl_tag": xbrl_tag,
                            "units": unit,
                            "val": value_record.get("val"),
                            "fiscal_year": value_record.get("fy"),
                            "fiscal_period": value_record.get("fp"),
                            "form": value_record.get("form"),
                            "filed_date": value_record.get("filed"),
                            "start_date": value_record.get("start"),
                            "end_date": value_record.get("end"),
                            "frame": value_record.get("frame"),
                        }
                        records.append(record)

        if not records:
            return pl.DataFrame()

        df = pl.DataFrame(records, infer_schema_length=None)

        # Convert date strings to datetime
        for col in ["filed_date", "start_date", "end_date"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8).str.to_datetime(strict=False).alias(col)
                )

        return df

    except Exception as e:
        print(f"Error parsing {json_path}: {e}")
        return pl.DataFrame()


def create_financial_table():
    """Create the financial table in QuestDB if it doesn't exist."""
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=8812, user="admin", password="quest", database="qdb"
    )
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS financial (
        cik INT,
        ticker SYMBOL CAPACITY 8192 CACHE,
        entity_name SYMBOL,
        taxonomy SYMBOL,
        concept SYMBOL CAPACITY 128 CACHE,
        xbrl_tag SYMBOL CAPACITY 512 CACHE,
        units SYMBOL,
        val DOUBLE,
        fiscal_year INT,
        fiscal_period SYMBOL CAPACITY 16 CACHE,
        form SYMBOL CAPACITY 32 CACHE,
        filed_date TIMESTAMP,
        start_date TIMESTAMP,
        frame SYMBOL,
        end_date TIMESTAMP
    ) TIMESTAMP(end_date) PARTITION BY YEAR
      DEDUP UPSERT KEYS(cik, concept, fiscal_year, fiscal_period, end_date);
    """

    cursor.execute(create_table_sql)
    
    # Also attempt to enable Dedup if the table already existed without it
    try:
        cursor.execute("ALTER TABLE financial SET DEDUP UPSERT KEYS(cik, concept, fiscal_year, fiscal_period, end_date);")
    except Exception:
        # If it's already enabled or the version doesn't support it, skip
        pass
        
    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Table 'financial' created/verified in QuestDB")


def load_ticker_map() -> dict[int, str]:
    """
    Load a CIK -> ticker mapping from company_tickers.json if available.

    Falls back to empty mapping if the file doesn't exist.
    """
    tickers_path = Path.home() / ".edgar" / "company_tickers.json"
    if not tickers_path.exists():
        # Try the project data directory
        tickers_path = Path(__file__).resolve().parent.parent.parent / "data" / "company_tickers.json"

    if not tickers_path.exists():
        return {}

    try:
        with open(tickers_path, "rb") as f:
            data = orjson.loads(f.read())

        ticker_map = {}
        for entry in data.values():
            cik = entry.get("cik_str")
            ticker = entry.get("ticker", "")
            if cik and ticker:
                ticker_map[int(cik)] = ticker
        return ticker_map
    except Exception:
        return {}


def load_to_questdb(df: pl.DataFrame, sender, ticker_map: dict[int, str]) -> int:
    """Load a Polars DataFrame into QuestDB using ILP."""
    if df.is_empty():
        return 0

    rows_sent = 0

    for row in df.iter_rows(named=True):
        try:
            cik = row.get("cik")
            if cik is None:
                continue
            try:
                cik = int(cik)
            except (ValueError, TypeError):
                continue

            # Use end_date as the designated timestamp
            ts = row.get("end_date") or row.get("filed_date")
            if ts is None:
                continue  # Skip rows without a valid timestamp

            # Look up ticker from CIK
            ticker = ticker_map.get(cik, "")

            sender.row(
                "financial",
                symbols={
                    "ticker": ticker,
                    "entity_name": str(row.get("entity_name", "")),
                    "taxonomy": str(row.get("taxonomy", "")),
                    "concept": str(row.get("concept", "")),
                    "xbrl_tag": str(row.get("xbrl_tag", "")),
                    "units": str(row.get("units", "")),
                    "fiscal_period": str(row.get("fiscal_period", "")),
                    "form": str(row.get("form", "")),
                    "frame": str(row.get("frame", "")),
                },
                columns={
                    "cik": cik,
                    "val": row.get("val"),
                    "fiscal_year": row.get("fiscal_year"),
                },
                at=ts,
            )
            rows_sent += 1

        except Exception:
            continue

    return rows_sent


def dry_run_report(df: pl.DataFrame):
    """Print a normalization report for a filtered DataFrame (dry-run mode)."""
    if df.is_empty():
        return

    concept_pairs = (
        df.select(["xbrl_tag", "concept"])
        .unique()
        .sort("concept")
    )

    print(f"\n  ✅ Imported {len(concept_pairs)} distinct concepts ({len(df)} rows):")
    for row in concept_pairs.iter_rows(named=True):
        print(f"     {row['xbrl_tag']:50s} → {row['concept']}")


def main():
    """Main ETL process."""
    parser = argparse.ArgumentParser(description="Load Edgar facts into QuestDB (normalized)")
    parser.add_argument("--limit", type=int, help="Limit number of companies to process")
    parser.add_argument("--batch-size", type=int, default=100, help="Flush batch size (companies)")
    parser.add_argument("--skip-table-creation", action="store_true", help="Skip table creation")
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview normalization without loading data"
    )
    parser.add_argument(
        "--all-concepts",
        action="store_true",
        help="Import all concepts, including unmapped XBRL tags (default: only known canonical concepts)",
    )
    args = parser.parse_args()

    print("🚀 Starting Edgar → QuestDB ETL (with concept normalization)")

    # Initialize synonym groups and build lookup
    print("📚 Building concept normalization lookup...")
    synonym_groups = SynonymGroups()
    concept_lookup = build_concept_lookup(synonym_groups)
    print(f"   {len(concept_lookup)} XBRL tags mapped to canonical concept names")

    # Build the allowed set (all canonical concept names, unless --all-concepts)
    allowed_concepts: set[str] | None = None
    if not args.all_concepts:
        allowed_concepts = build_allowed_concepts(synonym_groups)
        print(f"   Filtering to {len(allowed_concepts)} canonical concepts (use --all-concepts to import everything)")

    # Load ticker mapping
    ticker_map = load_ticker_map()
    if ticker_map:
        print(f"🏷️  Loaded ticker mapping for {len(ticker_map)} companies")

    if not args.dry_run:
        # Create table
        if not args.skip_table_creation:
            try:
                create_financial_table()
            except Exception as e:
                print(f"⚠️  Could not create table (may already exist): {e}")

    # Get all JSON files
    companyfacts_dir = get_companyfacts_dir()
    json_files = sorted(companyfacts_dir.glob("CIK*.json"))

    if args.limit:
        json_files = json_files[: args.limit]

    print(f"📂 Found {len(json_files)} company fact files")

    if args.dry_run:
        print("\n🔍 DRY RUN — previewing filtered import:\n")
        for json_file in json_files:
            df = parse_company_facts_json(json_file, concept_lookup, allowed_concepts)
            if not df.is_empty():
                entity = df["entity_name"][0] if "entity_name" in df.columns else json_file.name
                print(f"📄 {entity} ({json_file.name})")
                dry_run_report(df)
        return

    # Process files
    total_rows = 0
    companies_processed = 0
    skipped_companies = []

    try:
        from questdb.ingress import IngressError, Sender

        with Sender.from_conf("http::addr=localhost:9000;") as sender:
            for json_file in tqdm(json_files, desc="Processing companies"):
                df = parse_company_facts_json(json_file, concept_lookup, allowed_concepts)

                if not df.is_empty():
                    rows = load_to_questdb(df, sender, ticker_map)
                    total_rows += rows
                    companies_processed += 1
                else:
                    skipped_companies.append(json_file.name)

                # Flush periodically by batch size
                if companies_processed % args.batch_size == 0 and companies_processed > 0:
                    sender.flush()

            # Final flush
            sender.flush()

    except ImportError:
        print("❌ questdb package not installed. Run: pip install questdb")
        sys.exit(1)
    except Exception as e:
        print(f"❌ QuestDB error: {e}")
        sys.exit(1)

    print(f"\n✅ ETL Complete!")
    print(f"   Companies processed: {companies_processed}")
    print(f"   Companies skipped:   {len(skipped_companies)}")
    print(f"   Total rows loaded:   {total_rows:,}")

    if skipped_companies:
        print(f"\n跳 Skip Report (Companies with no standard canonical concepts):")
        # Show first 10 skipped, then "..." if more
        for comp in skipped_companies[:10]:
            print(f"     - {comp}")
        if len(skipped_companies) > 10:
            print(f"     ... and {len(skipped_companies) - 10} more")
    print(f"\n🌐 View data at: http://localhost:9000")
    print(f"\n   Example query:")
    print(f"   SELECT * FROM financial WHERE concept = 'revenue' LIMIT 10;")


if __name__ == "__main__":
    main()
