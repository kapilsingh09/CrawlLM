"""
CrawlLM — Utility Functions
CSV saving, data deduplication, and data processing helpers.
"""

import os
import sys
import hashlib
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from logger_config import setup_logger

logger = setup_logger("utils")

# Output directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def deduplicate_data(data: list[dict]) -> list[dict]:
    """Remove duplicate rows based on content hash."""
    seen = set()
    unique_data = []
    for row in data:
        # Create hash of the text + type + source combo
        key = hashlib.md5(
            f"{row.get('text', '')}|{row.get('type', '')}|{row.get('source', '')}".encode()
        ).hexdigest()
        if key not in seen:
            seen.add(key)
            unique_data.append(row)

    removed = len(data) - len(unique_data)
    if removed > 0:
        logger.info(f"🧹 Deduplicated: removed {removed} duplicate rows")
    return unique_data


def save_to_csv(data: list[dict], filename: str = "output.csv", append: bool = False) -> str:
    """
    Save scraped data to CSV with optional append mode.
    Returns the full path to the saved file.
    """
    if not data:
        logger.warning("⚠️ No data to save!")
        return ""

    # Deduplicate before saving
    data = deduplicate_data(data)

    # Build DataFrame
    df = pd.DataFrame(data)

    # Ensure filename is in output directory
    if not os.path.isabs(filename):
        filepath = os.path.join(OUTPUT_DIR, filename)
    else:
        filepath = filename

    # Append or overwrite
    if append and os.path.exists(filepath):
        existing_df = pd.read_csv(filepath)
        df = pd.concat([existing_df, df], ignore_index=True)
        # Deduplicate again after merge
        df = df.drop_duplicates(subset=["text", "type", "source"], keep="first")
        logger.info(f"📎 Appended to existing file. Total rows now: {len(df)}")

    df.to_csv(filepath, index=False, encoding="utf-8-sig")

    logger.info(f"💾 Saved {len(df)} rows to: {filepath}")
    logger.info(f"   Columns: {list(df.columns)}")
    logger.info(f"   File size: {os.path.getsize(filepath):,} bytes")

    return filepath


def save_timestamped_csv(data: list[dict], prefix: str = "crawl") -> str:
    """Save data with a timestamped filename for historical tracking."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.csv"
    return save_to_csv(data, filename)


def print_data_summary(data: list[dict]):
    """Print a beautiful summary of collected data to terminal."""
    if not data:
        print("\n⚠️ No data collected.")
        return

    df = pd.DataFrame(data)

    print("\n" + "=" * 70)
    print("📊  DATA COLLECTION SUMMARY")
    print("=" * 70)

    print(f"\n  Total rows collected  : {len(df):,}")
    print(f"  Total columns         : {len(df.columns)}")
    print(f"  Columns               : {', '.join(df.columns)}")

    if "source" in df.columns:
        print(f"\n  📡 Data by Source:")
        for source, count in df["source"].value_counts().items():
            print(f"     • {source}: {count:,} rows")

    if "type" in df.columns:
        print(f"\n  📦 Data by Type:")
        for dtype, count in df["type"].value_counts().items():
            print(f"     • {dtype}: {count:,} rows")

    # Show sample data
    print(f"\n  📋 Sample Data (first 5 rows):")
    print("  " + "-" * 66)

    sample = df.head(5)
    for idx, row in sample.iterrows():
        text = str(row.get("text", ""))[:80]
        source = str(row.get("source", ""))[:20]
        dtype = str(row.get("type", ""))[:15]
        print(f"  [{source}] ({dtype}) {text}")

    print("  " + "-" * 66)
    print("=" * 70)