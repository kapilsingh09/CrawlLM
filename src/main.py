"""
CrawlLM v2 -- AI-Powered Intelligent Web Scraper
Main entry point. The user provides what they want to build,
and CrawlLM automatically scrapes relevant data from multiple free sources.
"""

import sys
import os
import io
import json
import time
from datetime import datetime

# Fix Windows console encoding for Unicode
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from component.gemni_model import get_scrape_plan
from component.web_scraper import scrape_multiple_urls, scrape_single_url
from utils import save_to_csv, save_timestamped_csv, print_data_summary
from exception import CustomException
from logger_config import setup_logger

logger = setup_logger("main")


BANNER = """
+=====================================================================+
|                                                                     |
|    ######  ########     ###    ##      ## ##       ##       ##   ##  |
|   ##       ##    ##    ## ##   ##      ## ##       ##       ### ###  |
|   ##       ########  ##   ##  ##  ##  ## ##       ##       ## # ##  |
|   ##       ##   ##   #######  ## #### ## ##       ##       ##   ##  |
|    ######  ##    ##  ##    ##  ###  ###  ######## ########  ##   ##  |
|                                                                     |
|   [*] AI-Powered Intelligent Web Data Collector                     |
|   [*] For ML Training Data | Powered by Gemini AI                   |
|                                                                     |
+=====================================================================+
"""


def process_prompt(prompt: str) -> list:
    """
    Main processing pipeline:
    1. Send prompt to Gemini AI -> get multi-source scraping plan
    2. Scrape all URLs from the plan
    3. Show live progress on terminal
    4. Save to CSV
    """

    print("\n" + "=" * 70)
    print("[*]  STEP 1: Analyzing your request with Gemini AI...")
    print("=" * 70)

    start_time = time.time()
    plan = get_scrape_plan(prompt)

    # Display the AI plan
    print("\n[+]  AI SCRAPING PLAN:")
    print("-" * 70)
    print(f"  Project     : {plan.get('project_description', 'N/A')}")
    print(f"  Data Type   : {plan.get('data_type', 'N/A')}")
    print(f"  Strategy    : {plan.get('scraping_strategy', 'N/A')}")
    print(f"  Est. Rows   : {plan.get('estimated_rows', 'N/A')}")
    print(f"  Columns     : {', '.join(plan.get('target_columns', []))}")
    print(f"  URLs to scrape: {len(plan.get('urls', []))}")
    print("-" * 70)

    urls = plan.get("urls", [])
    if not urls:
        logger.error("[X] No URLs in scraping plan!")
        return []

    print(f"\n  Sources:")
    for i, u in enumerate(urls, 1):
        print(f"    {i}. [{u.get('source_name', '?')}] {u.get('url', '?')}")
        print(f"       -> {u.get('description', '')}")

    # -- STEP 2: Scrape all URLs --
    print("\n" + "=" * 70)
    print("[*]  STEP 2: Scraping data from all sources...")
    print("=" * 70)

    all_data = scrape_multiple_urls(urls)

    scrape_time = time.time() - start_time

    # -- STEP 3: Save data --
    print("\n" + "=" * 70)
    print("[*]  STEP 3: Saving data to CSV...")
    print("=" * 70)

    # Save main output
    main_csv = save_to_csv(all_data, "output.csv")

    # Also save timestamped copy
    ts_csv = save_timestamped_csv(all_data, "crawl")

    # -- STEP 4: Summary --
    print_data_summary(all_data)

    print(f"\n  [T] Total time: {scrape_time:.1f} seconds")
    print(f"  [F] Main CSV: {main_csv}")
    print(f"  [F] Backup CSV: {ts_csv}")
    print(f"\n{'='*70}")
    print(f"  [OK] DONE! Collected {len(all_data):,} rows of data for your ML model.")
    print(f"{'='*70}\n")

    return all_data


def main():
    """Interactive CLI mode -- user enters what they want to build."""
    print(BANNER)

    while True:
        print("\n" + "-" * 70)
        print("[?] Tell me what you want to build, and I'll collect training data for it.")
        print("   Examples:")
        print("   * 'I want to build a book recommendation system'")
        print("   * 'I need data for sentiment analysis on tech news'")
        print("   * 'I want to build a job matching ML model'")
        print("   * 'Collect product data for price prediction'")
        print("   * 'I need quotes data for NLP text classification'")
        print("   Type 'quit' or 'exit' to stop.")
        print("-" * 70)

        try:
            user_prompt = input("\n>> Your prompt: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\nGoodbye!")
            break

        if not user_prompt:
            print("[!] Please enter a prompt.")
            continue

        if user_prompt.lower() in ("quit", "exit", "q"):
            print("\nGoodbye! Happy training!")
            break

        try:
            result = process_prompt(user_prompt)

            if result:
                # Show a few sample rows
                print("\n[+] Sample of collected data:")
                for row in result[:5]:
                    text = str(row.get("text", ""))[:100]
                    print(f"   [{row.get('source', '?')}] ({row.get('type', '?')}) {text}")
                print(f"   ... and {max(0, len(result) - 5)} more rows\n")

        except CustomException as e:
            print(f"\n[X] Error: {e}")
            logger.error(f"CustomException: {e}")
        except Exception as e:
            print(f"\n[X] Unexpected error: {e}")
            logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()