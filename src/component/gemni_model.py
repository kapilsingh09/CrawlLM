import json
import re
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from google import genai

from logger_config import setup_logger
from exception import CustomException
from web_scraper import save_to_csv, scrape_urls

logger = setup_logger("gemini_controller")

GEMINI_API_KEY = ""  # <-- paste your key here
GEMINI_MODEL = "gemini-2.0-flash"

SYSTEM_PROMPT = """
You are an intelligent ML data collection orchestrator.

Your job:
1. Understand what ML model the user wants to build
2. Decide if it is SUPERVISED or UNSUPERVISED learning
3. Choose the best scraping-friendly public website that has relevant data
4. Return a scraping plan so the data can be used directly for ML training

=== SCRAPING-FRIENDLY SITES YOU CAN USE ===
Only pick from these — they allow bots and have no login walls:

| Site | Best for |
|------|----------|
| https://quotes.toscrape.com | Sentiment analysis, text classification, NLP clustering |
| https://quotes.toscrape.com/page/{n}/ | Same, paginated (n=1..10) |
| https://books.toscrape.com | Price regression, rating classification, genre clustering |
| https://books.toscrape.com/catalogue/page-{n}.html | Same, paginated (n=1..50) |
| https://news.ycombinator.com | Tech topic classification, upvote regression |
| https://news.ycombinator.com/news?p={n} | Same, paginated (n=1..5) |
| https://www.worldometers.info/world-population/population-by-country/ | Country stats regression/clustering |
| https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal) | GDP regression/clustering |
| https://en.wikipedia.org/wiki/List_of_countries_by_life_expectancy | Health data regression |
| https://en.wikipedia.org/wiki/List_of_most-viewed_YouTube_videos | Popularity regression |

=== ML TASK DETECTION ===

SUPERVISED_CLASSIFICATION: "predict category", "classify", "spam", "sentiment",
  "is it X or Y", "label", "which type", "detect fraud"
  → needs a target column with discrete labels (e.g. genre, rating_label, author)

SUPERVISED_REGRESSION: "predict price", "estimate value", "forecast", "how much",
  "how many", "predict score", "predict rating"
  → needs a numeric target column (e.g. price, score, population)

UNSUPERVISED_CLUSTERING: "group similar", "find patterns", "cluster", "segment",
  "no labels", "explore data", "discover groups"
  → no target column, just feature columns

UNSUPERVISED_DIMENSIONALITY_REDUCTION: "reduce features", "PCA", "visualize high-dim",
  "compress", "find structure"
  → needs many numeric feature columns

=== RESPONSE FORMAT ===
Respond with ONLY a valid JSON object. No markdown, no backticks, no extra text.

{
  "ml_task_type": "supervised_classification" | "supervised_regression" | "unsupervised_clustering" | "unsupervised_dimensionality_reduction",
  "task_summary": "one sentence: what model + what data",
  "recommended_models": ["Model1", "Model2", "Model3"],
  "urls": ["https://..."],
  "max_pages": 3,
  "fields": ["col1", "col2", "col3"],
  "target_column": "col_name or null if unsupervised",
  "feature_columns": ["col1", "col2"],
  "extraction_hints": {
    "col1": "css_selector1, css_selector2",
    "col2": "css_selector3"
  },
  "output_filename": "snake_case_name.csv",
  "notes": "preprocessing tips, label encoding needed, etc."
}

=== EXTRACTION HINTS REFERENCE ===

books.toscrape.com:
  title       -> "article.product_pod h3 a"
  price       -> "article.product_pod p.price_color"
  rating      -> "article.product_pod p.star-rating"
  availability-> "article.product_pod p.availability"

quotes.toscrape.com:
  text   -> "span.text"
  author -> "small.author"
  tags   -> "div.tags a.tag"

news.ycombinator.com:
  title    -> "span.titleline a"
  score    -> "span.score"
  comments -> "span.subtext a:last-child"
  site     -> "span.sitestr"

worldometers population:
  country            -> "td:nth-child(2)"
  population         -> "td:nth-child(3)"
  yearly_change_pct  -> "td:nth-child(4)"
  density_per_km2    -> "td:nth-child(6)"
  land_area_km2      -> "td:nth-child(7)"
  urban_pop_pct      -> "td:nth-child(9)"

wikipedia list tables:
  use -> "table.wikitable td, table.wikitable th"
"""


def ask_gemini(user_query: str) -> dict:
    logger.info(f"User query: {user_query!r}")
    client = genai.Client(api_key=GEMINI_API_KEY)

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=f"{SYSTEM_PROMPT}\n\nUser ML task: {user_query}",
        )
        raw = response.text.strip()
        logger.debug(f"Gemini raw response:\n{raw}")
    except Exception as e:
        logger.exception(f"Gemini API call failed: {e}")
        raise

    json_match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not json_match:
        logger.error("No JSON found in Gemini response")
        raise ValueError(f"Gemini returned no JSON:\n{raw}")

    try:
        plan = json.loads(json_match.group())
        logger.info(f"ML task type  : {plan.get('ml_task_type')}")
        logger.info(f"Task summary  : {plan.get('task_summary')}")
        logger.info(f"Models        : {plan.get('recommended_models')}")
        logger.info(f"Target column : {plan.get('target_column')}")
        logger.info(f"Feature cols  : {plan.get('feature_columns')}")
        logger.info(f"URLs          : {plan.get('urls')}")
        if plan.get("notes"):
            logger.warning(f"Notes         : {plan['notes']}")
        return plan
    except json.JSONDecodeError as e:
        logger.exception(f"JSON parse failed: {e}\nRaw: {raw}")
        raise


def validate_plan(plan: dict) -> bool:
    required = ["urls", "fields", "extraction_hints", "output_filename", "ml_task_type"]
    missing = [k for k in required if k not in plan or not plan[k]]
    if missing:
        logger.error(f"Plan missing keys: {missing}")
        return False
    if not plan["urls"]:
        logger.error("No URLs in plan")
        return False
    if not plan["fields"]:
        logger.error("No fields in plan")
        return False
    return True


def expand_paginated_urls(plan: dict) -> list[str]:
    urls = plan.get("urls", [])
    max_pages = int(plan.get("max_pages", 1))
    expanded = []
    for url in urls:
        if "{n}" in url:
            for n in range(1, max_pages + 1):
                expanded.append(url.replace("{n}", str(n)))
            logger.info(f"Paginated URL expanded to {max_pages} pages")
        else:
            expanded.append(url)
    return expanded


def print_ml_summary(plan: dict, output_path: str, row_count: int):
    feat = plan.get("feature_columns", [])
    target = plan.get("target_column")
    models = ", ".join(plan.get("recommended_models", []))
    sep = "=" * 55

    print(f"\n{sep}")
    print("  ML DATA COLLECTION COMPLETE")
    print(sep)
    print(f"  Task type    : {plan.get('ml_task_type', '')}")
    print(f"  Summary      : {plan.get('task_summary', '')}")
    print(f"  Models       : {models}")
    print(f"  Target col   : {target if target else 'N/A  (unsupervised)'}")
    print(f"  Feature cols : {feat}")
    print(f"  Rows scraped : {row_count}")
    print(f"  CSV saved to : {output_path}")
    print(sep)
    print("\n  Quick-start code:")
    print("    import pandas as pd")
    print(f"    df = pd.read_csv('{output_path}')")
    if target:
        print(f"    X = df[{feat}]")
        print(f"    y = df['{target}']")
    else:
        print(f"    X = df[{feat}]")
        print("    # no y — unsupervised task")
    print(f"{sep}\n")


def run_pipeline(user_query: str) -> str:
    logger.info("=" * 60)
    logger.info("Pipeline START")
    logger.info(f"Query: {user_query}")
    logger.info("=" * 60)

    try:
        plan = ask_gemini(user_query)
    except Exception:
        logger.error("Pipeline aborted — Gemini failed")
        return ""

    if not validate_plan(plan):
        logger.error("Pipeline aborted — invalid plan")
        return ""

    urls = expand_paginated_urls(plan)
    fields: list[str] = plan["fields"]
    hints: dict = plan.get("extraction_hints", {})
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{plan['output_filename']}"

    rows = scrape_urls(urls, fields, hints)

    if not rows:
        logger.warning("No data scraped — CSV not saved")
        return ""

    output_path = save_to_csv(rows, filename)

    logger.info("=" * 60)
    logger.info(f"Pipeline COMPLETE — {len(rows)} rows → {output_path}")
    logger.info("=" * 60)

    print_ml_summary(plan, output_path, len(rows))
    return output_path


if __name__ == "__main__":
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        print("\n  AutoML Web Scraper — Powered by Gemini")
        print("  ----------------------------------------")
        print("  Examples:")
        print("    > I want to predict book prices")
        print("    > Build a quote author classifier")
        print("    > Cluster countries by population stats")
        print("    > Predict HackerNews post score")
        print()
        query = input("  What ML model do you want to build? > ").strip()

    if not query:
        print("No query provided.")
        sys.exit(1)

    output = run_pipeline(query)
    if not output:
        print("\nFailed. Check logs/ for details.")
        sys.exit(1)