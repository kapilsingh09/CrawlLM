"""
Gemini AI Model Integration — Smart Scraping Planner
Uses Google Gemini to analyze user intent and generate comprehensive
multi-source scraping plans with intelligent URL selection.
"""

import os
import sys
import json
import re

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
from logger_config import setup_logger

logger = setup_logger("gemini_model")

# ─── Load API key from environment ───────────────────────────────────────────
API_KEY = os.environ.get("GEMINI_API_KEY", "AIzaSyC9UMh9MCrURILgGrefJzGRbQrm2N4MGFo")
client = genai.Client(api_key=API_KEY)


# ═══════════════════════════════════════════════════════════════════════════════
#  FREE SCRAPING SOURCES CATALOG
# ═══════════════════════════════════════════════════════════════════════════════
FREE_SOURCES = """
## Free & Legal Data Sources (no API key needed):

### 📰 News & Articles
- https://news.ycombinator.com — Tech news headlines
- https://news.ycombinator.com/newest — Latest tech stories
- https://lite.cnn.com — CNN headlines (lightweight)
- https://text.npr.org — NPR news in text format
- https://www.reddit.com/r/{subreddit}.json — Reddit posts as JSON
- https://lobste.rs — Tech stories

### 📚 Books & Quotes
- https://books.toscrape.com — Fake bookstore (titles, prices, ratings)
- https://books.toscrape.com/catalogue/page-{n}.html — Paginated books
- https://quotes.toscrape.com — Famous quotes with authors & tags
- https://quotes.toscrape.com/page/{n}/ — Paginated quotes

### 🛒 Products & E-Commerce (practice sites)
- https://webscraper.io/test-sites/e-commerce/allinone — Products
- https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops — Laptops
- https://webscraper.io/test-sites/e-commerce/allinone/phones — Phones
- https://scrapingclub.com/exercise/list_basic/ — Products list

### 💼 Jobs
- https://realpython.github.io/fake-jobs/ — Fake job listings (titles, companies, locations)

### 🌐 Wikipedia
- https://en.wikipedia.org/wiki/{topic} — Any topic's Wikipedia page
- https://en.wikipedia.org/wiki/Special:Random — Random article

### 💻 Code & Tech
- https://api.github.com/search/repositories?q={topic}&sort=stars — GitHub repos
- https://api.github.com/search/topics?q={topic} — GitHub topics
- https://api.stackexchange.com/2.3/search?order=desc&sort=votes&intitle={topic}&site=stackoverflow — StackOverflow

### 🎬 Movies & Entertainment
- https://www.imdb.com/chart/top/ — Top 250 movies
- https://www.imdb.com/chart/moviemeter/ — Most popular movies

### 🍽️ Recipes
- https://www.allrecipes.com/search?q={topic} — Recipe search

### 📊 Data & Statistics
- https://www.worldometers.info/ — World statistics
- https://www.numbeo.com/cost-of-living/ — Cost of living data

### 🏋️ Health & Fitness
- https://www.bodybuilding.com/exercises — Exercise database
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  ENHANCED SYSTEM PROMPT
# ═══════════════════════════════════════════════════════════════════════════════
SYSTEM_PROMPT = f"""
You are CrawlLM — an expert AI web scraping planner. Your job is to convert a user's 
description of what they want to build (ML model, dataset, app, etc.) into a comprehensive
multi-source scraping plan that collects MAXIMUM data.

{FREE_SOURCES}

## Your Task:
1. Understand EXACTLY what kind of data the user needs for their project
2. Pick the BEST free sources from the catalog above (use multiple sources!)
3. Generate a scraping plan with MULTIPLE URLs to maximize data collection
4. For paginated sites, include MULTIPLE page URLs (page 1, 2, 3... up to 10+)
5. Think about what columns/fields would be useful for their ML model

## CRITICAL RULES:
- Return ONLY valid JSON, no markdown, no code fences, no explanation
- Always include at least 3-5 different URLs for comprehensive data
- For paginated sites, include at LEAST 5-10 page URLs
- Use REAL URLs from the catalog above that actually work
- If the topic doesn't match any catalog source, use Wikipedia + Reddit + GitHub
- Think about what STRUCTURED data fields are needed for ML training

## Output Format (STRICT JSON):
{{
    "project_description": "Brief description of what user wants to build",
    "data_type": "the type of data being collected",
    "urls": [
        {{
            "url": "https://...",
            "source_name": "Name of source",
            "description": "What data this provides",
            "expected_elements": ["h1", "h2", "h3", "p", "a", "table", "li", "span.price"]
        }}
    ],
    "target_columns": ["column1", "column2", "column3"],
    "scraping_strategy": "deep|paginated|multi-source|api",
    "estimated_rows": 500
}}

IMPORTANT: Return ONLY the JSON object. No backticks, no ```json, no extra text.
"""


def get_scrape_plan(user_prompt: str) -> dict:
    """
    Send the user's prompt to Gemini AI and get back a comprehensive
    multi-source scraping plan as a parsed dictionary.
    """
    logger.info(f"🤖 Sending prompt to Gemini AI: '{user_prompt}'")

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=SYSTEM_PROMPT + "\n\nUser's Request: " + user_prompt,
        )

        raw_text = response.text.strip()
        logger.info(f"📥 Raw AI response length: {len(raw_text)} chars")

        # Clean up response — remove markdown code fences if present
        cleaned = raw_text
        if cleaned.startswith("```"):
            # Remove opening fence
            cleaned = re.sub(r'^```(?:json)?\s*\n?', '', cleaned)
            # Remove closing fence
            cleaned = re.sub(r'\n?```\s*$', '', cleaned)
        cleaned = cleaned.strip()

        # Parse JSON
        plan = json.loads(cleaned)

        url_count = len(plan.get("urls", []))
        logger.info(f"✅ AI generated scraping plan with {url_count} URLs")
        logger.info(f"📊 Data type: {plan.get('data_type', 'unknown')}")
        logger.info(f"📋 Target columns: {plan.get('target_columns', [])}")
        logger.info(f"📈 Estimated rows: {plan.get('estimated_rows', 'unknown')}")

        return plan

    except json.JSONDecodeError as e:
        logger.error(f"❌ Failed to parse AI response as JSON: {e}")
        logger.error(f"📄 Raw response was: {raw_text[:500]}")

        # Fallback plan
        fallback = _generate_fallback_plan(user_prompt)
        logger.info("🔄 Using fallback scraping plan")
        return fallback

    except Exception as e:
        logger.error(f"❌ Gemini API error: {e}")
        fallback = _generate_fallback_plan(user_prompt)
        logger.info("🔄 Using fallback scraping plan")
        return fallback


def _generate_fallback_plan(user_prompt: str) -> dict:
    """Generate a reasonable fallback plan when AI fails."""
    prompt_lower = user_prompt.lower()

    urls = []

    # Smart keyword-based URL selection
    if any(w in prompt_lower for w in ["book", "read", "literature", "library"]):
        urls = [
            {"url": f"https://books.toscrape.com/catalogue/page-{i}.html", "source_name": f"Books Page {i}", "description": "Book titles, prices, ratings", "expected_elements": ["h3", "p.price_color", "p.star-rating"]}
            for i in range(1, 11)
        ]
    elif any(w in prompt_lower for w in ["quote", "motivation", "inspire", "wisdom"]):
        urls = [
            {"url": f"https://quotes.toscrape.com/page/{i}/", "source_name": f"Quotes Page {i}", "description": "Quotes with authors and tags", "expected_elements": ["span.text", "small.author", "a.tag"]}
            for i in range(1, 11)
        ]
    elif any(w in prompt_lower for w in ["job", "career", "hire", "work", "employ"]):
        urls = [
            {"url": "https://realpython.github.io/fake-jobs/", "source_name": "Fake Jobs", "description": "Job listings", "expected_elements": ["h2", "h3", "p.location"]},
            {"url": "https://news.ycombinator.com/jobs", "source_name": "HN Jobs", "description": "Tech job listings", "expected_elements": ["a.titleline", "span.titleline"]},
        ]
    elif any(w in prompt_lower for w in ["news", "article", "headline", "current"]):
        urls = [
            {"url": "https://news.ycombinator.com", "source_name": "HackerNews Front", "description": "Tech news", "expected_elements": ["span.titleline", "a"]},
            {"url": "https://news.ycombinator.com/newest", "source_name": "HN Newest", "description": "Latest stories", "expected_elements": ["span.titleline", "a"]},
            {"url": "https://lobste.rs", "source_name": "Lobsters", "description": "Tech stories", "expected_elements": ["a.u-url", "span.tags"]},
            {"url": "https://lite.cnn.com", "source_name": "CNN Lite", "description": "Headlines", "expected_elements": ["li", "a"]},
            {"url": "https://text.npr.org", "source_name": "NPR Text", "description": "NPR headlines", "expected_elements": ["li", "a"]},
        ]
    elif any(w in prompt_lower for w in ["product", "shop", "ecommerce", "price", "laptop", "phone"]):
        urls = [
            {"url": "https://webscraper.io/test-sites/e-commerce/allinone", "source_name": "WebScraper E-Commerce", "description": "Products", "expected_elements": ["a.title", "h4", "span.price"]},
            {"url": "https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops", "source_name": "Laptops", "description": "Laptop products", "expected_elements": ["a.title", "h4", "span.price"]},
            {"url": "https://webscraper.io/test-sites/e-commerce/allinone/phones", "source_name": "Phones", "description": "Phone products", "expected_elements": ["a.title", "h4", "span.price"]},
            {"url": "https://books.toscrape.com", "source_name": "Books Store", "description": "Books with prices", "expected_elements": ["h3", "p.price_color"]},
        ]
    else:
        # General fallback — scrape from multiple sources
        topic = user_prompt.replace(" ", "_")[:50]
        urls = [
            {"url": f"https://en.wikipedia.org/wiki/{topic}", "source_name": "Wikipedia", "description": f"Wikipedia article on {user_prompt}", "expected_elements": ["h1", "h2", "h3", "p", "table", "li"]},
            {"url": "https://news.ycombinator.com", "source_name": "HackerNews", "description": "Tech headlines", "expected_elements": ["span.titleline", "a"]},
            {"url": "https://quotes.toscrape.com", "source_name": "Quotes", "description": "Quotes data", "expected_elements": ["span.text", "small.author"]},
            {"url": "https://books.toscrape.com", "source_name": "Books", "description": "Books data", "expected_elements": ["h3", "p.price_color"]},
            {"url": "https://lobste.rs", "source_name": "Lobsters", "description": "Tech stories", "expected_elements": ["a.u-url"]},
        ]

    return {
        "project_description": f"Data collection for: {user_prompt}",
        "data_type": "mixed",
        "urls": urls,
        "target_columns": ["source", "type", "text", "href", "extra"],
        "scraping_strategy": "multi-source",
        "estimated_rows": 500,
    }