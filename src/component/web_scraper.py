"""
CrawlLM — Powerful Multi-Source Web Scraper
Extracts structured data from multiple websites with rich terminal output,
retry logic, rate limiting, and intelligent content extraction.
"""

import os
import sys
import re
import time
import random
import hashlib
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger_config import setup_logger

logger = setup_logger("web_scraper")

# ─── User Agent Rotation ────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]


def _get_headers():
    """Return request headers with a random User-Agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _fetch_page(url: str, retries: int = 3, timeout: int = 15) -> str | None:
    """
    Fetch a URL with retry logic and exponential backoff.
    Returns HTML content or None on failure.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"  [>] Fetching: {url} (attempt {attempt}/{retries})")
            response = requests.get(url, headers=_get_headers(), timeout=timeout)
            response.raise_for_status()
            logger.info(f"  [OK] Success: {response.status_code} | Size: {len(response.text):,} bytes")
            return response.text
        except requests.exceptions.Timeout:
            logger.warning(f"  [T] Timeout on attempt {attempt} for {url}")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"  [X] HTTP {e.response.status_code} on attempt {attempt} for {url}")
            if e.response.status_code == 404:
                return None  # Don't retry 404s
        except requests.exceptions.ConnectionError:
            logger.warning(f"  [!] Connection error on attempt {attempt} for {url}")
        except Exception as e:
            logger.warning(f"  [!] Error on attempt {attempt}: {e}")

        if attempt < retries:
            wait = 2 ** attempt + random.uniform(0, 1)
            logger.info(f"  [..] Waiting {wait:.1f}s before retry...")
            time.sleep(wait)

    logger.error(f"  [FAIL] Failed to fetch {url} after {retries} attempts")
    return None


def _clean_text(text: str) -> str:
    """Clean and normalize extracted text."""
    if not text:
        return ""
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove non-printable characters
    text = ''.join(c for c in text if c.isprintable() or c in '\n\t')
    return text[:2000]  # Cap at 2000 chars per field


def _extract_tables(soup: BeautifulSoup, source_name: str) -> list[dict]:
    """Extract all table data from the page."""
    data = []
    tables = soup.find_all("table")
    for table_idx, table in enumerate(tables[:10]):  # Max 10 tables
        headers = []
        for th in table.find_all("th"):
            headers.append(_clean_text(th.get_text()))

        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            row_data = {
                "source": source_name,
                "type": "table_row",
                "table_index": table_idx,
            }
            for i, cell in enumerate(cells):
                col_name = headers[i] if i < len(headers) else f"col_{i}"
                row_data[col_name] = _clean_text(cell.get_text())
                # Also capture any links in cells
                link = cell.find("a")
                if link and link.get("href"):
                    row_data[f"{col_name}_link"] = link.get("href")
            data.append(row_data)
    return data


def _extract_lists(soup: BeautifulSoup, source_name: str) -> list[dict]:
    """Extract list items (ol, ul) from the page."""
    data = []
    for list_elem in soup.find_all(["ul", "ol"]):
        # Skip navigation/menu lists
        parent_class = " ".join(list_elem.parent.get("class", []))
        if any(skip in parent_class.lower() for skip in ["nav", "menu", "footer", "sidebar"]):
            continue

        for li in list_elem.find_all("li", recursive=False):
            text = _clean_text(li.get_text())
            if len(text) > 10:  # Skip short/empty items
                link = li.find("a")
                data.append({
                    "source": source_name,
                    "type": "list_item",
                    "text": text,
                    "href": link.get("href", "") if link else "",
                })
    return data


def _extract_paragraphs(soup: BeautifulSoup, source_name: str) -> list[dict]:
    """Extract meaningful paragraph text."""
    data = []
    for p in soup.find_all("p"):
        text = _clean_text(p.get_text())
        if len(text) > 30:  # Only meaningful paragraphs
            data.append({
                "source": source_name,
                "type": "paragraph",
                "text": text,
                "href": "",
            })
    return data


def _extract_headings(soup: BeautifulSoup, source_name: str) -> list[dict]:
    """Extract all heading levels."""
    data = []
    for level in range(1, 7):
        for h in soup.find_all(f"h{level}"):
            text = _clean_text(h.get_text())
            if text and len(text) > 2:
                data.append({
                    "source": source_name,
                    "type": f"heading_h{level}",
                    "text": text,
                    "href": "",
                })
    return data


def _extract_links(soup: BeautifulSoup, source_name: str, base_url: str) -> list[dict]:
    """Extract all meaningful links."""
    data = []
    seen_links = set()
    for a in soup.find_all("a", href=True):
        text = _clean_text(a.get_text())
        href = a.get("href", "")

        # Resolve relative URLs
        if href and not href.startswith(("http://", "https://", "mailto:", "javascript:")):
            href = urljoin(base_url, href)

        # Deduplicate
        link_key = f"{text}|{href}"
        if link_key in seen_links:
            continue
        seen_links.add(link_key)

        if text and len(text) > 2:
            data.append({
                "source": source_name,
                "type": "link",
                "text": text,
                "href": href,
            })
    return data


def _extract_metadata(soup: BeautifulSoup, source_name: str) -> list[dict]:
    """Extract page metadata (title, description, keywords)."""
    data = []

    title = soup.find("title")
    if title:
        data.append({
            "source": source_name,
            "type": "page_title",
            "text": _clean_text(title.get_text()),
            "href": "",
        })

    for meta in soup.find_all("meta"):
        name = meta.get("name", "").lower()
        content = meta.get("content", "")
        if name in ["description", "keywords", "author"] and content:
            data.append({
                "source": source_name,
                "type": f"meta_{name}",
                "text": _clean_text(content),
                "href": "",
            })

    return data


def _extract_structured_content(soup: BeautifulSoup, source_name: str) -> list[dict]:
    """
    Extract specific structured content patterns common in scraping targets:
    - Quotes (quotes.toscrape.com)
    - Books (books.toscrape.com)
    - Jobs (fake-jobs)
    - Products (webscraper.io)
    """
    data = []

    # ── Quotes pattern ──
    for quote_div in soup.find_all("div", class_="quote"):
        text_span = quote_div.find("span", class_="text")
        author = quote_div.find("small", class_="author")
        tags = [tag.get_text(strip=True) for tag in quote_div.find_all("a", class_="tag")]
        if text_span:
            data.append({
                "source": source_name,
                "type": "quote",
                "text": _clean_text(text_span.get_text()),
                "author": _clean_text(author.get_text()) if author else "",
                "tags": ", ".join(tags),
                "href": "",
            })

    # ── Books pattern ──
    for article in soup.find_all("article", class_="product_pod"):
        title_elem = article.find("h3")
        price_elem = article.find("p", class_="price_color")
        rating_elem = article.find("p", class_=lambda c: c and "star-rating" in c)
        link = article.find("a")

        rating = ""
        if rating_elem:
            classes = rating_elem.get("class", [])
            for cls in classes:
                if cls != "star-rating":
                    rating = cls

        if title_elem:
            data.append({
                "source": source_name,
                "type": "book",
                "text": _clean_text(title_elem.get_text()),
                "price": _clean_text(price_elem.get_text()) if price_elem else "",
                "rating": rating,
                "href": link.get("href", "") if link else "",
            })

    # ── Jobs pattern (realpython fake-jobs) ──
    for card in soup.find_all("div", class_="card-content"):
        title = card.find("h2", class_="title")
        company = card.find("h3", class_="company")
        location = card.find("p", class_="location")
        posted = card.find("time")

        if title:
            data.append({
                "source": source_name,
                "type": "job",
                "text": _clean_text(title.get_text()),
                "company": _clean_text(company.get_text()) if company else "",
                "location": _clean_text(location.get_text()) if location else "",
                "posted": _clean_text(posted.get_text()) if posted else "",
                "href": "",
            })

    # ── Product cards pattern (webscraper.io) ──
    for card in soup.find_all("div", class_="thumbnail"):
        title = card.find("a", class_="title")
        price = card.find("h4", class_="price") or card.find("h4")
        desc = card.find("p", class_="description")
        rating_div = card.find("div", class_="ratings")
        stars = len(rating_div.find_all("span", class_="ws-icon-star")) if rating_div else 0

        if title:
            data.append({
                "source": source_name,
                "type": "product",
                "text": _clean_text(title.get_text()),
                "price": _clean_text(price.get_text()) if price else "",
                "description": _clean_text(desc.get_text()) if desc else "",
                "rating": str(stars),
                "href": title.get("href", ""),
            })

    # ── HackerNews pattern ──
    for item in soup.find_all("tr", class_="athing"):
        title_elem = item.find("span", class_="titleline")
        if title_elem:
            link = title_elem.find("a")
            score_row = item.find_next_sibling("tr")
            score_elem = score_row.find("span", class_="score") if score_row else None
            age_elem = score_row.find("span", class_="age") if score_row else None

            data.append({
                "source": source_name,
                "type": "news_story",
                "text": _clean_text(link.get_text()) if link else "",
                "score": _clean_text(score_elem.get_text()) if score_elem else "",
                "age": _clean_text(age_elem.get_text()) if age_elem else "",
                "href": link.get("href", "") if link else "",
            })

    return data


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN SCRAPING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_single_url(url: str, source_name: str = "", expected_elements: list = None) -> list[dict]:
    """
    Scrape a single URL and extract ALL available structured data.
    Returns a list of dictionaries.
    """
    if not source_name:
        source_name = urlparse(url).netloc

    html = _fetch_page(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")

    all_data = []

    # 1. Try structured content patterns first (these are the richest)
    structured = _extract_structured_content(soup, source_name)
    if structured:
        logger.info(f"  [+] Found {len(structured)} structured items")
        all_data.extend(structured)

    # 2. Extract metadata
    meta = _extract_metadata(soup, source_name)
    all_data.extend(meta)

    # 3. Extract headings
    headings = _extract_headings(soup, source_name)
    logger.info(f"  [+] Found {len(headings)} headings")
    all_data.extend(headings)

    # 4. Extract tables
    tables = _extract_tables(soup, source_name)
    if tables:
        logger.info(f"  [+] Found {len(tables)} table rows")
        all_data.extend(tables)

    # 5. Extract paragraphs
    paragraphs = _extract_paragraphs(soup, source_name)
    logger.info(f"  [+] Found {len(paragraphs)} paragraphs")
    all_data.extend(paragraphs)

    # 6. Extract links
    links = _extract_links(soup, source_name, url)
    logger.info(f"  [+] Found {len(links)} links")
    all_data.extend(links)

    # 7. Extract lists
    lists = _extract_lists(soup, source_name)
    logger.info(f"  [+] Found {len(lists)} list items")
    all_data.extend(lists)

    return all_data


def scrape_multiple_urls(url_configs: list[dict]) -> list[dict]:
    """
    Scrape multiple URLs from the AI-generated plan.
    Each url_config is a dict with: url, source_name, description, expected_elements
    """
    all_data = []
    total_urls = len(url_configs)

    for i, config in enumerate(url_configs, 1):
        url = config.get("url", "")
        source_name = config.get("source_name", urlparse(url).netloc)
        description = config.get("description", "")
        expected = config.get("expected_elements", [])

        print()
        logger.info(f"{'='*60}")
        logger.info(f">>> [{i}/{total_urls}] Scraping: {source_name}")
        logger.info(f"   URL: {url}")
        logger.info(f"   Description: {description}")
        logger.info(f"{'='*60}")

        data = scrape_single_url(url, source_name, expected)

        logger.info(f"  [+] Collected {len(data)} rows from {source_name}")
        all_data.extend(data)

        # Rate limiting — be polite
        if i < total_urls:
            delay = random.uniform(1.0, 2.5)
            logger.info(f"  [..] Rate limiting: waiting {delay:.1f}s...")
            time.sleep(delay)

    return all_data


# Legacy function for backward compatibility
def generic_scraper(url: str) -> list[dict]:
    """Legacy single-URL scraper. Use scrape_single_url instead."""
    return scrape_single_url(url)