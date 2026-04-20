import csv
import os
import sys
import time
from datetime import datetime
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
from bs4 import BeautifulSoup, Tag

from logger_config import setup_logger
from exception import CustomException

logger = setup_logger("web_scraper")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch_page(url: str, retries: int = 3, delay: float = 2.0) -> BeautifulSoup | None:
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Fetching [{attempt}/{retries}]: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            logger.debug(f"OK {resp.status_code} — {len(resp.content)} bytes")
            return BeautifulSoup(resp.text, "html.parser")

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error attempt {attempt}: {e}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error attempt {attempt}: {e}")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout attempt {attempt}: {url}")
        except requests.exceptions.RequestException as e:
            logger.exception(f"Request error: {e}")
            break

        if attempt < retries:
            logger.info(f"Retrying in {delay}s...")
            time.sleep(delay)

    logger.error(f"All {retries} attempts failed: {url}")
    return None


def get_text(element: Tag | None) -> str:
    if element is None:
        return "N/A"
    return element.get_text(separator=" ", strip=True)


def extract_field(soup: BeautifulSoup, field: str, selectors_str: str) -> str:
    if not selectors_str:
        return "N/A"

    selectors = [s.strip() for s in selectors_str.split(",")]
    for selector in selectors:
        try:
            el = soup.select_one(selector)
            if el:
                val = get_text(el)
                if val and val != "N/A":
                    logger.debug(f"  '{field}' matched '{selector}': {val[:60]}")
                    return val
        except Exception as e:
            logger.warning(f"  Bad selector '{selector}' for '{field}': {e}")

    logger.debug(f"  '{field}' — no selector matched")
    return "N/A"


def detect_page_structure(soup: BeautifulSoup) -> str:
    if soup.select("article.product_pod"):
        return "product_cards"
    if soup.select("div.quote"):
        return "quote_cards"
    if soup.select("tr.athing") or soup.select("span.titleline"):
        return "hn_list"
    if soup.select("table.wikitable"):
        return "wiki_table"
    if soup.select("table"):
        return "generic_table"
    if soup.select("article") or soup.select(".post") or soup.select(".card"):
        return "generic_cards"
    return "single_page"


def extract_rows_product_cards(
    soup: BeautifulSoup,
    fields: list[str],
    hints: dict[str, str],
    url: str,
) -> list[dict[str, Any]]:
    cards = soup.select("article.product_pod")
    logger.info(f"Found {len(cards)} product cards")
    rows = []
    for card in cards:
        row: dict[str, Any] = {}
        for field in fields:
            selector = hints.get(field, "")
            el = card.select_one(selector.split(",")[0].strip()) if selector else None
            if el is None and selector:
                for sel in selector.split(","):
                    el = card.select_one(sel.strip())
                    if el:
                        break
            if field == "rating" and el:
                classes = el.get("class", [])
                rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
                for cls in classes:
                    if cls in rating_map:
                        row[field] = rating_map[cls]
                        break
                else:
                    row[field] = get_text(el)
            elif field == "price" and el:
                row[field] = get_text(el).replace("Â", "").replace("£", "").strip()
            else:
                row[field] = get_text(el) if el else "N/A"
        row["source_url"] = url
        row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(row)
    return rows


def extract_rows_quote_cards(
    soup: BeautifulSoup,
    fields: list[str],
    hints: dict[str, str],
    url: str,
) -> list[dict[str, Any]]:
    cards = soup.select("div.quote")
    logger.info(f"Found {len(cards)} quote cards")
    rows = []
    for card in cards:
        row: dict[str, Any] = {}
        for field in fields:
            selector = hints.get(field, "")
            el = None
            if selector:
                for sel in selector.split(","):
                    el = card.select_one(sel.strip())
                    if el:
                        break
            if field == "tags" and el:
                tags = [a.get_text(strip=True) for a in card.select("a.tag")]
                row[field] = "|".join(tags)
            else:
                row[field] = get_text(el) if el else "N/A"
        row["source_url"] = url
        row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(row)
    return rows


def extract_rows_hn_list(
    soup: BeautifulSoup,
    fields: list[str],
    hints: dict[str, str],
    url: str,
) -> list[dict[str, Any]]:
    titles = soup.select("span.titleline")
    scores = soup.select("span.score")
    subtexts = soup.select("td.subtext")
    logger.info(f"Found {len(titles)} HN items")
    rows = []
    for i, title_el in enumerate(titles):
        link = title_el.find("a")
        row: dict[str, Any] = {
            "title": get_text(link) if link else "N/A",
            "url_link": link.get("href", "N/A") if link else "N/A",
            "score": get_text(scores[i]).replace(" points", "") if i < len(scores) else "0",
        }
        if i < len(subtexts):
            comments_link = subtexts[i].select("a")
            row["comments"] = get_text(comments_link[-1]) if comments_link else "0"
            site_el = subtexts[i].select_one("span.sitestr")
            row["site"] = get_text(site_el)
        for field in fields:
            if field not in row:
                selector = hints.get(field, "")
                row[field] = extract_field(soup, field, selector)
        row["source_url"] = url
        row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(row)
    return rows


def extract_rows_wiki_table(
    soup: BeautifulSoup,
    fields: list[str],
    hints: dict[str, str],
    url: str,
) -> list[dict[str, Any]]:
    table = soup.select_one("table.wikitable")
    if not table:
        logger.warning("No wikitable found")
        return []

    headers_els = table.select("th")
    headers = [get_text(h).lower().replace(" ", "_") for h in headers_els]
    logger.info(f"Wiki table headers: {headers}")

    rows = []
    for tr in table.select("tr")[1:]:
        cells = tr.select("td")
        if not cells:
            continue
        row: dict[str, Any] = {}
        for i, cell in enumerate(cells):
            col_name = headers[i] if i < len(headers) else f"col_{i}"
            row[col_name] = get_text(cell)
        row["source_url"] = url
        row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(row)

    logger.info(f"Extracted {len(rows)} rows from wikitable")
    return rows


def extract_rows_generic_table(
    soup: BeautifulSoup,
    fields: list[str],
    hints: dict[str, str],
    url: str,
) -> list[dict[str, Any]]:
    table = soup.find("table")
    if not table:
        return []

    header_row = table.find("tr")
    headers = [get_text(th) for th in header_row.find_all(["th", "td"])] if header_row else []
    logger.info(f"Generic table headers: {headers}")

    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue
        row: dict[str, Any] = {}
        for i, cell in enumerate(cells):
            col = headers[i].lower().replace(" ", "_") if i < len(headers) else f"col_{i}"
            row[col] = get_text(cell)
        row["source_url"] = url
        row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(row)

    logger.info(f"Extracted {len(rows)} rows from generic table")
    return rows


def extract_rows_single_page(
    soup: BeautifulSoup,
    fields: list[str],
    hints: dict[str, str],
    url: str,
) -> list[dict[str, Any]]:
    row: dict[str, Any] = {}
    for field in fields:
        selector = hints.get(field, "")
        row[field] = extract_field(soup, field, selector)
    row["source_url"] = url
    row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [row]


def extract_all_rows(
    soup: BeautifulSoup,
    fields: list[str],
    hints: dict[str, str],
    url: str,
) -> list[dict[str, Any]]:
    structure = detect_page_structure(soup)
    logger.info(f"Page structure detected: {structure}")

    if structure == "product_cards":
        return extract_rows_product_cards(soup, fields, hints, url)
    elif structure == "quote_cards":
        return extract_rows_quote_cards(soup, fields, hints, url)
    elif structure == "hn_list":
        return extract_rows_hn_list(soup, fields, hints, url)
    elif structure == "wiki_table":
        return extract_rows_wiki_table(soup, fields, hints, url)
    elif structure == "generic_table":
        return extract_rows_generic_table(soup, fields, hints, url)
    else:
        return extract_rows_single_page(soup, fields, hints, url)


def scrape_urls(
    urls: list[str],
    fields: list[str],
    hints: dict[str, str],
    delay_between: float = 1.5,
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []

    for i, url in enumerate(urls, 1):
        logger.info(f"--- URL {i}/{len(urls)}: {url}")
        soup = fetch_page(url)

        if soup is None:
            logger.warning(f"Skipping (fetch failed): {url}")
            continue

        try:
            rows = extract_all_rows(soup, fields, hints, url)
            all_rows.extend(rows)
            logger.info(f"Got {len(rows)} rows from this page (total so far: {len(all_rows)})")
        except Exception as e:
            logger.exception(f"Extraction failed for {url}: {e}")

        if i < len(urls):
            time.sleep(delay_between)

    logger.info(f"Total rows scraped: {len(all_rows)}")
    return all_rows


def save_to_csv(rows: list[dict[str, Any]], filename: str) -> str:
    os.makedirs("output", exist_ok=True)
    filepath = os.path.join("output", filename)

    if not rows:
        logger.warning("No rows to save")
        return ""

    try:
        all_keys: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for k in row:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Saved {len(rows)} rows, {len(all_keys)} columns → {filepath}")
        return filepath

    except OSError as e:
        logger.exception(f"Failed to write CSV: {e}")
        return ""
    except Exception as e:
        logger.exception(f"Unexpected CSV error: {e}")
        return ""