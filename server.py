"""
CrawlLM — FastAPI Server
Provides REST API endpoints for the web scraping engine.
"""

import sys
import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from logger_config import setup_logger
from exception import CustomException
from component.gemni_model import get_scrape_plan
from component.web_scraper import scrape_multiple_urls
from utils import save_to_csv, print_data_summary

logger = setup_logger("fastapi_server")

app = FastAPI(
    title="CrawlLM API",
    description="AI-Powered Intelligent Web Data Collector for ML Training",
    version="2.0.0",
)



class ScrapeRequest(BaseModel):
    prompt: str
    save_csv: bool = True


class ScrapeResponse(BaseModel):
    status: str
    total_rows: int
    sources_scraped: int
    csv_path: str
    sample_data: list


@app.get("/")
async def root():
    logger.info("GET / endpoint called")
    return {
        "message": "Welcome to CrawlLM API — AI-Powered Web Data Collector",
        "version": "2.0.0",
        "endpoints": {
            "/scrape": "POST — Submit a prompt to scrape data",
            "/health": "GET — Health check",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "version": "2.0.0"}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_data(request: ScrapeRequest):
    """
    Submit a prompt describing what data you need.
    The AI will plan and execute multi-source scraping.
    """
    logger.info(f"POST /scrape — prompt: '{request.prompt}'")

    try:
        # Get AI scraping plan
        plan = get_scrape_plan(request.prompt)
        urls = plan.get("urls", [])

        if not urls:
            raise HTTPException(status_code=400, detail="AI could not generate a scraping plan")

        # Scrape all URLs
        data = scrape_multiple_urls(urls)

        csv_path = ""
        if request.save_csv and data:
            csv_path = save_to_csv(data, "output.csv")

        return ScrapeResponse(
            status="success",
            total_rows=len(data),
            sources_scraped=len(urls),
            csv_path=csv_path,
            sample_data=data[:10],
        )

    except Exception as e:
        logger.error(f"Scrape error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting CrawlLM API server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
