from google import genai
import json

client = genai.Client(api_key="AIzaSyC9UMh9MCrURILgGrefJzGRbQrm2N4MGFo")

def get_scrape_plan(prompt):
    system_prompt = """
    You are an AI that converts user prompts into scraping plans.

    Return JSON like:
    {
        "url": "...",
        "data_type": "jobs/news/products/other"
    }

    Use free websites like:
    - https://news.ycombinator.com
    - https://books.toscrape.com
    - https://quotes.toscrape.com
    """

    response = client.models.generate_content(
        model="gemini-3-flash-preview",
        contents=system_prompt + "\nUser: " + prompt
    )

    return response.text