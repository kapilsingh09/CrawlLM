import sys
import json
from component.gemni_model import get_scrape_plan
from component.web_scraper import generic_scraper
from utils import save_to_csv
from exception import CustomException
from logger_config import setup_logger

logger = setup_logger("main")

def process_prompt(prompt):
    logger.info(f"Processing prompt: {prompt}")
    plan = get_scrape_plan(prompt)

    try:
        plan_json = json.loads(plan)
        url = plan_json["url"]
        logger.info(f"AI extracted URL: {url}")
    except Exception as e:
        logger.error("AI failed to extract URL from plan, using default site.")
        url = "https://news.ycombinator.com"
        logger.info(f"Using default URL: {url}")

    try:
        logger.info(f"Starting generic_scraper for {url}")
        data = generic_scraper(url)

        logger.info(f"Saving data to output.csv")
        save_to_csv(data, "output.csv")

        return data
    except Exception as e:
        raise CustomException(e, sys)


if __name__ == "__main__":
    try:
        user_prompt = input("Enter your prompt: ")
        result = process_prompt(user_prompt)

        print(result[:5])
        
    except Exception as e:
        print(f"An error occurred: {e}")