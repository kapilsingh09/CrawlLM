import requests
from bs4 import BeautifulSoup

def generic_scraper(url):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    data = []

    # Extract common elements
    titles = soup.find_all(["h1", "h2", "h3"])
    links = soup.find_all("a")

    for t in titles[:20]:  # limit
        data.append({
            "type": "title",
            "text": t.get_text(strip=True)
        })

    for l in links[:20]:
        data.append({
            "type": "link",
            "text": l.get_text(strip=True),
            "href": l.get("href")
        })

    return data