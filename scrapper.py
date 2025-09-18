

import os
import json
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from urllib.parse import quote_plus


QUERY = "Who invented Apple, its co-founders, and history from 1976 to 2025" # change this to what u want to search for
NUM_PAGES = 3
RESULTS_PER_PAGE = 10
OUTPUT_DIR = "scraped_content"
JSONL_OUT = os.path.join(OUTPUT_DIR, "apple_multi.jsonl")

MIN_TEXT_LEN = 50
PAUSE_BETWEEN_REQUESTS = 2
TAGS_TO_SCRAPE = ["h1", "h2", "h3", "h4", "p", "i", "em", "strong", "b", "li"]
SKIP_EXTENSIONS = [".pdf", ".mp4", ".zip", ".exe", ".jpg", ".png", ".gif"]

os.makedirs(OUTPUT_DIR, exist_ok=True)


chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--no-proxy-server")
chrome_options.add_argument("--proxy-server='direct://'")
chrome_options.add_argument("--proxy-bypass-list=*")

driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(60)


SEARCH_ENGINES = {
    "duckduckgo": "https://duckduckgo.com/?q={query}&s={start}",
    "bing":       "https://www.bing.com/search?q={query}&first={start}",
    "yandex":     "https://yandex.com/search/?text={query}&p={start_page}",
    "aol":        "https://search.aol.com/aol/search?q={query}&s_it=sb-top&b={start}"
}

SELECTORS = {
    "duckduckgo": ["a.result__a"],
    "bing": ["li.b_algo h2 a"],
    "yandex": ["a.link.link_theme_normal.organic__url"],
    "aol": ["div.algo a"]
}


def search_urls(query, pages=1):
    urls = set()
    for engine, template in SEARCH_ENGINES.items():
        print(f"[ENGINE] Searching {engine}")
        for page in range(pages):
            start = page * RESULTS_PER_PAGE
            if engine == "yandex":
                url = template.format(query=quote_plus(query), start_page=page)
            else:
                url = template.format(query=quote_plus(query), start=start)
            try:
                print(f"[SEARCH] {url}")
                driver.get(url)
                time.sleep(PAUSE_BETWEEN_REQUESTS)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                for sel in SELECTORS.get(engine, []):
                    for a in soup.select(sel):
                        href = a.get("href")
                        if href and href.startswith("http") and not any(href.endswith(ext) for ext in SKIP_EXTENSIONS):
                            urls.add(href)
            except Exception as e:
                print(f"[ERROR] {engine} search failed: {e}")
            time.sleep(PAUSE_BETWEEN_REQUESTS)
    return list(urls)


def scrape_text(urls):
    texts = []
    for url in urls:
        try:
            if any(url.endswith(ext) for ext in SKIP_EXTENSIONS):
                continue
            print(f"[SCRAPE] {url}")
            driver.get(url)
            time.sleep(PAUSE_BETWEEN_REQUESTS)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            for tag in TAGS_TO_SCRAPE:
                for el in soup.find_all(tag):
                    text = el.get_text().strip()
                    if len(text) >= MIN_TEXT_LEN:
                        texts.append(text)
        except Exception as e:
            print(f"[ERROR] Failed {url}: {e}")
    return texts


def save_jsonl(dataset):
    with open(JSONL_OUT, "w", encoding="utf-8") as f:
        for item in dataset:
            f.write(json.dumps({"text": item}, ensure_ascii=False) + "\n")
    print(f"\n✅ Done. Saved {len(dataset)} items to {JSONL_OUT}")


if __name__ == "__main__":
    urls = search_urls(QUERY, NUM_PAGES)
    print(f"[INFO] Found {len(urls)} URLs.")
    all_texts = scrape_text(urls)
    print(f"[INFO] Collected {len(all_texts)} text elements.")
    save_jsonl(all_texts)
    driver.quit()
