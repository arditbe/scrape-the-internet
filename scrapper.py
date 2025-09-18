

import os
import json
import time
import random
import re
import hashlib
from urllib.parse import quote_plus, urlparse
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

QUERY = "Who invented Apple, its co-founders, and history from 1976 to 2025"
NUM_PAGES = 3
RESULTS_PER_PAGE = 10
OUTPUT_DIR = "scraped_content"
JSONL_OUT = os.path.join(OUTPUT_DIR, "apple_multi.jsonl")

MIN_TEXT_LEN = 50
PAUSE_BETWEEN_REQUESTS = 1.0         
PAGE_FETCH_TIMEOUT = 12             
SEL_PAGE_LOAD_TIMEOUT = 25         
MAX_URLS = 120                    
TAGS_TO_SCRAPE = ["h1", "h2", "h3", "h4", "p", "li"]
SKIP_EXTENSIONS = [".pdf", ".mp4", ".zip", ".exe", ".jpg", ".png", ".gif", ".svg"]

os.makedirs(OUTPUT_DIR, exist_ok=True)


chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--no-proxy-server")
chrome_options.add_argument("--proxy-server='direct://'")
chrome_options.add_argument("--proxy-bypass-list=*")
chrome_options.add_argument("--window-size=1200,900")
chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(SEL_PAGE_LOAD_TIMEOUT)


SEARCH_ENGINES = {
    "duckduckgo": "https://duckduckgo.com/?q={query}&s={start}",
    "bing":       "https://www.bing.com/search?q={query}&first={start}",
    "yandex":     "https://yandex.com/search/?text={query}&p={start_page}",
    "aol":        "https://search.aol.com/aol/search?q={query}&s_it=sb-top&b={start}"
}

SELECTORS = {
    "duckduckgo": ["a.result__a"],
    "bing": ["li.b_algo h2 a"],
    "yandex": ["a.link.link_theme_normal.organic__url", "a.organic__url"],
    "aol": ["div.algo a", "div.algo h3 a"]
}


session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
})


def safe_url(href: str) -> bool:
    if not href or not href.startswith("http"):
        return False
    low = href.lower().split("?")[0].split("#")[0]
    if any(low.endswith(ext) for ext in SKIP_EXTENSIONS):
        return False
    try:
        host = urlparse(href).hostname or ""
        if host.startswith("localhost") or host.startswith("127.") or host.endswith(".local"):
            return False
    except Exception:
        return False
    return True

def short_hash(text: str) -> str:
    h = hashlib.sha1(text.encode("utf-8", errors="ignore"))
    return h.hexdigest()[:16]

def write_jsonl_record(outfile_path: str, record: dict):

    with open(outfile_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def search_urls(query: str, pages: int = 1, max_urls: int = MAX_URLS):
    found = []
    seen = set()
    for engine, template in SEARCH_ENGINES.items():
        for page in range(pages):
            if len(found) >= max_urls:
                return found
            start = page * RESULTS_PER_PAGE
            if engine == "yandex":
                url = template.format(query=quote_plus(query), start_page=page)
            else:
                url = template.format(query=quote_plus(query), start=start)
            try:
                driver.get(url)
                time.sleep(PAUSE_BETWEEN_REQUESTS + random.uniform(0.2, 0.6))
                soup = BeautifulSoup(driver.page_source, "lxml")
                for sel in SELECTORS.get(engine, []):
                    for a in soup.select(sel):
                        href = a.get("href") or a.attrs.get("data-href") or a.attrs.get("data-url") or ""
                        if not href:
                            continue
                        if not safe_url(href):
                            continue
                        if href in seen:
                            continue
                        seen.add(href)
                        found.append(href)
                        if len(found) >= max_urls:
                            return found
            except (TimeoutException, WebDriverException) as e:

                time.sleep(0.5)
            except Exception:
                time.sleep(0.2)
    return found


def fetch_and_extract(url: str, min_len: int = MIN_TEXT_LEN):
    try:
        r = session.get(url, timeout=PAGE_FETCH_TIMEOUT)
        if r.status_code != 200 or not r.text:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        texts = []
        for tag in TAGS_TO_SCRAPE:
            for el in soup.find_all(tag):
                txt = el.get_text(separator=" ", strip=True)
                if not txt:
                    continue
                txt = re.sub(r"\s+", " ", txt)
                if len(txt) >= min_len:
                    texts.append(txt)

        if not texts:
            body = soup.body.get_text(" ", strip=True) if soup.body else soup.get_text(" ", strip=True)
            if body:
                body = re.sub(r"\s+", " ", body)
                if len(body) >= min_len:
                    texts.append(body)
        return texts
    except requests.exceptions.RequestException:
        return []
    except Exception:
        return []

if __name__ == "__main__":

    open(JSONL_OUT, "w", encoding="utf-8").close()


    seen_hashes = set()
    try:
        urls = search_urls(QUERY, NUM_PAGES, MAX_URLS)
        for u in urls:
            if not safe_url(u):
                continue

            extracted_texts = fetch_and_extract(u, MIN_TEXT_LEN)
            for t in extracted_texts:
                h = short_hash(t[:400])  
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)
                rec = {"url": u, "text": t}
                write_jsonl_record(JSONL_OUT, rec)

            time.sleep(PAUSE_BETWEEN_REQUESTS + random.uniform(0.1, 0.4))
    finally:
        try:
            driver.quit()
        except Exception:
            pass
