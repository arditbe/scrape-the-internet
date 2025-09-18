

import os
import json
import time
import random
import re
import base64
import socket
from urllib.parse import quote_plus, urlparse, parse_qs, unquote
from typing import List
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait

# -----------------------------
# change those querys to your needs
# -----------------------------
QUERIES = [
    {"query": "Who invented the Romani Language", "file": "romani_language.jsonl"},
    {"query": "History of Roma migration", "file": "migration_history.jsonl"},
    {"query": "Romani folklore and tales", "file": "folklore.jsonl"},
    {"query": "Romani music traditions", "file": "music_traditions.jsonl"},
    {"query": "Romani language dialects", "file": "dialects.jsonl"},
    {"query": "Famous Roma personalities", "file": "famous_personalities.jsonl"},
    {"query": "Roma cultural practices", "file": "cultural_practices.jsonl"},
    {"query": "Challenges faced by Roma communities", "file": "challenges.jsonl"},
    {"query": "Romani crafts and art", "file": "crafts_art.jsonl"},
    {"query": "Romani proverbs and sayings", "file": "proverbs.jsonl"}
]

NUM_PAGES = 3
RESULTS_PER_PAGE = 10
OUTPUT_DIR = "scraped_content"
COMBINED_TEXTS_OUT = os.path.join(OUTPUT_DIR, "romavision_texts_all.jsonl")
COMBINED_PROVENANCE_OUT = os.path.join(OUTPUT_DIR, "romavision_provenance_all.jsonl")


MIN_TEXT_LEN = 500
PAUSE_BETWEEN_REQUESTS = 2.0
PAGE_LOAD_TIMEOUT = 30 
REQUESTS_TIMEOUT = 15  
MAX_URLS_PER_QUERY = 200

TAGS_TO_SCRAPE = ["h1", "h2", "h3", "h4", "p", "i", "em", "strong", "b", "li"]
SKIP_EXTENSIONS = [".pdf", ".mp4", ".zip", ".exe", ".jpg", ".png", ".gif", ".svg", ".woff", ".woff2", ".ttf"]

os.makedirs(OUTPUT_DIR, exist_ok=True)

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--no-proxy-server")
chrome_options.add_argument("--proxy-server='direct://'")
chrome_options.add_argument("--proxy-bypass-list=*")
chrome_options.add_argument("--window-size=1400,1000")

chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

SEARCH_ENGINES = {
    "duckduckgo": "https://duckduckgo.com/?q={query}&s={start}",
    "bing":       "https://www.bing.com/search?q={query}&first={start}",
    "yandex":     "https://yandex.com/search/?text={query}&p={start_page}",
    "aol":        "https://search.aol.com/aol/search?q={query}&s_it=sb-top&b={start}"
}

SELECTORS = {
    "duckduckgo": ["a.result__a", "a[data-testid='result-title-a']"],
    "bing": ["li.b_algo h2 a", "li.b_algo a[href^='http']"],
    "yandex": ["a.link.link_theme_normal.organic__url", "a.organic__url"],
    "aol": ["div.algo a", "div.algo h3 a"]
}


def slugify(text: str, maxlen: int = 120) -> str:
    s = text.lower()
    s = re.sub(r"[^0-9a-zA-Z\-_.]+", "_", s)
    return s[:maxlen].strip("_")

def ensure_jsonl_filename(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return None
    if not name.lower().endswith(".jsonl"):
        name = name + ".jsonl"
    name = re.sub(r"[^0-9A-Za-z._\-]+", "_", name)
    return name

def is_local_or_internal(url: str) -> bool:
    try:
        p = urlparse(url)
        hostname = p.hostname or ""
        if hostname in ("localhost", "127.0.0.1") or hostname.startswith("192.") or hostname.startswith("10.") or hostname.endswith(".local"):
            return True

        return False
    except Exception:
        return True

def has_skip_ext(url: str) -> bool:
    u = url.lower().split("?")[0].split("#")[0]
    return any(u.endswith(ext) for ext in SKIP_EXTENSIONS)

def safe_href(href: str) -> bool:
    if not href:
        return False
    href = href.strip()
    if href.startswith("javascript:"):
        return False
    if href.startswith("/"):
        return False
    if not (href.startswith("http://") or href.startswith("https://")):
        return False
    if is_local_or_internal(href):
        return False
    if has_skip_ext(href):
        return False
    return True

def try_decode_possible_wrapped_url(href: str) -> str:

    try:
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        candidates = []
     
        for k in ("u", "uddg", "r", "ru", "q", "u1", "u2", "url"):
            if k in qs and qs[k]:
                candidates.extend(qs[k])
        # also try fragment-style encodings
        frag = parsed.fragment
        if frag and ("http" in frag or "aHR0" in frag):
            candidates.append(frag)


        if not candidates:

            m = re.search(r"u=([A-Za-z0-9_\-=%]+)", href)
            if m:
                candidates.append(unquote(m.group(1)))

        for c in candidates:
            c = c.strip()
       
            try:
                dec = unquote(c)
            except Exception:
                dec = c
       
            if "aHR0" in dec or re.match(r"^[A-Za-z0-9+/=]+$", dec):
           
                try:
                    b = dec.encode("utf-8")
                    b = re.sub(rb'[^A-Za-z0-9+/=]', b'', b)
                    decoded = base64.b64decode(b + b'==', validate=False).decode("utf-8", errors="ignore")
                    if decoded.startswith("http"):
                        return decoded
                except Exception:
                    pass
    
            if dec.startswith("http://") or dec.startswith("https://"):
                return dec
        return href
    except Exception:
        return href

def search_urls(query: str, pages: int = 1) -> List[str]:
    urls = []
    seen = set()
    for engine, template in SEARCH_ENGINES.items():
        for page in range(pages):
            start = page * RESULTS_PER_PAGE
            if engine == "yandex":
                url = template.format(query=quote_plus(query), start_page=page)
            else:
                url = template.format(query=quote_plus(query), start=start)
            try:
                print(f"[SEARCH] {engine} -> {url}")
                driver.get(url)
    
                time.sleep(PAUSE_BETWEEN_REQUESTS + random.uniform(0.25, 1.0))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                selectors = SELECTORS.get(engine, [])
                for sel in selectors:
                    for a in soup.select(sel):
                        href = a.get("href") or a.get("data-href") or a.get("data-url")
                        if not href:
                    
                            href = a.attrs.get("href", "")
                    
                        href = try_decode_possible_wrapped_url(href)
                        if not safe_href(href):
                            continue
                        if href in seen:
                            continue
                        seen.add(href)
                        urls.append(href)
                        if len(urls) >= MAX_URLS_PER_QUERY:
                            break
                    if len(urls) >= MAX_URLS_PER_QUERY:
                        break
                time.sleep(0.2 + random.uniform(0.1, 0.6))
            except TimeoutException:
                print(f"[WARN] search page load timed out for: {url} (continuing)")
            except WebDriverException as e:
                print(f"[ERROR] WebDriver error while searching {engine}: {e}")
            except Exception as e:
                print(f"[ERROR] Unexpected error during search {engine}: {e}")
            if len(urls) >= MAX_URLS_PER_QUERY:
                break
    return urls

def fetch_page_html(url: str) -> str:
    """
    Try fetching page HTML via Selenium (preferred). If Selenium fails
    (timeout, webdriver issues), fallback to requests with a short timeout.
    Returns HTML string or empty string on failure.
    """
    if is_local_or_internal(url):
        print(f"[SKIP] internal/local url: {url}")
        return ""

    try:
        driver.get(url)
        try:
            WebDriverWait(driver, min(5, PAGE_LOAD_TIMEOUT)).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except Exception:
            pass
        time.sleep(0.5 + random.uniform(0.2, 0.8))
        return driver.page_source
    except TimeoutException:
        print(f"[WARN] Selenium timed out for {url}, trying requests fallback")
    except WebDriverException as e:
        print(f"[WARN] Selenium WebDriver error for {url}: {e} -- trying requests fallback")
    except Exception as e:
        print(f"[WARN] Selenium unexpected error for {url}: {e} -- trying requests fallback")

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=REQUESTS_TIMEOUT)
        if resp.status_code == 200 and resp.text:
            return resp.text
        else:
            print(f"[WARN] requests returned status {resp.status_code} for {url}")
            return ""
    except requests.exceptions.ReadTimeout:
        print(f"[ERROR] requests read timeout for {url}")
    except requests.exceptions.ConnectTimeout:
        print(f"[ERROR] requests connect timeout for {url}")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] requests exception for {url}: {e}")
    return ""


def scrape_text(urls: List[str], min_text_len: int = MIN_TEXT_LEN) -> List[str]:
    texts = []
    seen_keys = set()
    for url in urls:
        if not url:
            continue
        if has_skip_ext(url):
            print(f"[SKIP] skipping binary/media url: {url}")
            continue
        print(f"[SCRAPE] {url}")
        html = fetch_page_html(url)
        if not html:
            print(f"[WARN] empty html for {url} -> skipped")
            continue
        soup = BeautifulSoup(html, "html.parser")

        page_texts = []
        for tag in TAGS_TO_SCRAPE:
            for el in soup.find_all(tag):
                text = el.get_text(separator=" ", strip=True)
                if not text:
                    continue
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) >= min_text_len:
                    key = text[:200]
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    page_texts.append(text)


        if not page_texts:
            body = soup.body.get_text(" ", strip=True) if soup.body else soup.get_text(" ", strip=True)
            if body:
                body = re.sub(r"\s+", " ", body).strip()
                if len(body) >= min_text_len:
                    key = body[:200]
                    if key not in seen_keys:
                        seen_keys.add(key)
                        page_texts.append(body)

        for t in page_texts:
            texts.append(t)

        time.sleep(PAUSE_BETWEEN_REQUESTS + random.uniform(0.1, 0.8))
    return texts


def save_texts_jsonl(text_list: List[str], outpath: str) -> None:
    with open(outpath, "w", encoding="utf-8") as f:
        for item in text_list:
            f.write(json.dumps({"text": item}, ensure_ascii=False) + "\n")
    print(f"[SAVE] Saved {len(text_list)} texts to {outpath}")

def save_provenance_jsonl(items: List[dict], outpath: str) -> None:
    with open(outpath, "w", encoding="utf-8") as f:
        for itm in items:
            f.write(json.dumps(itm, ensure_ascii=False) + "\n")
    print(f"[SAVE] Saved {len(items)} provenance records to {outpath}")

if __name__ == "__main__":
    try:
        combined_texts = []
        combined_provenance = []

        for idx, qspec in enumerate(QUERIES, start=1):
            query = qspec.get("query") if isinstance(qspec, dict) else str(qspec)
            desired_file = qspec.get("file") if isinstance(qspec, dict) else None
            print("\n" + "=" * 80)
            print(f"[{idx}/{len(QUERIES)}] Processing query: {query}")

            out_filename = ensure_jsonl_filename(desired_file) if desired_file else f"{slugify(query)}.jsonl"
            per_query_out = os.path.join(OUTPUT_DIR, out_filename)

            urls = search_urls(query, NUM_PAGES)
            print(f"[INFO] Found {len(urls)} candidate URLs for query: {query}")

            texts = scrape_text(urls)
            print(f"[INFO] Collected {len(texts)} text elements for query: {query}")

            save_texts_jsonl(texts, per_query_out)

           
            for t in texts:
                combined_texts.append(t)
                combined_provenance.append({"query": query, "text": t})

        save_texts_jsonl(combined_texts, COMBINED_TEXTS_OUT)
        save_provenance_jsonl(combined_provenance, COMBINED_PROVENANCE_OUT)

    except Exception as e:
        print(f"[FATAL] Unexpected error: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
