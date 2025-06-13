import csv
import requests
import os
import uuid
import tldextract
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

SERPER_ENDPOINT = "https://google.serper.dev/search"
UPLOADS_DIR = "uploads"
MAX_WORKERS = 200
QPS_LIMIT = None  # Set to an int (e.g., 5) to enforce QPS throttling

lock = threading.Lock()
seen_domains = set()
all_fields = set()
all_rows = []

if QPS_LIMIT:
    semaphore = threading.Semaphore(QPS_LIMIT)
else:
    semaphore = None

def normalize_domain(url):
    try:
        ext = tldextract.extract(url)
        return f"{ext.domain}.{ext.suffix}".lower()
    except:
        return None

def load_queries(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        return [row[0] for row in reader if row]

def fetch_query(query, index, total, api_key):
    print(f"\n[{index}/{total}] Query: {query}", flush=True)
    session = requests.Session()
    session.headers.update({
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    })

    page = 1
    local_rows = []

    while True:
        if semaphore:
            with semaphore:
                sleep(1.0 / QPS_LIMIT)
                payload = {"q": query, "page": page}
                try:
                    response = session.post(SERPER_ENDPOINT, json=payload, timeout=10)
                except Exception as e:
                    print(f"❌ Request failed on page {page}: {e}", flush=True)
                    break
        else:
            payload = {"q": query, "page": page}
            try:
                response = session.post(SERPER_ENDPOINT, json=payload, timeout=10)
            except Exception as e:
                print(f"❌ Request failed on page {page}: {e}", flush=True)
                break

        try:
            response.raise_for_status()
            data = response.json()
            organic = data.get("organic", [])
            if not organic:
                break

            new_domains = 0
            for result in organic:
                url = result.get("link", "")
                domain = normalize_domain(url)
                if not domain:
                    continue

                with lock:
                    if domain in seen_domains:
                        continue
                    seen_domains.add(domain)

                result["normalized_domain"] = domain
                result["query"] = query
                with lock:
                    all_fields.update(result.keys())
                    all_rows.append(result)
                new_domains += 1

            print(f"→ Page {page} returned {new_domains} new domains", flush=True)
            page += 1
        except Exception as e:
            print(f"❌ Error on page {page}: {e}", flush=True)
            break

def run_search_scraper(queries_path, api_key):
    queries = load_queries(queries_path)
    session_id = str(uuid.uuid4())
    output_path = os.path.join(UPLOADS_DIR, f"{session_id}.csv")

    print(f"🔎 Starting scrape for {len(queries)} queries with up to {MAX_WORKERS} workers", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch_query, query, idx + 1, len(queries), api_key)
            for idx, query in enumerate(queries)
        ]

        for future in as_completed(futures):
            pass  # All print and data collection is handled inside `fetch_query`

    if all_rows:
        os.makedirs(UPLOADS_DIR, exist_ok=True)
        with open(output_path, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(all_fields))
            writer.writeheader()
            writer.writerows(all_rows)

    print(f"\n✅ Done. {len(seen_domains)} unique domains written to {output_path}", flush=True)
    return output_path
