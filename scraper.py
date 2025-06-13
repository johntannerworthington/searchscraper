import csv
import requests
import os
import uuid
import tldextract
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

SERPER_ENDPOINT = "https://google.serper.dev/search"
UPLOADS_DIR = "uploads"
MAX_WORKERS = 200
QPS_LIMIT = None  # Optional: set to int like 5 for throttling
BASE_URL = "https://searchscraper.onrender.com"

lock = threading.Lock()
seen_domains = set()
all_fields = set()
all_rows = []
completed_counter = 0
api_call_count = 0

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
    global completed_counter, api_call_count
    print(f"\n[{index}/{total}] Query: {query}", flush=True)

    session = requests.Session()
    session.headers.update({
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    })

    page = 1
    while True:
        if semaphore:
            with semaphore:
                sleep(1.0 / QPS_LIMIT)

        payload = {"q": query, "page": page}
        try:
            response = session.post(SERPER_ENDPOINT, json=payload, timeout=10)
            with lock:
                api_call_count += 1
            response.raise_for_status()
            data = response.json()
            organic = data.get("organic", [])
        except Exception as e:
            print(f"❌ Error on page {page}: {e}", flush=True)
            break

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

    with lock:
        completed_counter += 1
        if completed_counter % 100 == 0 or completed_counter == total:
            print(f"✅ Completed {completed_counter}/{total} queries. Current unique domains: {len(seen_domains)}", flush=True)

def run_search_scraper(queries_path, api_key):
    queries = load_queries(queries_path)
    session_id = str(uuid.uuid4())
    session_dir = os.path.join(UPLOADS_DIR, session_id)
    os.makedirs(session_dir, exist_ok=True)
    output_path = os.path.join(session_dir, "output.csv")

    print(f"🔎 Starting scrape for {len(queries)} queries with up to {MAX_WORKERS} workers", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch_query, query, idx + 1, len(queries), api_key)
            for idx, query in enumerate(queries)
        ]
        for _ in as_completed(futures):
            pass

    if all_rows:
        with open(output_path, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(all_fields))
            writer.writeheader()
            writer.writerows(all_rows)

    print(f"\n✅ Done! Wrote {len(seen_domains)} deduplicated domains to '{output_path}'", flush=True)
    print(f"📊 Total API calls made to Serper: {api_call_count}", flush=True)
    print(f"✅ Download your results here: {BASE_URL}/download/{session_id}", flush=True)

    return session_id
