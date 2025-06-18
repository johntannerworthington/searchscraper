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
QPS_LIMIT = None  # Set to an integer like 5 if throttling is needed
BASE_URL = "https://searchscraper.onrender.com"

lock = threading.Lock()
seen_domains = set()
all_fields = set()
all_rows = []
completed_counter = 0
api_call_count = 0
page_tracker = {}

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
    zero_pages_in_a_row = 0
    page_tracker[query] = {}

    while True:
        if semaphore:
            with semaphore:
                sleep(1.0 / QPS_LIMIT)

        payload = {"q": query, "num": 100}
        if page > 1:
            payload["page"] = page

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

        result_count = len(organic)
        with lock:
            page_tracker[query][f"Page {page}"] = result_count

        if result_count == 0:
            zero_pages_in_a_row += 1
            print(f"⚠️  Page {page} returned 0 results (in a row: {zero_pages_in_a_row})", flush=True)
            if zero_pages_in_a_row >= 2:
                break
        else:
            zero_pages_in_a_row = 0  # Reset if we get a non-zero page

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
        print(f"✅ Completed {completed_counter}/{total} queries. Total unique businesses: {len(seen_domains)} | API credits used: {api_call_count}", flush=True)

def write_tracking_csv(path):
    all_pages = sorted({page for q in page_tracker.values() for page in q})
    with open(path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        header = ["Query"] + all_pages
        writer.writerow(header)
        for query, pages in page_tracker.items():
            row = [query] + [pages.get(page, "") for page in all_pages]
            writer.writerow(row)

def run_search_scraper(queries_path, api_key):
    queries = load_queries(queries_path)
    session_id = str(uuid.uuid4())
    session_dir = os.path.join(UPLOADS_DIR, session_id)
    os.makedirs(session_dir, exist_ok=True)
    output_path = os.path.join(session_dir, "output.csv")
    tracking_path = os.path.join(session_dir, "page_tracker.csv")

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

    write_tracking_csv(tracking_path)

    print(f"\n✅ Done! Wrote {len(seen_domains)} deduplicated domains to '{output_path}'", flush=True)
    print(f"📊 Page result tracker saved to '{tracking_path}'", flush=True)
    print(f"📈 Total API calls made to Serper: {api_call_count}", flush=True)
    print(f"✅ Download your results here: {BASE_URL}/download/{session_id}", flush=True)

    return session_id
