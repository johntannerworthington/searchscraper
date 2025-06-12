import csv
import requests
import os
import uuid
import tldextract
from urllib.parse import urlparse

SERPER_ENDPOINT = "https://google.serper.dev/search"
UPLOADS_DIR = "uploads"

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

def run_search_scraper(queries_path, api_key):
    queries = load_queries(queries_path)
    session_id = str(uuid.uuid4())
    output_path = os.path.join(UPLOADS_DIR, f"{session_id}.csv")

    seen_domains = set()
    all_fields = set()
    all_rows = []

    print(f"🔎 Starting scrape for {len(queries)} queries")

    for idx, query in enumerate(queries, 1):
        print(f"\n[{idx}/{len(queries)}] Query: {query}")
        page = 1

        while True:
            payload = {"q": query, "page": page}
            headers = {
                "X-API-KEY": api_key,
                "Content-Type": "application/json"
            }

            try:
                response = requests.post(SERPER_ENDPOINT, json=payload, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                organic = data.get("organic", [])

                if not organic:
                    break

                rows = []
                for result in organic:
                    url = result.get("link", "")
                    domain = normalize_domain(url)
                    if not domain or domain in seen_domains:
                        continue

                    seen_domains.add(domain)
                    result["normalized_domain"] = domain
                    result["query"] = query
                    all_fields.update(result.keys())
                    rows.append(result)

                all_rows.extend(rows)
                print(f"→ Page {page} returned {len(rows)} new domains")
                page += 1
            except Exception as e:
                print(f"❌ Error on page {page}: {e}")
                break

    if all_rows:
        with open(output_path, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(all_fields))
            writer.writeheader()
            writer.writerows(all_rows)

    print(f"\n✅ Done. {len(seen_domains)} unique domains written to {output_path}")
    return output_path
