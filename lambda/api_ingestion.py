"""olist_api_ingest: pulls NEW data from the Olist API into S3.

1. Reference tables (Brazilian states), small, refreshed every run
       GET /tables, /tables/<name>   -> raw/api_<name>/<name>.jsonl
2. New days from the feed, incremental
       GET /feed/status              which days are published
       GET /feed/events?date=<day>   for each day after the bookmark
   -> raw/api_<table>/event_date=<day>/events.jsonl   (one folder per table)

The bookmark (last day saved) is s3://<BUCKET>/state/api_ingest_bookmark.json.
It moves forward after every saved day, so a failed run resumes where it
stopped, and a day is never saved twice.

Environment variables: BUCKET, OLIST_API_URL, OLIST_API_KEY (optional),
MAX_DAYS_PER_RUN (optional, default 31)

Test events:
  {}                                       new days since the bookmark
  {"max_days": 60}                         at most 60 new days this run
  {"date": "2018-10-20"}                   re-save one day, bookmark unchanged
  {"from_date": "2018-10-18", "max_days": 60}   start again from that day
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
BOOKMARK_KEY = "state/api_ingest_bookmark.json"


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_json(url, api_key="", attempts=3):
    """GET a URL and return the JSON. Retries twice on network or server errors."""
    headers = {"Accept": "application/json", "User-Agent": "olistcap-ingest/2.0"}
    if api_key:
        headers["x-api-key"] = api_key
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:300]
            if e.code < 500 or attempt == attempts:          # 4xx: retrying will not help
                raise RuntimeError(f"GET {url} failed: HTTP {e.code} {detail}") from None
        except urllib.error.URLError as e:
            if attempt == attempts:
                raise RuntimeError(f"GET {url} failed: {e.reason}") from None
        print(f"GET {url} failed (attempt {attempt}), retrying")
        time.sleep(2 * attempt)


def save_jsonl(bucket, key, rows, source):
    """Write rows to S3 as JSON Lines (one JSON object per line)."""
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"),
                  ContentType="application/x-ndjson",
                  Metadata={"source": source, "ingested-at": now_utc(), "row-count": str(len(rows))})
    print(f"saved {len(rows):>6} rows -> s3://{bucket}/{key}")


def read_bookmark(bucket):
    """The last day saved by an earlier run, or None on the very first run."""
    try:
        body = s3.get_object(Bucket=bucket, Key=BOOKMARK_KEY)["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise                                                # e.g. AccessDenied: fix the role
    return date.fromisoformat(json.loads(body)["last_date"])


def write_bookmark(bucket, day):
    s3.put_object(Bucket=bucket, Key=BOOKMARK_KEY, ContentType="application/json",
                  Body=json.dumps({"last_date": day.isoformat(), "updated_at": now_utc()}).encode())


def pull_table(api_url, api_key, table, page_size=2000):
    """Every row of one reference table, page after page."""
    rows, page = [], 1
    while True:
        query = urllib.parse.urlencode({"page": page, "page_size": page_size})
        data = get_json(f"{api_url}/tables/{urllib.parse.quote(table)}?{query}", api_key)
        rows.extend(data["rows"])
        if not data["has_more"]:
            break
        page += 1
    if len(rows) != data["total_rows"]:                      # completeness check
        raise RuntimeError(f"{table}: got {len(rows)} rows, API says {data['total_rows']}")
    return rows


def save_day(bucket, api_url, day, events):
    """Split one day's events by table and write one file per table."""
    groups = defaultdict(list)
    for event in events:
        row = dict(event)
        groups[row.pop("entity")].append(row)
    source = f"{api_url}/feed/events?date={day}"
    for table, rows in sorted(groups.items()):
        save_jsonl(bucket, f"raw/api_{table}/event_date={day}/events.jsonl", rows, source)
    return {table: len(rows) for table, rows in sorted(groups.items())}


def lambda_handler(event, context):
    event = event if isinstance(event, dict) else {}
    bucket = os.environ["BUCKET"]
    api_url = os.environ["OLIST_API_URL"].strip().rstrip("/")
    api_key = os.environ.get("OLIST_API_KEY", "").strip()

    # 1. reference tables: small, so simply refreshed every run
    reference = {}
    for table in [t["name"] for t in get_json(f"{api_url}/tables", api_key)["tables"]]:
        rows = pull_table(api_url, api_key, table)
        save_jsonl(bucket, f"raw/api_{table}/{table}.jsonl", rows, f"{api_url}/tables/{table}")
        reference[f"api_{table}"] = len(rows)

    # 2. new days from the feed
    status = get_json(f"{api_url}/feed/status", api_key)
    first = date.fromisoformat(status["first_date"])
    latest = date.fromisoformat(status["latest_available"])
    bookmark = read_bookmark(bucket)

    if event.get("date"):                                    # re-save one day only
        days, move_bookmark = [date.fromisoformat(event["date"])], False
    else:
        if event.get("from_date"):                           # start again from this day
            last = max(date.fromisoformat(event["from_date"]), first) - timedelta(days=1)
        else:
            last = bookmark or first - timedelta(days=1)
        max_days = int(event.get("max_days") or os.environ.get("MAX_DAYS_PER_RUN", "31"))
        days = [last + timedelta(days=i) for i in range(1, max_days + 1)
                if last + timedelta(days=i) <= latest]
        move_bookmark = True

    extracted = []
    for day in days:
        data = get_json(f"{api_url}/feed/events?date={day}", api_key)
        counts = save_day(bucket, api_url, day, data["events"])
        if move_bookmark:                                    # saved after every day
            write_bookmark(bucket, day)
            bookmark = day
        extracted.append({"date": day.isoformat(), "events": data["count"], **counts})

    result = {
        "status": "ok" if extracted else "up to date, no new days",
        "reference_rows": reference,
        "days_saved": len(extracted),
        "bookmark": bookmark.isoformat() if bookmark else None,
        "latest_available": latest.isoformat(),
        "days": extracted,
    }
    print(json.dumps({k: v for k, v in result.items() if k != "days"}))
    return result