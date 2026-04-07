#!/usr/bin/env python3
"""
Download CMS Hospital Compare datasets from data.cms.gov.

The CMS provider-data API returns paginated JSON.  This script fetches all
pages and saves clean CSVs to the data/ directory.

Usage:
    python data/download_data.py
    python data/download_data.py --force   # re-download even if files exist
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent

# CMS DKAN datastore API endpoints (stable resource UUIDs, not versioned file paths)
CMS_ENDPOINTS = {
    "Hospital_General_Information.csv": {
        "url": "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0",
        "description": "Hospital General Information",
    },
    "Timely_and_Effective_Care-Hospital.csv": {
        "url": "https://data.cms.gov/provider-data/api/1/datastore/query/yv7e-xc69/0",
        "description": "Timely and Effective Care",
    },
}

HEADERS = {"User-Agent": "hospital-dashboard-etl/1.0 (healthcare portfolio project)"}
PAGE_SIZE = 1500   # CMS API hard limit; 2000+ returns 400


def fetch_all_pages(base_url: str, description: str) -> list[dict]:
    """
    Paginate the CMS DKAN API and return all result records.

    The API uses offset/limit pagination.  We keep fetching until the returned
    page is smaller than PAGE_SIZE (last page) or is empty.
    """
    all_records: list[dict] = []
    offset = 0
    page = 1

    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        try:
            resp = requests.get(base_url, params=params, headers=HEADERS, timeout=90)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error(f"  Request failed at offset={offset}: {exc}")
            break

        try:
            payload = resp.json()
        except json.JSONDecodeError:
            logger.error(f"  Response is not JSON (offset={offset}). Aborting pagination.")
            break

        results = payload.get("results", [])
        total = payload.get("count", "?")

        if not results:
            break

        all_records.extend(results)
        logger.info(
            f"  Page {page:3d} — fetched {len(results):,} rows "
            f"(total so far: {len(all_records):,} / {total})"
        )

        if len(results) < PAGE_SIZE:
            break   # last page

        offset += PAGE_SIZE
        page += 1
        time.sleep(0.3)  # polite rate limit

    return all_records


def download_dataset(filename: str, info: dict, force: bool = False) -> bool:
    """Download one dataset and save as CSV. Returns True on success."""
    dest = DATA_DIR / filename

    if dest.exists() and not force:
        size_mb = dest.stat().st_size / (1024 ** 2)
        logger.info(f"Already exists ({size_mb:.1f} MB) — skipping: {filename}")
        logger.info("  Pass --force to re-download.")
        return True

    logger.info(f"Downloading {info['description']} ...")
    records = fetch_all_pages(info["url"], info["description"])

    if not records:
        logger.error(f"  No records returned for {filename}")
        return False

    df = pd.DataFrame(records)
    df.to_csv(dest, index=False)
    size_mb = dest.stat().st_size / (1024 ** 2)
    logger.info(f"  ✓ Saved {filename}  ({len(df):,} rows, {size_mb:.1f} MB)")
    return True


def parse_args():
    p = argparse.ArgumentParser(description="Download CMS hospital datasets")
    p.add_argument("--force", action="store_true", help="Re-download even if files exist")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    any_failed = False
    for filename, info in CMS_ENDPOINTS.items():
        if not download_dataset(filename, info, force=args.force):
            any_failed = True
            logger.error(
                f"\n  ✗ Could not download {filename}.\n"
                "  Manual alternative:\n"
                "    Visit https://data.cms.gov/provider-data/topics/hospitals\n"
                f"    Save CSV as  data/{filename}\n"
                "  Or skip real data and run:\n"
                "    python etl/load_data.py --simulated\n"
            )

    if any_failed:
        return 1

    logger.info("\nAll datasets ready in data/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
