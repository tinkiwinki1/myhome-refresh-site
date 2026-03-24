#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import requests

API_URL = "https://api-statements.tnet.ge/v1/statements"
DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "locale": "en",
    "x-website-key": "myhome",
    "user-agent": "Mozilla/5.0 (compatible; myhome-tbilisi-bot/1.0)",
}
DEFAULT_FILTERS = {
    "cities": "1",  # Tbilisi
    "deal_types": "1,2",  # sale + rent
    "real_estate_types": "1",  # apartment
    "currency_id": "1",
}
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_listings (
    listing_id INTEGER PRIMARY KEY,
    first_seen_utc TEXT NOT NULL,
    last_updated TEXT,
    deal_type_id INTEGER,
    real_estate_type_id INTEGER,
    city_name TEXT,
    district_name TEXT,
    address TEXT,
    listing_url TEXT,
    payload_json TEXT NOT NULL
);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Track new myhome.ge listings for Tbilisi apartments "
            "(sale + rent) and export only newly seen ads."
        )
    )
    parser.add_argument("--interval", type=int, default=120, help="Polling interval in seconds.")
    parser.add_argument("--per-page", type=int, default=50, help="Items per API page.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="How many first pages to scan each cycle.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("state/myhome_seen.sqlite3"),
        help="SQLite path for seen listing IDs.",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        default=Path("exports"),
        help="Directory for exports.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one scan cycle and exit.",
    )
    parser.add_argument(
        "--export-initial",
        action="store_true",
        help=(
            "On first run with empty DB, export all currently fetched listings. "
            "By default, first run is used as baseline and exports nothing."
        ),
    )
    return parser.parse_args()


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(DB_SCHEMA)
    conn.commit()
    return conn


def get_seen_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM seen_listings").fetchone()
    return int(row[0]) if row else 0


def fetch_page(session: requests.Session, page: int, per_page: int) -> list[dict[str, Any]]:
    params = dict(DEFAULT_FILTERS)
    params.update({"page": str(page), "per_page": str(per_page)})

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = session.get(API_URL, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            if not payload.get("result"):
                raise RuntimeError(f"API returned result=false: {payload}")
            data = payload.get("data", {})
            items = data.get("data", [])
            if not isinstance(items, list):
                raise RuntimeError(f"Unexpected payload format: {payload}")
            return items
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < 3:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to fetch page {page}") from last_error


def build_listing_url(item: dict[str, Any]) -> str:
    listing_id = item.get("id")
    dynamic_slug = item.get("dynamic_slug") or ""
    if listing_id and dynamic_slug:
        return f"https://www.myhome.ge/en/pr/{listing_id}/{dynamic_slug}"
    if listing_id:
        return f"https://www.myhome.ge/en/pr/{listing_id}"
    return "https://www.myhome.ge/en/"


def normalize_listing(item: dict[str, Any]) -> dict[str, Any]:
    deal_type_id = item.get("deal_type_id")
    deal_type = {1: "sale", 2: "rent"}.get(deal_type_id, str(deal_type_id))
    price = item.get("price") or {}

    return {
        "id": item.get("id"),
        "deal_type_id": deal_type_id,
        "deal_type": deal_type,
        "real_estate_type_id": item.get("real_estate_type_id"),
        "city_name": item.get("city_name"),
        "district_name": item.get("district_name"),
        "address": item.get("address"),
        "room": item.get("room"),
        "bedroom": item.get("bedroom"),
        "area": item.get("area"),
        "floor": item.get("floor"),
        "total_floors": item.get("total_floors"),
        "last_updated": item.get("last_updated"),
        "listing_url": build_listing_url(item),
        "price_gel_total": _extract_price(price, "1"),
        "price_usd_total": _extract_price(price, "2"),
        "price_eur_total": _extract_price(price, "3"),
        "raw": item,
    }


def _extract_price(price_obj: dict[str, Any], key: str) -> Any:
    value = price_obj.get(key, {})
    if isinstance(value, dict):
        return value.get("price_total")
    return None


def remember_listing(conn: sqlite3.Connection, listing: dict[str, Any]) -> bool:
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO seen_listings (
            listing_id, first_seen_utc, last_updated, deal_type_id,
            real_estate_type_id, city_name, district_name, address,
            listing_url, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            listing["id"],
            dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            listing.get("last_updated"),
            listing.get("deal_type_id"),
            listing.get("real_estate_type_id"),
            listing.get("city_name"),
            listing.get("district_name"),
            listing.get("address"),
            listing.get("listing_url"),
            json.dumps(listing["raw"], ensure_ascii=False),
        ),
    )
    return cursor.rowcount == 1


def export_new_batch(export_dir: Path, listings: list[dict[str, Any]]) -> tuple[Path, Path]:
    export_dir.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
    json_path = export_dir / f"new_listings_{timestamp}.json"
    csv_path = export_dir / "new_listings_all.csv"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)

    file_exists = csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "deal_type",
                "city_name",
                "district_name",
                "address",
                "room",
                "bedroom",
                "area",
                "floor",
                "total_floors",
                "price_gel_total",
                "price_usd_total",
                "price_eur_total",
                "last_updated",
                "listing_url",
            ],
        )
        if not file_exists:
            writer.writeheader()
        for listing in listings:
            writer.writerow(
                {
                    "id": listing.get("id"),
                    "deal_type": listing.get("deal_type"),
                    "city_name": listing.get("city_name"),
                    "district_name": listing.get("district_name"),
                    "address": listing.get("address"),
                    "room": listing.get("room"),
                    "bedroom": listing.get("bedroom"),
                    "area": listing.get("area"),
                    "floor": listing.get("floor"),
                    "total_floors": listing.get("total_floors"),
                    "price_gel_total": listing.get("price_gel_total"),
                    "price_usd_total": listing.get("price_usd_total"),
                    "price_eur_total": listing.get("price_eur_total"),
                    "last_updated": listing.get("last_updated"),
                    "listing_url": listing.get("listing_url"),
                }
            )

    return json_path, csv_path


def run_cycle(
    session: requests.Session,
    conn: sqlite3.Connection,
    per_page: int,
    max_pages: int,
    export_dir: Path,
    export_initial: bool,
) -> tuple[int, Path | None, Path | None]:
    seen_before_cycle = get_seen_count(conn)
    all_items: list[dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        items = fetch_page(session, page=page, per_page=per_page)
        if not items:
            break
        all_items.extend(items)
        if len(items) < per_page:
            break

    if not all_items:
        return 0, None, None

    normalized = [normalize_listing(item) for item in all_items if item.get("id") is not None]

    if seen_before_cycle == 0 and not export_initial:
        for listing in normalized:
            remember_listing(conn, listing)
        conn.commit()
        return 0, None, None

    new_listings: list[dict[str, Any]] = []
    for listing in normalized:
        if remember_listing(conn, listing):
            new_listings.append(listing)
    conn.commit()

    if not new_listings:
        return 0, None, None

    new_listings.sort(key=lambda x: x.get("last_updated") or "", reverse=True)
    json_path, csv_path = export_new_batch(export_dir, new_listings)
    return len(new_listings), json_path, csv_path


def main() -> int:
    args = parse_args()

    if args.per_page <= 0 or args.max_pages <= 0 or args.interval <= 0:
        print("interval/per-page/max-pages must be positive integers", file=sys.stderr)
        return 2

    conn = init_db(args.db_path)
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    print(
        "Started bot:",
        f"interval={args.interval}s",
        f"per_page={args.per_page}",
        f"max_pages={args.max_pages}",
        f"db={args.db_path}",
        f"exports={args.export_dir}",
    )

    try:
        while True:
            cycle_started = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                count, json_path, csv_path = run_cycle(
                    session=session,
                    conn=conn,
                    per_page=args.per_page,
                    max_pages=args.max_pages,
                    export_dir=args.export_dir,
                    export_initial=args.export_initial,
                )
                if count > 0:
                    print(
                        f"[{cycle_started}] New listings: {count}. "
                        f"JSON: {json_path} | CSV append: {csv_path}"
                    )
                else:
                    print(f"[{cycle_started}] New listings: 0")
            except Exception as exc:  # noqa: BLE001
                print(f"[{cycle_started}] Cycle failed: {exc}", file=sys.stderr)

            if args.once:
                break
            time.sleep(args.interval)
    finally:
        session.close()
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
