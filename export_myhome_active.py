#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

API_URL = "https://api-statements.tnet.ge/v1/statements"
STREETS_API_URL = "https://api-statements.tnet.ge/v1/streets"
DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "locale": "en",
    "x-website-key": "myhome",
    "user-agent": "Mozilla/5.0 (compatible; myhome-export/1.0)",
}
DEFAULT_EXPORTS_DIR = Path(__file__).resolve().parent / "exports"
DEFAULT_STATE_DIR = Path(__file__).resolve().parent / "state"
DEFAULT_CATALOG_CACHE = DEFAULT_STATE_DIR / "myhome_location_catalog.json"
CATALOG_TTL_HOURS = 24
DISTRICT_ID_SCAN_MAX = 20
URBAN_ID_SCAN_MAX = 220
CSV_FIELDS = [
    "id",
    "deal_type_id",
    "deal_type",
    "district_id",
    "district_name",
    "urban_id",
    "urban_name",
    "address",
    "street_id",
    "room",
    "bedroom",
    "area",
    "floor",
    "total_floors",
    "last_updated",
    "price_gel_total",
    "price_usd_total",
    "price_eur_total",
    "price_gel_sqm",
    "price_usd_sqm",
    "price_eur_sqm",
    "listing_url",
]
PROGRESS_PREFIX = "MYHOME_PROGRESS "


def emit_progress(phase: str, message: str, **payload: Any) -> None:
    event = {"phase": phase, "message": message, **payload}
    print(f"{PROGRESS_PREFIX}{json.dumps(event, ensure_ascii=False)}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export current active myhome.ge apartment listings for Tbilisi with "
            "custom period, district, area and street filters."
        )
    )
    parser.add_argument("--city-id", type=int, default=1, help="City ID. Default: 1 (Tbilisi).")
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="How many trailing days to keep based on last_updated. Ignored if --date-from is set.",
    )
    parser.add_argument(
        "--date-from",
        type=str,
        default=None,
        help="Keep rows with last_updated >= YYYY-MM-DD (local date).",
    )
    parser.add_argument(
        "--date-to",
        type=str,
        default=None,
        help="Keep rows with last_updated <= YYYY-MM-DD (local date). Default: today.",
    )
    parser.add_argument(
        "--deal-types",
        type=str,
        default="1,2",
        help="Comma-separated myhome deal type IDs. Default: 1,2 (sale,rent).",
    )
    parser.add_argument(
        "--real-estate-types",
        type=str,
        default="1",
        help="Comma-separated real estate type IDs. Default: 1 (apartment).",
    )
    parser.add_argument(
        "--statuses",
        type=str,
        default="2",
        help="Comma-separated listing status IDs. Default: 2 (active).",
    )
    parser.add_argument(
        "--currency-id",
        type=str,
        default="1",
        help="Currency ID used by API. Default: 1.",
    )
    parser.add_argument(
        "--district-id",
        action="append",
        default=[],
        help="District ID filter. Repeat or pass comma-separated values.",
    )
    parser.add_argument(
        "--district",
        action="append",
        default=[],
        help="District name filter. Repeatable. Example: --district 'Vake-Saburtalo'",
    )
    parser.add_argument(
        "--urban-id",
        action="append",
        default=[],
        help="Urban/area ID filter. Repeat or pass comma-separated values.",
    )
    parser.add_argument(
        "--urban",
        action="append",
        default=[],
        help="Urban/area name filter. Repeatable. Example: --urban 'Vake'",
    )
    parser.add_argument(
        "--street-id",
        action="append",
        default=[],
        help="Street ID filter. Repeat or pass comma-separated values.",
    )
    parser.add_argument(
        "--street",
        action="append",
        default=[],
        help=(
            "Street lookup query. Can be repeated. The script resolves it via the myhome "
            "street search API and uses the matched street IDs."
        ),
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Items per API page. Default: 100.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=400,
        help="Hard stop for paging. Default: 400.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_EXPORTS_DIR,
        help="Directory for CSV/JSON export files.",
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default=None,
        help="Custom filename prefix. If omitted, it is generated from the selected scope.",
    )
    parser.add_argument(
        "--lookup-only",
        action="store_true",
        help="Only resolve and print --street matches, do not export data.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Interactive mode: choose period, district, area and optional streets in the terminal.",
    )
    parser.add_argument(
        "--build-site",
        action="store_true",
        help="After CSV/JSON export, also build the standalone analytics HTML in one run.",
    )
    parser.add_argument(
        "--site-output-html",
        type=Path,
        default=None,
        help="Optional output HTML path used together with --build-site.",
    )
    parser.add_argument(
        "--catalog-cache",
        type=Path,
        default=DEFAULT_CATALOG_CACHE,
        help="Cache path for active district/area catalog used by name-based selection.",
    )
    parser.add_argument(
        "--refresh-catalog",
        action="store_true",
        help="Ignore cached district/area catalog and rebuild it from the API.",
    )
    return parser.parse_args()


def parse_local_date_start(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%d")


def parse_local_date_end(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%d").replace(hour=23, minute=59, second=59)


def parse_int_values(values: list[str]) -> list[int]:
    items: list[int] = []
    for chunk in values:
        for raw in chunk.split(","):
            value = raw.strip()
            if not value:
                continue
            try:
                items.append(int(value))
            except ValueError as exc:
                raise SystemExit(f"Invalid integer value: {value}") from exc
    return sorted(set(items))


def normalize_text(value: str) -> str:
    cleaned = re.sub(r"[^\w]+", " ", value.lower(), flags=re.UNICODE)
    return re.sub(r"\s+", " ", cleaned).strip()


def prompt_text(message: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default not in (None, "") else ""
    raw = input(f"{message}{suffix}: ").strip()
    return raw if raw else (default or "")


def prompt_yes_no(message: str, default: bool = True) -> bool:
    suffix = "Y/n" if default else "y/N"
    raw = input(f"{message} [{suffix}]: ").strip().lower()
    if not raw:
        return default
    return raw in {"y", "yes", "1", "true"}


def parse_index_selection(raw: str, limit: int) -> list[int]:
    values: list[int] = []
    for chunk in raw.split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            index = int(token)
        except ValueError as exc:
            raise SystemExit(f"Invalid selection value: {token}") from exc
        if index < 1 or index > limit:
            raise SystemExit(f"Selection out of range: {index}")
        values.append(index - 1)
    return sorted(set(values))


def cache_is_fresh(path: Path, max_age_hours: int) -> bool:
    if not path.exists():
        return False
    age = dt.datetime.now(dt.UTC) - dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC)
    return age <= dt.timedelta(hours=max_age_hours)


def load_catalog_cache(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def save_catalog_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_period(args: argparse.Namespace) -> tuple[dt.datetime, dt.datetime]:
    today = dt.datetime.now().replace(microsecond=0)
    period_end = parse_local_date_end(args.date_to) if args.date_to else today
    if args.date_from:
        period_start = parse_local_date_start(args.date_from)
    else:
        if args.days <= 0:
            raise SystemExit("--days must be positive when --date-from is not provided")
        period_start = period_end - dt.timedelta(days=args.days)
    if period_start > period_end:
        raise SystemExit("date range is invalid: period start is after period end")
    return period_start, period_end


def build_base_filters(args: argparse.Namespace) -> dict[str, str]:
    return {
        "cities": str(args.city_id),
        "deal_types": args.deal_types,
        "real_estate_types": args.real_estate_types,
        "statuses": args.statuses,
        "currency_id": args.currency_id,
    }


def build_listing_url(item: dict[str, Any]) -> str:
    listing_id = item.get("id")
    dynamic_slug = item.get("dynamic_slug") or ""
    if listing_id and dynamic_slug:
        return f"https://www.myhome.ge/en/pr/{listing_id}/{dynamic_slug}"
    if listing_id:
        return f"https://www.myhome.ge/en/pr/{listing_id}"
    return "https://www.myhome.ge/en/"


def fetch_page(session: requests.Session, params: dict[str, str], page: int, per_page: int) -> list[dict[str, Any]]:
    request_params = dict(params)
    request_params.update({"page": str(page), "per_page": str(per_page)})

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = session.get(API_URL, params=request_params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            if not payload.get("result"):
                raise RuntimeError(f"API returned result=false: {payload}")
            items = payload.get("data", {}).get("data", [])
            if not isinstance(items, list):
                raise RuntimeError(f"Unexpected payload format: {payload}")
            return items
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < 3:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to fetch page {page}") from last_error


def fetch_street_matches(session: requests.Session, city_id: int, query: str) -> list[dict[str, Any]]:
    response = session.get(
        STREETS_API_URL,
        params={"city_id": str(city_id), "q": query},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    matches = payload.get("data", [])
    return [m for m in matches if isinstance(m, dict)]


def probe_location_id(
    headers: dict[str, str],
    base_filters: dict[str, str],
    filter_key: str,
    location_id: int,
) -> dict[str, Any] | None:
    params = dict(base_filters)
    params[filter_key] = str(location_id)

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = requests.get(API_URL, headers=headers, params=params | {"page": "1", "per_page": "1"}, timeout=20)
            response.raise_for_status()
            payload = response.json()
            if not payload.get("result"):
                return None
            items = payload.get("data", {}).get("data", [])
            if isinstance(items, list) and items:
                item = items[0]
                return item if isinstance(item, dict) else None
            return None
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < 3:
                time.sleep(0.5 * attempt)
    if last_error:
        raise RuntimeError(f"Failed to probe {filter_key}={location_id}") from last_error
    return None


def build_location_catalog(args: argparse.Namespace, force_refresh: bool = False) -> dict[str, Any]:
    if not force_refresh and not args.refresh_catalog and cache_is_fresh(args.catalog_cache, CATALOG_TTL_HOURS):
        cached = load_catalog_cache(args.catalog_cache)
        if cached is not None:
            return cached

    headers = dict(DEFAULT_HEADERS)
    base_filters = build_base_filters(args)
    districts: dict[int, dict[str, Any]] = {}
    urbans: dict[int, dict[str, Any]] = {}

    def consume_probe(filter_key: str, location_id: int) -> tuple[str, int, dict[str, Any] | None]:
        item = probe_location_id(headers=headers, base_filters=base_filters, filter_key=filter_key, location_id=location_id)
        return filter_key, location_id, item

    futures = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        for district_id in range(1, DISTRICT_ID_SCAN_MAX + 1):
            futures.append(pool.submit(consume_probe, "districts", district_id))
        for urban_id in range(1, URBAN_ID_SCAN_MAX + 1):
            futures.append(pool.submit(consume_probe, "urbans", urban_id))

        for future in as_completed(futures):
            filter_key, _, item = future.result()
            if not item:
                continue
            district_id = int(item.get("district_id") or 0)
            urban_id = int(item.get("urban_id") or 0)
            district_name = str(item.get("district_name") or "").strip()
            urban_name = str(item.get("urban_name") or "").strip()
            if district_id and district_name:
                districts[district_id] = {"id": district_id, "name": district_name}
            if filter_key == "urbans" and urban_id and urban_name:
                urbans[urban_id] = {
                    "id": urban_id,
                    "name": urban_name,
                    "district_id": district_id or None,
                    "district_name": district_name or "Unknown",
                }

    payload = {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "city_id": args.city_id,
        "districts": sorted(districts.values(), key=lambda x: (normalize_text(x["name"]), x["id"])),
        "urbans": sorted(
            urbans.values(),
            key=lambda x: (normalize_text(x["district_name"]), normalize_text(x["name"]), x["id"]),
        ),
    }
    save_catalog_cache(args.catalog_cache, payload)
    return payload


def candidate_strings(item: dict[str, Any], kind: str) -> list[str]:
    values = [str(item.get("name") or "")]
    if kind == "urban":
        district = str(item.get("district_name") or "")
        urban = str(item.get("name") or "")
        values.extend(
            [
                f"{district} {urban}",
                f"{district}/{urban}",
                f"{urban} {district}",
            ]
        )
    return [normalize_text(value) for value in values if value]


def choose_catalog_match(
    query: str,
    items: list[dict[str, Any]],
    kind: str,
    district_ids: set[int] | None = None,
) -> dict[str, Any]:
    if district_ids and kind == "urban":
        narrowed = [item for item in items if int(item.get("district_id") or 0) in district_ids]
        if narrowed:
            items = narrowed

    if not items:
        raise SystemExit(f"No {kind} choices available for current selection")

    query_key = normalize_text(query)
    if not query_key:
        raise SystemExit(f"Empty {kind} query")

    ranked: list[tuple[tuple[int, str, int], dict[str, Any]]] = []
    for item in items:
        keys = candidate_strings(item, kind)
        rank = 4
        if any(key == query_key for key in keys):
            rank = 0
        elif any(key.startswith(query_key) for key in keys):
            rank = 1
        elif any(query_key in key for key in keys):
            rank = 2
        elif any(query_key in normalize_text(str(item.get(field) or "")) for field in ("district_name", "name")):
            rank = 3
        ranked.append(
            (
                (
                    rank,
                    normalize_text(str(item.get("district_name") or "")),
                    normalize_text(str(item.get("name") or "")),
                    int(item.get("id") or 0),
                ),
                item,
            )
        )

    ranked.sort(key=lambda pair: pair[0])
    best_rank = ranked[0][0][0]
    if best_rank >= 4:
        raise SystemExit(f'No {kind} match found for "{query}"')

    best = [item for score, item in ranked if score[0] == best_rank]
    if len(best) == 1:
        return best[0]

    lines = [f'Ambiguous {kind} query "{query}".']
    for item in best[:10]:
        if kind == "urban":
            lines.append(f'  - id={item["id"]} | {item["name"]} | {item.get("district_name") or "Unknown"}')
        else:
            lines.append(f'  - id={item["id"]} | {item["name"]}')
    raise SystemExit("\n".join(lines))


def resolve_catalog_queries(
    queries: list[str],
    items: list[dict[str, Any]],
    kind: str,
    district_ids: set[int] | None = None,
) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for query in queries:
        for raw in query.split(","):
            value = raw.strip()
            if not value:
                continue
            chosen = choose_catalog_match(value, items=items, kind=kind, district_ids=district_ids)
            chosen_id = int(chosen.get("id") or 0)
            if chosen_id and chosen_id not in seen_ids:
                resolved.append(chosen)
                seen_ids.add(chosen_id)
    return resolved


def choose_street_match(
    query: str,
    matches: list[dict[str, Any]],
    district_ids: set[int],
    urban_ids: set[int],
) -> dict[str, Any]:
    if not matches:
        raise SystemExit(f'No street matches found for query "{query}"')

    narrowed = matches
    if district_ids:
        by_district = [m for m in matches if int(m.get("district_id") or 0) in district_ids]
        if not by_district:
            raise SystemExit(f'No street match for "{query}" inside selected district filter')
        narrowed = by_district
    if urban_ids:
        by_urban = [m for m in narrowed if int(m.get("urban_id") or 0) in urban_ids]
        if not by_urban:
            raise SystemExit(f'No street match for "{query}" inside selected area filter')
        narrowed = by_urban

    query_key = normalize_text(query)

    def score(item: dict[str, Any]) -> tuple[int, str, int]:
        display = normalize_text(str(item.get("display_name") or ""))
        search_display = normalize_text(str(item.get("search_display_name") or ""))
        rank = 3
        if query_key and (display == query_key or search_display == query_key):
            rank = 0
        elif query_key and (
            display.startswith(query_key)
            or search_display.startswith(query_key)
        ):
            rank = 1
        elif query_key and (
            query_key in display
            or query_key in search_display
        ):
            rank = 2
        return (
            rank,
            normalize_text(str(item.get("district_name") or "")),
            int(item.get("id") or 0),
        )

    ranked = sorted(narrowed, key=score)
    best_rank = score(ranked[0])[0]
    best = [item for item in ranked if score(item)[0] == best_rank]
    if len(best) == 1:
        return best[0]

    message_lines = [f'Ambiguous street query "{query}". Please narrow it down.']
    for item in best[:10]:
        message_lines.append(
            "  - "
            f'id={item.get("id")} | {item.get("display_name")} | '
            f'{item.get("urban_name")} | {item.get("district_name")}'
        )
    raise SystemExit("\n".join(message_lines))


def normalize_listing(item: dict[str, Any]) -> dict[str, Any]:
    deal_type_id = item.get("deal_type_id")
    deal_type = {1: "sale", 2: "rent"}.get(deal_type_id, str(deal_type_id))
    price = item.get("price") or {}

    return {
        "id": item.get("id"),
        "deal_type_id": deal_type_id,
        "deal_type": deal_type,
        "district_id": item.get("district_id"),
        "district_name": item.get("district_name"),
        "urban_id": item.get("urban_id"),
        "urban_name": item.get("urban_name"),
        "address": item.get("address"),
        "street_id": item.get("street_id"),
        "room": item.get("room"),
        "bedroom": item.get("bedroom"),
        "area": item.get("area"),
        "floor": item.get("floor"),
        "total_floors": item.get("total_floors"),
        "last_updated": item.get("last_updated"),
        "price_gel_total": _extract_price(price, "1", "price_total"),
        "price_usd_total": _extract_price(price, "2", "price_total"),
        "price_eur_total": _extract_price(price, "3", "price_total"),
        "price_gel_sqm": _extract_price(price, "1", "price_square"),
        "price_usd_sqm": _extract_price(price, "2", "price_square"),
        "price_eur_sqm": _extract_price(price, "3", "price_square"),
        "listing_url": build_listing_url(item),
    }


def _extract_price(price_obj: dict[str, Any], currency_key: str, field: str) -> Any:
    value = price_obj.get(currency_key, {})
    if isinstance(value, dict):
        return value.get(field)
    return None


def parse_last_updated(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def filter_period(rows: list[dict[str, Any]], period_start: dt.datetime, period_end: dt.datetime) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        last_updated = parse_last_updated(row.get("last_updated"))
        if last_updated is None:
            continue
        if last_updated < period_start or last_updated > period_end:
            continue
        filtered.append(row)
    return filtered


def slugify(value: str) -> str:
    value = normalize_text(value)
    value = value.replace(" ", "_")
    return value[:80] or "selection"


def build_scope_label(
    district_records: list[dict[str, Any]],
    district_ids: list[int],
    urban_records: list[dict[str, Any]],
    urban_ids: list[int],
    street_records: list[dict[str, Any]],
) -> str:
    if street_records:
        if len(street_records) == 1:
            return slugify(str(street_records[0].get("display_name") or "street"))
        return f"{len(street_records)}_streets"
    if urban_records:
        if len(urban_records) == 1:
            return slugify(str(urban_records[0].get("name") or f"urban_{urban_ids[0]}"))
        return f"{len(urban_records)}_areas"
    if urban_ids:
        return f"{len(urban_ids)}_areas" if len(urban_ids) > 1 else f"urban_{urban_ids[0]}"
    if district_records:
        if len(district_records) == 1:
            return slugify(str(district_records[0].get("name") or f"district_{district_ids[0]}"))
        return f"{len(district_records)}_districts"
    if district_ids:
        return f"{len(district_ids)}_districts" if len(district_ids) > 1 else f"district_{district_ids[0]}"
    return "tbilisi"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in CSV_FIELDS})


def write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def choose_from_menu(
    title: str,
    items: list[dict[str, Any]],
    label_fn,
    allow_all: bool = True,
) -> list[dict[str, Any]]:
    print(f"\n{title}")
    if not items:
        print("  No options available.")
        return []
    for idx, item in enumerate(items, start=1):
        print(f"{idx:>2}. {label_fn(item)}")
    prompt = "Selection numbers, comma-separated"
    if allow_all:
        prompt += " (blank = no filter)"
    raw = input(f"{prompt}: ").strip()
    if not raw:
        return []
    indexes = parse_index_selection(raw, len(items))
    return [items[index] for index in indexes]


def apply_interactive_inputs(args: argparse.Namespace, catalog: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    print("Interactive mode: choose period and location filters.")
    use_custom_dates = prompt_yes_no("Use explicit date range instead of trailing days?", default=False)
    if use_custom_dates:
        args.date_from = prompt_text("Date from (YYYY-MM-DD)")
        args.date_to = prompt_text("Date to (YYYY-MM-DD)", default=dt.date.today().isoformat())
    else:
        args.date_from = None
        args.days = int(prompt_text("Days back", default=str(args.days)))
        args.date_to = prompt_text("Date to (YYYY-MM-DD, blank = today)", default="")
        if not args.date_to:
            args.date_to = None

    district_items = catalog.get("districts", [])
    selected_districts = choose_from_menu(
        "Available districts",
        district_items,
        label_fn=lambda item: f'{item["name"]} (id={item["id"]})',
        allow_all=True,
    )

    district_ids = {int(item["id"]) for item in selected_districts}
    urban_items = catalog.get("urbans", [])
    if district_ids:
        urban_items = [item for item in urban_items if int(item.get("district_id") or 0) in district_ids]

    selected_urbans = choose_from_menu(
        "Available areas",
        urban_items,
        label_fn=lambda item: f'{item["name"]} | {item.get("district_name") or "Unknown"} (id={item["id"]})',
        allow_all=True,
    )

    street_queries = prompt_text(
        "Street names or search queries, comma-separated (blank = all streets in selection)",
        default="",
    )
    args.street = [part.strip() for part in street_queries.split(",") if part.strip()]
    if not args.build_site:
        args.build_site = prompt_yes_no("Build analytics HTML after export?", default=True)

    return selected_districts, selected_urbans


def build_site_html(csv_path: Path, html_path: Path) -> None:
    import build_street_analytics_site as street_site

    rows, meta = street_site.load_rows(csv_path)
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(street_site.build_html(rows=rows, meta=meta, source_csv=csv_path), encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.per_page <= 0 or args.max_pages <= 0:
        print("per-page and max-pages must be positive integers", file=sys.stderr)
        return 2

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    try:
        district_records: list[dict[str, Any]] = []
        urban_records: list[dict[str, Any]] = []

        needs_catalog = args.interactive or bool(args.district) or bool(args.urban)
        emit_progress(
            "preparing",
            "Resolving catalog and export filters",
            page=0,
            pages_scanned=0,
            page_limit=args.max_pages,
            collected=0,
            exported=0,
        )
        catalog = build_location_catalog(args) if needs_catalog else {"districts": [], "urbans": []}

        if args.interactive:
            selected_districts, selected_urbans = apply_interactive_inputs(args, catalog)
            district_records.extend(selected_districts)
            urban_records.extend(selected_urbans)

        period_start, period_end = resolve_period(args)
        district_ids = parse_int_values(args.district_id)
        urban_ids = parse_int_values(args.urban_id)
        street_ids = parse_int_values(args.street_id)

        if args.district:
            resolved = resolve_catalog_queries(args.district, catalog.get("districts", []), kind="district")
            district_records.extend(resolved)
        if args.urban:
            resolved = resolve_catalog_queries(
                args.urban,
                catalog.get("urbans", []),
                kind="urban",
                district_ids={int(item["id"]) for item in district_records} or None,
            )
            urban_records.extend(resolved)

        district_records = sorted(
            {int(item["id"]): item for item in district_records}.values(),
            key=lambda item: int(item["id"]),
        )
        urban_records = sorted(
            {int(item["id"]): item for item in urban_records}.values(),
            key=lambda item: int(item["id"]),
        )
        district_ids = sorted(set(district_ids) | {int(item["id"]) for item in district_records})
        urban_ids = sorted(set(urban_ids) | {int(item["id"]) for item in urban_records})

        street_records: list[dict[str, Any]] = []
        for query in args.street:
            matches = fetch_street_matches(session, args.city_id, query)
            chosen = choose_street_match(
                query=query,
                matches=matches,
                district_ids=set(district_ids),
                urban_ids=set(urban_ids),
            )
            street_records.append(chosen)
            street_ids.append(int(chosen["id"]))

        street_ids = sorted(set(street_ids))

        if args.lookup_only:
            print(
                json.dumps(
                    {
                        "districts": district_records,
                        "urbans": urban_records,
                        "streets": street_records,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        filters = build_base_filters(args)
        if district_ids:
            filters["districts"] = ",".join(str(x) for x in district_ids)
        if urban_ids:
            filters["urbans"] = ",".join(str(x) for x in urban_ids)
        if street_ids:
            filters["streets"] = ",".join(str(x) for x in street_ids)

        all_rows: list[dict[str, Any]] = []
        stop_due_to_period = False
        pages_scanned = 0
        emit_progress(
            "fetching",
            "Fetching listing pages",
            page=0,
            pages_scanned=0,
            page_limit=args.max_pages,
            collected=0,
            exported=0,
        )
        for page in range(1, args.max_pages + 1):
            items = fetch_page(session, params=filters, page=page, per_page=args.per_page)
            pages_scanned = page
            if not items:
                emit_progress(
                    "fetching",
                    "Reached the end of available pages",
                    page=page,
                    pages_scanned=pages_scanned,
                    page_limit=args.max_pages,
                    collected=len(all_rows),
                    exported=0,
                    last_page_rows=0,
                )
                break

            normalized_page = [normalize_listing(item) for item in items if item.get("id") is not None]
            all_rows.extend(normalized_page)
            emit_progress(
                "fetching",
                f"Scanned page {page}",
                page=page,
                pages_scanned=pages_scanned,
                page_limit=args.max_pages,
                collected=len(all_rows),
                exported=0,
                last_page_rows=len(normalized_page),
            )

            page_dates = [
                parse_last_updated(row.get("last_updated"))
                for row in normalized_page
                if row.get("last_updated")
            ]
            page_dates = [d for d in page_dates if d is not None]
            if page_dates and min(page_dates) < period_start and max(page_dates) < period_start:
                stop_due_to_period = True
                emit_progress(
                    "fetching",
                    "Stopped page scan after crossing the selected period",
                    page=page,
                    pages_scanned=pages_scanned,
                    page_limit=args.max_pages,
                    collected=len(all_rows),
                    exported=0,
                    last_page_rows=len(normalized_page),
                )
                break
            if len(items) < args.per_page:
                emit_progress(
                    "fetching",
                    "Last partial page reached",
                    page=page,
                    pages_scanned=pages_scanned,
                    page_limit=args.max_pages,
                    collected=len(all_rows),
                    exported=0,
                    last_page_rows=len(normalized_page),
                )
                break

        emit_progress(
            "filtering",
            "Filtering rows to the selected time period",
            page=pages_scanned,
            pages_scanned=pages_scanned,
            page_limit=args.max_pages,
            collected=len(all_rows),
            exported=0,
        )
        filtered_rows = filter_period(all_rows, period_start=period_start, period_end=period_end)
        filtered_rows.sort(key=lambda row: row.get("last_updated") or "", reverse=True)
        emit_progress(
            "writing",
            "Writing CSV and JSON files",
            page=pages_scanned,
            pages_scanned=pages_scanned,
            page_limit=args.max_pages,
            collected=len(all_rows),
            exported=len(filtered_rows),
        )

        scope_label = args.output_prefix or build_scope_label(
            district_records=district_records,
            district_ids=district_ids,
            urban_records=urban_records,
            urban_ids=urban_ids,
            street_records=street_records,
        )
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
        period_label = f"{period_start.date().isoformat()}_to_{period_end.date().isoformat()}"
        file_stem = f"myhome_active_{scope_label}_{period_label}_{stamp}"

        args.output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = args.output_dir / f"{file_stem}.csv"
        json_path = args.output_dir / f"{file_stem}.json"
        write_csv(csv_path, filtered_rows)
        write_json(json_path, filtered_rows)

        html_path: Path | None = None
        if args.build_site:
            if args.site_output_html:
                html_path = args.site_output_html
            else:
                html_path = args.output_dir / f"street_analytics_site_{scope_label}_{period_label}_{stamp}.html"
            emit_progress(
                "building_html",
                "Building standalone analytics HTML",
                page=pages_scanned,
                pages_scanned=pages_scanned,
                page_limit=args.max_pages,
                collected=len(all_rows),
                exported=len(filtered_rows),
            )
            build_site_html(csv_path=csv_path, html_path=html_path)

        summary = {
            "generated_local": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "period_start_local": period_start.strftime("%Y-%m-%d %H:%M:%S"),
            "period_end_local": period_end.strftime("%Y-%m-%d %H:%M:%S"),
            "source_filters": filters,
            "pages_scanned": pages_scanned,
            "stopped_by_period_cutoff": stop_due_to_period,
            "rows_before_period_filter": len(all_rows),
            "rows_exported": len(filtered_rows),
            "district_matches": district_records,
            "urban_matches": urban_records,
            "street_matches": street_records,
            "csv_path": str(csv_path),
            "json_path": str(json_path),
            "html_path": str(html_path) if html_path else None,
        }
        emit_progress(
            "completed",
            "Export and HTML build completed",
            page=pages_scanned,
            pages_scanned=pages_scanned,
            page_limit=args.max_pages,
            collected=len(all_rows),
            exported=len(filtered_rows),
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        emit_progress("failed", f"Export failed: {exc}", error=str(exc))
        raise
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
