#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests


DEFAULT_EXPORTS_DIR = Path(__file__).resolve().parent / "exports"
DEFAULT_INPUT_PATTERN = "abashidze_listings_last_3_months_*.csv"
DETAIL_API_BASE = "https://api-statements.tnet.ge/v1/statements"
DETAIL_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "locale": "en",
    "x-website-key": "myhome",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a standalone HTML dashboard for Abashidze street analytics."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help="Path to source CSV. If omitted, latest exports/abashidze_listings_last_3_months_*.csv is used.",
    )
    parser.add_argument(
        "--output-html",
        type=Path,
        default=None,
        help="Path to output HTML. Default: exports/abashidze_dashboard_<timestamp>.html",
    )
    parser.add_argument(
        "--detail-cache",
        type=Path,
        default=DEFAULT_EXPORTS_DIR / "abashidze_detail_cache.json",
        help="Path to JSON cache with statement details used for extra filters.",
    )
    parser.add_argument(
        "--skip-detail-enrichment",
        action="store_true",
        help="Do not call detail API and do not enrich rows with condition/building status.",
    )
    parser.add_argument(
        "--detail-workers",
        type=int,
        default=10,
        help="Parallel workers for detail API enrichment.",
    )
    return parser.parse_args()


def find_latest_csv() -> Path | None:
    files = sorted(
        DEFAULT_EXPORTS_DIR.glob(DEFAULT_INPUT_PATTERN),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_last_updated(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def safe_min(values: list[float]) -> float:
    return min(values) if values else 0.0


def safe_max(values: list[float]) -> float:
    return max(values) if values else 1.0


def normalize_condition(value: str | None) -> str:
    if not value:
        return "Unknown"
    return value.strip() or "Unknown"


def derive_building_status(is_old: bool | None) -> str:
    if is_old is True:
        return "Old building"
    if is_old is False:
        return "New building"
    return "Unknown"


def load_detail_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    return {}


def save_detail_cache(path: Path, cache: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_statement_detail(session: requests.Session, listing_id: str | int) -> dict[str, Any] | None:
    url = f"{DETAIL_API_BASE}/{listing_id}"
    for attempt in range(3):
        try:
            response = session.get(url, timeout=20)
            response.raise_for_status()
            payload = response.json()
            statement = payload.get("data", {}).get("statement")
            if not isinstance(statement, dict):
                return None
            return {
                "condition": normalize_condition(statement.get("condition")),
                "condition_id": statement.get("condition_id"),
                "is_old": statement.get("is_old"),
                "project_type_id": statement.get("project_type_id"),
                "status_id": statement.get("status_id"),
            }
        except Exception:  # noqa: BLE001
            if attempt == 2:
                return None
    return None


def enrich_rows_with_details(
    rows: list[dict[str, Any]],
    cache_path: Path,
    workers: int,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], int]:
    cache = load_detail_cache(cache_path)

    ids_to_fetch: list[str] = []
    for row in rows:
        listing_id = str(row.get("id") or "").strip()
        if not listing_id:
            continue
        cached = cache.get(listing_id)
        if cached is None:
            ids_to_fetch.append(listing_id)

    fetched_count = 0
    if ids_to_fetch:
        session = requests.Session()
        session.headers.update(DETAIL_HEADERS)
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(fetch_statement_detail, session, listing_id): listing_id for listing_id in ids_to_fetch}
            for future in as_completed(futures):
                listing_id = futures[future]
                detail = future.result()
                if detail is not None:
                    cache[listing_id] = detail
                    fetched_count += 1
        session.close()
        save_detail_cache(cache_path, cache)

    for row in rows:
        listing_id = str(row.get("id") or "").strip()
        detail = cache.get(listing_id, {})

        row_condition = normalize_condition(row.get("condition"))
        detail_condition = normalize_condition(detail.get("condition"))
        condition = detail_condition if (row_condition == "Unknown" and detail_condition != "Unknown") else row_condition

        is_old = row.get("is_old")
        if is_old is None:
            is_old = detail.get("is_old")
        if isinstance(is_old, str):
            lower = is_old.strip().lower()
            if lower in {"true", "1", "yes"}:
                is_old = True
            elif lower in {"false", "0", "no"}:
                is_old = False
            else:
                is_old = None

        row["condition"] = condition
        row["condition_id"] = row.get("condition_id") or detail.get("condition_id")
        row["is_old"] = is_old
        row["building_status"] = derive_building_status(is_old)
        row["project_type_id"] = row.get("project_type_id") or detail.get("project_type_id")

    return rows, cache, fetched_count


def load_rows(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    dates: list[dt.datetime] = []
    rooms: list[float] = []
    areas: list[float] = []
    prices: list[float] = []
    condition_values: set[str] = set()
    building_status_values: set[str] = set()

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            last_updated_raw = raw.get("last_updated")
            last_updated = parse_last_updated(last_updated_raw)
            room = to_float(raw.get("room"))
            area = to_float(raw.get("area"))
            price_usd_total = to_float(raw.get("price_usd_total"))
            price_gel_total = to_float(raw.get("price_gel_total"))
            price_eur_total = to_float(raw.get("price_eur_total"))
            price_usd_sqm = to_float(raw.get("price_usd_sqm"))
            price_gel_sqm = to_float(raw.get("price_gel_sqm"))
            price_eur_sqm = to_float(raw.get("price_eur_sqm"))

            row = {
                "id": raw.get("id"),
                "deal_type": (raw.get("deal_type") or "").lower(),
                "address": raw.get("address") or "Unknown address",
                "room": room,
                "bedroom": to_float(raw.get("bedroom")),
                "area": area,
                "floor": to_float(raw.get("floor")),
                "total_floors": to_float(raw.get("total_floors")),
                "last_updated": last_updated_raw,
                "price_usd_total": price_usd_total,
                "price_gel_total": price_gel_total,
                "price_eur_total": price_eur_total,
                "price_usd_sqm": price_usd_sqm,
                "price_gel_sqm": price_gel_sqm,
                "price_eur_sqm": price_eur_sqm,
                "listing_url": raw.get("listing_url") or "",
                "condition": normalize_condition(raw.get("condition")),
                "condition_id": raw.get("condition_id"),
                "is_old": None,
                "building_status": raw.get("building_status") or "Unknown",
                "project_type_id": raw.get("project_type_id"),
            }

            is_old_raw = raw.get("is_old")
            if isinstance(is_old_raw, str) and is_old_raw != "":
                lower = is_old_raw.lower().strip()
                if lower in {"true", "1", "yes"}:
                    row["is_old"] = True
                elif lower in {"false", "0", "no"}:
                    row["is_old"] = False
            elif isinstance(is_old_raw, bool):
                row["is_old"] = is_old_raw

            if row["is_old"] is not None and (not raw.get("building_status")):
                row["building_status"] = derive_building_status(row["is_old"])

            rows.append(row)

            if last_updated is not None:
                dates.append(last_updated)
            if room is not None:
                rooms.append(room)
            if area is not None:
                areas.append(area)
            if price_usd_total is not None:
                prices.append(price_usd_total)
            condition_values.add(row["condition"])
            building_status_values.add(row["building_status"])

    min_date = min(dates).strftime("%Y-%m-%d") if dates else dt.date.today().isoformat()
    max_date = max(dates).strftime("%Y-%m-%d") if dates else dt.date.today().isoformat()
    meta = {
        "min_date": min_date,
        "max_date": max_date,
        "room_min": safe_min(rooms),
        "room_max": safe_max(rooms),
        "area_min": safe_min(areas),
        "area_max": safe_max(areas),
        "price_min": safe_min(prices),
        "price_max": safe_max(prices),
        "total_rows": len(rows),
        "condition_values": sorted(condition_values),
        "building_status_values": sorted(building_status_values),
    }
    return rows, meta


def build_html(rows: list[dict[str, Any]], meta: dict[str, Any], source_csv: Path) -> str:
    payload = json.dumps(rows, ensure_ascii=False)
    meta_json = json.dumps(meta, ensure_ascii=False)
    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Abashidze Street Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #f7f8fa;
      --panel: #ffffff;
      --ink: #19212b;
      --muted: #5b6673;
      --line: #d9dee6;
      --accent: #1f7ae0;
      --accent2: #0c9f77;
      --warn: #ca5a14;
      --shadow: 0 8px 24px rgba(16, 24, 40, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
      background: linear-gradient(180deg, #f4f7fb 0%, #f9fafc 100%);
      color: var(--ink);
    }}
    .page {{
      max-width: 1480px;
      margin: 24px auto 32px;
      padding: 0 16px;
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 16px;
    }}
    @media (max-width: 1080px) {{
      .page {{ grid-template-columns: 1fr; }}
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 14px;
    }}
    .sidebar h1 {{
      margin: 0 0 6px;
      font-size: 1.2rem;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.4;
    }}
    .field {{
      margin-top: 12px;
    }}
    .field label {{
      display: block;
      font-size: 0.85rem;
      color: var(--muted);
      margin-bottom: 5px;
      font-weight: 600;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }}
    .field input[type="number"],
    .field input[type="date"],
    .field select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 9px;
      padding: 8px;
      font-size: 0.95rem;
      background: #fff;
      color: var(--ink);
    }}
    .field .checks {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px;
      font-size: 0.95rem;
    }}
    .btn-row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-top: 14px;
    }}
    button {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 10px;
      font-weight: 700;
      cursor: pointer;
      background: #fff;
      color: var(--ink);
    }}
    button.primary {{
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }}
    .main-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }}
    @media (max-width: 1200px) {{
      .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 640px) {{
      .kpis {{ grid-template-columns: 1fr; }}
    }}
    .kpi {{
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      min-height: 88px;
    }}
    .kpi .name {{
      color: var(--muted);
      font-size: 0.8rem;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }}
    .kpi .value {{
      margin-top: 4px;
      font-size: 1.35rem;
      font-weight: 800;
    }}
    .chart-grid-2 {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }}
    @media (max-width: 1020px) {{
      .chart-grid-2 {{ grid-template-columns: 1fr; }}
    }}
    .chart {{
      min-height: 360px;
    }}
    .table-wrap {{
      overflow-x: auto;
      max-height: 520px;
      border: 1px solid var(--line);
      border-radius: 10px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9rem;
      background: #fff;
    }}
    thead th {{
      position: sticky;
      top: 0;
      background: #f2f5f9;
      z-index: 1;
    }}
    th, td {{
      border-bottom: 1px solid #eceff4;
      padding: 8px 10px;
      text-align: left;
      white-space: nowrap;
    }}
    tbody tr:hover {{
      background: #f7fbff;
    }}
    .footer {{
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.85rem;
    }}
    .download {{
      margin-top: 10px;
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 9px;
      padding: 8px 10px;
      color: var(--ink);
      text-decoration: none;
      font-weight: 700;
      background: #fff;
    }}
  </style>
</head>
<body>
  <div class="page">
    <aside class="panel sidebar">
      <h1>Abashidze Street Dashboard</h1>
      <div class="subtle">Myhome active listings, analytics for the last 3 months.</div>
      <div class="subtle" style="margin-top:8px;">Rows in source: <b id="rowsInSource"></b></div>

      <div class="field">
        <label>Deal Type</label>
        <div class="checks">
          <label><input type="checkbox" id="dealSale" checked> Sale</label>
          <label><input type="checkbox" id="dealRent" checked> Rent</label>
        </div>
      </div>

      <div class="field">
        <label>Building Status</label>
        <div class="checks">
          <label><input type="checkbox" id="statusNewBuilding" checked> New building</label>
          <label><input type="checkbox" id="statusOldBuilding" checked> Old building</label>
        </div>
        <div class="checks" style="margin-top:6px; grid-template-columns: 1fr;">
          <label><input type="checkbox" id="statusUnknownBuilding" checked> Unknown</label>
        </div>
      </div>

      <div class="field">
        <label>Condition</label>
        <div id="conditionFilters" class="checks" style="grid-template-columns:1fr;"></div>
      </div>

      <div class="field">
        <label>Date From</label>
        <input type="date" id="dateFrom" />
      </div>
      <div class="field">
        <label>Date To</label>
        <input type="date" id="dateTo" />
      </div>

      <div class="field">
        <label>Rooms Min</label>
        <input type="number" id="roomMin" step="1" />
      </div>
      <div class="field">
        <label>Rooms Max</label>
        <input type="number" id="roomMax" step="1" />
      </div>

      <div class="field">
        <label>Area Min (m2)</label>
        <input type="number" id="areaMin" step="1" />
      </div>
      <div class="field">
        <label>Area Max (m2)</label>
        <input type="number" id="areaMax" step="1" />
      </div>

      <div class="field">
        <label>Price Min (USD)</label>
        <input type="number" id="priceMin" step="100" />
      </div>
      <div class="field">
        <label>Price Max (USD)</label>
        <input type="number" id="priceMax" step="100" />
      </div>

      <div class="field">
        <label>Duplicate handling</label>
        <div class="checks" style="grid-template-columns:1fr;">
          <label><input type="checkbox" id="dedupeEnabled" checked> Remove duplicate ads</label>
        </div>
        <select id="dedupeMode" style="margin-top:8px;">
          <option value="relaxed" selected>Relaxed (ignore bedroom/floor)</option>
          <option value="strict">Strict (keep floor)</option>
        </select>
      </div>

      <div class="btn-row">
        <button class="primary" id="applyBtn">Apply</button>
        <button id="resetBtn">Reset</button>
      </div>
      <a class="download" id="downloadCsv" href="#">Download filtered CSV</a>
      <div class="subtle" id="dedupeInfo" style="margin-top:8px;"></div>
    </aside>

    <main class="main-grid">
      <section class="panel">
        <div class="kpis">
          <div class="kpi"><div class="name">Listings</div><div id="kpiListings" class="value">0</div></div>
          <div class="kpi"><div class="name">Sale / Rent</div><div id="kpiDeals" class="value">0 / 0</div></div>
          <div class="kpi"><div class="name">Median Price (USD)</div><div id="kpiMedianPrice" class="value">n/a</div></div>
          <div class="kpi"><div class="name">Median Price/m2 (USD)</div><div id="kpiMedianSqm" class="value">n/a</div></div>
        </div>
        <div class="kpis" style="margin-top:10px;">
          <div class="kpi"><div class="name">Average Price (USD)</div><div id="kpiAvgPrice" class="value">n/a</div></div>
          <div class="kpi"><div class="name">Average Price/m2 (USD)</div><div id="kpiAvgSqm" class="value">n/a</div></div>
          <div class="kpi"><div class="name">Weighted Price/m2 (USD)</div><div id="kpiWeightedSqm" class="value">n/a</div></div>
          <div class="kpi"><div class="name">Last Update</div><div id="kpiLastUpdate" class="value">n/a</div></div>
        </div>
      </section>

      <section class="panel chart-grid-2">
        <div id="chartTrend" class="chart"></div>
        <div id="chartHistogram" class="chart"></div>
      </section>

      <section class="panel chart-grid-2">
        <div id="chartBox" class="chart"></div>
        <div id="chartTopAddresses" class="chart"></div>
      </section>

      <section class="panel">
        <div id="chartScatter" class="chart"></div>
      </section>

      <section class="panel">
        <h3 style="margin:0 0 8px;">Filtered Listings</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Deal</th>
                <th>Address</th>
                <th>Room</th>
                <th>Area</th>
                <th>Price USD</th>
                <th>USD/m2</th>
                <th>Copies</th>
                <th>Building</th>
                <th>Condition</th>
                <th>Updated</th>
                <th>Listing</th>
              </tr>
            </thead>
            <tbody id="tableBody"></tbody>
          </table>
        </div>
        <div class="footer">
          Built at: {generated_at} | Source CSV: {source_csv}
        </div>
      </section>
    </main>
  </div>

  <script>
    const RAW_DATA = {payload};
    const META = {meta_json};

    const $ = (id) => document.getElementById(id);

    function n(v) {{
      if (v === null || v === undefined || v === "") return null;
      const x = Number(v);
      return Number.isFinite(x) ? x : null;
    }}

    function fmt(v, digits = 2) {{
      if (v === null || v === undefined || Number.isNaN(v)) return "n/a";
      return Number(v).toLocaleString(undefined, {{ minimumFractionDigits: digits, maximumFractionDigits: digits }});
    }}

    function fmt0(v) {{
      if (v === null || v === undefined || Number.isNaN(v)) return "n/a";
      return Number(v).toLocaleString(undefined, {{ maximumFractionDigits: 0 }});
    }}

    function parseDate(s) {{
      if (!s) return null;
      const d = new Date(s.replace(" ", "T"));
      return Number.isNaN(d.getTime()) ? null : d;
    }}

    function median(values) {{
      const arr = values.filter(v => v !== null && !Number.isNaN(v)).slice().sort((a, b) => a - b);
      if (!arr.length) return null;
      const mid = Math.floor(arr.length / 2);
      return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
    }}

    function average(values) {{
      const arr = values.filter(v => v !== null && !Number.isNaN(v));
      if (!arr.length) return null;
      return arr.reduce((a, b) => a + b, 0) / arr.length;
    }}

    function weightedSqm(rows) {{
      let totalPrice = 0;
      let totalArea = 0;
      for (const r of rows) {{
        const area = n(r.area);
        const price = n(r.price_usd_total);
        if (area && area > 0 && price && price > 0) {{
          totalArea += area;
          totalPrice += price;
        }}
      }}
      if (!totalArea) return null;
      return totalPrice / totalArea;
    }}

    function normalizeAddress(value) {{
      if (!value) return "";
      return String(value)
        .toLowerCase()
        .normalize("NFKD")
        .replace(/[^\\p{{L}}\\p{{N}}]+/gu, " ")
        .replace(/\\s+/g, " ")
        .trim();
    }}

    function bucket(value, step) {{
      const x = n(value);
      if (x === null) return "";
      return String(Math.round(x / step) * step);
    }}

    function dedupeKey(row, mode) {{
      const areaStep = mode === "strict" ? 0.2 : 1.0;
      const priceStep = mode === "strict" ? 100 : 500;
      const primaryPrice = n(row.price_usd_total) ?? n(row.price_gel_total) ?? n(row.price_eur_total);
      const base = [
        row.deal_type || "",
        normalizeAddress(row.address),
        bucket(row.room, 1),
        bucket(row.area, areaStep),
        bucket(primaryPrice, priceStep),
      ];
      if (mode === "strict") {{
        base.push(bucket(row.bedroom, 1));
        base.push(bucket(row.floor, 1));
        base.push(bucket(row.total_floors, 1));
      }}
      return base.join("|");
    }}

    function dedupeRows(rows, mode) {{
      const grouped = new Map();
      for (const row of rows) {{
        const key = dedupeKey(row, mode);
        const incoming = {{ ...row }};
        const existing = grouped.get(key);
        if (!existing) {{
          incoming._dupe_count = 1;
          grouped.set(key, incoming);
          continue;
        }}

        existing._dupe_count = (existing._dupe_count || 1) + 1;
        const existingDate = parseDate(existing.last_updated);
        const incomingDate = parseDate(incoming.last_updated);

        if (incomingDate && (!existingDate || incomingDate > existingDate)) {{
          incoming._dupe_count = existing._dupe_count;
          if ((!incoming.condition || incoming.condition === "Unknown") && existing.condition && existing.condition !== "Unknown") {{
            incoming.condition = existing.condition;
          }}
          if ((!incoming.building_status || incoming.building_status === "Unknown") && existing.building_status && existing.building_status !== "Unknown") {{
            incoming.building_status = existing.building_status;
          }}
          grouped.set(key, incoming);
        }} else {{
          if ((!existing.condition || existing.condition === "Unknown") && incoming.condition && incoming.condition !== "Unknown") {{
            existing.condition = incoming.condition;
          }}
          if ((!existing.building_status || existing.building_status === "Unknown") && incoming.building_status && incoming.building_status !== "Unknown") {{
            existing.building_status = incoming.building_status;
          }}
        }}
      }}
      return Array.from(grouped.values());
    }}

    function buildConditionFilterOptions(selectedValues = null) {{
      const wrapper = $("conditionFilters");
      wrapper.innerHTML = "";
      const conditions = (META.condition_values || ["Unknown"]).slice();
      conditions.sort((a, b) => String(a).localeCompare(String(b)));

      for (const condition of conditions) {{
        const id = "cond_" + String(condition).replace(/[^a-zA-Z0-9]+/g, "_").toLowerCase();
        const label = document.createElement("label");
        const input = document.createElement("input");
        input.type = "checkbox";
        input.name = "conditionOpt";
        input.value = condition;
        input.id = id;
        input.checked = selectedValues ? selectedValues.has(condition) : true;
        label.appendChild(input);
        label.appendChild(document.createTextNode(" " + condition));
        wrapper.appendChild(label);
      }}
    }}

    function selectedConditions() {{
      const out = new Set();
      const checks = document.querySelectorAll('input[name="conditionOpt"]');
      for (const c of checks) {{
        if (c.checked) out.add(c.value);
      }}
      return out;
    }}

    function initControls() {{
      $("rowsInSource").textContent = META.total_rows.toLocaleString();

      $("dateFrom").value = META.min_date;
      $("dateTo").value = META.max_date;

      $("roomMin").value = Math.floor(META.room_min || 0);
      $("roomMax").value = Math.ceil(META.room_max || 10);

      $("areaMin").value = Math.floor(META.area_min || 0);
      $("areaMax").value = Math.ceil(META.area_max || 1000);

      $("priceMin").value = Math.floor(META.price_min || 0);
      $("priceMax").value = Math.ceil(META.price_max || 1000000);

      buildConditionFilterOptions();
    }}

    function getFilteredRows() {{
      const includeSale = $("dealSale").checked;
      const includeRent = $("dealRent").checked;
      const includeNewBuilding = $("statusNewBuilding").checked;
      const includeOldBuilding = $("statusOldBuilding").checked;
      const includeUnknownBuilding = $("statusUnknownBuilding").checked;
      const conditionSet = selectedConditions();
      const dateFrom = $("dateFrom").value ? new Date($("dateFrom").value + "T00:00:00") : null;
      const dateTo = $("dateTo").value ? new Date($("dateTo").value + "T23:59:59") : null;

      const roomMin = n($("roomMin").value);
      const roomMax = n($("roomMax").value);
      const areaMin = n($("areaMin").value);
      const areaMax = n($("areaMax").value);
      const priceMin = n($("priceMin").value);
      const priceMax = n($("priceMax").value);

      return RAW_DATA.filter((r) => {{
        if (r.deal_type === "sale" && !includeSale) return false;
        if (r.deal_type === "rent" && !includeRent) return false;

        if (r.building_status === "New building" && !includeNewBuilding) return false;
        if (r.building_status === "Old building" && !includeOldBuilding) return false;
        if ((r.building_status === "Unknown" || !r.building_status) && !includeUnknownBuilding) return false;

        const rowCondition = r.condition || "Unknown";
        if (conditionSet.size > 0 && !conditionSet.has(rowCondition)) return false;
        if (conditionSet.size === 0) return false;

        const d = parseDate(r.last_updated);
        if (dateFrom && d && d < dateFrom) return false;
        if (dateTo && d && d > dateTo) return false;

        const room = n(r.room);
        if (roomMin !== null && room !== null && room < roomMin) return false;
        if (roomMax !== null && room !== null && room > roomMax) return false;

        const area = n(r.area);
        if (areaMin !== null && area !== null && area < areaMin) return false;
        if (areaMax !== null && area !== null && area > areaMax) return false;

        const price = n(r.price_usd_total);
        if (priceMin !== null && price !== null && price < priceMin) return false;
        if (priceMax !== null && price !== null && price > priceMax) return false;

        return true;
      }});
    }}

    function weekStartIso(s) {{
      const d = parseDate(s);
      if (!d) return null;
      const day = (d.getDay() + 6) % 7;
      d.setHours(0, 0, 0, 0);
      d.setDate(d.getDate() - day);
      return d.toISOString().slice(0, 10);
    }}

    function escapeHtml(str) {{
      return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }}

    function buildCsv(rows) {{
      const cols = [
        "id","deal_type","address","room","area","floor","total_floors",
        "last_updated","price_usd_total","price_usd_sqm","_dupe_count","building_status","condition","listing_url"
      ];
      const lines = [cols.join(",")];
      for (const r of rows) {{
        const line = cols.map((c) => {{
          const val = r[c] === null || r[c] === undefined ? "" : String(r[c]);
          return `"${{val.replace(/"/g, '""')}}"`;
        }}).join(",");
        lines.push(line);
      }}
      return lines.join("\\n");
    }}

    function renderMetrics(rows) {{
      const saleCount = rows.filter(r => r.deal_type === "sale").length;
      const rentCount = rows.filter(r => r.deal_type === "rent").length;
      const prices = rows.map(r => n(r.price_usd_total));
      const sqm = rows.map(r => n(r.price_usd_sqm));
      const lastUpdate = rows
        .map(r => parseDate(r.last_updated))
        .filter(Boolean)
        .sort((a, b) => b - a)[0];

      $("kpiListings").textContent = rows.length.toLocaleString();
      $("kpiDeals").textContent = `${{saleCount.toLocaleString()}} / ${{rentCount.toLocaleString()}}`;
      $("kpiMedianPrice").textContent = fmt0(median(prices));
      $("kpiMedianSqm").textContent = fmt(median(sqm));
      $("kpiAvgPrice").textContent = fmt0(average(prices));
      $("kpiAvgSqm").textContent = fmt(average(sqm));
      $("kpiWeightedSqm").textContent = fmt(weightedSqm(rows));
      $("kpiLastUpdate").textContent = lastUpdate ? lastUpdate.toISOString().slice(0, 16).replace("T", " ") : "n/a";
    }}

    function renderTrend(rows) {{
      const buckets = new Map();
      for (const r of rows) {{
        const wk = weekStartIso(r.last_updated);
        if (!wk) continue;
        const key = `${{wk}}|${{r.deal_type}}`;
        if (!buckets.has(key)) buckets.set(key, []);
        buckets.get(key).push(r);
      }}

      const sale = [];
      const rent = [];
      for (const [key, items] of buckets.entries()) {{
        const [wk, deal] = key.split("|");
        const sqm = median(items.map(x => n(x.price_usd_sqm)));
        const total = median(items.map(x => n(x.price_usd_total)));
        const rec = {{ wk, sqm, total }};
        if (deal === "sale") sale.push(rec);
        if (deal === "rent") rent.push(rec);
      }}
      sale.sort((a, b) => a.wk.localeCompare(b.wk));
      rent.sort((a, b) => a.wk.localeCompare(b.wk));

      const traces = [
        {{
          x: sale.map(x => x.wk),
          y: sale.map(x => x.sqm),
          name: "Sale median USD/m2",
          mode: "lines+markers",
          line: {{ color: "#1f7ae0", width: 3 }}
        }},
        {{
          x: rent.map(x => x.wk),
          y: rent.map(x => x.sqm),
          name: "Rent median USD/m2",
          mode: "lines+markers",
          line: {{ color: "#0c9f77", width: 3 }}
        }}
      ];

      Plotly.newPlot("chartTrend", traces, {{
        title: "Weekly Median Price per m2 (USD)",
        margin: {{ t: 45, r: 10, b: 45, l: 55 }},
        xaxis: {{ title: "Week" }},
        yaxis: {{ title: "USD/m2" }},
        legend: {{ orientation: "h" }}
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderHistogram(rows) {{
      const sale = rows.filter(r => r.deal_type === "sale").map(r => n(r.price_usd_sqm)).filter(v => v !== null);
      const rent = rows.filter(r => r.deal_type === "rent").map(r => n(r.price_usd_sqm)).filter(v => v !== null);

      Plotly.newPlot("chartHistogram", [
        {{ x: sale, type: "histogram", name: "Sale", marker: {{ color: "#1f7ae0" }}, opacity: 0.65 }},
        {{ x: rent, type: "histogram", name: "Rent", marker: {{ color: "#0c9f77" }}, opacity: 0.65 }}
      ], {{
        barmode: "overlay",
        title: "Distribution of Price per m2 (USD)",
        margin: {{ t: 45, r: 10, b: 45, l: 55 }},
        xaxis: {{ title: "USD/m2" }},
        yaxis: {{ title: "Listings" }}
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderBox(rows) {{
      const sale = rows.filter(r => r.deal_type === "sale").map(r => n(r.price_usd_sqm)).filter(v => v !== null);
      const rent = rows.filter(r => r.deal_type === "rent").map(r => n(r.price_usd_sqm)).filter(v => v !== null);

      Plotly.newPlot("chartBox", [
        {{ y: sale, type: "box", name: "Sale", marker: {{ color: "#1f7ae0" }} }},
        {{ y: rent, type: "box", name: "Rent", marker: {{ color: "#0c9f77" }} }}
      ], {{
        title: "Price per m2 (USD) by Deal Type",
        margin: {{ t: 45, r: 10, b: 45, l: 55 }},
        yaxis: {{ title: "USD/m2" }}
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderTopAddresses(rows) {{
      const m = new Map();
      for (const r of rows) {{
        const address = r.address || "Unknown";
        if (!m.has(address)) m.set(address, []);
        m.get(address).push(r);
      }}
      const ranked = [];
      for (const [address, items] of m.entries()) {{
        ranked.push({{
          address,
          count: items.length,
          medianSsqm: median(items.map(x => n(x.price_usd_sqm))),
          medianPrice: median(items.map(x => n(x.price_usd_total)))
        }});
      }}
      ranked.sort((a, b) => b.count - a.count);
      const top = ranked.slice(0, 15).reverse();

      Plotly.newPlot("chartTopAddresses", [{{
        x: top.map(x => x.count),
        y: top.map(x => x.address),
        type: "bar",
        orientation: "h",
        marker: {{ color: "#ca5a14" }},
        hovertemplate: "<b>%{{y}}</b><br>Listings: %{{x}}<extra></extra>"
      }}], {{
        title: "Top Addresses by Listing Count",
        margin: {{ t: 45, r: 10, b: 45, l: 180 }},
        xaxis: {{ title: "Listings" }}
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderScatter(rows) {{
      const sale = rows.filter(r => r.deal_type === "sale" && n(r.area) !== null && n(r.price_usd_total) !== null);
      const rent = rows.filter(r => r.deal_type === "rent" && n(r.area) !== null && n(r.price_usd_total) !== null);

      Plotly.newPlot("chartScatter", [
        {{
          x: sale.map(r => n(r.area)),
          y: sale.map(r => n(r.price_usd_total)),
          mode: "markers",
          type: "scattergl",
          name: "Sale",
          text: sale.map(r => `ID ${{r.id}} | ${{r.address}}`),
          marker: {{ color: "#1f7ae0", size: 8, opacity: 0.65 }}
        }},
        {{
          x: rent.map(r => n(r.area)),
          y: rent.map(r => n(r.price_usd_total)),
          mode: "markers",
          type: "scattergl",
          name: "Rent",
          text: rent.map(r => `ID ${{r.id}} | ${{r.address}}`),
          marker: {{ color: "#0c9f77", size: 8, opacity: 0.65 }}
        }}
      ], {{
        title: "Area vs Listing Price (USD)",
        margin: {{ t: 45, r: 10, b: 45, l: 55 }},
        xaxis: {{ title: "Area (m2)" }},
        yaxis: {{ title: "Price (USD)" }}
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderTable(rows) {{
      const body = $("tableBody");
      body.innerHTML = "";
      const sorted = rows.slice().sort((a, b) => (b.last_updated || "").localeCompare(a.last_updated || ""));
      for (const r of sorted) {{
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${{escapeHtml(r.id ?? "")}}</td>
          <td>${{escapeHtml(r.deal_type ?? "")}}</td>
          <td>${{escapeHtml(r.address ?? "")}}</td>
          <td>${{escapeHtml(r.room ?? "")}}</td>
          <td>${{escapeHtml(r.area ?? "")}}</td>
          <td>${{escapeHtml(r.price_usd_total ?? "")}}</td>
          <td>${{escapeHtml(r.price_usd_sqm ?? "")}}</td>
          <td>${{escapeHtml(r._dupe_count ?? 1)}}</td>
          <td>${{escapeHtml(r.building_status ?? "")}}</td>
          <td>${{escapeHtml(r.condition ?? "")}}</td>
          <td>${{escapeHtml(r.last_updated ?? "")}}</td>
          <td><a href="${{escapeHtml(r.listing_url ?? "#")}}" target="_blank">open</a></td>
        `;
        body.appendChild(tr);
      }}
    }}

    function refresh() {{
      const filtered = getFilteredRows();
      let rows = filtered.map((r) => ({{ ...r, _dupe_count: 1 }}));
      let removed = 0;

      if ($("dedupeEnabled").checked) {{
        rows = dedupeRows(filtered, $("dedupeMode").value);
        removed = filtered.length - rows.length;
      }}

      $("dedupeInfo").textContent = `After filters: ${{filtered.length.toLocaleString()}} ads, removed as duplicates: ${{removed.toLocaleString()}}, final: ${{rows.length.toLocaleString()}}`;

      renderMetrics(rows);
      renderTrend(rows);
      renderHistogram(rows);
      renderBox(rows);
      renderTopAddresses(rows);
      renderScatter(rows);
      renderTable(rows);

      const csv = buildCsv(rows);
      const blob = new Blob([csv], {{ type: "text/csv;charset=utf-8;" }});
      const url = URL.createObjectURL(blob);
      $("downloadCsv").href = url;
      $("downloadCsv").download = "abashidze_filtered.csv";
    }}

    function resetFilters() {{
      $("dealSale").checked = true;
      $("dealRent").checked = true;
      $("statusNewBuilding").checked = true;
      $("statusOldBuilding").checked = true;
      $("statusUnknownBuilding").checked = true;
      $("dateFrom").value = META.min_date;
      $("dateTo").value = META.max_date;
      $("roomMin").value = Math.floor(META.room_min || 0);
      $("roomMax").value = Math.ceil(META.room_max || 10);
      $("areaMin").value = Math.floor(META.area_min || 0);
      $("areaMax").value = Math.ceil(META.area_max || 1000);
      $("priceMin").value = Math.floor(META.price_min || 0);
      $("priceMax").value = Math.ceil(META.price_max || 1000000);
      $("dedupeEnabled").checked = true;
      $("dedupeMode").value = "relaxed";
      buildConditionFilterOptions();
      refresh();
    }}

    initControls();
    $("applyBtn").addEventListener("click", refresh);
    $("resetBtn").addEventListener("click", resetFilters);
    $("dedupeEnabled").addEventListener("change", refresh);
    $("dedupeMode").addEventListener("change", refresh);
    refresh();
  </script>
</body>
</html>"""


def main() -> int:
    args = parse_args()
    if args.detail_workers <= 0:
        raise SystemExit("--detail-workers must be > 0")

    input_csv = args.input_csv if args.input_csv else find_latest_csv()
    if input_csv is None or not input_csv.exists():
        raise SystemExit(
            f"Input CSV not found. Provide --input-csv or place files in {DEFAULT_EXPORTS_DIR}/"
            f" with pattern {DEFAULT_INPUT_PATTERN}"
        )

    rows, meta = load_rows(input_csv)
    if not rows:
        raise SystemExit(f"No rows found in {input_csv}")

    fetched_count = 0
    if not args.skip_detail_enrichment:
        rows, _, fetched_count = enrich_rows_with_details(
            rows=rows,
            cache_path=args.detail_cache,
            workers=args.detail_workers,
        )

    # Rebuild dynamic lists after enrichment.
    meta["condition_values"] = sorted(
        {str(row.get("condition") or "Unknown") for row in rows},
        key=lambda x: x.lower(),
    )
    meta["building_status_values"] = sorted(
        {str(row.get("building_status") or "Unknown") for row in rows},
        key=lambda x: x.lower(),
    )

    if args.output_html:
        output_html = args.output_html
    else:
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
        output_html = DEFAULT_EXPORTS_DIR / f"abashidze_dashboard_{stamp}.html"

    output_html.parent.mkdir(parents=True, exist_ok=True)
    html = build_html(rows=rows, meta=meta, source_csv=input_csv)
    output_html.write_text(html, encoding="utf-8")

    if args.skip_detail_enrichment:
        print("Detail enrichment skipped.")
    else:
        print(f"Detail enrichment fetched: {fetched_count} new records.")
    print(f"Dashboard generated: {output_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
