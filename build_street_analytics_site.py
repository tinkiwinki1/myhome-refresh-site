#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

DEFAULT_EXPORTS_DIR = Path(__file__).resolve().parent / "exports"
DEFAULT_INPUT_PATTERNS = [
    "myhome_active_*.csv",
    "vake_active_last_3_months_*.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build standalone HTML analytics site for multiple streets or the whole district "
            "from exported myhome CSV."
        )
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help=(
            "Input CSV. If omitted, the latest matching export is used from patterns: "
            + ", ".join(DEFAULT_INPUT_PATTERNS)
        ),
    )
    parser.add_argument(
        "--output-html",
        type=Path,
        default=None,
        help="Output HTML path. Default: exports/street_analytics_site_<timestamp>.html",
    )
    return parser.parse_args()


def find_latest_csv() -> Path | None:
    files: list[Path] = []
    for pattern in DEFAULT_INPUT_PATTERNS:
        files.extend(DEFAULT_EXPORTS_DIR.glob(pattern))
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
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


def normalize_street_key(value: str) -> str:
    v = value.lower().strip()
    v = re.sub(r"[^\w]+", " ", v, flags=re.UNICODE)
    v = re.sub(r"\s+", " ", v).strip()
    return v or "unknown"


def normalize_location_key(value: str) -> str:
    return normalize_street_key(value)


def make_urban_key(district_name: str, urban_name: str) -> str:
    return f"{normalize_location_key(district_name)}::{normalize_location_key(urban_name)}"


def make_street_key(street_id: str | None, district_name: str, urban_name: str, street_name: str) -> str:
    raw_street_id = (street_id or "").strip()
    if raw_street_id and raw_street_id != "0":
        return f"street-id:{raw_street_id}"
    return (
        "street-name:"
        f"{normalize_location_key(district_name)}::"
        f"{normalize_location_key(urban_name)}::"
        f"{normalize_street_key(street_name)}"
    )


def extract_street_from_address(address: str | None) -> tuple[str, str]:
    if not address:
        return "unknown", "Unknown"

    raw = address.strip()
    if not raw:
        return "unknown", "Unknown"

    # Keep main part before comma and trim trailing punctuation.
    main = raw.split(",")[0].strip(" .,-")
    main = re.sub(r"\s+", " ", main).strip()
    if not main:
        return "unknown", "Unknown"

    # Remove trailing house number and suffix if present.
    no_house = re.sub(r"\s+\d+[^\s,]*$", "", main, flags=re.UNICODE).strip(" .,-")
    street = no_house if len(no_house) >= 2 else main
    key = normalize_street_key(street)
    return key, street


def safe_min(values: list[float]) -> float:
    return min(values) if values else 0.0


def safe_max(values: list[float]) -> float:
    return max(values) if values else 1.0


def load_rows(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    dates: list[dt.datetime] = []
    rooms: list[float] = []
    areas: list[float] = []
    prices: list[float] = []
    district_counter: Counter[str] = Counter()
    urban_counter: Counter[str] = Counter()
    urban_counts: Counter[str] = Counter()
    street_key_counts: Counter[str] = Counter()
    street_name_by_key_counts: dict[str, Counter[str]] = defaultdict(Counter)
    urban_meta_by_key: dict[str, dict[str, Any]] = {}
    street_meta_by_key: dict[str, dict[str, Any]] = {}

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            last_updated_raw = raw.get("last_updated")
            last_updated_dt = parse_last_updated(last_updated_raw)
            room = to_float(raw.get("room"))
            area = to_float(raw.get("area"))
            price_usd_total = to_float(raw.get("price_usd_total"))
            price_gel_total = to_float(raw.get("price_gel_total"))
            price_eur_total = to_float(raw.get("price_eur_total"))
            price_usd_sqm = to_float(raw.get("price_usd_sqm"))
            price_gel_sqm = to_float(raw.get("price_gel_sqm"))
            price_eur_sqm = to_float(raw.get("price_eur_sqm"))

            district = (raw.get("district_name") or "").strip()
            urban = (raw.get("urban_name") or "").strip()
            district_name = district or "Unknown"
            urban_name = urban or "Unknown"
            urban_key = make_urban_key(district_name, urban_name)
            street_name = extract_street_from_address(raw.get("address"))[1]
            street_key = make_street_key(raw.get("street_id"), district_name, urban_name, street_name)
            street_key_counts[street_key] += 1
            street_name_by_key_counts[street_key][street_name] += 1

            if district:
                district_counter[district_name] += 1
            if urban:
                urban_counter[urban_name] += 1
            urban_counts[urban_key] += 1
            urban_meta_by_key.setdefault(
                urban_key,
                {
                    "key": urban_key,
                    "name": urban_name,
                    "district_name": district_name,
                },
            )
            street_meta_by_key.setdefault(
                street_key,
                {
                    "key": street_key,
                    "street_id": (raw.get("street_id") or "").strip() or None,
                    "district_name": district_name,
                    "urban_name": urban_name,
                    "urban_key": urban_key,
                },
            )

            row = {
                "id": raw.get("id"),
                "deal_type": (raw.get("deal_type") or "").lower(),
                "district_name": district_name,
                "urban_name": urban_name,
                "urban_key": urban_key,
                "street_id": (raw.get("street_id") or "").strip() or None,
                "street_key": street_key,
                "street_name": street_name,
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
            }
            rows.append(row)

            if last_updated_dt is not None:
                dates.append(last_updated_dt)
            if room is not None:
                rooms.append(room)
            if area is not None:
                areas.append(area)
            if price_usd_total is not None:
                prices.append(price_usd_total)

    districts = [
        {"name": name, "count": total_count}
        for name, total_count in district_counter.items()
    ]
    districts.sort(key=lambda x: (-x["count"], x["name"].lower()))

    urbans: list[dict[str, Any]] = []
    for key, total_count in urban_counts.items():
        meta = urban_meta_by_key[key]
        urbans.append(
            {
                "key": key,
                "name": meta["name"],
                "district_name": meta["district_name"],
                "count": total_count,
            }
        )
    urbans.sort(key=lambda x: (-x["count"], x["district_name"].lower(), x["name"].lower()))

    streets: list[dict[str, Any]] = []
    for key, total_count in street_key_counts.items():
        display_name = street_name_by_key_counts[key].most_common(1)[0][0]
        meta = street_meta_by_key[key]
        streets.append(
            {
                "key": key,
                "name": display_name,
                "count": total_count,
                "street_id": meta["street_id"],
                "district_name": meta["district_name"],
                "urban_name": meta["urban_name"],
                "urban_key": meta["urban_key"],
            }
        )
    streets.sort(
        key=lambda x: (
            -x["count"],
            x["district_name"].lower(),
            x["urban_name"].lower(),
            x["name"].lower(),
        )
    )

    min_date = min(dates).strftime("%Y-%m-%d") if dates else dt.date.today().isoformat()
    max_date = max(dates).strftime("%Y-%m-%d") if dates else dt.date.today().isoformat()

    primary_district = district_counter.most_common(1)[0][0] if district_counter else "Unknown district"
    primary_urban = urban_counter.most_common(1)[0][0] if urban_counter else "Unknown area"
    if len(districts) > 1:
        source_label = f"{len(districts)} districts / {len(urbans)} areas"
    elif len(urbans) > 1:
        source_label = f"{primary_district} / {len(urbans)} areas"
    else:
        source_label = primary_urban or primary_district

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
        "district_name": primary_district,
        "urban_name": primary_urban,
        "district_count": len(districts),
        "urban_count": len(urbans),
        "source_label": source_label,
        "districts": districts,
        "urbans": urbans,
        "streets": streets,
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
  <title>Street Analytics Site</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #f6f8fb;
      --panel: #ffffff;
      --ink: #12202f;
      --muted: #566575;
      --line: #d9e0e8;
      --accent: #176bc5;
      --accent-2: #0f9f79;
      --warn: #d06b19;
      --shadow: 0 8px 24px rgba(13, 24, 36, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
      background: linear-gradient(180deg, #f3f6fb 0%, #f8fafd 100%);
      color: var(--ink);
    }}
    .page {{
      max-width: 1560px;
      margin: 18px auto;
      padding: 0 14px;
      display: grid;
      grid-template-columns: 360px 1fr;
      gap: 14px;
    }}
    @media (max-width: 1150px) {{
      .page {{ grid-template-columns: 1fr; }}
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 12px;
    }}
    .sidebar h1 {{
      margin: 0;
      font-size: 1.18rem;
      line-height: 1.3;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.35;
    }}
    .field {{
      margin-top: 10px;
    }}
    .field label {{
      display: block;
      margin-bottom: 5px;
      color: var(--muted);
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
    }}
    .field input[type="number"],
    .field input[type="date"],
    .field input[type="text"],
    .field select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 9px;
      padding: 8px;
      font-size: 0.92rem;
      background: #fff;
      color: var(--ink);
    }}
    .checks {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px;
      font-size: 0.92rem;
    }}
    .street-list {{
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      max-height: 260px;
      overflow: auto;
      padding: 8px;
    }}
    .street-item {{
      display: block;
      margin: 4px 0;
      font-size: 0.9rem;
    }}
    .street-item small {{
      color: var(--muted);
      margin-left: 4px;
    }}
    .btn-row {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
      margin-top: 8px;
    }}
    .btn-row-3 {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      margin-top: 8px;
    }}
    button {{
      border: 1px solid var(--line);
      border-radius: 9px;
      padding: 9px;
      background: #fff;
      color: var(--ink);
      font-weight: 700;
      cursor: pointer;
    }}
    button.primary {{
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }}
    .download {{
      margin-top: 8px;
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 9px;
      padding: 8px 10px;
      background: #fff;
      color: var(--ink);
      text-decoration: none;
      font-weight: 700;
    }}
    .main-grid {{
      display: grid;
      gap: 12px;
    }}
    .kpis {{
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }}
    @media (max-width: 1260px) {{
      .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 640px) {{
      .kpis {{ grid-template-columns: 1fr; }}
    }}
    .kpi {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      min-height: 82px;
    }}
    .kpi .name {{
      font-size: 0.78rem;
      font-weight: 800;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    .kpi .value {{
      margin-top: 5px;
      font-size: 1.3rem;
      font-weight: 800;
    }}
    .chart-grid-2 {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    @media (max-width: 1080px) {{
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
      font-size: 0.88rem;
      background: #fff;
    }}
    th, td {{
      border-bottom: 1px solid #edf1f6;
      padding: 8px 9px;
      white-space: nowrap;
      text-align: left;
    }}
    thead th {{
      position: sticky;
      top: 0;
      background: #f2f6fb;
      z-index: 1;
    }}
    tbody tr:hover {{
      background: #f7fbff;
    }}
    .footer {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.84rem;
      line-height: 1.4;
    }}
    .disabled {{
      opacity: 0.55;
      pointer-events: none;
    }}
  </style>
</head>
<body>
  <div class="page">
    <aside class="panel sidebar">
      <h1>Street Analytics</h1>
      <div class="subtle">Source: <b id="districtName"></b> | Rows: <b id="rowsInSource"></b></div>
      <div class="subtle" id="scopeText" style="margin-top:6px;"></div>

      <div class="field">
        <label>District</label>
        <select id="districtFilter"></select>
      </div>

      <div class="field">
        <label>Area</label>
        <select id="urbanFilter"></select>
      </div>

      <div class="field">
        <label>Scope</label>
        <div class="checks" style="grid-template-columns:1fr;">
          <label><input type="checkbox" id="allDistrict" checked> Whole selected location (all streets)</label>
        </div>
      </div>

      <div id="streetControls">
        <div class="field">
          <label>Search street</label>
          <input type="text" id="streetSearch" placeholder="Type to filter street list..." />
        </div>
        <div class="street-list" id="streetList"></div>
        <div class="btn-row-3">
          <button id="selectAllStreets">All</button>
          <button id="selectTop2">Top 2</button>
          <button id="clearStreets">Clear</button>
        </div>
      </div>

      <div class="field">
        <label>Deal type</label>
        <div class="checks">
          <label><input type="checkbox" id="dealSale" checked> Sale</label>
          <label><input type="checkbox" id="dealRent" checked> Rent</label>
        </div>
      </div>

      <div class="field">
        <label>Date from</label>
        <input type="date" id="dateFrom" />
      </div>
      <div class="field">
        <label>Date to</label>
        <input type="date" id="dateTo" />
      </div>

      <div class="field">
        <label>Rooms min</label>
        <input type="number" id="roomMin" step="1" />
      </div>
      <div class="field">
        <label>Rooms max</label>
        <input type="number" id="roomMax" step="1" />
      </div>

      <div class="field">
        <label>Area min (m2)</label>
        <input type="number" id="areaMin" step="1" />
      </div>
      <div class="field">
        <label>Area max (m2)</label>
        <input type="number" id="areaMax" step="1" />
      </div>

      <div class="field">
        <label>Price min (USD)</label>
        <input type="number" id="priceMin" step="100" />
      </div>
      <div class="field">
        <label>Price max (USD)</label>
        <input type="number" id="priceMax" step="100" />
      </div>

      <div class="field">
        <label>Duplicate handling</label>
        <div class="checks" style="grid-template-columns:1fr;">
          <label><input type="checkbox" id="dedupeEnabled" checked> Remove duplicate ads</label>
        </div>
        <select id="dedupeMode" style="margin-top:8px;">
          <option value="relaxed" selected>Relaxed</option>
          <option value="strict">Strict</option>
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
        <div class="kpis" style="margin-top:8px;">
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
        <div id="chartTopStreets" class="chart"></div>
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
                <th>District</th>
                <th>Area</th>
                <th>Street</th>
                <th>Address</th>
                <th>Room</th>
                <th>Area</th>
                <th>Price USD</th>
                <th>USD/m2</th>
                <th>Copies</th>
                <th>Updated</th>
                <th>Listing</th>
              </tr>
            </thead>
            <tbody id="tableBody"></tbody>
          </table>
        </div>
        <div class="footer">
          Built at: {generated_at}<br />
          Source CSV: {source_csv}<br />
          Legacy fallback dashboard kept intact: <code>build_abashidze_dashboard.py</code>
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
      return Number(v).toLocaleString(undefined, {{
        minimumFractionDigits: digits,
        maximumFractionDigits: digits
      }});
    }}

    function fmt0(v) {{
      if (v === null || v === undefined || Number.isNaN(v)) return "n/a";
      return Number(v).toLocaleString(undefined, {{ maximumFractionDigits: 0 }});
    }}

    function parseDate(s) {{
      if (!s) return null;
      const d = new Date(String(s).replace(" ", "T"));
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
          totalPrice += price;
          totalArea += area;
        }}
      }}
      if (!totalArea) return null;
      return totalPrice / totalArea;
    }}

    function normalizeAddress(value) {{
      if (!value) return "";
      return String(value)
        .toLowerCase()
        .replace(/[^\\w]+/g, " ")
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
      const parts = [
        row.deal_type || "",
        normalizeAddress(row.street_name || row.address),
        bucket(row.room, 1),
        bucket(row.area, areaStep),
        bucket(primaryPrice, priceStep),
      ];
      if (mode === "strict") {{
        parts.push(bucket(row.bedroom, 1));
        parts.push(bucket(row.floor, 1));
        parts.push(bucket(row.total_floors, 1));
      }}
      return parts.join("|");
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
          grouped.set(key, incoming);
        }}
      }}
      return Array.from(grouped.values());
    }}

    function selectedStreetKeys() {{
      const selected = new Set();
      const checks = document.querySelectorAll('input[name="streetOpt"]');
      for (const c of checks) {{
        if (c.checked) selected.add(c.value);
      }}
      return selected;
    }}

    function selectedDistrictName() {{
      const value = $("districtFilter").value || "__all__";
      return value === "__all__" ? "" : value;
    }}

    function selectedUrbanKey() {{
      const value = $("urbanFilter").value || "__all__";
      return value === "__all__" ? "" : value;
    }}

    function selectedUrbanMeta() {{
      const key = selectedUrbanKey();
      if (!key) return null;
      return (META.urbans || []).find((item) => item.key === key) || null;
    }}

    function visibleUrbanCatalog() {{
      const district = selectedDistrictName();
      return (META.urbans || []).filter((item) => !district || item.district_name === district);
    }}

    function visibleStreetCatalog() {{
      const district = selectedDistrictName();
      const urbanKey = selectedUrbanKey();
      return (META.streets || []).filter((item) => {{
        if (district && item.district_name !== district) return false;
        if (urbanKey && item.urban_key !== urbanKey) return false;
        return true;
      }});
    }}

    function buildDistrictOptions() {{
      const select = $("districtFilter");
      const current = select.value || "__all__";
      const items = META.districts || [];
      const options = ['<option value="__all__">All districts in source</option>'];
      for (const item of items) {{
        options.push(
          `<option value="${{escapeHtml(item.name)}}">${{escapeHtml(item.name)}} (${{item.count}})</option>`
        );
      }}
      select.innerHTML = options.join("");
      const exists = items.some((item) => item.name === current);
      select.value = exists ? current : "__all__";
    }}

    function buildUrbanOptions() {{
      const select = $("urbanFilter");
      const current = select.value || "__all__";
      const items = visibleUrbanCatalog();
      const options = ['<option value="__all__">All areas in selection</option>'];
      for (const item of items) {{
        options.push(
          `<option value="${{escapeHtml(item.key)}}">${{escapeHtml(item.name)}} (${{item.count}})</option>`
        );
      }}
      select.innerHTML = options.join("");
      const exists = items.some((item) => item.key === current);
      select.value = exists ? current : "__all__";
    }}

    function buildStreetList(selected = null) {{
      const wrapper = $("streetList");
      wrapper.innerHTML = "";
      const query = ($("streetSearch").value || "").toLowerCase().trim();
      const streets = visibleStreetCatalog();

      for (const s of streets) {{
        const labelText = s.name || "Unknown";
        const key = s.key || "unknown";
        if (query && !labelText.toLowerCase().includes(query) && !key.includes(query)) {{
          continue;
        }}
        const label = document.createElement("label");
        label.className = "street-item";

        const input = document.createElement("input");
        input.type = "checkbox";
        input.name = "streetOpt";
        input.value = key;
        input.checked = selected ? selected.has(key) : false;

        const txt = document.createTextNode(" " + labelText + " ");
        const small = document.createElement("small");
        small.textContent = `(${{s.count}})`;

        label.appendChild(input);
        label.appendChild(txt);
        label.appendChild(small);
        wrapper.appendChild(label);
      }}
    }}

    function setStreetSelection(keys) {{
      const checks = document.querySelectorAll('input[name="streetOpt"]');
      for (const c of checks) {{
        c.checked = keys.has(c.value);
      }}
    }}

    function updateScopeUi() {{
      const allDistrict = $("allDistrict").checked;
      const streetControls = $("streetControls");
      if (allDistrict) streetControls.classList.add("disabled");
      else streetControls.classList.remove("disabled");
    }}

    function streetScopeLabel(row) {{
      const street = row.street_name || "Unknown";
      if ((META.district_count || 0) > 1) {{
        return `${{street}} | ${{row.urban_name || "Unknown"}} | ${{row.district_name || "Unknown"}}`;
      }}
      if ((META.urban_count || 0) > 1) {{
        return `${{street}} | ${{row.urban_name || "Unknown"}}`;
      }}
      return street;
    }}

    function initControls() {{
      $("districtName").textContent = META.source_label || META.urban_name || META.district_name || "Source";
      $("rowsInSource").textContent = Number(META.total_rows || 0).toLocaleString();
      $("scopeText").textContent = `Source range: ${{META.min_date}} to ${{META.max_date}}`;

      $("dateFrom").value = META.min_date;
      $("dateTo").value = META.max_date;
      $("roomMin").value = Math.floor(META.room_min || 0);
      $("roomMax").value = Math.ceil(META.room_max || 10);
      $("areaMin").value = Math.floor(META.area_min || 0);
      $("areaMax").value = Math.ceil(META.area_max || 1000);
      $("priceMin").value = Math.floor(META.price_min || 0);
      $("priceMax").value = Math.ceil(META.price_max || 1000000);

      $("dealSale").checked = true;
      $("dealRent").checked = true;
      $("dedupeEnabled").checked = true;
      $("dedupeMode").value = "relaxed";
      $("allDistrict").checked = true;
      $("streetSearch").value = "";

      buildDistrictOptions();
      buildUrbanOptions();
      buildStreetList(new Set());
      updateScopeUi();
    }}

    function getFilteredRows() {{
      const includeSale = $("dealSale").checked;
      const includeRent = $("dealRent").checked;
      const allDistrict = $("allDistrict").checked;
      const district = selectedDistrictName();
      const urbanKey = selectedUrbanKey();
      const streets = selectedStreetKeys();

      if (!allDistrict && streets.size === 0) {{
        return [];
      }}

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
        if (district && r.district_name !== district) return false;
        if (urbanKey && r.urban_key !== urbanKey) return false;

        if (!allDistrict && !streets.has(r.street_key)) return false;

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
        "id","deal_type","district_name","urban_name","street_name","address","room","bedroom","area","floor","total_floors",
        "last_updated","price_usd_total","price_usd_sqm","_dupe_count","listing_url"
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

    function plotEmpty(id, title, message) {{
      Plotly.newPlot(id, [], {{
        title,
        margin: {{ t: 45, r: 10, b: 35, l: 40 }},
        xaxis: {{ visible: false }},
        yaxis: {{ visible: false }},
        annotations: [{{
          text: message || "No data",
          x: 0.5,
          y: 0.5,
          xref: "paper",
          yref: "paper",
          showarrow: false,
          font: {{ size: 14, color: "#566575" }}
        }}]
      }}, {{ responsive: true, displaylogo: false }});
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
      $("kpiLastUpdate").textContent = lastUpdate
        ? lastUpdate.toISOString().slice(0, 16).replace("T", " ")
        : "n/a";
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
        const rec = {{
          wk,
          sqm: median(items.map(x => n(x.price_usd_sqm))),
          total: median(items.map(x => n(x.price_usd_total))),
        }};
        if (deal === "sale") sale.push(rec);
        if (deal === "rent") rent.push(rec);
      }}
      sale.sort((a, b) => a.wk.localeCompare(b.wk));
      rent.sort((a, b) => a.wk.localeCompare(b.wk));

      if (!sale.length && !rent.length) {{
        plotEmpty("chartTrend", "Weekly Median Price per m2 (USD)", "No date points in current selection");
        return;
      }}

      Plotly.newPlot("chartTrend", [
        {{
          x: sale.map(x => x.wk),
          y: sale.map(x => x.sqm),
          mode: "lines+markers",
          name: "Sale",
          line: {{ color: "#176bc5", width: 3 }},
        }},
        {{
          x: rent.map(x => x.wk),
          y: rent.map(x => x.sqm),
          mode: "lines+markers",
          name: "Rent",
          line: {{ color: "#0f9f79", width: 3 }},
        }}
      ], {{
        title: "Weekly Median Price per m2 (USD)",
        margin: {{ t: 45, r: 10, b: 45, l: 55 }},
        xaxis: {{ title: "Week" }},
        yaxis: {{ title: "USD/m2" }},
        legend: {{ orientation: "h" }},
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderHistogram(rows) {{
      const sale = rows.filter(r => r.deal_type === "sale").map(r => n(r.price_usd_sqm)).filter(v => v !== null);
      const rent = rows.filter(r => r.deal_type === "rent").map(r => n(r.price_usd_sqm)).filter(v => v !== null);
      if (!sale.length && !rent.length) {{
        plotEmpty("chartHistogram", "Distribution of Price per m2 (USD)", "No values");
        return;
      }}
      Plotly.newPlot("chartHistogram", [
        {{ x: sale, type: "histogram", name: "Sale", opacity: 0.64, marker: {{ color: "#176bc5" }} }},
        {{ x: rent, type: "histogram", name: "Rent", opacity: 0.64, marker: {{ color: "#0f9f79" }} }},
      ], {{
        barmode: "overlay",
        title: "Distribution of Price per m2 (USD)",
        margin: {{ t: 45, r: 10, b: 45, l: 55 }},
        xaxis: {{ title: "USD/m2" }},
        yaxis: {{ title: "Listings" }},
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderBox(rows) {{
      const sale = rows.filter(r => r.deal_type === "sale").map(r => n(r.price_usd_sqm)).filter(v => v !== null);
      const rent = rows.filter(r => r.deal_type === "rent").map(r => n(r.price_usd_sqm)).filter(v => v !== null);
      if (!sale.length && !rent.length) {{
        plotEmpty("chartBox", "Price per m2 by Deal Type", "No values");
        return;
      }}
      Plotly.newPlot("chartBox", [
        {{ y: sale, type: "box", name: "Sale", marker: {{ color: "#176bc5" }} }},
        {{ y: rent, type: "box", name: "Rent", marker: {{ color: "#0f9f79" }} }},
      ], {{
        title: "Price per m2 by Deal Type (USD)",
        margin: {{ t: 45, r: 10, b: 40, l: 55 }},
        yaxis: {{ title: "USD/m2" }},
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderTopStreets(rows) {{
      const grouped = new Map();
      for (const r of rows) {{
        const key = r.street_key || r.street_name || "Unknown";
        const entry = grouped.get(key) || {{ label: streetScopeLabel(r), rows: [] }};
        entry.rows.push(r);
        grouped.set(key, entry);
      }}
      const items = [];
      for (const [, entry] of grouped.entries()) {{
        const arr = entry.rows;
        items.push({{
          street: entry.label,
          count: arr.length,
          medianSqm: median(arr.map(x => n(x.price_usd_sqm))),
          medianPrice: median(arr.map(x => n(x.price_usd_total))),
        }});
      }}
      items.sort((a, b) => b.count - a.count);
      const top = items.slice(0, 20).reverse();
      if (!top.length) {{
        plotEmpty("chartTopStreets", "Top Streets by Listing Count", "No streets");
        return;
      }}
      Plotly.newPlot("chartTopStreets", [{{
        x: top.map(x => x.count),
        y: top.map(x => x.street),
        type: "bar",
        orientation: "h",
        marker: {{ color: "#d06b19" }},
        customdata: top.map(x => [x.medianSqm, x.medianPrice]),
        hovertemplate:
          "<b>%{{y}}</b><br>" +
          "Listings: %{{x}}<br>" +
          "Median USD/m2: %{{customdata[0]:.2f}}<br>" +
          "Median price USD: %{{customdata[1]:.0f}}" +
          "<extra></extra>",
      }}], {{
        title: "Top Streets by Listing Count",
        margin: {{ t: 45, r: 10, b: 45, l: 210 }},
        xaxis: {{ title: "Listings" }},
      }}, {{ responsive: true, displaylogo: false }});
    }}

    function renderScatter(rows) {{
      const sale = rows.filter(r => r.deal_type === "sale" && n(r.area) !== null && n(r.price_usd_total) !== null);
      const rent = rows.filter(r => r.deal_type === "rent" && n(r.area) !== null && n(r.price_usd_total) !== null);
      if (!sale.length && !rent.length) {{
        plotEmpty("chartScatter", "Area vs Listing Price (USD)", "No points");
        return;
      }}
      Plotly.newPlot("chartScatter", [
        {{
          x: sale.map(r => n(r.area)),
          y: sale.map(r => n(r.price_usd_total)),
          text: sale.map(r => `ID ${{r.id}} | ${{streetScopeLabel(r)}}`),
          mode: "markers",
          type: "scattergl",
          name: "Sale",
          marker: {{ color: "#176bc5", size: 8, opacity: 0.64 }},
        }},
        {{
          x: rent.map(r => n(r.area)),
          y: rent.map(r => n(r.price_usd_total)),
          text: rent.map(r => `ID ${{r.id}} | ${{streetScopeLabel(r)}}`),
          mode: "markers",
          type: "scattergl",
          name: "Rent",
          marker: {{ color: "#0f9f79", size: 8, opacity: 0.64 }},
        }},
      ], {{
        title: "Area vs Listing Price (USD)",
        margin: {{ t: 45, r: 10, b: 45, l: 55 }},
        xaxis: {{ title: "Area (m2)" }},
        yaxis: {{ title: "Price (USD)" }},
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
          <td>${{escapeHtml(r.district_name ?? "")}}</td>
          <td>${{escapeHtml(r.urban_name ?? "")}}</td>
          <td>${{escapeHtml(r.street_name ?? "")}}</td>
          <td>${{escapeHtml(r.address ?? "")}}</td>
          <td>${{escapeHtml(r.room ?? "")}}</td>
          <td>${{escapeHtml(r.area ?? "")}}</td>
          <td>${{escapeHtml(r.price_usd_total ?? "")}}</td>
          <td>${{escapeHtml(r.price_usd_sqm ?? "")}}</td>
          <td>${{escapeHtml(r._dupe_count ?? 1)}}</td>
          <td>${{escapeHtml(r.last_updated ?? "")}}</td>
          <td><a href="${{escapeHtml(r.listing_url ?? "#")}}" target="_blank">open</a></td>
        `;
        body.appendChild(tr);
      }}
    }}

    function refresh() {{
      const filtered = getFilteredRows();
      let rows = filtered.map(r => ({{ ...r, _dupe_count: 1 }}));
      let removed = 0;

      if ($("dedupeEnabled").checked) {{
        rows = dedupeRows(filtered, $("dedupeMode").value);
        removed = filtered.length - rows.length;
      }}

      const allDistrict = $("allDistrict").checked;
      const streetCount = selectedStreetKeys().size;
      const district = selectedDistrictName();
      const urban = selectedUrbanMeta();
      const locationParts = [];
      locationParts.push(district || "all districts");
      if (urban) locationParts.push(urban.name || "selected area");
      const streetScope = allDistrict ? "all streets" : `${{streetCount}} selected street(s)`;
      $("scopeText").textContent = `Scope: ${{locationParts.join(" / ")}} | ${{streetScope}}`;
      $("dedupeInfo").textContent =
        `After base filters: ${{filtered.length.toLocaleString()}}, removed duplicates: ` +
        `${{removed.toLocaleString()}}, final: ${{rows.length.toLocaleString()}}`;

      renderMetrics(rows);
      renderTrend(rows);
      renderHistogram(rows);
      renderBox(rows);
      renderTopStreets(rows);
      renderScatter(rows);
      renderTable(rows);

      const csv = buildCsv(rows);
      const blob = new Blob([csv], {{ type: "text/csv;charset=utf-8;" }});
      const url = URL.createObjectURL(blob);
      $("downloadCsv").href = url;
      $("downloadCsv").download = "street_analytics_filtered.csv";
    }}

    function resetFilters() {{
      $("districtFilter").value = "__all__";
      buildUrbanOptions();
      $("urbanFilter").value = "__all__";
      $("allDistrict").checked = true;
      $("dealSale").checked = true;
      $("dealRent").checked = true;
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
      $("streetSearch").value = "";
      buildDistrictOptions();
      buildUrbanOptions();
      buildStreetList(new Set());
      updateScopeUi();
      refresh();
    }}

    initControls();

    $("streetSearch").addEventListener("input", () => {{
      const selected = selectedStreetKeys();
      buildStreetList(selected);
    }});
    $("districtFilter").addEventListener("change", () => {{
      const selected = selectedStreetKeys();
      buildUrbanOptions();
      buildStreetList(selected);
      refresh();
    }});
    $("urbanFilter").addEventListener("change", () => {{
      const selected = selectedStreetKeys();
      buildStreetList(selected);
      refresh();
    }});
    $("allDistrict").addEventListener("change", () => {{
      updateScopeUi();
      refresh();
    }});

    $("selectAllStreets").addEventListener("click", () => {{
      $("allDistrict").checked = false;
      updateScopeUi();
      const keys = new Set(visibleStreetCatalog().map(s => s.key));
      setStreetSelection(keys);
      refresh();
    }});
    $("selectTop2").addEventListener("click", () => {{
      $("allDistrict").checked = false;
      updateScopeUi();
      const keys = new Set(visibleStreetCatalog().slice(0, 2).map(s => s.key));
      setStreetSelection(keys);
      refresh();
    }});
    $("clearStreets").addEventListener("click", () => {{
      $("allDistrict").checked = false;
      updateScopeUi();
      setStreetSelection(new Set());
      refresh();
    }});

    $("applyBtn").addEventListener("click", refresh);
    $("resetBtn").addEventListener("click", resetFilters);
    $("dedupeEnabled").addEventListener("change", refresh);
    $("dedupeMode").addEventListener("change", refresh);
    $("dealSale").addEventListener("change", refresh);
    $("dealRent").addEventListener("change", refresh);

    refresh();
  </script>
</body>
</html>"""


def main() -> int:
    args = parse_args()
    input_csv = args.input_csv if args.input_csv else find_latest_csv()
    if input_csv is None or not input_csv.exists():
        raise SystemExit(
            f"Input CSV not found. Provide --input-csv or place files in {DEFAULT_EXPORTS_DIR}/"
            f" with one of patterns: {', '.join(DEFAULT_INPUT_PATTERNS)}"
        )

    rows, meta = load_rows(input_csv)
    if not rows:
        raise SystemExit(f"No rows in {input_csv}")

    if args.output_html:
        output_html = args.output_html
    else:
        stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
        output_html = DEFAULT_EXPORTS_DIR / f"street_analytics_site_{stamp}.html"

    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(build_html(rows=rows, meta=meta, source_csv=input_csv), encoding="utf-8")
    print(f"Street analytics site generated: {output_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
