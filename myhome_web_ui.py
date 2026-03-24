#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import uuid
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import export_myhome_active as export_mod

ROOT_DIR = Path(__file__).resolve().parent
EXPORTS_DIR = ROOT_DIR / "exports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local web UI for myhome export and analytics.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind. Default: 8765")
    parser.add_argument("--open-browser", action="store_true", help="Open the browser automatically on start.")
    return parser.parse_args()


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Myhome Control Panel</title>
  <style>
    :root {
      --bg: #f2ede3;
      --bg-2: #e8dfcf;
      --panel: rgba(255, 251, 245, 0.92);
      --panel-strong: rgba(255, 248, 239, 0.98);
      --line: #cdbda5;
      --ink: #182229;
      --muted: #655b4f;
      --accent: #a3472f;
      --accent-2: #2f6c63;
      --accent-3: #d8b36a;
      --ok: #226d53;
      --warn: #b35a28;
      --shadow: 0 18px 50px rgba(61, 44, 21, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(211, 143, 89, 0.26), transparent 34%),
        radial-gradient(circle at bottom right, rgba(47, 108, 99, 0.18), transparent 28%),
        linear-gradient(180deg, var(--bg) 0%, var(--bg-2) 100%);
    }
    .shell {
      max-width: 1480px;
      margin: 0 auto;
      padding: 20px 18px 28px;
    }
    .hero {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 18px;
      align-items: stretch;
    }
    .hero-card,
    .panel {
      border: 1px solid rgba(118, 94, 52, 0.18);
      border-radius: 28px;
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
    }
    .hero-card {
      padding: 26px 28px;
      position: relative;
      overflow: hidden;
    }
    .hero-card::after {
      content: "";
      position: absolute;
      right: -48px;
      top: -48px;
      width: 180px;
      height: 180px;
      border-radius: 999px;
      background: radial-gradient(circle, rgba(163, 71, 47, 0.18), transparent 68%);
    }
    .eyebrow {
      color: var(--accent);
      font-weight: 800;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      font-size: 0.76rem;
    }
    h1 {
      margin: 10px 0 10px;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-size: clamp(2rem, 4vw, 3.4rem);
      line-height: 0.95;
      max-width: 10ch;
    }
    .lead {
      max-width: 62ch;
      color: var(--muted);
      line-height: 1.5;
      font-size: 1rem;
    }
    .hero-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 9px 12px;
      border: 1px solid rgba(101, 91, 79, 0.2);
      border-radius: 999px;
      background: rgba(255,255,255,0.55);
      color: var(--muted);
      font-size: 0.92rem;
    }
    .status-card {
      padding: 24px 24px 18px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      background:
        linear-gradient(180deg, rgba(24,34,41,0.98), rgba(34,47,52,0.96)),
        var(--panel-strong);
      color: #f5efe6;
    }
    .status-card h2 {
      margin: 8px 0 8px;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-size: 1.6rem;
    }
    .status-banner {
      margin-top: 12px;
      padding: 14px 16px;
      border-radius: 18px;
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.12);
      min-height: 76px;
      line-height: 1.45;
      color: rgba(245, 239, 230, 0.88);
    }
    .status-stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }
    .status-stat {
      padding: 10px 11px;
      border-radius: 14px;
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.12);
    }
    .status-stat strong {
      display: block;
      font-size: 1rem;
      color: #fff8ef;
    }
    .status-stat small {
      color: rgba(245, 239, 230, 0.72);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-size: 0.72rem;
    }
    .layout {
      display: grid;
      grid-template-columns: 420px 1fr;
      gap: 18px;
      margin-top: 18px;
    }
    .panel {
      padding: 20px;
    }
    .panel h3 {
      margin: 0 0 12px;
      font-size: 1rem;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .stack {
      display: grid;
      gap: 12px;
    }
    .field {
      display: grid;
      gap: 6px;
    }
    .field label {
      font-size: 0.85rem;
      font-weight: 700;
      color: var(--muted);
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    input, select, button, textarea {
      font: inherit;
    }
    input[type="text"],
    input[type="number"],
    input[type="date"],
    select,
    textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 13px;
      background: rgba(255,255,255,0.82);
      color: var(--ink);
    }
    textarea {
      min-height: 90px;
      resize: vertical;
    }
    .inline {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .toggle-group {
      display: inline-grid;
      grid-template-columns: 1fr 1fr;
      background: rgba(255,255,255,0.65);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 4px;
      gap: 4px;
    }
    .toggle-group button {
      border: 0;
      border-radius: 12px;
      background: transparent;
      padding: 10px 12px;
      color: var(--muted);
      font-weight: 700;
      cursor: pointer;
    }
    .toggle-group button.active {
      background: var(--ink);
      color: #fff8ef;
    }
    .action-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: end;
    }
    .action-row-main {
      grid-template-columns: 1fr auto auto;
    }
    .search-box {
      display: grid;
      gap: 10px;
    }
    .btn {
      border: 1px solid transparent;
      border-radius: 14px;
      padding: 12px 14px;
      font-weight: 800;
      cursor: pointer;
      transition: transform 0.16s ease, background 0.16s ease, border-color 0.16s ease;
    }
    .btn:hover { transform: translateY(-1px); }
    .btn:disabled {
      transform: none;
      opacity: 0.68;
      cursor: default;
    }
    .btn-primary {
      background: linear-gradient(135deg, var(--accent), #c35d34);
      color: white;
      box-shadow: 0 10px 24px rgba(163, 71, 47, 0.24);
    }
    .btn-secondary {
      background: rgba(255,255,255,0.76);
      color: var(--ink);
      border-color: var(--line);
    }
    .btn-ghost {
      background: transparent;
      color: var(--muted);
      border-color: rgba(101,91,79,0.24);
    }
    .result-list,
    .recent-list,
    .selected-list {
      display: grid;
      gap: 10px;
    }
    .street-option,
    .recent-card,
    .selected-chip {
      border: 1px solid rgba(118, 94, 52, 0.16);
      border-radius: 16px;
      background: rgba(255,255,255,0.7);
    }
    .street-option {
      padding: 12px 13px;
      cursor: pointer;
    }
    .street-option strong,
    .selected-chip strong {
      display: block;
      font-size: 0.98rem;
    }
    .street-option small,
    .selected-chip small,
    .recent-card small {
      color: var(--muted);
      line-height: 1.45;
    }
    .selected-chip {
      padding: 10px 12px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }
    .selected-chip button {
      border: 0;
      background: transparent;
      color: var(--accent);
      font-weight: 800;
      cursor: pointer;
      padding: 0;
    }
    .recent-card {
      padding: 14px 15px;
      display: grid;
      gap: 10px;
    }
    .recent-links {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .recent-links a {
      text-decoration: none;
      color: var(--accent-2);
      font-weight: 800;
    }
    .empty {
      color: var(--muted);
      padding: 16px;
      border: 1px dashed rgba(101,91,79,0.28);
      border-radius: 16px;
      text-align: center;
      background: rgba(255,255,255,0.36);
    }
    .mono {
      font-family: "SFMono-Regular", "Menlo", monospace;
      font-size: 0.88rem;
      white-space: pre-wrap;
    }
    .success { color: #dff5eb; }
    .warning { color: #ffd8c6; }
    .foot {
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.88rem;
    }
    .hidden { display: none; }
    @media (max-width: 1100px) {
      .hero, .layout { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .shell { padding: 14px; }
      .hero-card, .status-card, .panel { border-radius: 22px; }
      .inline, .action-row { grid-template-columns: 1fr; }
      .status-stats { grid-template-columns: 1fr 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-card">
        <div class="eyebrow">Local Control Panel</div>
        <h1>Myhome Analytics Builder</h1>
        <div class="lead">
          Choose district, area, streets and time range in the browser. The server exports data,
          builds the standalone analytics page and gives you direct links to open the result.
        </div>
        <div class="hero-meta">
          <div class="pill">District + Area dropdowns</div>
          <div class="pill">Street search with click-to-add</div>
          <div class="pill">Export + HTML in one run</div>
        </div>
      </div>
      <div class="hero-card status-card">
        <div>
          <div class="eyebrow" style="color:#d8b36a;">Run Status</div>
          <h2 id="jobTitle">Idle</h2>
          <div id="jobBanner" class="status-banner">Ready. Pick filters and run a build.</div>
        </div>
        <div class="foot" id="jobFoot">Results will appear here and in the recent runs panel.</div>
      </div>
    </section>

    <section class="layout">
      <div class="panel">
        <h3>Filters</h3>
        <div class="stack">
          <div class="field">
            <label>Period Mode</label>
            <div class="toggle-group">
              <button type="button" id="modeDays" class="active">Days Back</button>
              <button type="button" id="modeRange">Date Range</button>
            </div>
          </div>

          <div id="daysBlock" class="field">
            <label>Days Back</label>
              <input type="number" id="daysInput" min="1" step="1" value="14" />
          </div>

          <div id="rangeBlock" class="inline hidden">
            <div class="field">
              <label>Date From</label>
              <input type="date" id="dateFromInput" />
            </div>
            <div class="field">
              <label>Date To</label>
              <input type="date" id="dateToInput" />
            </div>
          </div>

          <div class="field">
            <label>District</label>
            <select id="districtSelect"></select>
          </div>

          <div class="field">
            <label>Area</label>
            <select id="urbanSelect"></select>
          </div>

          <div class="field search-box">
            <label>Street Search</label>
            <div class="action-row">
              <input type="text" id="streetQuery" placeholder="Type street name and click search..." />
              <button type="button" class="btn btn-secondary" id="searchStreetBtn">Search</button>
            </div>
            <div id="streetSearchResults" class="result-list"></div>
          </div>

          <div class="field">
            <label>Selected Streets</label>
            <div id="selectedStreets" class="selected-list"></div>
          </div>

          <div class="action-row action-row-main">
            <button type="button" class="btn btn-primary" id="runBtn">Export And Build</button>
            <button type="button" class="btn btn-ghost hidden" id="stopBtn">Stop Current Run</button>
            <button type="button" class="btn btn-secondary" id="refreshCatalogBtn">Refresh Catalog</button>
          </div>
          <div class="foot">
            Full mode: the UI keeps scanning pages until the selected period is covered or listings end. Large districts can take longer.
          </div>
        </div>
      </div>

      <div class="panel">
        <h3>Results</h3>
        <div id="latestResult" class="empty">No build started yet.</div>
        <div style="height:18px;"></div>
        <h3>Recent Runs</h3>
        <div id="recentRuns" class="recent-list"></div>
      </div>
    </section>
  </div>

  <script>
    const state = {
      mode: "days",
      catalog: { districts: [], urbans: [] },
      selectedStreetMatches: [],
      lastStreetResults: [],
      currentJobId: null,
      pollTimer: null,
    };

    const $ = (id) => document.getElementById(id);
    const todayIso = new Date().toISOString().slice(0, 10);
    $("dateToInput").value = todayIso;

    function esc(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    function setMode(mode) {
      state.mode = mode;
      $("modeDays").classList.toggle("active", mode === "days");
      $("modeRange").classList.toggle("active", mode === "range");
      $("daysBlock").classList.toggle("hidden", mode !== "days");
      $("rangeBlock").classList.toggle("hidden", mode !== "range");
    }

    function selectedDistrictId() {
      const value = $("districtSelect").value || "";
      return value ? Number(value) : null;
    }

    function selectedUrbanId() {
      const value = $("urbanSelect").value || "";
      return value ? Number(value) : null;
    }

    function visibleUrbans() {
      const districtId = selectedDistrictId();
      return (state.catalog.urbans || []).filter((item) => !districtId || item.district_id === districtId);
    }

    function renderDistricts() {
      const items = state.catalog.districts || [];
      const options = ['<option value="">All districts</option>']
        .concat(items.map((item) => `<option value="${item.id}">${esc(item.name)}</option>`));
      $("districtSelect").innerHTML = options.join("");
    }

    function renderUrbans() {
      const current = $("urbanSelect").value || "";
      const items = visibleUrbans();
      const options = ['<option value="">All areas</option>']
        .concat(items.map((item) => `<option value="${item.id}">${esc(item.name)}</option>`));
      $("urbanSelect").innerHTML = options.join("");
      const exists = items.some((item) => String(item.id) === current);
      $("urbanSelect").value = exists ? current : "";
    }

    function streetAlreadySelected(id) {
      return state.selectedStreetMatches.some((item) => item.id === id);
    }

    function renderSelectedStreets() {
      const wrap = $("selectedStreets");
      if (!state.selectedStreetMatches.length) {
        wrap.innerHTML = '<div class="empty">All streets in the selected district/area.</div>';
        return;
      }
      wrap.innerHTML = state.selectedStreetMatches.map((item) => `
        <div class="selected-chip">
          <div>
            <strong>${esc(item.display_name)}</strong>
            <small>${esc(item.urban_name)} | ${esc(item.district_name)} | id=${item.id}</small>
          </div>
          <button type="button" data-remove-street="${item.id}">Remove</button>
        </div>
      `).join("");
    }

    function renderStreetResults(items) {
      state.lastStreetResults = items.slice();
      const wrap = $("streetSearchResults");
      if (!items.length) {
        wrap.innerHTML = '<div class="empty">No matches.</div>';
        return;
      }
      wrap.innerHTML = items.map((item) => `
        <div class="street-option" data-add-street="${item.id}">
          <strong>${esc(item.display_name)}</strong>
          <small>${esc(item.urban_name)} | ${esc(item.district_name)} | street id=${item.id}</small>
        </div>
      `).join("");
    }

    function renderRecentRuns(items) {
      const wrap = $("recentRuns");
      if (!items.length) {
        wrap.innerHTML = '<div class="empty">No recent exports yet.</div>';
        return;
      }
      wrap.innerHTML = items.map((item) => `
        <div class="recent-card">
          <div>
            <strong>${esc(item.name)}</strong><br />
            <small>${esc(item.modified_local)} | ${esc(item.size_label)}</small>
          </div>
          <div class="recent-links">
            <a href="${esc(item.url)}" target="_blank">Open HTML</a>
          </div>
        </div>
      `).join("");
    }

    function setStatus(title, body, foot = "") {
      $("jobTitle").textContent = title;
      $("jobBanner").innerHTML = body;
      $("jobFoot").textContent = foot;
    }

    function setStopBusy(isBusy) {
      $("stopBtn").disabled = isBusy;
      $("stopBtn").textContent = isBusy ? "Stopping..." : "Stop Current Run";
    }

    function setRunBusy(isBusy) {
      $("runBtn").disabled = isBusy;
      $("runBtn").textContent = isBusy ? "Running..." : "Export And Build";
      $("stopBtn").classList.toggle("hidden", !isBusy);
      if (!isBusy) {
        setStopBusy(false);
      }
    }

    function renderLatestResult(summary) {
      const links = [];
      if (summary.html_url) links.push(`<a href="${esc(summary.html_url)}" target="_blank">Open HTML</a>`);
      if (summary.csv_url) links.push(`<a href="${esc(summary.csv_url)}" target="_blank">Open CSV</a>`);
      if (summary.json_url) links.push(`<a href="${esc(summary.json_url)}" target="_blank">Open JSON</a>`);
      $("latestResult").innerHTML = `
        <div class="recent-card">
          <div>
            <strong>Build finished</strong><br />
            <small>Rows exported: ${esc(summary.rows_exported)} | Period: ${esc(summary.period_start_local)} to ${esc(summary.period_end_local)}</small>
          </div>
          <div class="recent-links">${links.join("")}</div>
          <div class="mono">${esc(JSON.stringify(summary.source_filters || {}, null, 2))}</div>
        </div>
      `;
    }

    function renderProgressStats(progress) {
      const cards = [];
      if (progress.phase) {
        cards.push(`<div class="status-stat"><small>Phase</small><strong>${esc(progress.phase)}</strong></div>`);
      }
      const pageValue = progress.pages_scanned ?? progress.page;
      if (pageValue != null) {
        cards.push(`<div class="status-stat"><small>Pages</small><strong>${esc(pageValue)}</strong></div>`);
      }
      if (progress.collected != null) {
        cards.push(`<div class="status-stat"><small>Collected</small><strong>${esc(progress.collected)}</strong></div>`);
      }
      if (progress.exported != null) {
        cards.push(`<div class="status-stat"><small>In range</small><strong>${esc(progress.exported)}</strong></div>`);
      }
      if (!cards.length) {
        return "";
      }
      return `<div class="status-stats">${cards.join("")}</div>`;
    }

    function renderRunningState(payload) {
      const progress = payload.progress || {};
      const lead = payload.cancel_requested
        ? '<span class="warning">Stopping current run...</span>'
        : '<span class="success">Working...</span>';
      const body = `
        <div>${lead}</div>
        <div>${esc(payload.message || "Exporting active listings and building HTML.")}</div>
        ${renderProgressStats(progress)}
      `;
      setStatus(
        payload.cancel_requested ? "Stopping" : "Running",
        body,
        payload.cancel_requested ? "Waiting for the exporter process to stop." : "The page updates automatically."
      );
    }

    async function loadCatalog(refresh = false) {
      const url = refresh ? "/api/catalog?refresh=1" : "/api/catalog";
      const response = await fetch(url);
      const payload = await response.json();
      state.catalog = payload;
      renderDistricts();
      renderUrbans();
    }

    async function loadRecentRuns() {
      const response = await fetch("/api/recent");
      const payload = await response.json();
      renderRecentRuns(payload.items || []);
    }

    async function searchStreets() {
      const q = $("streetQuery").value.trim();
      if (!q) {
        $("streetSearchResults").innerHTML = '<div class="empty">Type a street query first.</div>';
        return;
      }
      const params = new URLSearchParams({ q });
      if (selectedDistrictId()) params.set("district_id", String(selectedDistrictId()));
      if (selectedUrbanId()) params.set("urban_id", String(selectedUrbanId()));
      const response = await fetch(`/api/streets?${params.toString()}`);
      const payload = await response.json();
      renderStreetResults(payload.items || []);
    }

    function buildRunPayload() {
      const payload = {
        mode: state.mode,
        days: Number($("daysInput").value || 14),
        date_from: $("dateFromInput").value || null,
        date_to: $("dateToInput").value || null,
        district_id: selectedDistrictId(),
        urban_id: selectedUrbanId(),
        street_ids: state.selectedStreetMatches.map((item) => item.id),
        build_site: true,
      };
      return payload;
    }

    async function resumeCurrentJob() {
      const response = await fetch("/api/jobs/current");
      const payload = await response.json();
      if (!payload.job) {
        return;
      }
      state.currentJobId = payload.job.id;
      setRunBusy(true);
      renderRunningState(payload.job);
      if (state.pollTimer) clearInterval(state.pollTimer);
      state.pollTimer = setInterval(pollJob, 1500);
      await pollJob();
    }

    async function startJob() {
      const response = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(buildRunPayload()),
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Failed to start job");
      }
      state.currentJobId = payload.job_id;
      setRunBusy(true);
      setStopBusy(false);
      setStatus("Running", "Build started. Export is in progress...", "The page will update automatically.");
      if (state.pollTimer) clearInterval(state.pollTimer);
      state.pollTimer = setInterval(pollJob, 1500);
      await pollJob();
    }

    async function cancelCurrentJob() {
      if (!state.currentJobId) return;
      setStopBusy(true);
      const response = await fetch(`/api/jobs/${state.currentJobId}/cancel`, {
        method: "POST",
      });
      const payload = await response.json();
      if (!response.ok) {
        setStopBusy(false);
        throw new Error(payload.error || "Failed to stop job");
      }
      renderRunningState({
        status: "running",
        cancel_requested: true,
        message: payload.message || "Stopping current export...",
        progress: payload.job?.progress || {},
      });
    }

    async function pollJob() {
      if (!state.currentJobId) return;
      const response = await fetch(`/api/jobs/${state.currentJobId}`);
      const payload = await response.json();
      if (payload.status === "running") {
        setRunBusy(true);
        renderRunningState(payload);
        return;
      }
      if (state.pollTimer) {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
      }
      state.currentJobId = null;
      if (payload.status === "done") {
        setRunBusy(false);
        setStatus("Finished", '<span class="success">Export and HTML build completed.</span>', 'Use the links below or in recent runs.');
        renderLatestResult(payload.summary);
        await loadRecentRuns();
        return;
      }
      if (payload.status === "cancelled") {
        setRunBusy(false);
        setStatus("Stopped", '<span class="warning">Current export was stopped.</span>', 'You can adjust filters and run again.');
        return;
      }
      setRunBusy(false);
      setStatus("Failed", `<span class="warning">${esc(payload.error || "Build failed")}</span>`, 'Fix the filters and try again.');
    }

    document.addEventListener("click", async (event) => {
      const addStreet = event.target.closest("[data-add-street]");
      if (addStreet) {
        const streetId = Number(addStreet.getAttribute("data-add-street"));
        if (!streetAlreadySelected(streetId)) {
          const item = state.lastStreetResults.find((row) => row.id === streetId);
          if (item) {
            state.selectedStreetMatches.push(item);
            renderSelectedStreets();
          }
        }
      }

      const removeStreet = event.target.closest("[data-remove-street]");
      if (removeStreet) {
        const streetId = Number(removeStreet.getAttribute("data-remove-street"));
        state.selectedStreetMatches = state.selectedStreetMatches.filter((item) => item.id !== streetId);
        renderSelectedStreets();
      }
    });

    $("modeDays").addEventListener("click", () => setMode("days"));
    $("modeRange").addEventListener("click", () => setMode("range"));
    $("districtSelect").addEventListener("change", () => {
      renderUrbans();
      state.selectedStreetMatches = state.selectedStreetMatches.filter((item) => {
        const districtId = selectedDistrictId();
        return !districtId || item.district_id === districtId;
      });
      renderSelectedStreets();
    });
    $("urbanSelect").addEventListener("change", () => {
      state.selectedStreetMatches = state.selectedStreetMatches.filter((item) => {
        const urbanId = selectedUrbanId();
        return !urbanId || item.urban_id === urbanId;
      });
      renderSelectedStreets();
    });
    $("searchStreetBtn").addEventListener("click", searchStreets);
    $("streetQuery").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        searchStreets();
      }
    });
    $("refreshCatalogBtn").addEventListener("click", async () => {
      setStatus("Refreshing", "Updating district and area catalog from API...");
      await loadCatalog(true);
      setStatus("Idle", "Catalog updated. Ready for a new build.");
    });
    $("stopBtn").addEventListener("click", async () => {
      try {
        await cancelCurrentJob();
      } catch (error) {
        setStopBusy(false);
        setStatus("Failed", `<span class="warning">${esc(error.message || String(error))}</span>`);
      }
    });
    $("runBtn").addEventListener("click", async () => {
      try {
        await startJob();
      } catch (error) {
        setRunBusy(false);
        setStatus("Failed", `<span class="warning">${esc(error.message || String(error))}</span>`);
      }
    });

    async function boot() {
      setRunBusy(false);
      renderSelectedStreets();
      await loadCatalog(false);
      await loadRecentRuns();
      await resumeCurrentJob();
    }

    boot();
  </script>
</body>
</html>
"""


class JobStore:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.jobs: dict[str, dict[str, Any]] = {}

    def create(self, payload: dict[str, Any]) -> str:
        job_id = uuid.uuid4().hex[:10]
        with self.lock:
            self.jobs[job_id] = {
                "id": job_id,
                "status": "running",
                "payload": payload,
                "message": "Queued",
                "summary": None,
                "error": None,
                "progress": {
                    "phase": "queued",
                    "page": 0,
                    "pages_scanned": 0,
                    "collected": 0,
                    "exported": 0,
                },
                "cancel_requested": False,
                "process": None,
            }
        return job_id

    def update(self, job_id: str, **changes: Any) -> None:
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id].update(changes)

    def attach_process(self, job_id: str, process: subprocess.Popen[str]) -> None:
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id]["process"] = process

    def clear_process(self, job_id: str) -> None:
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id]["process"] = None

    def _public_job(self, job: dict[str, Any]) -> dict[str, Any]:
        payload = {key: value for key, value in job.items() if key != "process"}
        if isinstance(payload.get("progress"), dict):
            payload["progress"] = dict(payload["progress"])
        if isinstance(payload.get("summary"), dict):
            payload["summary"] = dict(payload["summary"])
        if isinstance(payload.get("payload"), dict):
            payload["payload"] = dict(payload["payload"])
        return payload

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self.lock:
            job = self.jobs.get(job_id)
            return self._public_job(job) if job else None

    def current_running(self) -> dict[str, Any] | None:
        with self.lock:
            for job in self.jobs.values():
                if job.get("status") == "running":
                    return self._public_job(job)
            return None

    def has_running(self) -> bool:
        with self.lock:
            return any(job.get("status") == "running" for job in self.jobs.values())

    def cancel(self, job_id: str) -> tuple[bool, str, dict[str, Any] | None]:
        process: subprocess.Popen[str] | None = None
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return False, "Job not found", None
            if job.get("status") != "running":
                return False, "Job is not running", self._public_job(job)
            job["cancel_requested"] = True
            job["message"] = "Stopping current export..."
            process = job.get("process")
            payload = self._public_job(job)
        if process and process.poll() is None:
            def terminate_process(proc: subprocess.Popen[str]) -> None:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        return
                except ProcessLookupError:
                    return

            threading.Thread(target=terminate_process, args=(process,), daemon=True).start()
        return True, "Stopping current export...", payload


JOB_STORE = JobStore()


def make_catalog_args(refresh: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        city_id=1,
        deal_types="1,2",
        real_estate_types="1",
        statuses="2",
        currency_id="1",
        catalog_cache=export_mod.DEFAULT_CATALOG_CACHE,
        refresh_catalog=refresh,
    )


def load_catalog(refresh: bool = False) -> dict[str, Any]:
    return export_mod.build_location_catalog(make_catalog_args(refresh=refresh), force_refresh=refresh)


def search_streets(query: str, district_id: int | None, urban_id: int | None) -> list[dict[str, Any]]:
    session = export_mod.requests.Session()
    session.headers.update(export_mod.DEFAULT_HEADERS)
    try:
        items = export_mod.fetch_street_matches(session, 1, query)
    finally:
        session.close()

    if district_id:
        items = [item for item in items if int(item.get("district_id") or 0) == district_id]
    if urban_id:
        items = [item for item in items if int(item.get("urban_id") or 0) == urban_id]
    return items[:20]


def file_url_from_path(path_str: str | None) -> str | None:
    if not path_str:
        return None
    path = Path(path_str)
    if path.exists() and path.parent == EXPORTS_DIR:
        return f"/exports/{path.name}"
    return None


def recent_html_items() -> list[dict[str, Any]]:
    files = sorted(EXPORTS_DIR.glob("street_analytics_site_*.html"), key=lambda p: p.stat().st_mtime, reverse=True)[:12]
    items: list[dict[str, Any]] = []
    for path in files:
        stat = path.stat()
        items.append(
            {
                "name": path.name,
                "url": f"/exports/{path.name}",
                "modified_local": export_mod.dt.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "size_label": f"{stat.st_size / 1024:.1f} KB",
            }
        )
    return items


def build_command(payload: dict[str, Any]) -> list[str]:
    cmd = [
        sys.executable,
        "export_myhome_active.py",
        "--output-dir",
        str(EXPORTS_DIR),
        "--build-site",
        "--per-page",
        "100",
        "--max-pages",
        "2000",
    ]

    mode = payload.get("mode")
    if mode == "range":
        date_from = payload.get("date_from")
        date_to = payload.get("date_to")
        if date_from:
            cmd.extend(["--date-from", str(date_from)])
        if date_to:
            cmd.extend(["--date-to", str(date_to)])
    else:
        days = int(payload.get("days") or 30)
        cmd.extend(["--days", str(days)])
        date_to = payload.get("date_to")
        if date_to:
            cmd.extend(["--date-to", str(date_to)])

    district_id = payload.get("district_id")
    if district_id:
        cmd.extend(["--district-id", str(district_id)])
    urban_id = payload.get("urban_id")
    if urban_id:
        cmd.extend(["--urban-id", str(urban_id)])
    for street_id in payload.get("street_ids", []):
        cmd.extend(["--street-id", str(street_id)])
    return cmd


def handle_progress_line(job_id: str, line: str) -> bool:
    prefix = export_mod.PROGRESS_PREFIX
    if not line.startswith(prefix):
        return False
    try:
        payload = json.loads(line[len(prefix) :].strip())
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    JOB_STORE.update(job_id, message=payload.get("message") or "Working...", progress=payload)
    return True


def consume_stdout(pipe: Any, sink: list[str]) -> None:
    try:
        for line in iter(pipe.readline, ""):
            sink.append(line)
    finally:
        pipe.close()


def consume_stderr(job_id: str, pipe: Any, sink: list[str]) -> None:
    try:
        for raw_line in iter(pipe.readline, ""):
            line = raw_line.rstrip("\n")
            if handle_progress_line(job_id, line):
                continue
            if line.strip():
                sink.append(line)
                JOB_STORE.update(job_id, message=line.strip())
    finally:
        pipe.close()


def run_job(job_id: str, payload: dict[str, Any]) -> None:
    JOB_STORE.update(job_id, message="Starting export...")
    cmd = build_command(payload)
    try:
        process = subprocess.Popen(
            cmd,
            cwd=ROOT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except Exception as exc:  # noqa: BLE001
        JOB_STORE.update(job_id, status="failed", error=str(exc))
        return

    JOB_STORE.attach_process(job_id, process)
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    stdout_thread = threading.Thread(target=consume_stdout, args=(process.stdout, stdout_lines), daemon=True)
    stderr_thread = threading.Thread(target=consume_stderr, args=(job_id, process.stderr, stderr_lines), daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    return_code = process.wait()
    stdout_thread.join()
    stderr_thread.join()
    JOB_STORE.clear_process(job_id)

    job = JOB_STORE.get(job_id) or {}
    stdout_text = "".join(stdout_lines).strip()
    stderr_text = "\n".join(stderr_lines).strip()

    if return_code != 0:
        if job.get("cancel_requested"):
            JOB_STORE.update(job_id, status="cancelled", error=None, message="Export stopped by user")
            return
        message = stderr_text or stdout_text or "Unknown error"
        JOB_STORE.update(job_id, status="failed", error=message)
        return

    try:
        summary = json.loads(stdout_text)
    except json.JSONDecodeError:
        JOB_STORE.update(job_id, status="failed", error=stdout_text or "Failed to parse export summary")
        return

    summary["html_url"] = file_url_from_path(summary.get("html_path"))
    summary["csv_url"] = file_url_from_path(summary.get("csv_path"))
    summary["json_url"] = file_url_from_path(summary.get("json_path"))
    JOB_STORE.update(
        job_id,
        status="done",
        summary=summary,
        message="Completed",
        progress={
            "phase": "completed",
            "pages_scanned": summary.get("pages_scanned"),
            "collected": summary.get("rows_before_period_filter"),
            "exported": summary.get("rows_exported"),
        },
    )


class AppHandler(BaseHTTPRequestHandler):
    server_version = "MyhomeWebUI/1.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.respond_html(HTML_PAGE)
            return
        if parsed.path == "/api/catalog":
            query = parse_qs(parsed.query)
            refresh = query.get("refresh", ["0"])[0] == "1"
            try:
                payload = load_catalog(refresh=refresh)
                self.respond_json(payload)
            except Exception as exc:  # noqa: BLE001
                self.respond_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/streets":
            query = parse_qs(parsed.query)
            q = (query.get("q", [""])[0] or "").strip()
            district_id = int(query.get("district_id", ["0"])[0] or 0) or None
            urban_id = int(query.get("urban_id", ["0"])[0] or 0) or None
            if not q:
                self.respond_json({"items": []})
                return
            try:
                items = search_streets(q, district_id=district_id, urban_id=urban_id)
                self.respond_json({"items": items})
            except Exception as exc:  # noqa: BLE001
                self.respond_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/recent":
            self.respond_json({"items": recent_html_items()})
            return
        if parsed.path == "/api/jobs/current":
            self.respond_json({"job": JOB_STORE.current_running()})
            return
        if parsed.path.startswith("/api/jobs/"):
            job_id = parsed.path.rsplit("/", 1)[-1]
            payload = JOB_STORE.get(job_id)
            if not payload:
                self.respond_json({"error": "Job not found"}, status=HTTPStatus.NOT_FOUND)
                return
            self.respond_json(payload)
            return
        if parsed.path.startswith("/exports/"):
            name = Path(parsed.path).name
            path = EXPORTS_DIR / name
            if not path.exists() or path.parent != EXPORTS_DIR:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self.respond_file(path)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/run":
            body = self.read_json()
            if body is None:
                self.respond_json({"error": "Invalid JSON body"}, status=HTTPStatus.BAD_REQUEST)
                return

            if JOB_STORE.has_running():
                self.respond_json(
                    {"error": "Another export is already running. Wait until it finishes."},
                    status=HTTPStatus.CONFLICT,
                )
                return

            job_id = JOB_STORE.create(body)
            thread = threading.Thread(target=run_job, args=(job_id, body), daemon=True)
            thread.start()
            self.respond_json({"job_id": job_id}, status=HTTPStatus.ACCEPTED)
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/cancel"):
            job_id = parsed.path.split("/")[-2]
            ok, message, job = JOB_STORE.cancel(job_id)
            if not ok:
                self.respond_json({"error": message, "job": job}, status=HTTPStatus.CONFLICT)
                return
            self.respond_json({"message": message, "job": job}, status=HTTPStatus.ACCEPTED)
            return

        if parsed.path != "/api/run":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

    def read_json(self) -> dict[str, Any] | None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def respond_html(self, body: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def respond_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def respond_file(self, path: Path) -> None:
        data = path.read_bytes()
        suffix = path.suffix.lower()
        content_type = {
            ".html": "text/html; charset=utf-8",
            ".csv": "text/csv; charset=utf-8",
            ".json": "application/json; charset=utf-8",
        }.get(suffix, "application/octet-stream")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def main() -> int:
    args = parse_args()
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    url = f"http://{args.host}:{args.port}"
    print(f"Myhome web UI running at {url}")
    if args.open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
