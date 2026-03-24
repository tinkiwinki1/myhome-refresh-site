#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import threading
import uuid
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

import export_myhome_active as export_mod

ROOT_DIR = Path(__file__).resolve().parent


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


DATA_DIR = Path(os.environ.get("MYHOME_DATA_DIR") or (ROOT_DIR / "render_data"))
DB_PATH = DATA_DIR / "myhome_render.sqlite3"
CATALOG_CACHE_PATH = DATA_DIR / "myhome_location_catalog.json"
DEFAULT_HOST = os.environ.get("MYHOME_HOST", "0.0.0.0")
DEFAULT_PORT = env_int("PORT", env_int("MYHOME_PORT", 10000))
DEFAULT_CITY_ID = env_int("MYHOME_CITY_ID", 1)
DEFAULT_PER_PAGE = env_int("MYHOME_PER_PAGE", 100)
DEFAULT_MAX_PAGES = env_int("MYHOME_MAX_PAGES", 12)
MAX_RETURN_ITEMS = env_int("MYHOME_MAX_RETURN_ITEMS", 120)

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS scope_seen (
    scope_key TEXT NOT NULL,
    listing_id INTEGER NOT NULL,
    first_seen_utc TEXT NOT NULL,
    last_updated TEXT,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (scope_key, listing_id)
);

CREATE TABLE IF NOT EXISTS refresh_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    scope_label TEXT NOT NULL,
    filters_json TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    pages_scanned INTEGER NOT NULL DEFAULT 0,
    rows_scanned INTEGER NOT NULL DEFAULT 0,
    new_count INTEGER NOT NULL DEFAULT 0,
    baseline_mode INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    summary_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_refresh_runs_started_at
ON refresh_runs(started_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_refresh_runs_scope_started_at
ON refresh_runs(scope_key, started_at_utc DESC);
"""

HTML_PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Myhome Refresh Tracker</title>
  <style>
    :root {
      --bg: #efe7da;
      --bg-2: #d9d0bf;
      --panel: rgba(255, 252, 247, 0.92);
      --panel-dark: rgba(26, 37, 41, 0.94);
      --line: rgba(110, 89, 60, 0.22);
      --ink: #182126;
      --muted: #645a4d;
      --accent: #9c4028;
      --accent-2: #2d6a61;
      --accent-3: #d4a85c;
      --ok: #1e6a51;
      --warn: #b35d2f;
      --shadow: 0 22px 50px rgba(55, 37, 19, 0.14);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(212, 149, 96, 0.24), transparent 30%),
        radial-gradient(circle at bottom right, rgba(45, 106, 97, 0.18), transparent 26%),
        linear-gradient(180deg, var(--bg) 0%, var(--bg-2) 100%);
    }
    a { color: inherit; }
    .shell {
      max-width: 1540px;
      margin: 0 auto;
      padding: 18px;
    }
    .hero {
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 18px;
      align-items: stretch;
    }
    .card {
      border-radius: 28px;
      border: 1px solid rgba(122, 98, 68, 0.16);
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
    }
    .hero-main {
      padding: 28px;
      position: relative;
      overflow: hidden;
    }
    .hero-main::after {
      content: "";
      position: absolute;
      top: -48px;
      right: -42px;
      width: 196px;
      height: 196px;
      border-radius: 999px;
      background: radial-gradient(circle, rgba(156, 64, 40, 0.2), transparent 68%);
    }
    .eyebrow {
      color: var(--accent);
      font-size: 0.74rem;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      font-weight: 800;
    }
    h1 {
      margin: 12px 0 10px;
      max-width: 11ch;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-size: clamp(2.1rem, 4vw, 3.7rem);
      line-height: 0.95;
    }
    .lead {
      max-width: 64ch;
      color: var(--muted);
      line-height: 1.55;
      font-size: 1rem;
    }
    .hero-tags {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      border: 1px solid rgba(100, 90, 77, 0.18);
      background: rgba(255,255,255,0.54);
      padding: 10px 13px;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .hero-status {
      padding: 24px;
      color: #f8f1e8;
      background:
        linear-gradient(180deg, rgba(24,33,38,0.98), rgba(31,46,50,0.95)),
        var(--panel-dark);
    }
    .hero-status h2 {
      margin: 10px 0 8px;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-size: 1.7rem;
    }
    .status-box {
      margin-top: 12px;
      min-height: 86px;
      padding: 16px;
      border-radius: 18px;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.08);
      line-height: 1.5;
      color: rgba(248, 241, 232, 0.88);
    }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }
    .status-stat {
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.1);
      background: rgba(255,255,255,0.08);
      padding: 11px 12px;
    }
    .status-stat small {
      display: block;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: rgba(248, 241, 232, 0.7);
      font-size: 0.72rem;
    }
    .status-stat strong {
      display: block;
      margin-top: 4px;
      color: #fff8ef;
      font-size: 1rem;
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
      margin: 0 0 14px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 0.9rem;
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
      font-size: 0.84rem;
      font-weight: 800;
      color: var(--muted);
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    input, select, button {
      font: inherit;
    }
    input[type="text"],
    select {
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.82);
      color: var(--ink);
      padding: 12px 13px;
    }
    .action-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: end;
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
      opacity: 0.7;
      cursor: default;
      transform: none;
    }
    .btn-primary {
      background: linear-gradient(135deg, var(--accent), #c55d34);
      color: #fff8ef;
      box-shadow: 0 12px 24px rgba(156, 64, 40, 0.22);
    }
    .btn-secondary {
      background: rgba(255,255,255,0.72);
      color: var(--ink);
      border-color: var(--line);
    }
    .selected-list,
    .result-list,
    .history-list,
    .listing-grid {
      display: grid;
      gap: 10px;
    }
    .selected-chip,
    .street-option,
    .history-card,
    .listing-card,
    .summary-card {
      border-radius: 18px;
      border: 1px solid rgba(118, 94, 52, 0.14);
      background: rgba(255,255,255,0.72);
    }
    .selected-chip {
      padding: 10px 12px;
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: start;
    }
    .selected-chip strong,
    .street-option strong,
    .listing-card strong,
    .history-card strong {
      display: block;
      font-size: 0.98rem;
    }
    .selected-chip small,
    .street-option small,
    .listing-card small,
    .history-card small,
    .summary-card small,
    .note {
      color: var(--muted);
      line-height: 1.45;
    }
    .selected-chip button {
      border: 0;
      background: transparent;
      color: var(--accent);
      font-weight: 800;
      cursor: pointer;
      padding: 0;
    }
    .street-option {
      padding: 12px 13px;
      cursor: pointer;
    }
    .street-option.is-added {
      border-color: rgba(45, 106, 97, 0.34);
      background: rgba(229, 245, 240, 0.88);
    }
    .summary-card {
      padding: 18px;
      display: grid;
      gap: 16px;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
    }
    .summary-stat {
      padding: 12px;
      border-radius: 16px;
      background: rgba(255,255,255,0.64);
      border: 1px solid rgba(118, 94, 52, 0.12);
    }
    .summary-stat small {
      display: block;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-size: 0.72rem;
    }
    .summary-stat strong {
      display: block;
      margin-top: 4px;
      font-size: 1.12rem;
    }
    .listing-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
      margin-top: 14px;
    }
    .listing-card {
      padding: 16px;
      display: grid;
      gap: 10px;
    }
    .listing-top {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }
    .listing-badge {
      white-space: nowrap;
      padding: 8px 10px;
      border-radius: 999px;
      font-size: 0.8rem;
      font-weight: 800;
      background: rgba(45, 106, 97, 0.12);
      color: var(--accent-2);
    }
    .listing-price {
      font-size: 1.3rem;
      font-weight: 900;
      color: var(--ink);
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
    }
    .listing-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .listing-meta span {
      padding: 7px 9px;
      border-radius: 999px;
      background: rgba(24,33,38,0.06);
      color: var(--muted);
      font-size: 0.84rem;
    }
    .listing-link {
      display: inline-flex;
      width: fit-content;
      text-decoration: none;
      font-weight: 800;
      color: var(--accent);
    }
    .history-card {
      padding: 14px 15px;
      display: grid;
      gap: 8px;
    }
    .status-ok { color: #daf4e8; }
    .status-warn { color: #ffd9c8; }
    .muted { color: var(--muted); }
    .empty {
      padding: 18px;
      border-radius: 18px;
      border: 1px dashed rgba(101,91,79,0.28);
      background: rgba(255,255,255,0.35);
      color: var(--muted);
      text-align: center;
    }
    .foot {
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.88rem;
    }
    .hidden { display: none; }
    @media (max-width: 1120px) {
      .hero, .layout { grid-template-columns: 1fr; }
      .summary-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .listing-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 760px) {
      .shell { padding: 14px; }
      .hero-main, .hero-status, .panel { border-radius: 22px; }
      .status-grid, .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .action-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="card hero-main">
        <div class="eyebrow">Сайт для Render</div>
        <h1>Новые объявления Myhome</h1>
        <div class="lead">
          Сайт хранит уже просмотренные объявления для выбранной локации и при каждом нажатии кнопки
          <strong>«Обновить объявления»</strong> показывает только новые объекты, которые появились после
          предыдущей проверки этого же набора фильтров.
        </div>
        <div class="hero-tags">
          <div class="pill">Район + area + улицы</div>
          <div class="pill">Кнопка ручного обновления</div>
          <div class="pill">История проверок</div>
        </div>
      </div>
      <div class="card hero-status">
        <div class="eyebrow" style="color:#d4a85c;">Статус</div>
        <h2 id="jobTitle">Готов</h2>
        <div id="jobBanner" class="status-box">
          Сервис запущен. Выберите локацию и нажмите «Обновить объявления».
        </div>
        <div id="jobStats" class="status-grid"></div>
        <div class="foot" id="jobFoot">
          По умолчанию сервис проверяет `__DEFAULT_MAX_PAGES__` страниц по `__DEFAULT_PER_PAGE__` объявлений.
        </div>
      </div>
    </section>

    <section class="layout">
      <div class="card panel">
        <h3>Фильтры</h3>
        <div class="stack">
          <div class="field">
            <label>Район</label>
            <select id="districtSelect"></select>
          </div>

          <div class="field">
            <label>Area</label>
            <select id="urbanSelect"></select>
          </div>

          <div class="field">
            <label>Поиск улицы</label>
            <div class="action-row">
              <input type="text" id="streetQuery" placeholder="Введите улицу и нажмите Поиск" />
              <button type="button" class="btn btn-secondary" id="searchStreetBtn">Поиск</button>
            </div>
            <div id="streetResults" class="result-list"></div>
          </div>

          <div class="field">
            <label>Выбранные улицы</label>
            <div id="selectedStreets" class="selected-list"></div>
          </div>

          <div class="action-row">
            <button type="button" class="btn btn-primary" id="refreshBtn">Обновить объявления</button>
            <button type="button" class="btn btn-secondary" id="refreshCatalogBtn">Обновить каталог</button>
          </div>

          <div class="foot">
            Если улицы не выбраны, сервис отслеживает все улицы внутри выбранного района или area.
          </div>
        </div>
      </div>

      <div class="card panel">
        <h3>Последнее обновление</h3>
        <div id="latestSummary" class="empty">Пока нет результатов. Запустите первую проверку.</div>
        <div style="height:18px;"></div>
        <h3>Новые объявления</h3>
        <div id="latestListings" class="listing-grid"></div>
        <div id="listingsEmpty" class="empty">После обновления новые объявления появятся здесь.</div>
        <div style="height:18px;"></div>
        <h3>История</h3>
        <div id="historyList" class="history-list"></div>
      </div>
    </section>
  </div>

  <script>
    const state = {
      catalog: { districts: [], urbans: [] },
      selectedStreetMatches: [],
      lastStreetResults: [],
      currentJobId: null,
      pollTimer: null,
    };

    const DEFAULT_MAX_PAGES = Number("__DEFAULT_MAX_PAGES__");
    const DEFAULT_PER_PAGE = Number("__DEFAULT_PER_PAGE__");
    const $ = (id) => document.getElementById(id);

    function esc(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    function formatCount(value) {
      return Number(value || 0).toLocaleString("ru-RU");
    }

    function priceLabel(item) {
      if (item.price_usd_total != null) {
        return `$${Number(item.price_usd_total).toLocaleString("en-US")}`;
      }
      if (item.price_gel_total != null) {
        return `${Number(item.price_gel_total).toLocaleString("ru-RU")} GEL`;
      }
      if (item.price_eur_total != null) {
        return `€${Number(item.price_eur_total).toLocaleString("ru-RU")}`;
      }
      return "Цена не указана";
    }

    function detailTags(item) {
      const tags = [];
      if (item.room != null) tags.push(`${esc(item.room)} комн.`);
      if (item.bedroom != null) tags.push(`${esc(item.bedroom)} спальни`);
      if (item.area != null) tags.push(`${esc(item.area)} м²`);
      if (item.floor != null && item.total_floors != null) {
        tags.push(`${esc(item.floor)}/${esc(item.total_floors)} этаж`);
      }
      return tags;
    }

    function selectedDistrictId() {
      const value = $("districtSelect").value || "";
      return value ? Number(value) : null;
    }

    function selectedUrbanId() {
      const value = $("urbanSelect").value || "";
      return value ? Number(value) : null;
    }

    function selectedDistrictName() {
      const item = (state.catalog.districts || []).find((row) => row.id === selectedDistrictId());
      return item ? item.name : null;
    }

    function selectedUrbanName() {
      const item = (state.catalog.urbans || []).find((row) => row.id === selectedUrbanId());
      return item ? item.name : null;
    }

    function visibleUrbans() {
      const districtId = selectedDistrictId();
      return (state.catalog.urbans || []).filter((item) => !districtId || item.district_id === districtId);
    }

    function renderDistricts() {
      const items = state.catalog.districts || [];
      const options = ['<option value="">Все районы</option>']
        .concat(items.map((item) => `<option value="${item.id}">${esc(item.name)}</option>`));
      $("districtSelect").innerHTML = options.join("");
    }

    function renderUrbans() {
      const current = $("urbanSelect").value || "";
      const items = visibleUrbans();
      const options = ['<option value="">Все area</option>']
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
        wrap.innerHTML = '<div class="empty">Все улицы в текущей локации.</div>';
        return;
      }
      wrap.innerHTML = state.selectedStreetMatches.map((item) => `
        <div class="selected-chip">
          <div>
            <strong>${esc(item.display_name)}</strong>
            <small>${esc(item.urban_name)} | ${esc(item.district_name)} | id=${item.id}</small>
          </div>
          <button type="button" data-remove-street="${item.id}">Удалить</button>
        </div>
      `).join("");
    }

    function renderStreetResults(items) {
      state.lastStreetResults = items.slice();
      const wrap = $("streetResults");
      if (!items.length) {
        wrap.innerHTML = '<div class="empty">Совпадений не найдено.</div>';
        return;
      }
      wrap.innerHTML = items.map((item) => {
        const isAdded = streetAlreadySelected(item.id);
        return `
          <div class="street-option ${isAdded ? "is-added" : ""}" data-add-street="${item.id}">
            <strong>${esc(item.display_name)}</strong>
            <small>${esc(item.urban_name)} | ${esc(item.district_name)} | street id=${item.id}</small>
          </div>
        `;
      }).join("");
    }

    function renderStatusStats(items) {
      const wrap = $("jobStats");
      if (!items.length) {
        wrap.innerHTML = "";
        return;
      }
      wrap.innerHTML = items.map((item) => `
        <div class="status-stat">
          <small>${esc(item.label)}</small>
          <strong>${esc(item.value)}</strong>
        </div>
      `).join("");
    }

    function setStatus(title, body, foot = "", stats = []) {
      $("jobTitle").textContent = title;
      $("jobBanner").innerHTML = body;
      $("jobFoot").textContent = foot;
      renderStatusStats(stats);
    }

    function setRefreshBusy(isBusy) {
      $("refreshBtn").disabled = isBusy;
      $("refreshCatalogBtn").disabled = isBusy;
      $("refreshBtn").textContent = isBusy ? "Идёт обновление..." : "Обновить объявления";
    }

    function renderHistory(items) {
      const wrap = $("historyList");
      if (!items.length) {
        wrap.innerHTML = '<div class="empty">История появится после первой проверки.</div>';
        return;
      }
      wrap.innerHTML = items.map((item) => `
        <div class="history-card">
          <div>
            <strong>${esc(item.scope_label)}</strong><br />
            <small>${esc(item.finished_at_local || item.started_at_local || "")}</small>
          </div>
          <small>
            Статус: ${esc(item.status)} |
            Новых: ${esc(item.new_count)} |
            Страниц: ${esc(item.pages_scanned)}${item.baseline_mode ? " | первый проход" : ""}
          </small>
        </div>
      `).join("");
    }

    function renderLatestSummary(summary) {
      const noteBits = [];
      if (summary.baseline_mode) {
        noteBits.push("Первый запуск для этой локации: текущий срез сохранён как база.");
      }
      if (summary.remaining_count > 0) {
        noteBits.push(`Показаны первые ${summary.displayed_count} из ${summary.new_count} новых объявлений.`);
      }
      if (!summary.new_count) {
        noteBits.push("Новых объявлений с прошлого обновления не найдено.");
      }
      $("latestSummary").innerHTML = `
        <div class="summary-card">
          <div>
            <strong>${esc(summary.scope_label)}</strong><br />
            <small>${esc(summary.finished_at_local)} | ${esc(summary.message)}</small>
          </div>
          <div class="summary-grid">
            <div class="summary-stat"><small>Новых</small><strong>${esc(summary.new_count)}</strong></div>
            <div class="summary-stat"><small>Отсканировано</small><strong>${esc(summary.rows_scanned)}</strong></div>
            <div class="summary-stat"><small>Страниц</small><strong>${esc(summary.pages_scanned)}</strong></div>
            <div class="summary-stat"><small>Было в seen</small><strong>${esc(summary.seen_before)}</strong></div>
            <div class="summary-stat"><small>Стало в seen</small><strong>${esc(summary.seen_after)}</strong></div>
          </div>
          <small>${esc(noteBits.join(" "))}</small>
        </div>
      `;
    }

    function renderListings(items) {
      const wrap = $("latestListings");
      const empty = $("listingsEmpty");
      if (!items.length) {
        wrap.innerHTML = "";
        empty.classList.remove("hidden");
        return;
      }
      empty.classList.add("hidden");
      wrap.innerHTML = items.map((item) => `
        <article class="listing-card">
          <div class="listing-top">
            <div>
              <strong>${esc(item.address || "Без адреса")}</strong>
              <small>${esc(item.district_name || "")}${item.urban_name ? " | " + esc(item.urban_name) : ""}</small>
            </div>
            <div class="listing-badge">${esc(item.deal_type || "объявление")}</div>
          </div>
          <div class="listing-price">${esc(priceLabel(item))}</div>
          <div class="listing-meta">
            ${detailTags(item).map((tag) => `<span>${tag}</span>`).join("")}
          </div>
          <small>Обновлено: ${esc(item.last_updated || "нет данных")}</small>
          <a class="listing-link" href="${esc(item.listing_url)}" target="_blank" rel="noreferrer">Открыть на Myhome</a>
        </article>
      `).join("");
    }

    function buildPayload() {
      return {
        district_id: selectedDistrictId(),
        district_name: selectedDistrictName(),
        urban_id: selectedUrbanId(),
        urban_name: selectedUrbanName(),
        street_ids: state.selectedStreetMatches.map((item) => item.id),
        street_names: state.selectedStreetMatches.map((item) => item.display_name),
      };
    }

    function runningStats(progress) {
      const stats = [];
      if (progress.phase) stats.push({ label: "Фаза", value: progress.phase });
      if (progress.pages_scanned != null) stats.push({ label: "Страниц", value: progress.pages_scanned });
      if (progress.rows_scanned != null) stats.push({ label: "Объявлений", value: progress.rows_scanned });
      stats.push({ label: "Лимит", value: `${DEFAULT_MAX_PAGES} x ${DEFAULT_PER_PAGE}` });
      return stats;
    }

    function renderRunning(payload) {
      const progress = payload.progress || {};
      setStatus(
        "Обновление",
        `<span class="status-ok">Сканирование в процессе.</span><br />${esc(payload.message || "Получаем новые объявления с Myhome.")}`,
        "Страница обновляется автоматически, дождитесь завершения.",
        runningStats(progress)
      );
    }

    async function loadCatalog(refresh = false) {
      const response = await fetch(refresh ? "/api/catalog?refresh=1" : "/api/catalog");
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Не удалось загрузить каталог");
      state.catalog = payload;
      renderDistricts();
      renderUrbans();
    }

    async function loadHistory() {
      const response = await fetch("/api/history");
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Не удалось загрузить историю");
      renderHistory(payload.items || []);
    }

    async function searchStreets() {
      const q = $("streetQuery").value.trim();
      if (!q) {
        $("streetResults").innerHTML = '<div class="empty">Сначала введите название улицы.</div>';
        return;
      }
      const params = new URLSearchParams({ q });
      if (selectedDistrictId()) params.set("district_id", String(selectedDistrictId()));
      if (selectedUrbanId()) params.set("urban_id", String(selectedUrbanId()));
      const response = await fetch(`/api/streets?${params.toString()}`);
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Ошибка поиска улицы");
      renderStreetResults(payload.items || []);
    }

    async function resumeCurrentJob() {
      const response = await fetch("/api/jobs/current");
      const payload = await response.json();
      if (!response.ok || !payload.job) {
        return;
      }
      state.currentJobId = payload.job.id;
      setRefreshBusy(true);
      renderRunning(payload.job);
      if (state.pollTimer) clearInterval(state.pollTimer);
      state.pollTimer = setInterval(pollJob, 1500);
      await pollJob();
    }

    async function startRefresh() {
      const response = await fetch("/api/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(buildPayload()),
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Не удалось начать обновление");
      }
      state.currentJobId = payload.job_id;
      setRefreshBusy(true);
      setStatus(
        "Обновление",
        "Задача запущена. Подтягиваем объявления из Myhome...",
        "Страница будет получать прогресс автоматически.",
        [{ label: "Лимит", value: `${DEFAULT_MAX_PAGES} x ${DEFAULT_PER_PAGE}` }]
      );
      if (state.pollTimer) clearInterval(state.pollTimer);
      state.pollTimer = setInterval(pollJob, 1500);
      await pollJob();
    }

    async function pollJob() {
      if (!state.currentJobId) return;
      const response = await fetch(`/api/jobs/${state.currentJobId}`);
      const payload = await response.json();
      if (!response.ok) {
        setRefreshBusy(false);
        throw new Error(payload.error || "Не удалось получить статус");
      }
      if (payload.status === "running") {
        renderRunning(payload);
        return;
      }
      if (state.pollTimer) {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
      }
      state.currentJobId = null;
      setRefreshBusy(false);
      if (payload.status === "done") {
        renderLatestSummary(payload.summary);
        renderListings(payload.summary.items || []);
        setStatus(
          "Готово",
          `<span class="status-ok">Проверка завершена.</span><br />${esc(payload.summary.message || "")}`,
          `Локация: ${payload.summary.scope_label}`,
          [
            { label: "Новых", value: payload.summary.new_count },
            { label: "Страниц", value: payload.summary.pages_scanned },
            { label: "Всего", value: payload.summary.rows_scanned },
            { label: "В seen", value: payload.summary.seen_after },
          ]
        );
        await loadHistory();
        return;
      }
      setStatus(
        "Ошибка",
        `<span class="status-warn">${esc(payload.error || "Обновление завершилось ошибкой")}</span>`,
        "Попробуйте обновить ещё раз."
      );
    }

    document.addEventListener("click", (event) => {
      const addStreet = event.target.closest("[data-add-street]");
      if (addStreet) {
        const streetId = Number(addStreet.getAttribute("data-add-street"));
        if (!streetAlreadySelected(streetId)) {
          const item = state.lastStreetResults.find((row) => row.id === streetId);
          if (item) {
            state.selectedStreetMatches.push(item);
            renderSelectedStreets();
            renderStreetResults(state.lastStreetResults);
          }
        }
      }

      const removeStreet = event.target.closest("[data-remove-street]");
      if (removeStreet) {
        const streetId = Number(removeStreet.getAttribute("data-remove-street"));
        state.selectedStreetMatches = state.selectedStreetMatches.filter((item) => item.id !== streetId);
        renderSelectedStreets();
        renderStreetResults(state.lastStreetResults);
      }
    });

    $("districtSelect").addEventListener("change", () => {
      renderUrbans();
      const districtId = selectedDistrictId();
      state.selectedStreetMatches = state.selectedStreetMatches.filter((item) => !districtId || item.district_id === districtId);
      renderSelectedStreets();
      renderStreetResults(state.lastStreetResults.filter((item) => !districtId || item.district_id === districtId));
    });

    $("urbanSelect").addEventListener("change", () => {
      const urbanId = selectedUrbanId();
      state.selectedStreetMatches = state.selectedStreetMatches.filter((item) => !urbanId || item.urban_id === urbanId);
      renderSelectedStreets();
      renderStreetResults(state.lastStreetResults.filter((item) => !urbanId || item.urban_id === urbanId));
    });

    $("streetQuery").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        searchStreets().catch((error) => {
          setStatus("Ошибка", `<span class="status-warn">${esc(error.message || String(error))}</span>`);
        });
      }
    });

    $("searchStreetBtn").addEventListener("click", () => {
      searchStreets().catch((error) => {
        setStatus("Ошибка", `<span class="status-warn">${esc(error.message || String(error))}</span>`);
      });
    });

    $("refreshCatalogBtn").addEventListener("click", async () => {
      try {
        setStatus("Каталог", "Обновляем список районов и area из API...", "", []);
        await loadCatalog(true);
        setStatus("Готов", "Каталог обновлён. Можно запускать новую проверку.", "", []);
      } catch (error) {
        setStatus("Ошибка", `<span class="status-warn">${esc(error.message || String(error))}</span>`);
      }
    });

    $("refreshBtn").addEventListener("click", async () => {
      try {
        await startRefresh();
      } catch (error) {
        setRefreshBusy(false);
        setStatus("Ошибка", `<span class="status-warn">${esc(error.message || String(error))}</span>`);
      }
    });

    async function boot() {
      setRefreshBusy(false);
      renderSelectedStreets();
      renderListings([]);
      await loadCatalog(false);
      await loadHistory();
      await resumeCurrentJob();
    }

    boot().catch((error) => {
      setStatus("Ошибка", `<span class="status-warn">${esc(error.message || String(error))}</span>`);
    });
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the hosted myhome refresh site.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Host to bind. Default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Port to bind. Default: {DEFAULT_PORT}")
    parser.add_argument("--open-browser", action="store_true", help="Open the browser automatically on start.")
    return parser.parse_args()


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(microsecond=0)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    return current.isoformat().replace("+00:00", "Z")


def format_local(iso_value: str | None) -> str | None:
    if not iso_value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(iso_value.replace("Z", "+00:00"))
    except ValueError:
        return iso_value
    return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def init_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(DB_SCHEMA)
        conn.commit()


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def to_int(value: Any) -> int | None:
    if value in (None, "", 0, "0"):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def int_list(values: Any) -> list[int]:
    if not isinstance(values, list):
        return []
    parsed: list[int] = []
    for value in values:
        item = to_int(value)
        if item is not None:
            parsed.append(item)
    return sorted(set(parsed))


def text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def build_scope_label(payload: dict[str, Any], street_ids: list[int]) -> str:
    district_name = str(payload.get("district_name") or "").strip()
    urban_name = str(payload.get("urban_name") or "").strip()
    street_names = text_list(payload.get("street_names"))

    parts = ["Квартиры Тбилиси"]
    if district_name:
        parts.append(district_name)
    elif payload.get("district_id"):
        parts.append(f"район {payload['district_id']}")

    if urban_name:
        parts.append(urban_name)
    elif payload.get("urban_id"):
        parts.append(f"area {payload['urban_id']}")

    if street_names:
        if len(street_names) == 1:
            parts.append(street_names[0])
        else:
            parts.append(f"{len(street_names)} улиц")
    elif street_ids:
        parts.append(f"{len(street_ids)} улиц")
    else:
        parts.append("все улицы")

    return " / ".join(parts)


def normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    district_id = to_int(payload.get("district_id"))
    urban_id = to_int(payload.get("urban_id"))
    street_ids = int_list(payload.get("street_ids"))

    filters = {
        "city_id": DEFAULT_CITY_ID,
        "deal_types": "1,2",
        "real_estate_types": "1",
        "statuses": "2",
        "district_id": district_id,
        "urban_id": urban_id,
        "street_ids": street_ids,
    }
    signature = json.dumps(filters, sort_keys=True, ensure_ascii=False)
    return {
        "scope_key": hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16],
        "scope_label": build_scope_label(payload, street_ids),
        "district_id": district_id,
        "district_name": str(payload.get("district_name") or "").strip() or None,
        "urban_id": urban_id,
        "urban_name": str(payload.get("urban_name") or "").strip() or None,
        "street_ids": street_ids,
        "street_names": text_list(payload.get("street_names")),
        "filters": filters,
    }


def build_catalog_args(refresh: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        city_id=DEFAULT_CITY_ID,
        deal_types="1,2",
        real_estate_types="1",
        statuses="2",
        currency_id="1",
        catalog_cache=CATALOG_CACHE_PATH,
        refresh_catalog=refresh,
    )


def load_catalog(refresh: bool = False) -> dict[str, Any]:
    return export_mod.build_location_catalog(build_catalog_args(refresh=refresh), force_refresh=refresh)


def search_streets(query: str, district_id: int | None, urban_id: int | None) -> list[dict[str, Any]]:
    session = requests.Session()
    session.headers.update(export_mod.DEFAULT_HEADERS)
    try:
        items = export_mod.fetch_street_matches(session, DEFAULT_CITY_ID, query)
    finally:
        session.close()
    if district_id:
        items = [item for item in items if int(item.get("district_id") or 0) == district_id]
    if urban_id:
        items = [item for item in items if int(item.get("urban_id") or 0) == urban_id]
    return items[:20]


def query_seen_count(conn: sqlite3.Connection, scope_key: str) -> int:
    row = conn.execute("SELECT COUNT(*) FROM scope_seen WHERE scope_key = ?", (scope_key,)).fetchone()
    return int(row[0]) if row else 0


def remember_listing(conn: sqlite3.Connection, scope_key: str, row: dict[str, Any]) -> bool:
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO scope_seen (
            scope_key, listing_id, first_seen_utc, last_updated, payload_json
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            scope_key,
            row.get("id"),
            iso_utc(),
            row.get("last_updated"),
            json.dumps(row, ensure_ascii=False),
        ),
    )
    return cursor.rowcount == 1


def history_items(limit: int = 12) -> list[dict[str, Any]]:
    with open_db() as conn:
        rows = conn.execute(
            """
            SELECT scope_key, scope_label, status, started_at_utc, finished_at_utc,
                   pages_scanned, rows_scanned, new_count, baseline_mode, message
            FROM refresh_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "scope_key": row["scope_key"],
                "scope_label": row["scope_label"],
                "status": row["status"],
                "started_at_local": format_local(row["started_at_utc"]),
                "finished_at_local": format_local(row["finished_at_utc"]),
                "pages_scanned": int(row["pages_scanned"] or 0),
                "rows_scanned": int(row["rows_scanned"] or 0),
                "new_count": int(row["new_count"] or 0),
                "baseline_mode": bool(row["baseline_mode"]),
                "message": row["message"],
            }
        )
    return items


def save_run(
    job_id: str,
    scope: dict[str, Any],
    started_at: str,
    status: str,
    pages_scanned: int,
    rows_scanned: int,
    new_count: int,
    baseline_mode: bool,
    message: str,
    summary: dict[str, Any] | None,
) -> None:
    summary_for_storage = None
    if summary is not None:
        summary_for_storage = json.dumps({key: value for key, value in summary.items() if key != "items"}, ensure_ascii=False)

    with open_db() as conn:
        conn.execute(
            """
            INSERT INTO refresh_runs (
                job_id, scope_key, scope_label, filters_json, started_at_utc, finished_at_utc,
                status, pages_scanned, rows_scanned, new_count, baseline_mode, message, summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                scope["scope_key"],
                scope["scope_label"],
                json.dumps(scope["filters"], ensure_ascii=False),
                started_at,
                iso_utc(),
                status,
                pages_scanned,
                rows_scanned,
                new_count,
                1 if baseline_mode else 0,
                message,
                summary_for_storage,
            ),
        )
        conn.commit()


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
                "message": "В очереди",
                "summary": None,
                "error": None,
                "progress": {
                    "phase": "queued",
                    "pages_scanned": 0,
                    "rows_scanned": 0,
                },
            }
        return job_id

    def update(self, job_id: str, **changes: Any) -> None:
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id].update(changes)

    def _public_job(self, job: dict[str, Any]) -> dict[str, Any]:
        payload = dict(job)
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


JOB_STORE = JobStore()


def fetch_scope_rows(scope: dict[str, Any], job_id: str) -> tuple[list[dict[str, Any]], int]:
    args = SimpleNamespace(
        city_id=DEFAULT_CITY_ID,
        deal_types="1,2",
        real_estate_types="1",
        statuses="2",
        currency_id="1",
    )
    filters = export_mod.build_base_filters(args)
    if scope["district_id"]:
        filters["districts"] = str(scope["district_id"])
    if scope["urban_id"]:
        filters["urbans"] = str(scope["urban_id"])
    if scope["street_ids"]:
        filters["streets"] = ",".join(str(item) for item in scope["street_ids"])

    rows: list[dict[str, Any]] = []
    session = requests.Session()
    session.headers.update(export_mod.DEFAULT_HEADERS)
    try:
        for page in range(1, DEFAULT_MAX_PAGES + 1):
            JOB_STORE.update(
                job_id,
        message=f"Сканирую страницу {page} для {scope['scope_label']}",
                progress={
                    "phase": "fetching",
                    "pages_scanned": page - 1,
                    "rows_scanned": len(rows),
                },
            )
            items = export_mod.fetch_page(session, params=filters, page=page, per_page=DEFAULT_PER_PAGE)
            if not items:
                JOB_STORE.update(
                    job_id,
                    message="Больше страниц для этой локации нет",
                    progress={
                        "phase": "fetching",
                        "pages_scanned": page - 1,
                        "rows_scanned": len(rows),
                    },
                )
                return rows, page - 1

            normalized = [export_mod.normalize_listing(item) for item in items if item.get("id") is not None]
            rows.extend(normalized)
            JOB_STORE.update(
                job_id,
                message=f"Собрано {len(rows)} объявлений после страницы {page}",
                progress={
                    "phase": "fetching",
                    "pages_scanned": page,
                    "rows_scanned": len(rows),
                },
            )
            if len(items) < DEFAULT_PER_PAGE:
                return rows, page
    finally:
        session.close()
    return rows, DEFAULT_MAX_PAGES


def refresh_scope(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    scope = normalize_payload(payload)
    started_at = iso_utc()
    JOB_STORE.update(
        job_id,
        payload=scope,
        message=f"Подготавливаю обновление для {scope['scope_label']}",
        progress={"phase": "preparing", "pages_scanned": 0, "rows_scanned": 0},
    )

    rows, pages_scanned = fetch_scope_rows(scope, job_id)
    rows.sort(key=lambda row: row.get("last_updated") or "", reverse=True)

    new_rows: list[dict[str, Any]] = []
    with open_db() as conn:
        seen_before = query_seen_count(conn, scope["scope_key"])
        for row in rows:
            if remember_listing(conn, scope["scope_key"], row):
                new_rows.append(row)
        conn.commit()
        seen_after = query_seen_count(conn, scope["scope_key"])

    baseline_mode = seen_before == 0
    display_rows = new_rows[:MAX_RETURN_ITEMS]
    remaining_count = max(0, len(new_rows) - len(display_rows))

    if not rows:
        message = "По текущим фильтрам активные объявления не найдены."
    elif new_rows:
        if baseline_mode:
            message = f"Первый проход завершён: сохранено {len(new_rows)} текущих объявлений для этой локации."
        else:
            message = f"Найдено {len(new_rows)} новых объявлений с прошлого обновления."
    else:
        message = "Новых объявлений с прошлого обновления нет."

    finished_at = iso_utc()
    summary = {
        "scope_key": scope["scope_key"],
        "scope_label": scope["scope_label"],
        "filters": scope["filters"],
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "finished_at_local": format_local(finished_at),
        "pages_scanned": pages_scanned,
        "rows_scanned": len(rows),
        "new_count": len(new_rows),
        "displayed_count": len(display_rows),
        "remaining_count": remaining_count,
        "baseline_mode": baseline_mode,
        "seen_before": seen_before,
        "seen_after": seen_after,
        "message": message,
        "items": display_rows,
    }

    save_run(
        job_id=job_id,
        scope=scope,
        started_at=started_at,
        status="done",
        pages_scanned=pages_scanned,
        rows_scanned=len(rows),
        new_count=len(new_rows),
        baseline_mode=baseline_mode,
        message=message,
        summary=summary,
    )
    return summary


def run_job(job_id: str, payload: dict[str, Any]) -> None:
    started_at = iso_utc()
    try:
        summary = refresh_scope(job_id, payload)
    except Exception as exc:  # noqa: BLE001
        error_message = str(exc)
        scope = normalize_payload(payload)
        save_run(
            job_id=job_id,
            scope=scope,
            started_at=started_at,
            status="failed",
            pages_scanned=0,
            rows_scanned=0,
            new_count=0,
            baseline_mode=False,
            message=error_message,
            summary=None,
        )
        JOB_STORE.update(job_id, status="failed", error=error_message, message=error_message)
        return

    JOB_STORE.update(
        job_id,
        status="done",
        summary=summary,
        message=summary["message"],
        progress={
            "phase": "completed",
            "pages_scanned": summary["pages_scanned"],
            "rows_scanned": summary["rows_scanned"],
        },
    )


def build_html() -> str:
    return (
        HTML_PAGE
        .replace("__DEFAULT_MAX_PAGES__", str(DEFAULT_MAX_PAGES))
        .replace("__DEFAULT_PER_PAGE__", str(DEFAULT_PER_PAGE))
    )


class AppHandler(BaseHTTPRequestHandler):
    server_version = "MyhomeRenderSite/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.respond_html(build_html())
            return
        if parsed.path == "/healthz":
            self.respond_text("ok")
            return
        if parsed.path == "/api/catalog":
            query = parse_qs(parsed.query)
            refresh = query.get("refresh", ["0"])[0] == "1"
            try:
                self.respond_json(load_catalog(refresh=refresh))
            except Exception as exc:  # noqa: BLE001
                self.respond_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/streets":
            query = parse_qs(parsed.query)
            q = (query.get("q", [""])[0] or "").strip()
            district_id = to_int(query.get("district_id", [""])[0])
            urban_id = to_int(query.get("urban_id", [""])[0])
            if not q:
                self.respond_json({"items": []})
                return
            try:
                items = search_streets(q, district_id=district_id, urban_id=urban_id)
                self.respond_json({"items": items})
            except Exception as exc:  # noqa: BLE001
                self.respond_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/history":
            self.respond_json({"items": history_items()})
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
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/refresh":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        payload = self.read_json()
        if payload is None:
            self.respond_json({"error": "Invalid JSON body"}, status=HTTPStatus.BAD_REQUEST)
            return
        if JOB_STORE.has_running():
            self.respond_json(
                {"error": "Другое обновление уже выполняется. Дождитесь завершения."},
                status=HTTPStatus.CONFLICT,
            )
            return
        job_id = JOB_STORE.create(payload)
        thread = threading.Thread(target=run_job, args=(job_id, payload), daemon=True)
        thread.start()
        self.respond_json({"job_id": job_id}, status=HTTPStatus.ACCEPTED)

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

    def respond_text(self, body: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
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

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def main() -> int:
    init_storage()
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    url = f"http://{args.host}:{args.port}"
    print(f"Myhome refresh site is running at {url}")
    print(f"Data dir: {DATA_DIR}")
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
