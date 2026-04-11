# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Approach

- Think before acting. Form a short plan internally before making changes.
- Always inspect relevant files before making changes. Do not re-read unnecessarily.
- Identify root cause, not just symptoms. Do not assume missing context—ask or infer carefully.
- Prefer minimal, surgical edits over rewriting entire files. Preserve existing structure, style, and conventions.
- Do not introduce unnecessary abstractions.
- Ensure code runs after changes. Consider edge cases and failure modes.
- Test changes whenever possible; if tests exist, run and respect them.
- Optimize only when it matters.
- Output only what is necessary. No filler, no conversational fluff. Briefly indicate what was changed and why.
- If uncertain, state assumptions. If blocked, explain clearly instead of guessing.
- Do not declare completion unless the solution is verified.

## Commands

```bash
# Install dependencies
pip install -r requirements.lock
python -m playwright install chromium

# Linting
ruff check .

# Tests
pytest                          # all tests
pytest tests/test_smoke.py      # single test file
pytest -k "test_name"           # single test by name

# Scraping
python3 run.py                                    # all enabled sources
python3 run.py --source gamelife,cex              # specific sources
python3 run.py --source subito --subito-region lombardia
python3 run.py --full                             # Subito + eBay + verify sold + classify + view

# Viewer (web UI on 127.0.0.1:8080)
python3 run.py --view

# AI classification of pending Subito ads
python3 run.py --classify --classify-limit 100

# Verify sold Subito ads via Playwright
python3 run.py --verify-sold 500 --verify-xbox-only

# Valuation
python3 run.py --valuation-report
python3 run.py --tune-valuation                   # auto-tune CEX/eBay/Subito weights

# Maintenance (retention + archival + VACUUM)
python3 run.py --cleanup
```

## Architecture

Single-purpose price tracker for Xbox hardware on Italian resale markets. All persistent state lives in `tracker.db` (SQLite).

**Data flow:**
1. **Scrapers** (`scrapers/`) fetch data from 8 sources → JSON snapshots on disk
2. **DB ingestion** (`db.py`, `db_subito.py`, `db_ebay.py`) deduplicates and upserts into `tracker.db`, logging changes to `*_changes` tables
3. **Classification** (`classifier.py` + `model_rules.py` + `ai_classifier.py`) tags each item with `console_family`, `canonical_model`, `model_segment`, `edition_class` via a 3-tier pipeline: regex rules → CEX Jaccard matching → Claude Haiku fallback
4. **Verification** (`verify_sold.py`) uses async Playwright to detect sold Subito ads
5. **Valuation** (`valuation.py`) computes fair value as weighted average: CEX 45% + eBay 35% + Subito 20%, with trimmed mean (15% outlier removal)
6. **Alerting** (`alerts.py`) fires macOS notification + Telegram when price < CEX threshold; deduplicates via `alert_log.json`
7. **HTTP API** (`server.py`) serves 30+ REST endpoints (Bearer token auth) on `127.0.0.1:8080`
8. **Viewer SPA** (`viewer/`) renders 9-tab dashboard (Home, Riepilogo, Catalogo, Subito, eBay, Statistiche, Trend, Ricerca)

**Key design decisions:**
- `tracker.db` was merged from 3 separate DBs; `migrations.py` uses namespace-aware versioning (`products`/`ads`/`ebay`) to prevent migration conflicts
- Subito ads have a two-stage AI filter: `ai_classifier.py` scores hardware confidence (0–100, threshold ≥75 approved / ≤25 rejected), then `classifier.py` applies model classification
- Classification 3-tier: rules (fast regex) → CEX matching (Jaccard ≥ 0.45) → Claude Haiku (batch 15, only for `family="other"` or confidence < 0.6)
- Playwright scrapers use fresh browser contexts per request to bypass Akamai/Cloudflare; fall back from system Chrome to bundled Chromium
- Viewer auth: token from `/api/token` bootstrap stored in `sessionStorage` only; `sanitize.js` prevents XSS
- Snapshot retention: configurable (default 30 per source), auto-archived to `.json.gz` after N days

**DB schema tables:**
- `products`, `state_changes` — shop sources (CEX, GameLife, GamePeople, GameShock, ReBuy)
- `ads`, `ad_changes` — Subito marketplace ads
- `sold_items`, `sold_changes` — eBay sold listings
- `categories`, `storage_sizes` — classification lookup tables
- `schema_migrations` — versioned migrations per namespace

**Configuration:** `config.toml` with `TRADER_*` ENV overrides. `settings.py` validates on load and raises `ConfigError`.

**CI/CD:** GitHub Actions runs quality gate (lint + pytest) on push/PR; daily scrape workflows for each source group.

## Runbook

See `docs/runbook.md` for failure recovery procedures (DB degradation, scraper failures, viewer issues, schema updates).
