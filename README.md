# Xbox Trader

Tracker multi-sorgente per prezzi console Xbox da shop online, Subito.it ed eBay venduto.
Pipeline: scraper -> snapshot JSON -> DB SQLite -> viewer web locale.

## Requisiti
- Python 3.10+
- Dipendenze runtime: `pip install -r requirements.txt`
- Browser Playwright: `python -m playwright install chromium`

## Setup rapido
1. Installa dipendenze: `pip install -r requirements-dev.txt`
2. Copia e adatta `config.toml`
3. Avvia scraping: `python3 run.py`
4. Avvia viewer: `python3 run.py --view`

## Comandi principali
- `python3 run.py` -> scrape fonti abilitate
- `python3 run.py --source subito,ebay` -> scrape selettivo
- `python3 run.py --all` -> scrape + viewer
- `python3 run.py --full` -> subito + ebay + classify + viewer
- `python3 run.py --cleanup` -> retention + archiviazione snapshot + VACUUM DB
- `python3 run.py --setup-cron` -> installazione crontab locale

## Sicurezza viewer
- Default bind: `127.0.0.1` (`[viewer].host`)
- Endpoint POST protetto con bearer token opzionale (`[viewer].api_token`)
- Frontend con sanitizzazione input lato client per ridurre rischio XSS da contenuti esterni

## Configurazione
`settings.py` valida `config.toml` e supporta override ENV.
Variabili utili:
- `TRADER_VIEWER_HOST`, `TRADER_VIEWER_PORT`, `TRADER_VIEWER_OPEN_BROWSER`
- `TRADER_API_TOKEN`
- `TRADER_PLAYWRIGHT_CHANNEL` (`chrome` o `chromium`)
- `TRADER_RETENTION_KEEP`, `TRADER_ARCHIVE_AFTER_DAYS`

## Architettura
- `run.py`: orchestrazione CLI, viewer API, cleanup, report runtime
- `scrapers/`: collector per fonte (Playwright/requests/Algolia)
- `db.py`, `db_subito.py`, `db_ebay.py`: storage + change tracking
- `migrations.py`: gestione migrazioni schema versionate
- `viewer/`: UI statica con moduli `modules/api.js`, `modules/sanitize.js`, `modules/state.js`

## Dati e policy storage
- DB versionati: `trader.db`, `subito.db`, `ebay.db`
- Snapshot JSON operativi in `data/` (non versionati)
- Snapshot vecchi compressi in `data/archive/*.json.gz`
- Report runtime in `logs/run_report_latest.json`

## Test e quality gate
- Unit/smoke test: `pytest`
- Lint: `ruff check .`
- CI:
  - `Quality Gate` su push/PR (lint + test)
  - `Daily Scrape` schedulato (scrape + cleanup + commit DB)

## Troubleshooting veloce
- Playwright/Chrome non disponibile: imposta `TRADER_PLAYWRIGHT_CHANNEL=chromium`
- Viewer POST 401: passa token via URL `?token=<token>` o localStorage `trader_api_token`
- Config non valida: avvio fallisce con `ConfigError` e dettaglio nel log

Per procedure operative e incident response: vedi `docs/runbook.md`.
