# Runbook Operativo

## 1) Failure scraping (una o più fonti)
- Controlla `logs/run_report_latest.json` per step e source in errore.
- Lancia in locale la fonte specifica: `python3 run.py --source <fonte>`.
- Se errore browser anti-bot, prova `TRADER_PLAYWRIGHT_CHANNEL=chromium` e ripeti.

## 2) Viewer non accessibile
- Verifica host/porta: `python3 run.py --view --host 127.0.0.1 --port 8080`.
- Se 401 sui POST, passa token `?token=<token>` o setta `trader_api_token` in localStorage.
- Controlla collisione porta con `lsof -i :8080`.

## 3) Degrado DB / storage
- Esegui manutenzione: `python3 run.py --cleanup`.
- Verifica dimensioni DB e numero snapshot residui in `data/`.
- Se necessario, forzare `VACUUM` manuale e riavviare scraping selettivo.

## 4) CI Daily Scrape fallisce
- Ispeziona log workflow `Daily Scrape` e step con exit code != 0.
- Riprova localmente con env CI-like (`TRADER_PLAYWRIGHT_CHANNEL=chromium`).
- Se fixato, push su `main` e rilancia `workflow_dispatch`.

## 5) Aggiornamento schema DB
- Le migrazioni sono automatiche in `init_db()` tramite `migrations.py`.
- Aggiungi nuova `Migration(version, name, statements)` nel modulo DB target.
- Verifica applicazione con test e controlla tabella `schema_migrations`.

## 6) Incident response rapido
- Congela run schedulati (disabilita workflow o cron) se i dati sono corrotti.
- Fai backup dei DB correnti (`*.db`) prima di operazioni distruttive.
- Ripristina baseline con scrape mirato e valida UI/API prima di riattivare schedule.
