# Runbook Operativo

## 1) Failure scraping (una o più fonti)
- Controlla `logs/run_report_latest.json` per step e source in errore.
- Lancia in locale la fonte specifica: `python3 run.py --source <fonte>`.
- Se errore browser anti-bot, prova `TRADER_PLAYWRIGHT_CHANNEL=chromium` e ripeti.

## 2) Viewer non accessibile
- Locale: verifica host/porta con `python3 run.py --view --host 127.0.0.1 --port 8080`.
- Server: controlla `systemctl status trader-viewer.service`.
- Se 401 sui POST, verifica `TRADER_API_TOKEN` e il bootstrap `/api/token`.
- Se dominio HTTPS non risponde, controlla Nginx e certificati prima di riavviare il viewer.

## 3) Degrado DB / storage
- Esegui manutenzione: `python3 run.py --cleanup`.
- Verifica dimensioni DB e numero snapshot residui in `data/`.
- Se necessario, forzare `VACUUM` manuale e riavviare scraping selettivo.

## 4) Routine server fallisce
- Controlla timer e ultimo run: `systemctl list-timers 'trader-*'`.
- Leggi log unità: `journalctl -u trader-scrape-subito.service -n 80 --no-pager`.
- Riprova manualmente il job: `sudo -u trader /opt/trader/app/deploy/server_job.sh <job>`.
- Se il fix richiede codice: modifica in locale, commit, deploy sul server, poi restart unità interessata.

## 5) Aggiornamento schema DB
- Le migrazioni sono automatiche in `init_db()` tramite `migrations.py`.
- Aggiungi nuova `Migration(version, name, statements)` nel modulo DB target.
- Verifica applicazione con test e controlla tabella `schema_migrations`.

## 6) Incident response rapido
- Congela run schedulati (`systemctl disable --now trader-*.timer`) se i dati sono corrotti.
- Fai backup dei DB correnti (`*.db`) prima di operazioni distruttive.
- Ripristina baseline con scrape mirato e valida UI/API prima di riattivare schedule.

## 7) Catalogazione Xbox
- Dal 3 giugno 2026 `console_family` è solo `original`, `360`, `one`, `series`, `other`.
- Il modello specifico è in `sub_model` (`Base`, `S`, `X`, `E`, `Elite`, `Unknown`).
- Consulta `console_catalog.md` per gli slot operativi e `STORICI3GIUGNO/README.md` per leggere i dati legacy.
