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
- Stato a colpo d'occhio: `https://trader.byluke.org/log` (o `GET /api/logs/status`).
- Senza SSH: `curl "https://trader.byluke.org/api/logs/raw?job=<id>&lines=300"` (vedi §8).
- Con SSH: `systemctl list-timers 'trader-*'`; `journalctl -u trader-scrape-subito.service -n 80 --no-pager`.
- Riprova manualmente il job: `sudo -u trader /opt/trader/app/deploy/server_job.sh <job>`.
- Fix di codice: modifica in locale, commit + push su `main`; l'auto-deploy lo applica entro ~5 min (vedi §8).

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

## 8) Log via HTTP e auto-deploy
Lo scraping NON gira più su GitHub Actions (workflow archiviati in `STORICI3GIUGNO/github-workflows/`, GitHub Pages disabilitato). Tutto gira sul server via systemd timer; il codice si aggiorna da solo.

**Auto-deploy**: `trader-deploy.timer` ogni 5 min esegue `deploy/auto_deploy.sh` (`git fetch` + `reset --hard origin/main` + restart `trader-viewer`, reinstalla deps solo se `requirements.lock` cambia). Quindi basta `git push` su `main` per aggiornare il sito entro ~5 min. nginx NON è auto-deployato (config in `/etc/nginx/sites-available/trader.byluke.org`).
- Nota: gli script `deploy/*.sh` vanno committati con bit eseguibile (`git update-index --chmod=+x`), altrimenti il `reset --hard` li rende 644 e systemd fallisce.
- Forzare un deploy subito: `sudo /opt/trader/app/deploy/auto_deploy.sh`.

**Lettura log (pubblica, no SSH)** — comoda per persone, tool o LLM:
- Stato sintetico a colori (JSON): `GET https://trader.byluke.org/api/logs/status`.
- Pagina UI: `https://trader.byluke.org/log` (pulsante 🩺 nell'header).
- Log grezzo (tail): `GET https://trader.byluke.org/api/logs/raw?job=<id>&lines=<n>` (default 200, max 1000).
  - Fonti (una per voce): `scrape-cex`, `scrape-gameshock`, `scrape-gamepeople`, `scrape-gamelife`, `scrape-rebuy`, `scrape-subito`, `scrape-ebay`.
  - Verifiche/sistema: `verify-sold` (venduti Subito), `ai-classify`, `backup`.
  - Archivio GitHub: `Scraper_Fonti`, `Subito.it`, `eBay`, `AI_Classify`, `Verify_Sold`.
- Ogni fonte scrive un marker per-run in `source-<fonte>.log` (run.py:_write_source_marker), così lo stato è indipendente ovunque giri lo scrape (server / Mac residenziale / GitHub).
- Ripartizione scrape: rebuy → GitHub Actions; subito+gamelife → Mac residenziale (launchd, blocco IP datacenter); gamepeople+gameshock+cex+ebay → server.
- Stati: 🟢 ok · 🟠 warn/non recente · 🔴 fallimento reale (crash/exit-code) · ⚪ sconosciuto.
- File sul server: `/var/log/trader/*.log`; backend in `log_status.py`, endpoint in `server.py`.
