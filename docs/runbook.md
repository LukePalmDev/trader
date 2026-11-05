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
- Dal 5 giugno 2026 `Bibbia.md` è la fonte canonica dei contenitori Xbox.
- `canonical_model` e `ai_taxonomy_id` usano l'`ID` numerico della Bibbia.
- Consulta `Bibbia.md` e `console_catalog.md` per gli slot operativi; `STORICI3GIUGNO/README.md` resta solo per leggere i dati legacy.

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
  - Verifiche/sistema: `verify-sold` (venduti Subito), `ai-classify` compat, `ai-cascade`, `backup`.
  - Archivio GitHub: `Scraper_Fonti`, `Subito.it`, `eBay`, `AI_Classify`, `Verify_Sold`.
- Ogni fonte scrive un marker per-run in `source-<fonte>.log` (run.py:_write_source_marker), così lo stato è indipendente ovunque giri lo scrape (server / Mac residenziale / GitHub).
- Ripartizione scrape: rebuy → GitHub Actions; subito+gamelife → Mac residenziale (launchd, blocco IP datacenter); gamepeople+gameshock+cex+ebay → server.
- Stati: 🟢 ok · 🟠 warn/non recente · 🔴 fallimento reale (crash/exit-code) · ⚪ sconosciuto.
- File sul server: `/var/log/trader/*.log`; backend in `log_status.py`, endpoint in `server.py`.

## 9) Mac mini (produzione da nov 2026)
Il server DigitalOcean è dismesso: trader gira sul Mac mini in `~/trader`
(venv `.venv`, DB `tracker.db`, segreti in `.env`, mai in git).
- Installa/aggiorna i job: `deploy/install_launchd_mac.sh` (LaunchAgent `org.byluke.trader.*`
  in `~/Library/LaunchAgents`). Rimuovi: `deploy/install_launchd_mac.sh --uninstall`.
- Orari (ora locale): scrape-fonti 07:00 (gamepeople, gameshock, cex, gamelife, rebuy),
  scrape-rebuy 18:00, scrape-subito 04/10/16/22:00 (subito + gamelife), scrape-ebay 00:30,
  ai-cascade 05/11/17/23:10 (solo se `OPENAI_API_KEY` è nel `.env`), verify-sold 11/14:00,
  backup 03:15 in `~/trader/backups` (14 giorni), watchdog ogni 3h, viewer sempre attivo su 127.0.0.1:8080.
- Log: `~/trader/logs/<job>.log`. Stato: `launchctl list | grep org.byluke.trader`.
- Lancio a mano: `deploy/mac_job.sh scrape-subito` (un job pesante alla volta, lock in `runtime/`).
- Il Mac deve restare acceso con l'utente loggato (LaunchAgent utente).
