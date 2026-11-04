# Xbox Tracker

Tracker dei prezzi delle console Xbox sul mercato dell'usato italiano.
Legge i listini di negozi e marketplace, classifica gli annunci, stima un prezzo equo e segnala le occasioni.

Il programma gira in locale oppure sul server (droplet, `trader.byluke.org`).
Questo repository è solo il codice: non è pubblicato su GitHub Pages.
Il viewer usato è `viewer/`. La cartella `frontend/` è uno scaffold Preact incompleto e non va avviato al posto del viewer.

## Cosa fa

1. Gli scraper in `scrapers/` salvano uno snapshot JSON per fonte.
2. `db.py`, `db_subito.py` e `db_ebay.py` aggiornano `tracker.db` (SQLite) e lo storico delle variazioni.
3. `model_rules.py` e `ai_cascade_classifier.py` assegnano famiglia, modello e ID della Bibbia (`Bibbia.md`).
4. `valuation.py` stima il fair value (CEX 45%, eBay venduti 35%, Subito 20%, con taglio degli outlier).
5. `alerts.py` può notificare su macOS e Telegram.
6. `server.py` espone l'API e il viewer su `127.0.0.1:8080`. In produzione Nginx fa da reverse proxy.

Fonti abilitate in `config.toml`: CEX, Gamelife, GamePeople, Gameshock, Rebuy, Subito, eBay (solo venduti). JollyRogerBay è disabilitato perché vende giochi, non hardware.

## Requisiti

- Python 3.10+
- Dipendenze bloccate: `pip install -r requirements.lock`
- Per gli scraper con browser: `python3 -m playwright install chromium`
- Per i test: `pip install -r requirements-dev.txt` e poi `pytest`

`pyproject.toml` elenca le stesse dipendenze runtime del lockfile.

## Avvio locale

```bash
pip install -r requirements.lock
python3 -m playwright install chromium
cp .env.example .env          # opzionale: compila solo le chiavi che usi
python3 run.py --view         # viewer su http://127.0.0.1:8080/viewer/index.html
```

Senza `.env` il viewer parte lo stesso: genera un token di sessione e non apre il browser se `TRADER_VIEWER_OPEN_BROWSER=false`.
Il database `tracker.db` viene creato al primo avvio se manca. Non va committato.

Comandi utili, tutti senza obbligo di chiavi API tranne dove indicato:

```bash
python3 run.py --help
python3 run.py --valuation-report
python3 run.py --classify --classify-dry-run --classify-limit 20
python3 run.py --source cex                 # una fonte; serve rete
python3 run.py --cleanup
pytest
ruff check .
```

La classificazione AI (`--ai-classify`, `--ai-cascade-classify`) richiede `OPENAI_API_KEY`.
`--test-telegram` richiede il bot Telegram. Gli scraper Playwright richiedono il browser installato.

Sul server le routine periodiche sono timer systemd, non GitHub Actions.
Vedi `docs/server-routines.md` e `docs/digitalocean-migration.md`.
I workflow storici stanno in `STORICI3GIUGNO/github-workflows/`.
L'unico workflow attivo, `.github/workflows/scrape-remote.yml`, scrapa Rebuy (e Subito se raggiungibile) dagli IP GitHub e spedisce lo snapshot al server via SSH.

## Variabili d'ambiente

Copia `.env.example` in `.env`. Il processo legge `.env` all'avvio e non sovrascrive variabili già definite.
Non mettere token in `config.toml`: quel file è versionato.

| Variabile | Obbligatoria | Ruolo |
|-----------|--------------|--------|
| `OPENAI_API_KEY` | solo per l'AI | Chiave OpenRouter (o altro endpoint compatibile) |
| `OPENAI_BASE_URL` | no | Default `https://openrouter.ai/api/v1` |
| `OPENAI_CASCADE_MODELS` | no | Modelli in cascata, separati da virgola |
| `TRADER_TELEGRAM_TOKEN` | solo per gli alert Telegram | Token del bot da @BotFather |
| `TRADER_TELEGRAM_CHAT_ID` | solo per gli alert Telegram | Chat destinataria |
| `TRADER_TELEGRAM_ENABLED` | no | `true` per inviare. Resta disattivo se manca token o chat |
| `TRADER_API_TOKEN` | no in locale, sì in produzione | Bearer dell'API. Sul server: `openssl rand -hex 32` |
| `TRADER_DB_PATH` | no | Default `tracker.db`. Sul server: `/var/lib/trader/tracker.db` |
| `TRADER_OUTPUT_DIR` | no | Directory degli snapshot. Default `data` |
| `TRADER_VIEWER_HOST` | no | Default `127.0.0.1` |
| `TRADER_VIEWER_PORT` | no | Default `8080` |
| `TRADER_VIEWER_OPEN_BROWSER` | no | Default preso da `config.toml` (`true`). In server va `false` |
| `TRADER_PLAYWRIGHT_CHANNEL` | no | `chromium` (bundled) o `chrome` (installato sul sistema) |
| `TRADER_PROXY` | no | Proxy HTTP opzionale, `http://utente:password@host:porta` |
| `ANTHROPIC_API_KEY` | no | Solo se riattivi il fallback legacy con `TRADER_ENABLE_LEGACY_ANTHROPIC=true` |

Sul droplet i valori stanno in `/etc/trader/trader.env` (parti da `deploy/trader.env.example`).
Dopo aver cambiato quel file: `systemctl restart trader-viewer.service`.

## Comandi principali

```bash
python3 run.py                              # tutte le fonti abilitate
python3 run.py --source subito,ebay
python3 run.py --source subito --subito-region lombardia
python3 run.py --all                        # scrape + viewer
python3 run.py --full                       # Subito + eBay + verify sold + AI + viewer
python3 run.py --ai-cascade-classify --ai-cascade-limit 200
python3 run.py --verify-sold 500 --verify-xbox-only
python3 run.py --tune-valuation
python3 run.py --test-telegram
python3 migrate_to_tracker.py               # solo se hai i vecchi DB separati
```

## Viewer

Apri `http://127.0.0.1:8080/viewer/index.html` (il server reindirizza anche `/`).
La pagina stato log è `http://127.0.0.1:8080/log`.
Le chiamate API usano un bearer token letto da `/api/token` e tenuto in `sessionStorage`.

In produzione Nginx pubblica il sito e inoltra `/api/` al processo locale.
Il token di bootstrap è quindi raggiungibile da chi apre il sito: non è una password da distribuire, e le scritture (scrape, review) sono possibili a chiunque raggiunga l'API. Il sito va tenuto privato di rete se quei POST non devono essere pubblici.

## Dati e classificazione

Dal 5 giugno 2026 `Bibbia.md` è l'elenco canonico dei contenitori Xbox.
`canonical_model` è l'ID numerico di quella riga. Dettaglio operativo in `flusso.md`, `SYSTEM_ARCHITECTURE.md` e `docs/runbook.md`.

La cascata AI (OpenRouter) accetta solo un `taxonomy_id` della Bibbia oppure `other`.
Sotto la soglia di confidence l'annuncio finisce in review umana.

## Limitazioni note

Subito e alcuni negozi bloccano gli IP dei datacenter (Cloudflare / Akamai).
`verify_sold.py` usa un fast-path HTTP, backoff esponenziale sui blocchi (30s, 60s, 120s, tetto 300s), un secondo giro sugli annunci bloccati e, se gli passi `--min-coverage-ratio`, esce con codice 3 quando la coverage resta sotto soglia.
Non elimina il blocco: le run da IP residenziale o negli orari meno saturi restano più stabili.
Gamelife e Subito, sul droplet, non passano dallo scrape datacenter (vedi `deploy/server_job.sh`).

## Layout

```
config.toml          configurazione fonti (senza segreti)
.env.example         variabili richieste
run.py               CLI
server.py            API e viewer
viewer/              interfaccia usata
scrapers/            una fonte per modulo
deploy/              systemd, nginx, backup, env del server
docs/                runbook e routine
tests/               pytest
```
