# Xbox Tracker

Tracker multi-sorgente per prezzi console Xbox.
Scrapa 8 fonti online, classifica automaticamente gli annunci con AI, calcola il fair value di mercato e notifica le occasioni d'acquisto via macOS e Telegram.

## Architettura

```
Scraper (8 fonti)  ->  Snapshot JSON  ->  tracker.db (SQLite)  ->  API HTTP  ->  Viewer Web
       |                                        |
  Playwright/requests                    Change detection
  Algolia API                            AI classification
                                         Fair value engine
                                         Alert (macOS + Telegram)
```

### Componenti principali

| File | Ruolo |
|------|-------|
| `run.py` | CLI orchestrator — scraping, cleanup, cron, classificazione, viewer |
| `server.py` | Server HTTP con API REST JSON (30+ endpoint GET, 2 POST) |
| `db.py` | Modulo prodotti shop — schema `products` + `state_changes` |
| `db_subito.py` | Modulo annunci Subito.it — schema `ads` + `ad_changes` |
| `db_ebay.py` | Modulo eBay venduti — schema `sold_items` + `sold_changes` |
| `migrations.py` | Framework migrazioni schema versionato con namespace |
| `classifier.py` | Pipeline classificazione: regole + CEX matching + fallback AI |
| `ai_classifier.py` | Filtro AI asincrono: distingue console hardware da giochi/accessori |
| `valuation.py` | Fair value engine: mediana ponderata CEX/Subito/eBay con backtesting |
| `alerts.py` | Sistema alert prezzi con notifiche macOS + Telegram |
| `verify_sold.py` | Verifica asincrona stato venduto annunci Subito |
| `model_rules.py` | Classificazione deterministica regex (famiglia, segmento, edizione) |
| `settings.py` | Configurazione da `config.toml` + override ENV |
| `id_utils.py` | Generazione ID stabili per prodotti |
| `run_report.py` | Tracciamento esecuzione con timing e report JSON |

### Scrapers

| Fonte | Metodo | File |
|-------|--------|------|
| CEX (it.webuy.com) | Algolia API | `scrapers/cex.py` |
| Gamelife.it | Playwright (context fresco per Cloudflare) | `scrapers/gamelife.py` |
| GamePeople.it | Playwright | `scrapers/gamepeople.py` |
| Gameshock.it | requests + BeautifulSoup | `scrapers/gameshock.py` |
| Rebuy.it | requests + BeautifulSoup | `scrapers/rebuy.py` |
| Subito.it | Playwright (bypass Akamai) | `scrapers/subito.py` |
| eBay.it | Playwright (solo SOLD items) | `scrapers/ebay.py` |
| JollyRogerBay | Disabilitato (solo giochi) | `scrapers/jollyrogerbay.py` |

### Viewer Web

UI single-page statica in `viewer/` con dark mode e 9 tab:

| Tab | Descrizione |
|-----|-------------|
| Home | Griglia base models per famiglia (Series/One/360/Original) |
| Mercato | Solo prodotti disponibili con prezzo base model |
| Tutto | Panoramica globale con statistiche aggregate per store |
| Catalogo | Tabella prodotti shop con filtri, ordinamento e toggle base model |
| Subito | Annunci marketplace con stato AI (approved/pending/rejected) e storico prezzi |
| eBay | Venduti eBay con statistiche prezzo per famiglia |
| Statistiche | Z-score, spread, trend analysis, clustering, fair value |
| Trend | Grafici SVG storici dei prezzi per i modelli base con filtri periodo/famiglia |
| Ricerca | Ricerca avanzata per attributi (famiglia, modello, edizione, colore, storage) |

Funzionalita viewer:
- **Dark mode** — toggle chiaro/scuro con persistenza `localStorage`
- **Event delegation** — listener delegati per performance con migliaia di annunci
- **Caricamento resiliente** — `Promise.allSettled` con fallback per ogni API
- **Token bootstrap** — acquisizione automatica del token ad ogni avvio (no stale tokens)

Moduli JavaScript:
- `modules/api.js` — client REST con bearer token auth (bootstrap automatico)
- `modules/sanitize.js` — XSS prevention (sanitizeText, sanitizeUrl, sanitizeRecord)
- `modules/state.js` — gestione stato globale

## Database

Tutti i dati risiedono in un singolo file `tracker.db` (SQLite) con 8 tabelle:

| Tabella | Dominio | Record tipici |
|---------|---------|---------------|
| `categories` | Categorie console | 4 |
| `storage_sizes` | Dimensioni storage | 11 |
| `products` | Prodotti shop (6 fonti) | ~400 |
| `state_changes` | Storico prezzi/disponibilita shop | ~600+ |
| `ads` | Annunci Subito.it | ~18.000 |
| `ad_changes` | Storico modifiche annunci | ~18.500 |
| `sold_items` | eBay venduti | ~2.800 |
| `sold_changes` | Variazioni prezzo venduti | ~200 |

Le migrazioni sono gestite con namespace (`products`, `ads`, `ebay`) nella tabella `schema_migrations`.

## Pipeline di classificazione

```
Titolo annuncio
    |
    v
[1] Regole deterministiche (model_rules.py)
    -> famiglia, segmento, edizione, canonical_model
    |
    v
[2] CEX Matching (Jaccard similarity)
    -> boost confidence se match >= 0.45
    |
    v
[3] AI Fallback (Claude Haiku)
    -> solo per family="other" o confidence < 0.6
    -> batch da 15, max_tokens=4096
```

### AI Classifier (filtro hardware)

Processo separato per Subito: distingue console hardware da giochi/accessori.
- Score 0-100 (100 = console, 0 = gioco)
- >= 75 → approved, <= 25 → rejected, 25-75 → pending
- Batch da 50, 5 batch paralleli (asyncio.gather)

## Fair Value Engine

Calcola il "prezzo equo" per ogni modello Xbox:

```
fair_value = weighted_average(
    CEX_net_price * 0.45,    # prezzi CEX al netto IVA 22%
    eBay_sold_median * 0.35,  # mediana venduti eBay
    Subito_median * 0.20      # mediana annunci Subito
)
```

Con trimmed mean (rimuove outlier 15%), backtesting vs eBay venduti (MAPE, MAE), e tuning automatico dei pesi.

## Comandi principali

```bash
# Scrape tutte le fonti abilitate
python3 run.py

# Scrape fonti specifiche
python3 run.py --source subito,ebay

# Scrape + viewer web
python3 run.py --all

# Pipeline completo: Subito + eBay + verify sold + AI classify + viewer
python3 run.py --full

# Solo viewer web (senza scraping)
python3 run.py --view

# Classificazione AI manuale
python3 run.py --classify
python3 run.py --classify --classify-limit 100 --classify-dry-run

# Report fair value
python3 run.py --valuation-report

# Tuning pesi fair value
python3 run.py --tune-valuation

# Test notifica Telegram
python3 run.py --test-telegram

# Maintenance: retention snapshot + archiviazione + VACUUM
python3 run.py --cleanup

# Crontab per scraping automatico (ogni 6 ore)
python3 run.py --setup-cron

# Scrape Subito mirato (una o più regioni)
python3 run.py --source subito --subito-region lombardia
python3 run.py --source subito --subito-region "lombardia,veneto" --subito-max-pages 120

# Deduplica snapshot Subito senza rieseguire scraping
python3 run.py --subito-dedup --subito-dedup-latest 5
python3 run.py --subito-dedup --subito-dedup-files "data/subito_a.json,data/subito_b.json" --subito-dedup-update-db

# Verify sold mirata su annunci Xbox-only
python3 run.py --verify-sold 500 --verify-xbox-only --verify-chunk-size 200
```

## Configurazione

File `config.toml` con override via variabili d'ambiente `TRADER_*`:

| Variabile ENV | Default | Descrizione |
|---------------|---------|-------------|
| `TRADER_VIEWER_HOST` | `127.0.0.1` | Host del viewer |
| `TRADER_VIEWER_PORT` | `8080` | Porta del viewer |
| `TRADER_VIEWER_OPEN_BROWSER` | `true` | Apri browser automaticamente |
| `TRADER_API_TOKEN` | (vuoto) | Token bearer per API (auto-generato se vuoto) |
| `TRADER_PLAYWRIGHT_CHANNEL` | `chrome` | Browser: `chrome` (sistema) o `chromium` (bundled) |
| `TRADER_RETENTION_KEEP` | `30` | Snapshot da mantenere per fonte |
| `TRADER_ARCHIVE_AFTER_DAYS` | `45` | Comprimi snapshot dopo N giorni |
| `TRADER_TELEGRAM_ENABLED` | `false` | Abilita notifiche Telegram |
| `TRADER_TELEGRAM_TOKEN` | (vuoto) | Token bot Telegram (da @BotFather) |
| `TRADER_TELEGRAM_CHAT_ID` | (vuoto) | Chat ID destinatario Telegram |
| `ANTHROPIC_API_KEY` | — | Chiave API per classificazione AI |
| `ANTHROPIC_MODEL` | (auto-detect) | Modello Claude da usare |

## Notifiche Telegram

Le notifiche deal vengono inviate sia come alert macOS nativi che come messaggi Telegram (se configurato).

### Setup

1. Crea un bot con **@BotFather** su Telegram (`/newbot`)
2. Copia il token del bot
3. Avvia il bot e invia un messaggio, poi visita `https://api.telegram.org/botTUO_TOKEN/getUpdates` per ottenere il `chat_id`
4. Configura in `config.toml`:

```toml
[telegram]
enabled   = true
bot_token = "123456789:ABCdefGHIjklMNOpqrSTUvwxYZ"
chat_id   = "987654321"
```

5. Testa con `python3 run.py --test-telegram`

In alternativa, usa variabili d'ambiente:
```bash
export TRADER_TELEGRAM_ENABLED=true
export TRADER_TELEGRAM_TOKEN="123456789:ABCdef..."
export TRADER_TELEGRAM_CHAT_ID="987654321"
```

## Sicurezza

- Server bindato a `127.0.0.1` di default (solo localhost)
- Tutti gli endpoint API (GET e POST) protetti con Bearer token
- Token acquisito dal viewer via endpoint bootstrap `/api/token` (no query string)
- Token rinnovato ad ogni avvio server (nessun token stale in sessionStorage)
- Token in `sessionStorage` (si cancella alla chiusura del browser)
- Input sanitizzato lato client (XSS prevention)
- Input validato lato server (`_safe_int` con clamp)
- Timing-safe comparison per token (`secrets.compare_digest`)

## Alert

Il sistema controlla dopo ogni scrape Subito se ci sono annunci sotto soglia:

```
soglia = MIN(prezzo_CEX_base_model) x (1 - 0.22 IVA)
```

Se un annuncio approvato dall'AI ha prezzo < soglia:
- Invia notifica **macOS** nativa (osascript)
- Invia messaggio **Telegram** (se configurato)

I notificati sono tracciati in `alert_log.json` (con purge automatica a 90 giorni).

## Requisiti

- Python 3.10+
- Dipendenze: `pip install -r requirements.lock`
- Browser: `python -m playwright install chromium`

## Test e CI

- Unit/smoke test: `pytest`
- Lint: `ruff check .`
- CI GitHub Actions:
  - **Quality Gate** su push/PR (lint + test)
  - **Daily Scrape** schedulato (scrape + cleanup + commit DB)

## Migrazione da vecchi DB

Se provieni da una versione con 3 DB separati (`trader.db`, `subito.db`, `ebay.db`):

```bash
python3 migrate_to_tracker.py
```

Lo script unisce i dati in `tracker.db` preservando tutti i record. I vecchi file restano come backup.

## Struttura directory

```
trader/
├── config.toml              # Configurazione multi-fonte + Telegram
├── run.py                   # CLI orchestrator
├── server.py                # Server HTTP + API REST
├── db.py                    # Modulo DB prodotti shop
├── db_subito.py             # Modulo DB annunci Subito
├── db_ebay.py               # Modulo DB venduti eBay
├── migrations.py            # Framework migrazioni schema
├── classifier.py            # Pipeline classificazione (rules + AI)
├── ai_classifier.py         # Filtro AI hardware/giochi
├── valuation.py             # Fair value engine + backtesting
├── alerts.py                # Alert prezzi + notifiche macOS + Telegram
├── verify_sold.py           # Verifica stato venduto Subito
├── model_rules.py           # Regole classificazione regex
├── settings.py              # Config validation + ENV override
├── id_utils.py              # Generazione ID stabili
├── run_report.py            # Tracciamento esecuzione
├── migrate_to_tracker.py    # Script migrazione DB (una tantum)
├── tracker.db               # Database SQLite unificato
├── scrapers/
│   ├── base.py              # Utilities comuni (clean_price, retry, save_snapshot)
│   ├── cex.py               # CEX — Algolia API
│   ├── ebay.py              # eBay — Playwright (SOLD only)
│   ├── gamelife.py          # GameLife — Playwright + Cloudflare bypass
│   ├── gamepeople.py        # GamePeople — Playwright
│   ├── gameshock.py         # GameShock — requests
│   ├── rebuy.py             # ReBuy — requests
│   ├── subito.py            # Subito.it — Playwright + Akamai bypass
│   └── jollyrogerbay.py     # Disabilitato (solo giochi)
├── viewer/
│   ├── index.html           # SPA shell (9 tab + dark mode toggle)
│   ├── app.js               # Logica principale (rendering, filtri, sort, grafici SVG)
│   ├── style.css            # Stili responsive + dark theme via CSS variables
│   └── modules/
│       ├── api.js           # REST client + bearer token bootstrap (auto-refresh)
│       ├── sanitize.js      # XSS prevention
│       └── state.js         # Stato globale
├── data/                    # Snapshot JSON (non versionati)
│   └── archive/             # Snapshot compressi (.json.gz)
├── logs/                    # Report runtime
├── tests/                   # Test suite
├── docs/runbook.md          # Procedure operative
└── .github/workflows/       # CI/CD (Quality Gate + Daily Scrape)
```

## Criticità note: verify\_sold

Questa sezione documenta i problemi ricorrenti rilevati analizzando i run CI del 11 Aprile 2026 (v0→v3) e le sessioni di sviluppo precedenti.

---

### 1. Cloudflare cffi-block: degrado massiccio della coverage

**Descrizione**
Il fast-path di verifica (`cffi` — HTTP diretto senza Playwright) viene bloccato da Cloudflare con risposte HTTP non classificabili. Quando Cloudflare entra in "burst block mode", interi chunk da 350 annunci vengono skippati in meno di 1 secondo (`reason: skipped:cffi-block=350`).

**Impatto osservato (4 run dello stesso giorno)**

| Run | Ora | Pool | Verificati | Coverage | cffi-block chunks |
|-----|-----|------|-----------|----------|-------------------|
| v0 | 10:09 | 7.012 | 5.350 | **76%** | 0 |
| v1 | 12:11 | 6.868 | 6.088 | **89%** | 2 |
| v2 | 15:24 | 6.854 | 2.802 | **41%** | 13 |
| v3 | 21:48 | 9.471 | 2.464 | **26%** | 20 consecutivi |

La coverage crolla da 89% a 26% nello stesso giorno in base all'orario. I run mattutini sono sistematicamente migliori.

**Causa radice**
Subito.it usa Cloudflare con rate limiting adattivo. Alte concorrenze (`--concurrency 18`) e chunk rapidi saturano la finestra di tolleranza. I run pomeridiani/serali partono già con un budget IP esaurito da sessioni precedenti.

**Stato:** aperto — nessuna soluzione stabile implementata.

---

### 2. Anti-burst pause fissa non scala contro blocchi sostenuti

**Descrizione**
Il meccanismo anti-burst (commit `8329759`) rileva N chunk consecutivi con cffi-block e applica una pausa di **30 secondi fissi** prima di riprovare. Quando Cloudflare rimane bloccato per molti chunk consecutivi, la pausa si ripete indefinitamente senza escalation.

**Impatto osservato (v3)**
- 20 burst rilevati consecutivi → 20 × 30s = **10 minuti sprecati in pause**
- Cloudflare non si sblocca dopo 30s: la pausa non è abbastanza lunga
- Il run termina in soli 13 minuti con 74% degli annunci skippati

**Comportamento attuale**
```
WARNING: Cloudflare burst rilevato (N chunk consecutivi con cffi-block): pausa 30s
# N cresce ma la pausa resta sempre 30s
```

**Comportamento desiderato**
Backoff esponenziale: 30s → 60s → 120s → cap a 300s, oppure dopo soglia (es. 5 burst consecutivi) abbandonare il cffi per il resto del run e passare a Playwright puro.

**Stato:** aperto — patch necessaria in `verify_sold.py`.

---

### 3. Nessun fallback Playwright per annunci cffi-blocked

**Descrizione**
Quando un chunk viene skippato per cffi-block, gli annunci rimangono con status `pending` nel DB e vengono esclusi dai successivi run solo dopo che il loro status cambia. Non c'è un fallback automatico a Playwright per verificare quegli stessi annunci nello stesso run.

**Impatto**
- In v3: 6.998 annunci lasciati `pending` su 9.471 — rientreranno nel pool del prossimo run gonfiandolo ulteriormente
- Effetto cumulativo: più run con coverage bassa → il pool pending cresce → i run successivi peggiorano

**Mitigazione parziale**
La flag `--selection-sample N` randomizza quali annunci vengono prioritizzati, evitando che gli stessi annunci vengano sempre skippati, ma non risolve il problema strutturale.

**Stato:** aperto — nessun fallback implementato.

---

### 4. Inflazione del pool pending dopo restore DB

**Descrizione**
Il restore del DB da backup (commit `0644f80`, 27 Mar) ha riportato in stato `pending` annunci che erano già stati verificati. Il pool da verificare in v3 è saltato a **9.471** (+35% rispetto ai run precedenti ~7.000).

**Effetto combinato**
Pool più grande × coverage bassa per cffi-block = impatto molto maggiore degli skip.

**Stato:** conseguenza nota del restore — non evitabile retroattivamente. Per futuri restore: eseguire un run verify-sold dedicato subito dopo il restore, in orario mattutino.

---

### 5. Sensibilità oraria non gestita

**Descrizione**
I run mattutini (10:00–13:00) ottengono sistematicamente coverage >75%. I run pomeridiani/serali (15:00+) subiscono blocchi cffi massivi. Il workflow CI è schedulato ogni 2 ore indipendentemente dall'orario.

**Dati osservati**
- v0 (10:09): 76.3%, v1 (12:11): 88.6%
- v2 (15:24): 40.8%, v3 (21:48): 26.0%

**Stato:** aperto — considerare di concentrare i run verify-sold nelle fasce 06:00–12:00.

---

### 6. Timeout e 403 in pre-anti-burst (v0)

**Descrizione**
Prima dell'implementazione anti-burst, i blocchi Cloudflare si manifestavano come:
- 841 HTTP 403 (12.0% del pool)
- 698 timeout (wait completo prima del fallback)
- 123 errori di rete

Ogni timeout aspettava il timeout completo (`--nav-timeout-ms 7000`), sprecando ~81 minuti di runtime cumulativo solo in attese inutili.

**Risoluzione parziale**
Il cffi fast-path + anti-burst hanno eliminato quasi completamente i 403 e i timeout (v1: 0.5% e 179 timeout; v3: 0.18% e 0 timeout). Ma il problema si è spostato verso i cffi-block.

**Stato:** parzialmente risolto — i timeout sono eliminati ma i cffi-block ora skippano silenziosamente senza Playwright fallback.

---

### 7. Coverage warning senza fail del run

**Descrizione**
Il warning `Coverage bassa (26.0% < 70%)` viene loggato ma il run termina con exit code 0 e fa il commit del DB. Un run con 26% di coverage potrebbe mancare centinaia di annunci venduti nel frattempo.

**Rischio**
- Annunci venduti non marcati rimangono visibili nel viewer
- Il fair value di Subito viene calcolato su un campione non rappresentativo

**Stato:** aperto — valutare se forzare exit code ≠ 0 sotto una soglia critica (es. < 20%), o almeno sopprimere il commit automatico.

---

### 8. Totale=31.501 vs ~33.600 — discrepanza post-restore

**Descrizione**
Dopo il restore del DB, il campo `totale` (tutti gli annunci in DB) è sceso da ~33.600 a 31.501. Questo indica che il backup usato (27 Mar) non conteneva ~2.100 annunci scrappati nelle settimane successive.

**Stato:** dato acquisito — i 2.100 annunci mancanti verranno reintegrati man mano che Subito viene riscrappato nelle run successive.

---

### Riepilogo priorità interventi

| # | Problema | Impatto | Priorità |
|---|----------|---------|----------|
| 2 | Anti-burst pause fissa | Alto — spreca 10 min per run | P0 |
| 3 | Nessun Playwright fallback per cffi-block | Alto — 74% skip in v3 | P0 |
| 7 | Coverage warning senza fail | Medio — commit DB incompleto | P1 |
| 5 | Sensibilità oraria non gestita | Medio — scheduling subottimale | P1 |
| 1 | Cloudflare cffi-block strutturale | Alto — causa root | P2 (esterno) |
| 4 | Pool inflazione post-restore | Basso — una tantum | P3 |

---

## Troubleshooting

| Problema | Soluzione |
|----------|----------|
| Playwright/Chrome non disponibile | `TRADER_PLAYWRIGHT_CHANNEL=chromium` |
| API 401 Unauthorized | Hard refresh (`Cmd+Shift+R`), il token si rinnova automaticamente |
| Config non valida | Avvio fallisce con `ConfigError` — controlla `config.toml` |
| Porta occupata | Auto-increment automatico (porta+1), max 10 tentativi |
| Vecchi DB da migrare | `python3 migrate_to_tracker.py` |
| Dark mode non si attiva | Hard refresh per forzare il ricaricamento del CSS |
| Telegram non invia | Verifica `bot_token` e `chat_id`, testa con `--test-telegram` |

Per procedure operative e incident response: vedi `docs/runbook.md`.
