# TRADER — FLUSSO DI ESECUZIONE

> Documento di riferimento tecnico per il progetto Xbox Tracker.
> Aggiornato: 22 aprile 2026.

---

## 1. Overview

Trader è un price tracker specializzato per hardware console Xbox sul mercato italiano dell'usato. Monitora 8 fonti di dati (negozi fisici/online + marketplace) e le aggrega in un unico database SQLite (`tracker.db`), applicando una pipeline di classificazione AI per identificare e categorizzare i modelli Xbox.

Il sistema ha tre macro-funzioni:

- **Raccolta dati**: scraping periodico delle fonti, ingestion nel DB con change detection
- **Classificazione**: pipeline a 3 livelli (regole regex → matching CEX → Claude Haiku) che assegna a ogni annuncio `console_family`, `canonical_model`, `model_segment`, `edition_class`
- **Presentazione**: server HTTP che espone un'API REST e serve una SPA con 9 tab per esplorare prezzi, trend e valutazioni

Tutto lo stato persistente vive in `tracker.db`. I file JSON delle scrape sono snapshot temporanei su disco (cartella `data/`), archiviati come `.json.gz` dopo N giorni e poi eliminati.

---

## 2. Entry Points

### `run.py` — orchestratore principale

È il punto d'ingresso per tutte le operazioni batch. Usa `argparse` e instrada il flusso in base ai flag ricevuti. All'avvio inizializza sempre i tre namespace DB (`init_db()` per products, ads, ebay), poi sceglie il ramo di esecuzione:

| Flag | Azione |
|------|--------|
| `--source <nome>` / nessun flag | Scraping delle fonti abilitate |
| `--classify` | Pipeline di classificazione attributi |
| `--ai-classify` | Filtro AI (Haiku) su Subito e/o eBay |
| `--verify-sold N` | Verifica annunci Subito scaduti via Playwright |
| `--view` | Avvia solo il server web |
| `--full` | Pipeline completa (scrape + verify + AI + classify + viewer) |
| `--subito-rebuild-all` | Ricostruzione completa dataset Subito in 5 step |
| `--cleanup` | Retention snapshot + archiviazione + VACUUM DB |
| `--valuation-report` / `--tune-valuation` | Calcolo fair value e tuning pesi |
| `--setup-cron` | Installa crontab per esecuzione automatica |

Ogni esecuzione è wrappata in un `RunReport` che, al termine, scrive un JSON di log in `logs/` con comando, timing, step, errori e statistiche.

### `server.py` — HTTP API + viewer

Avviato da `run.py --view` (o indirettamente da `--full`). Espone ~30 endpoint REST su `127.0.0.1:8080` e serve i file statici del viewer SPA. Usa solo la stdlib (`http.server.SimpleHTTPRequestHandler`), senza framework. L'autenticazione è Bearer token; il token viene restituito senza auth da `/api/token` (accessibile solo da localhost). Il viewer SPA lo recupera al bootstrap e lo conserva in `sessionStorage`.

### `classifier.py` — classificazione attributi

Può girare sia da `run.py --classify` che standalone. Legge dal DB gli annunci Subito con classificazione mancante o versione obsoleta, applica la pipeline a 3 livelli (→ sezione 4b) e aggiorna i record.

---

## 3. Flusso Principale di Scraping

```
run.py
  └─ cmd_scrape(sources)
       ├─ per ogni source abilitata:
       │    ├─ _run_scraper(source)            # importa scrapers/<source>.py dinamicamente
       │    │    └─ mod.main() → Path          # restituisce il path dello snapshot JSON
       │    ├─ _update_db_from_snapshot(path)  # ingestion + change detection
       │    └─ _apply_retention(source)        # elimina snapshot oltre il limite
       └─ _archive_old_snapshots()             # comprimi in .gz quelli > N giorni
```

### Scrapers disponibili (`scrapers/`)

| Source | Tipo | Note |
|--------|------|------|
| `cex` | Shop usato | Prezzi acquisto/vendita CEX Italy |
| `gamelife` | Shop nuovo/usato | Filtra URL blocklist (ROG Ally) |
| `gamepeople` | Shop nuovo | Condizione sempre "Nuovo" |
| `gameshock` | Shop misto | Rileva "usata" dal titolo via regex |
| `jollyrogerbay` | Shop usato | — |
| `rebuy` | Shop usato | Varianti per grado qualità, dedup per URL disabilitata |
| `subito` | Marketplace | Playwright + anti-bot, scrape per regione |
| `ebay` | Marketplace (sold) | Solo articoli già venduti |

Ogni scraper implementa `main() → str` (path del JSON) e usa utility comuni da `scrapers/base.py` (`save_snapshot`, `deduplicate`, `launch_chromium`).

### Formato snapshot JSON

```json
{
  "source": "cex",
  "scraped_at": "2026-04-22T10:30:00+00:00",
  "total": 42,
  "url": "https://it.webuy.com/...",
  "products": [
    { "name": "Xbox Series X 1TB", "price": 199.0, "available": true, ... }
  ]
}
```

### Ingestion nel DB

`_update_db_from_snapshot()` smista in base alla sorgente:

- `source == "ebay"` → `db_ebay.process_sold_items()` → tabella `sold_items`
- `source == "subito"` → `db_subito.process_ads()` + `alerts.check_alerts()`
- tutto il resto → `db.process_products()` → tabella `products`

Ogni modulo di ingestion fa upsert: confronta il record esistente con quello nuovo e inserisce in `*_changes` **solo se** prezzo o disponibilità sono variati. I duplicati sono ignorati in modo idempotente.

Subito ha una logica aggiuntiva: dopo l'upsert degli annunci, `alerts.check_alerts()` confronta ogni annuncio disponibile con le soglie CEX (prezzo min CEX × 0.78) e spara notifica macOS + Telegram per i nuovi deal, deduplicando via `alert_log.json`.

### Retention e archiviazione snapshot

Dopo ogni scrape, due processi separati gestiscono la pulizia su disco:

- `_apply_retention(source)` → elimina i file più vecchi tenendo solo gli ultimi N (default: 30 per sorgente)
- `_archive_old_snapshots()` → comprime in `data/archive/*.json.gz` tutti i `.json` più vecchi di M giorni (default: 7)

---

## 4. Flusso di Classificazione AI

La classificazione opera in **due fasi distinte** con scopi diversi.

### 4a. Filtro hardware — `ai_classifier.py` (`run.py --ai-classify`)

**Scopo:** determinare se un annuncio vende realmente hardware console Xbox (non giochi, accessori, ecc.) e assegnare una prima classificazione del modello.

**Flusso:**

```
_load_rows()                    # annunci con ai_status='pending' (o tutti se --all)
    ↓
suddivisione in batch da N annunci (default 50)
    ↓
asyncio.gather(tutti i batch)   # concorrenza limitata da asyncio.Semaphore (default 5)
    ↓
per ogni batch → classify_batch(client, batch)
  ↓  chiamata API Anthropic (claude-haiku-4-5-20251001, temp=0, max_tokens=4096)
  ↓  sistema invia: titolo (max 240 char) + body (max 960 char) + prezzo
  ↓  Claude risponde: array JSON
     [{id, console_confidence, family, canonical, storage_gb, edition}]
    ↓
validazione campi (whitelist per family, canonical, edition)
    ↓
UPDATE ads SET ai_status, ai_confidence, console_family, canonical_model,
              classify_method='ai:v1', classify_confidence
```

**Soglie di decisione:**

| Confidence | ai_status | Azione |
|-----------|-----------|--------|
| ≥ 75 | `approved` | Classificazione salvata (family + canonical + edition) |
| ≤ 25 | `rejected` | family/canonical = 'other', classificazione azzerata |
| 26–74 | `pending` | Solo ai_confidence aggiornata, classificazione rules intatta |

**Fallback regex:** se la risposta JSON è malformata, un regex cerca almeno `id` e `console_confidence` nel testo grezzo e salva risultati parziali (senza classification).

La versione eBay (`run_ebay_classifier`) usa lo stesso SYSTEM_PROMPT e la stessa logica, ma opera su `sold_items`. Poiché `sold_items` non ha `ai_status`, usa `classify_method='ai:v1'` come tracciatore di stato.

### 4b. Classificazione attributi — `classifier.py` (`run.py --classify`)

**Scopo:** assegnare con alta precisione `model_segment`, `edition_class`, `canonical_model` tramite una pipeline ibrida a 3 livelli.

**Candidati:** annunci con `console_family='other'` OR `model_segment='unknown'` OR `canonical_model` NULL/vuoto OR `classify_version` diversa dalla corrente.

**Pipeline:**

```
[Livello 1] classify_title(rules_text, family_hint)   ← model_rules.py
  regex deterministiche su titolo + body_text
  → assegna family, segment, edition_class, canonical_model, confidence, method='rules:vX'

     ↓ se family != 'other' AND segment == 'base' AND edition == 'standard':

[Livello 2] _best_cex_match(name, family, cex_anchors)
  Jaccard similarity tra token del titolo e nomi prodotti CEX
  soglia ≥ 0.45 → canonical_model = match CEX, method='cex-match:v1'
  confidence = max(rules_conf, 0.65 + jaccard × 0.30)

     ↓ se ancora unresolved (family='other' OR segment='unknown' OR confidence < 0.6):

[Livello 3] classify_batch(ads, client)               ← Claude Haiku sincrono, batch=15
  input: titolo + body troncati
  output: [{id, family, segment, edition_class, canonical_model, confidence}]
  method = 'ai:claude-haiku-4-5-20251001'

     ↓
_apply_classifications() → UPDATE ads SET console_family, model_segment,
                           edition_class, canonical_model, classify_confidence,
                           classify_method, classify_version
```

I "CEX anchors" sono i prodotti CEX con `model_segment='base'` e `edition_class='standard'` usati come ground truth per il Jaccard. Il campo `classify_version` (es. `rules+ai:title+body:v2`) invalida la classificazione quando cambiano le regole: basta aggiornare la stringa costante per forzare il ricalcolo su tutti i record al prossimo run.

---

## 5. Flusso del Server Web

```
run.py --view
  └─ cmd_view() → server.start_server()
       └─ _make_handler(api_token, data_dir, sources_cfg, enabled_sources)
            └─ http.server.HTTPServer(host:port, Handler).serve_forever()
```

**Autenticazione:** ogni richiesta API verifica `Authorization: Bearer <token>`. L'unica eccezione è `/api/token` che restituisce il token senza auth (usabile solo da localhost per il bootstrap). Il viewer SPA salva il token in `sessionStorage` e lo allega a ogni chiamata successiva.

### Endpoint GET principali

| Endpoint | Descrizione |
|----------|-------------|
| `/api/token` | Bootstrap token (no auth richiesta) |
| `/api/sources` | Metadati di tutte le sorgenti abilitate + ultimi snapshot |
| `/api/latest?source=X` | Snapshot JSON più recente per la sorgente X |
| `/api/history?source=X` | Tutti gli snapshot storici di X |
| `/api/combined/latest` | Prodotti combinati da tutte le sorgenti shop (Subito escluso) |
| `/api/db/products` | Query diretta su `products` con filtri |
| `/api/db/ads` | Query su `ads` (Subito) con filtri e paginazione |
| `/api/db/sold` | Query su `sold_items` eBay |
| `/api/valuation` | Fair value per modello canonico |
| `/api/stats` | Statistiche aggregate |

### Endpoint POST

Richiedono auth. `/api/scrape` avvia un job di scraping in un **thread separato** (`_run_scrape_job`), cattura l'output riga per riga (max 200 righe in buffer) e lo espone via `/api/scrape/status`. Un lock (`threading.Lock`) garantisce un solo job attivo alla volta. `/api/classify` lancia la classificazione attributi.

### Viewer SPA

`viewer/` è una SPA a 9 tab: Home, Riepilogo, Catalogo, Subito, eBay, Statistiche, Trend, Ricerca. I file statici sono serviti da `SimpleHTTPRequestHandler` con header `Cache-Control: no-cache` per forzare sempre la versione aggiornata. Il JS usa `sanitize.js` per prevenire XSS nei contenuti dinamici mostrati a schermo.

---

## 6. Gestione del Database

### Schema `tracker.db`

Il database è unico ma logicamente suddiviso in tre namespace, ognuno con versioning di migrazione indipendente gestito da `migrations.py`. Questo evita conflitti di version number tra i tre moduli che storicamente erano DB separati.

**Namespace `products`** (gestito da `db.py`):

| Tabella | Descrizione |
|---------|-------------|
| `categories` | Categorie console (Xbox Series, Xbox One, Xbox 360, Xbox Original) |
| `storage_sizes` | Taglie storage (512 GB, 1 TB, 2 TB…) |
| `products` | Prodotto unico per `(source, name, condition)` con `last_price`, `last_available`, classificazione |
| `state_changes` | Storico ogni cambio prezzo/disponibilità con timestamp |

**Namespace `ads`** (gestito da `db_subito.py`):

| Tabella | Descrizione |
|---------|-------------|
| `ads` | Annuncio unico per `urn_id` (es. `SUBITO-639766302`) con classificazione AI e regole. Colonne chiave per verify_sold: `verify_status` (buyable/sold/pending), `last_verified_at` (migrazione v8, per selezione stratificata) |
| `ad_changes` | Storico cambi prezzo/disponibilità annunci |

**Namespace `ebay`** (gestito da `db_ebay.py`):

| Tabella | Descrizione |
|---------|-------------|
| `sold_items` | Articolo eBay venduto, chiave unica per URL/ID |
| `sold_changes` | Storico cambi |

**Tabella trasversale:**
- `schema_migrations(namespace, version, name, applied_at)` — versioning migrazioni con primary key composta

### Flusso dati nel DB

```
Scrape JSON
    ↓
process_products() / process_ads() / process_sold_items()
    ├─ upsert record principale
    │    INSERT OR IGNORE (nuovo record) + UPDATE se prezzo/disponibilità variati
    └─ INSERT INTO *_changes (solo se effettivamente cambiato)

AI classifier (ai_classifier.py)
    └─ UPDATE ads/sold_items SET ai_status, ai_confidence, console_family,
                                 canonical_model, classify_method='ai:v1'

Attribute classifier (classifier.py)
    └─ UPDATE ads SET console_family, model_segment, edition_class,
                      canonical_model, classify_confidence, classify_method, classify_version

Valuation (valuation.py)
    └─ SELECT + computed in-memory (no tabella dedicata, risultati ritornati via API)

Cleanup
    ├─ _apply_retention()    → DELETE file snapshot su disco
    ├─ db.clean_db()         → DELETE record obsoleti/orfani da products
    └─ VACUUM tracker.db     → deframmentazione file SQLite
```

### Deduplication Subito

Gli annunci Subito possono apparire in più snapshot regionali. La deduplicazione avviene a due livelli: nel client durante lo scraping (flag `strict_xbox` che filtra i non-Xbox prima del salvataggio) e post-facto via `cmd_subito_dedup()` che unisce snapshot multipli per `urn_id`, tenendo il record più completo.

### Valuation (`valuation.py`)

Il fair value di ogni modello è calcolato come media pesata con trimmed mean (15% outlier removal su entrambe le code):

| Sorgente | Peso default |
|----------|-------------|
| CEX (base + standard) | 45% |
| eBay sold | 35% |
| Subito approved | 20% |

I pesi possono essere ottimizzati automaticamente con `--tune-valuation` che prova combinazioni sistematiche e misura MAPE/MAE contro i prezzi reali, salvando il risultato in `logs/valuation_tuning_latest.json`.

---

## 7. Safeguards e Recovery

### Health check (`db_safeguards.py`)

La classe `DatabaseHealthCheck` esegue 8 controlli su ogni DB: esistenza file, leggibilità, dimensione, integrità SQLite (`PRAGMA integrity_check`), presenza tabelle attese, connessioni attive, file WAL orfani, conteggio record. L'esito aggregato è `healthy` / `unhealthy`. Utile come pre-flight check prima di operazioni critiche.

### WAL mode e backup

SQLite è configurato in WAL (Write-Ahead Logging): i file `-shm` e `-wal` affiancano il DB principale e vengono fusi al checkpoint. Questo permette letture concorrenti durante un write in corso. Il sistema mantiene backup nominali (`tracker.db.backup.<timestamp>`) creati prima di operazioni rischiose. I passi di recovery da corruzione sono in `docs/runbook.md`.

### Alert deduplication

`alerts.py` salva in `alert_log.json` ogni `urn_id` per cui è già stata inviata una notifica. Prima di ogni alert controlla il log per evitare duplicati. Il log viene purgato periodicamente dalle entry più vecchie di 90 giorni. Un cap di 50 alert per run (`_MAX_ALERTS_PER_RUN`) impedisce flood in caso di reset accidentale del log.

### Playwright anti-detection

I browser contexts usano `patchright` + `playwright_stealth` con:

- `navigator_webdriver = False` (nasconde il flag webdriver)
- `navigator_platform_override = 'MacIntel'`
- `navigator_languages_override = ('it-IT', 'it')`

Per `verify_sold`, il sistema usa `curl_cffi` come primo tentativo (più veloce, bypassa Cloudflare a livello TLS), con fallback a Playwright per i casi "unknown". Il browser viene riavviato ogni N chunk (default: 2) per evitare fingerprinting progressivo.

### Pipeline anti-Akamai in `verify_sold` (ottimizzata apr 2026)

Il sistema di verifica integra 5 meccanismi di difesa contro il ban Akamai:

1. **Rotazione fingerprint cffi** — `_CFFI_PROFILES = (chrome124, chrome128, chrome131, chrome136)` ruotati a ogni chunk per distribuire il JA4. Hardcoded `chrome136` rimpiazzato con parametro `cffi_impersonate`.

2. **Selezione stratificata** — `tiered_selection=True` (default): tier1 = ads stale (>24h) + mai verificati + pending; tier2 = 30% campione casuale degli ads recenti. Riduce il volume da ~7000 a ~2500-3000 per run (meno burst Akamai). Richiede migrazione DB v8 (`last_verified_at` + index). Sul primo run post-migrazione tutti gli ads hanno `last_verified_at=NULL` → tier1 li cattura tutti (zero regressione).

3. **Partial-block skip** — Se 50–84% di un chunk ritorna "unknown" da cffi (blocco parziale, non abbastanza per il full-block a 85%), gli unknown vengono marcati `pending` invece di essere inviati a Playwright dove riceverebbero 403. Risparmio stimato: ~16 min/chunk parzialmente bloccato.

4. **Retry pass intra-sessione** — Dopo il main loop, se ci sono ads bloccati da cffi-block e rimangono >120s di runtime, esegue `_warmup_probe` per verificare se l'IP si è ripreso. In caso positivo, rilancia `_process_rows` sui soli `blocked_rows` con profilo cffi ruotato. Recupero stimato: ~50% degli ads bloccati quando il backoff esponenziale ha avuto effetto.

5. **Scheduling orario ottimale** — Cron `0 8,13 * * *` (10:00 + 15:00 CEST). Dati empirici 7 run apr 2026: cffi-block 8–17% nella fascia 10:00–18:00 CEST vs 30–34% nelle fasce 22:00–07:00 CEST → coverage attesa 82–91% vs 65–73%.

### Fail-fast in `verify_sold`

Il processo si interrompe automaticamente se si verificano le condizioni:

- Dopo almeno `fail_fast_min_attempts` tentativi (default: 400)
- Ratio bloccati > `fail_fast_blocked_ratio` (0.85)
- Oppure ratio HTTP 403 > `fail_fast_403_ratio` (0.60)

Questo evita inutile consumo di risorse e potenziali ban IP in caso di blocco da parte di Subito.

### Gestione errori scraping

Ogni scraper è wrappato in try/except: se `mod.main()` solleva un'eccezione, l'errore viene loggato e registrato nel `RunReport`, ma il processo continua con le sorgenti successive (nessun fail totale per un singolo scraper). Un snapshot dichiarato ma non trovato su disco viene trattato come errore soft.

---

## 8. Ciclo di Esecuzione Automatica

### Setup cron (`run.py --setup-cron`)

Installa due voci nel crontab dell'utente corrente, identificate da marker commentati che permettono a `--setup-cron` di sovrascriverle in modo idempotente senza duplicare:

```cron
# Scraping ogni 6 ore
0 */6 * * *  cd "/path/trader" && python3 run.py --source subito >> logs/cron.log 2>&1
             # xbox-tracker-cron:scrape:subito

# Verifica venduti ogni giorno alle 00:30
30 0 * * *   cd "/path/trader" && python3 run.py --verify-sold 1200 \
             --verify-chunk-size 300 --verify-max-runtime-minutes 50 \
             --verify-browser-restart-every 3 --concurrency 5 >> logs/cron.log 2>&1
             # xbox-tracker-cron:verify-sold:daily
```

### Pipeline `--full` (esecuzione manuale completa)

Sequenza in 6 step con log esplicito per ogni fase:

```
[1/6] Scrape Subito        → cmd_scrape(["subito"])
[2/6] Scrape eBay sold     → cmd_scrape(["ebay"])
[3/6] Verify sold Subito   → asyncio.run(verify_batch(...))
[4/6] AI classify          → run_ai_classifier() + run_ebay_classifier()
                             ↳ saltato se ANTHROPIC_API_KEY non presente
[5/6] Classify attributi   → run_classifier()
                             ↳ saltato se ANTHROPIC_API_KEY non presente
[6/6] Avvio viewer         → start_server()
```

### Pipeline `--subito-rebuild-all` (ricostruzione completa)

Versione aggressiva per riallineare tutto il dataset Subito in 5 step:

```
[1/5] Scrape completo Subito
[2/5] Verify sold incrementale (batch=2000, concurrency=5, max_runtime=50 min)
[3/5] Reset ai_status/ai_confidence su TUTTI gli annunci
      → run_ai_classifier(classify_all=True, reset_first=True)
[4/5] Riclassifica TUTTI gli attributi
      → run_classifier(rebuild_all=True)
[5/5] Done — usa --view per validare
```

### GitHub Actions (CI/CD)

I workflow in `.github/workflows/` orchestrano l'esecuzione automatica in cloud:

| Workflow | Schedule | Runner |
|----------|----------|--------|
| `scrape-fonti.yml` | Daily 05:00 UTC | ubuntu (hosted) |
| `scrape-subito.yml` | Ogni 6 ore | self-hosted (IP fisso) |
| `ai-classify.yml` | Trigger da scrape-subito | ubuntu (hosted) |
| `verify-sold.yml` | 2x/day: 08:00 + 13:00 UTC (10:00 + 15:00 CEST) | self-hosted |
| `scrape-ebay.yml` | Daily 22:00 UTC | ubuntu (hosted) |
| `quality.yml` | Push/PR su main | ubuntu (hosted) |

Il runner self-hosted è necessario per Subito e verify-sold perché Subito.it blocca i range IP dei cloud provider (AWS, Azure, ecc.); serve un IP residenziale fisso con Chrome di sistema.

### RunReport

Ogni esecuzione di `run.py` produce un JSON in `logs/` con: comando completo, timestamp inizio/fine, durata totale, step eseguiti con metriche (n. record processati, errori), esito booleano `ok`. Il viewer espone questi log per il monitoraggio dello stato operativo.

---

*Fine documento — per procedure di recovery dettagliate vedere `docs/runbook.md`.*
