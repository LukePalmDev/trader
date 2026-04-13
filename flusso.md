# Flusso dell'Applicazione Xbox Price Tracker

Questo documento descrive il flusso completo dell'applicazione, partendo dai workflow automatizzati configurati in GitHub Actions fino ai processi interni eseguiti dai vari script Python.

## 1. Architettura dei Workflow (GitHub Actions)

L'applicazione è guidata principalmente da task schedulati tramite GitHub Actions. Ecco i workflow principali e il loro ciclo di esecuzione.

### 1.1. Scrape Fonti (CEX, GameLife, GamePeople, GameShock, ReBuy)
- **File:** `.github/workflows/scrape-fonti.yml`
- **Schedulazione:** Tutti i giorni alle 05:00 UTC
- **Flusso:**
  1. Checkout della repository.
  2. Setup ambiente Python 3.11 e installazione dipendenze.
  3. Installazione browser Playwright (Chromium).
  4. Esecuzione scrape parallelo delle fonti retail tramite:
     `python run.py --source gamelife,gamepeople,gameshock,rebuy,cex`
     - Per ogni fonte, il relativo modulo in `scrapers/` effettua la richiesta, estrae i dati e salva uno snapshot JSON in `data/`.
     - `run.py` legge lo snapshot e aggiorna il database principale `tracker.db` tramite `db.process_products()`.
  5. Esecuzione cleanup: `python run.py --cleanup`
     - Rimuove i vecchi snapshot JSON (retention).
     - Archivia e comprime in gzip i dati in `data/archive/`.
     - Esegue `VACUUM` sui database SQLite per ottimizzare lo spazio.
  6. Commit e push delle modifiche di `tracker.db` sulla repository GitHub.

### 1.2. Scrape Subito.it
- **File:** `.github/workflows/scrape-subito.yml`
- **Runner:** `self-hosted` (necessario per usare Chrome di sistema e un IP fisso non bannable)
- **Schedulazione:** Ogni 6 ore (02:00, 08:00, 14:00, 20:00 UTC)
- **Flusso:**
  1. Checkout e installazione dipendenze.
  2. Esecuzione scrape per Subito.it:
     `python3.11 run.py --source subito`
     - Viene eseguito `scrapers.subito.main()`.
     - Estrae i nuovi annunci e salva lo snapshot JSON in `data/`.
     - `run.py` aggiorna il database `tracker.db` tramite `db_subito.process_ads()`.
  3. Controllo delle modifiche al database (tramite hash SHA256).
  4. Se ci sono state modifiche (DB aggiornato con nuovi dati):
     - Esegue il cleanup: `python3.11 run.py --cleanup`
     - Effettua commit e push su GitHub di `tracker.db`.

### 1.3. Classificazione AI Subito (Haiku)
- **File:** `.github/workflows/ai-classify.yml`
- **Trigger:** Automatico al completamento (con successo) del workflow "Scrape Subito.it" (o manuale).
- **Flusso:**
  1. Checkout e pull dell'ultimo DB appena pushato dallo scraper.
  2. Installazione dipendenze.
  3. Classificazione tramite l'intelligenza artificiale:
     `python ai_classifier.py`
     - Carica dal DB tutti gli annunci con stato `ai_status = 'pending'`.
     - Invia batch di annunci (fino a 5 paralleli da 50 annunci ciascuno) alle API di Anthropic (Claude Haiku 4.5).
     - Determina se l'annuncio è effettivamente una console (confidence, status: approved/rejected) e ne estrae le caratteristiche (family, canonical_model, edition_class).
     - Salva i risultati della classificazione direttamente nel DB (aggiornando i campi di `ads`).
  4. Commit e push di `tracker.db` con gli stati aggiornati.

### 1.4. Verifica Venduti Subito
- **File:** `.github/workflows/verify-sold.yml`
- **Runner:** `self-hosted` (stesso runner dello scraper Subito, per usare lo stesso IP/Chrome)
- **Schedulazione:** 4 volte al giorno: 03:00, 09:00, 15:00, 21:00 UTC (1 ora dopo ogni scrape Subito)
- **Flusso:**
  1. Checkout e pull dell'ultimo DB.
  2. Installazione dipendenze e di `patchright` (versione stealth di Playwright).
  3. Esecuzione dello script di verifica:
     `python3.11 verify_sold.py --all --max-runtime-minutes 55 --concurrency 12 --min-coverage-ratio 0.75 …`
     - Legge dal database gli annunci Subito marcati come ancora attivi (`last_available = 1`) e approvati o in attesa dall'AI.
     - Effettua un controllo pre-check leggero a livello TLS con `curl_cffi` per capire se la pagina risponde 410/404/redirect (Venduto) o 200 (Attivo).
     - Per gli URL "unknown", utilizza il browser Playwright configurato in modalità stealth per simulare una vera navigazione e superare i blocchi Akamai di Subito.
     - Aggiorna il DB contrassegnando gli annunci non più disponibili (imposta `last_available = 0`, `sold_at` e ne calcola il timestamp stimato della vendita).
  4. Aggiorna lo storico del coverage delle verifiche in `logs/coverage_history.csv` (leggendo `logs/verify_sold_last_run.json` prodotto dallo script).
  5. Commit e push di `tracker.db` e del file CSV.
  6. Se la copertura minima è scesa sotto soglia (`--min-coverage-ratio`), lo script esce con **exit code 3**: il job GitHub Actions fallisce con un messaggio di errore esplicito e si attende il prossimo run schedulato (nessun retry automatico immediato).

### 1.5. Scrape eBay (Venduti)
- **File:** `.github/workflows/scrape-ebay.yml`
- **Schedulazione:** Tutti i giorni alle 22:00 UTC
- **Flusso:**
  1. Esegue lo scraper per eBay: `python run.py --source ebay`
     - `process_sold_items()` classifica ogni item con `model_rules.classify_title()` (regole, tier 1) all'insert.
  2. **Classificazione AI eBay:** `python ai_classifier.py --source ebay`
     - Seleziona i `sold_items` con `canonical_model IN ('other','unknown',NULL)` o `classify_confidence < 0.6` non ancora processati con AI.
     - Invia batch a Claude Haiku con lo stesso SYSTEM_PROMPT di Subito (stessi 16 canonical bucket).
     - Aggiorna `canonical_model`, `console_family`, `edition_class`, `classify_confidence`, `classify_method='ai:v1'`.
  3. Esegue la consueta routine di pulizia e invia le modifiche a GitHub.

### 1.6. Quality Gate
- **File:** `.github/workflows/quality.yml`
- **Trigger:** Push su `main` o su Pull Request
- **Flusso:**
  1. Installa dipendenze di sviluppo.
  2. Esegue il linting del codice con `ruff check .`
  3. Esegue la suite di test automatici con `pytest`.

---

## 2. Flussi Dettagliati degli Script Python

### 2.1. Il Punto di Accesso Principale (`run.py`)
Lo script `run.py` funge da orchestratore centrale dell'intera suite per le varie operazioni.

Le sorgenti disponibili (registrate in `_SCRAPER_MODULES`) sono:
`gamelife`, `gamepeople`, `gameshock`, `rebuy`, `cex`, `subito`, `ebay`, `jollyrogerbay`

- **Scraping (`cmd_scrape`)**:
  - Carica il modulo scraper richiesto in modo dinamico dalla cartella `scrapers/` (es. `scrapers/subito.py`).
  - Esegue la funzione `main()` del modulo per raccogliere i dati, che vengono salvati su disco in uno snapshot JSON dentro `data/`.
  - Al termine, legge il file JSON e passa i dati al modulo di inserimento DB appropriato (`db_ebay.process_sold_items`, `db_subito.process_ads`, o generico `db.process_products`).
  - Per Subito, dopo l'aggiornamento DB esegue anche `alerts.check_alerts()` per inviare messaggi su Telegram in caso di deal interessanti.
- **Pulizia (`cmd_cleanup`)**:
  - Rimuove vecchi file json in base alla retention policy.
  - Comprime i dati storici non recenti (spostati in `archive/`).
  - Lancia comandi `VACUUM` di SQLite per ottimizzare le dimensioni dei file `.db`.

### 2.2. La Classificazione tramite AI (`ai_classifier.py`)

> **Riferimento modelli:** vedi [`console_catalog.md`](console_catalog.md) per l'elenco completo dei 16 canonical slug ("raccoglitori") in cui vengono fatti confluire tutti gli annunci di Subito e eBay.

**Classificazione eBay (`run_ebay_classifier`):**
- Stessa funzione AI (stesso SYSTEM_PROMPT, stesso modello Haiku) di Subito.
- Seleziona `sold_items` con `canonical_model IN ('other','unknown',NULL)` o `classify_confidence < 0.6` e `classify_method NOT LIKE 'ai:%'`.
- Non usa `ai_status` (assente in `sold_items`); traccia lo stato via `classify_method='ai:v1'`.
- CLI: `python ai_classifier.py --source ebay|subito|all`


L'intelligenza artificiale interviene per pulire e strutturare lo stream di dati da Subito.it (che spesso contiene annunci spazzatura, accessori o descrizioni ambigue).
- **Recupero Pending**: Prende dal DB gli annunci dove `ai_status = 'pending'`.
- **Invio API**: Prepara dei prompt sintetizzando titolo, descrizione (accorciata) e prezzo dell'annuncio. Invia richieste batch concorrenti alle API Anthropic.
- **Validazione Risposta**: Riceve in output un JSON con `console_confidence`, classificazione della famiglia, modello canonico, storage, e edizione della console.
- **Aggiornamento DB**:
  - Se confidenza >= 75%: Lo stato diventa `approved`, e vengono compilati i campi strutturati nel DB (famiglia, modello, ecc.).
  - Se confidenza <= 25%: Lo stato diventa `rejected` (etichettato come 'other' console).
  - Casi intermedi o ambigui rimangono in uno stato pendente o parziale.

### 2.3. La Validazione dei Venduti (`verify_sold.py`)
Poiché Subito.it non fornisce una API diretta per sapere quando un oggetto viene venduto, occorre monitorarlo nel tempo visitandone l'URL.
- **Selezione Annunci**: Carica gli annunci ancora attivi (non venduti) approvati o in attesa di esserlo, possibilmente pre-filtrandoli tramite un check SQL di parole chiave.
- **Fase 1: Pre-check `curl_cffi`**:
  - Utilizza la libreria `curl_cffi` per inviare richieste leggere impersonando il livello TLS di Google Chrome. Questo aiuta a bypassare le protezioni base.
  - Se il server risponde `410`, o ridirige alla home, l'annuncio è segnato come *sold* (venduto).
  - Se risponde `200` mantenendo l'URL originario, è *active* (attivo).
  - Se la risposta è anomala o protetta da anti-bot (es. `403`), è *unknown* e passa alla fase 2.
- **Fase 2: Navigazione Stealth (Playwright)**:
  - Avvia un pool concorrente di browser Playwright in modalità stealth.
  - Accede alla pagina e verifica tramite il DOM (la presenza del testo "non più disponibile") per dedurre con certezza se è ancora attiva.
- **Aggiornamento DB**:
  - Se Attivo: aggiorna il campo `last_seen`.
  - Se Venduto: imposta `last_available = 0`, salva la data di vendita (`sold_at`) e ne stima una potenziale finestra di vendita (per avere metriche precise sulla durata sul mercato). Viene poi salvato l'evento nel tracking `ad_changes`.
