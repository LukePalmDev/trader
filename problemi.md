# Xbox Price Tracker — Problemi riscontrati e soluzioni

## Stato finale (al termine della sessione)

| Scraper     | Prodotti | Stato                |
|-------------|----------|----------------------|
| Gamelife    | 47       | ✅ Funzionante (tutte e 3 le pagine) |
| GamePeople  | 4        | ✅ Funzionante       |
| Gameshock   | 32       | ✅ Funzionante       |
| rebuy.it    | 35       | ✅ Funzionante       |
| **Totale**  | **118**  |                      |

---

## 1. GAMELIFE — Problema Cloudflare + paginazione

### Sintomo
Pages 2 e 3 restituivano sempre 0 prodotti. Il log mostrava:
```
WARNING: Nessun prodotto trovato su: .../page/2 (pagina vuota o inesistente)
```

### Approcci tentati

#### Tentativo 1: Riutilizzare la stessa pagina Playwright
Navigare dalla stessa pagina da page 1 → page 2 causava un conflitto nello stato JS di Odoo eCommerce. La pagina si caricava ma il DOM dei prodotti era vuoto.

#### Tentativo 2: Pagina fresca (`ctx.new_page()`) nello stesso context
Creare una nuova tab nello stesso browser context sembrava logico, ma Cloudflare intercettava la navigazione mostrando una challenge "Ci siamo quasi…" che Playwright headless non riusciva a risolvere.

Prova con debug:
- URL dopo navigazione conteneva `__cf_chl_rt_tk=...` (Cloudflare challenge token)
- Titolo della pagina sempre "Ci siamo quasi…" anche dopo 30+ secondi

#### Tentativo 3: Poll con `wait_for_timeout` (8×2s = 16s totali)
Ancora 0 prodotti. Il Cloudflare challenge bloccava e non si risolveva.

#### Causa radice identificata
Gamelife.it usa Cloudflare "Bot Protection" che:
1. Consente la **prima navigazione** da un context fresco (Playwright headless supera il JS challenge iniziale)
2. **Blocca le navigazioni successive** nello stesso context con un nuovo challenge che headless non supera

#### Soluzione adottata ✅
Usare un **browser context completamente nuovo** per ogni URL di pagina.
```python
# Per ogni pagina del pager:
ctx_n = await browser.new_context(...)  # context FRESCO = nuova sessione Cloudflare
page_n = await ctx_n.new_page()
await page_n.goto(url, wait_until='domcontentloaded', ...)
# ... estrai prodotti ...
await ctx_n.close()
```

Ogni context fresco ottiene il suo cookie `cf_clearance` indipendente e supera il challenge. Il metodo `wait_until='domcontentloaded'` è più veloce di `'load'` e più affidabile (evita timeout da background polling di Odoo).

### File modificato
`scrapers/gamelife.py` — funzione `run_scraper()`: aggiunta `_new_context(browser)` helper, loop che crea nuovo context per ogni URL pager.

---

## 2. GAMELIFE — Filtro ROG Ally

### Sintomo
Il ROG Ally (handheld ASUS) appariva nel catalogo console Gamelife perché Gamelife usa la categoria "Console" anche per handheld non-Xbox.

### Soluzione ✅
Aggiunto filtro nel parsing prodotti:
```python
_NON_XBOX_RE = re.compile(
    r"\bROG\s+Ally\b|\bSteam\s+Deck\b|\bPlayStation\b|\bPS[345]\b|\bNintendo\b",
    re.IGNORECASE,
)
# In _parse_product_locator():
if _NON_XBOX_RE.search(name):
    return None
```

---

## 3. GAMESHOCK — Condizione (Nuovo/Usato) errata

### Sintomo
Prodotti come "Console Xbox 360 SLIM 20GB+" mostravano condizione "Nuovo" anche se erano usati. Lo SKU (derivato dall'URL) conteneva "usata" ma il nome display non lo esplicitava.

### Causa
Il pattern `_USATO_PATTERN` veniva applicato solo al **nome** del prodotto, non all'URL. Gameshock spesso ha nomi display senza "usata" ma con "usata" nell'URL del prodotto.

Esempio:
- Nome: `Console Xbox 360 SLIM 20GB+`  → `Nuovo` (errato)
- URL: `.../2072-console-xbox-usata.html` → contiene "usata"

### Soluzione ✅
Controllo esteso a nome + URL:
```python
condition = "Usato" if (
    _USATO_PATTERN.search(name) or _USATO_PATTERN.search(url)
) else "Nuovo"
```

---

## 4. REBUY.IT — Samsung Galaxy Watch nella categoria Xbox One

### Sintomo
Un Samsung Galaxy Watch (SKU RBY-11197991) appariva nella categoria "Xbox One" di rebuy.it, perché rebuy a volte mette prodotti non-Xbox nelle sezioni Xbox.

### Soluzione ✅
Aggiunto filtro sul nome per escludere prodotti non-Xbox/Microsoft:
```python
if not re.search(r"\bxbox\b|\bmicrosoft\b", name, re.IGNORECASE):
    log.debug("Escluso prodotto non-Xbox: %r", name)
    continue
```

---

## 5. DISPONIBILITÀ prodotti — campo `available`

### Problema
Il viewer non mostrava se un prodotto era disponibile o esaurito.

### Soluzione ✅
Aggiunto campo `available: bool` in tutti gli scraper:

| Scraper    | Come rilevato |
|------------|---------------|
| Gamelife   | `ribbon` non contiene "esaurit/non disp" AND prezzo presente |
| Gameshock  | `.availability` DOM element: "Non disponibile"/"Esaurito" → False |
| Rebuy      | classe CSS `product--unavailable` → False |
| GamePeople | `availability` in ("Disponibile", "Prenotabile") → True |

Il viewer mostra un badge "Disp." (verde) o "Esaurito" (rosso) accanto al badge condizione.

---

## 6. JOLLYROGERBAY — Disabilitato

### Problema
JollyRogerBay vende solo **giochi** Xbox, non hardware console. Le categorie `/shop/54-xbox-360`, `/shop/14-xbox-one` ecc. contengono esclusivamente titoli videoludici.

### Soluzione ✅
Disabilitato in `config.toml`:
```toml
[sources.jollyrogerbay]
enabled = false  # vende solo giochi, non console hardware
```

---

## 7. GAMELIFE — `import re` mancante

### Problema
Il file `scrapers/gamelife.py` usava `re.match()` ma non importava il modulo `re`. Causava `NameError: name 're' is not defined`.

### Soluzione ✅
Aggiunto `import re` in cima al file.

---

## 8. Viewer — Fonte non visibile su viewport stretto

### Sintomo
Su viewport < 700px (come il preview interno di Claude), la colonna "Fonte" viene nascosta dal CSS responsive:
```css
@media (max-width: 700px) {
  th:nth-child(4), td:nth-child(4) { display: none; }
}
```

### Stato
Non è un bug: è comportamento responsive corretto. Su browser a larghezza piena la colonna è visibile.

---

## 9. Confronto Nuovo vs Usato — limitazione cross-source

### Problema noto (NON risolto nella sessione)
Il pannello "Confronto Nuovo vs Usato" raggruppa prodotti per `p.name` esatto. Questo funziona solo quando lo stesso prodotto ha il **medesimo nome** su store diversi. In pratica:
- Gamelife: "Xbox Series X"
- Rebuy: "Microsoft Xbox Series X 1TB"

Questi vengono trattati come prodotti diversi, quindi il "risparmio" usato→nuovo non viene calcolato cross-source.

**Soluzione possibile** (non implementata): normalizzazione del nome (es. via fuzzy matching o dizionario di sinonimi), oppure un campo `canonical_name` aggiunto durante lo scraping.

---

## Come avviare

```bash
# Scraping + avvio viewer (tutto in uno):
cd /Users/luke/Documents/Documenti\ Principali/Programmazione/trader
python3 run.py --all

# Solo viewer (senza riscraping):
python3 run.py --view

# Viewer su http://localhost:8080/viewer/index.html
```

---

## Struttura file principale

```
trader/
├── config.toml              # Configurazione sorgenti e parametri
├── run.py                   # Entry point: --all, --view, --source
├── scrapers/
│   ├── base.py              # Funzioni comuni: clean_price, save_snapshot, deduplicate
│   ├── gamelife.py          # Playwright + fresh context x pagina (Cloudflare)
│   ├── gamepeople.py        # requests + BeautifulSoup (SSL disabilitato)
│   ├── gameshock.py         # requests + BeautifulSoup (PrestaShop)
│   ├── rebuy.py             # requests + BeautifulSoup (Angular SSR)
│   └── jollyrogerbay.py     # DISABILITATO (solo giochi)
├── viewer/
│   ├── index.html
│   ├── app.js
│   └── style.css
└── data/                    # Snapshot JSON per ogni scrape
```
