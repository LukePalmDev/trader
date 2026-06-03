# STORICI3GIUGNO

Archivio creato il 3 giugno 2026, quando il progetto è passato dalla vecchia
classificazione piatta alla tassonomia a due livelli:

- prima: `console_family` conteneva anche il modello (`series-x`, `series-s`, `one-x`, `one-s`);
- da oggi: `console_family` contiene solo la famiglia (`series`, `one`, `360`, `original`) e il modello sta in `sub_model`.

## File archiviati

- `console_catalog_pre_2026-06-03.md`: catalogo operativo prima della migrazione.
- `github-workflows/`: vecchie routine GitHub Actions rimosse dall’esecuzione operativa.

## Come leggere i dati storici

Nei dati prodotti prima del 3 giugno 2026 puoi trovare questi valori legacy:

| Valore legacy `console_family` | Nuovo `console_family` | Nuovo `sub_model` |
|--------------------------------|------------------------|-------------------|
| `series-x` | `series` | `X` |
| `series-s` | `series` | `S` |
| `one-x` | `one` | `X` |
| `one-s` | `one` | `S` |
| `one` | `one` | `Base` |
| `360` | `360` | `Base`, `S`, `E` o `Elite` in base al titolo |
| `original` | `original` | `Base` |

Le migrazioni del DB riclassificano i record esistenti usando il titolo del prodotto o
dell’annuncio. Gli alias legacy restano supportati in lettura per non rompere vecchi JSON,
report o snapshot.

## Regole usate fino al 2 giugno 2026

Il vecchio catalogo raggruppava Series X/S e One S/X direttamente come famiglie. Per Xbox
360, invece, il modello fisico era dedotto solo da `standardize_title()` e non era sempre
un campo strutturato comune a Subito/eBay.

Questa scelta funzionava per liste semplici, ma rendeva meno chiara una struttura a grafo:
il primo livello non era una vera famiglia e un titolo con più parole modello poteva
produrre combinazioni ambigue. La nuova tassonomia evita questo problema separando slot
mutualmente esclusivi: famiglia, modello, storage, colore, edizione.

## Dati runtime

Il DB operativo non viene spostato fuori percorso perché il viewer e i timer server devono
continuare a funzionare. La conservazione storica avviene tramite:

- questo archivio documentale nel repository;
- backup DB con timestamp sul server prima del deploy delle nuove migrazioni;
- migrazioni idempotenti che preservano le righe e aggiornano solo i campi derivati.
