# Catalogo Console Xbox — Modelli di Riferimento

Questo file elenca tutti i **modelli standard** ("raccoglitori") in cui vengono classificati
gli annunci di Subito.it e eBay. Ogni annuncio viene fatto confluire in uno di questi slot,
indipendentemente dall'edizione limitata/speciale o dal bundle: quelli restano tracciati
nel campo `edition_class` (standard / limited / special / bundle).

I valori nella colonna **Canonical slug** sono quelli usati nel DB (`canonical_model`) e
nel prompt AI (`ai_classifier.py`).

---

## Xbox Series X  — generazione 2020

| Canonical slug        | Label esteso                  | Note hardware                          |
|-----------------------|-------------------------------|----------------------------------------|
| `series-x-1tb`        | Xbox Series X 1 TB            | Lettore Blu-ray. Modello principale.   |
| `series-x-2tb`        | Xbox Series X 2 TB            | Espansione SSD da 2 TB.                |
| `series-x-digital`    | Xbox Series X Digital 1 TB    | Senza lettore ottico (2024).           |

## Xbox Series S  — generazione 2020

| Canonical slug        | Label esteso                  | Note hardware                          |
|-----------------------|-------------------------------|----------------------------------------|
| `series-s-512gb`      | Xbox Series S 512 GB          | All-digital, SSD 512 GB. Bianco.       |
| `series-s-1tb`        | Xbox Series S 1 TB            | Refresh 2023, SSD 1 TB. Nero.          |

## Xbox One X  — generazione 2017

| Canonical slug        | Label esteso                  | Note hardware                          |
|-----------------------|-------------------------------|----------------------------------------|
| `one-x-1tb`           | Xbox One X 1 TB               | 4K, 1 TB HDD. Unica configurazione.   |

## Xbox One S  — generazione 2016

| Canonical slug        | Label esteso                  | Note hardware                          |
|-----------------------|-------------------------------|----------------------------------------|
| `one-s-500gb`         | Xbox One S 500 GB             | HDD 500 GB.                            |
| `one-s-1tb`           | Xbox One S 1 TB               | HDD 1 TB.                              |
| `one-s-2tb`           | Xbox One S 2 TB               | HDD 2 TB (launch edition).             |

## Xbox One (base)  — generazione 2013

| Canonical slug        | Label esteso                  | Note hardware                          |
|-----------------------|-------------------------------|----------------------------------------|
| `one-500gb`           | Xbox One 500 GB               | HDD 500 GB. Modello originale.         |
| `one-1tb`             | Xbox One 1 TB                 | HDD 1 TB.                              |

## Xbox 360  — generazione 2005

| Canonical slug        | Label esteso                  | Note hardware                          |
|-----------------------|-------------------------------|----------------------------------------|
| `360-120gb`           | Xbox 360 120 GB               | HDD 120 GB (Fat / Slim / Elite).       |
| `360-250gb`           | Xbox 360 250 GB               | HDD 250 GB.                            |
| `360-500gb`           | Xbox 360 500 GB               | HDD 500 GB (Slim S / E).               |
| `360`                 | Xbox 360                      | Storage non specificato / arcade.      |

## Xbox Original  — generazione 2001

| Canonical slug        | Label esteso                  | Note hardware                          |
|-----------------------|-------------------------------|----------------------------------------|
| `original`            | Xbox Original                 | Prima Xbox. Varie versioni hardware.   |

---

## Valore speciale

| Canonical slug | Significato                                                        |
|----------------|--------------------------------------------------------------------|
| `other`        | Non è una console Xbox (accessori, giochi, periferiche, spam).    |

---

## Note di classificazione

- **edition_class** registra separatamente se l'annuncio è `standard`, `limited`, `special`
  o `bundle`: lo stesso canonical può coprire qualsiasi edizione.
- **Le varianti fisiche del 360** (Fat / Slim / E / Elite) si classificano tutte nello stesso
  canonical in base allo storage: la sotto-variante è tracciata da `standardize_title()`.
- **Regola storage ambiguo One S:** se non è chiaro tra 500 GB e 512 GB → usa `one-s-500gb`.
- **Regola Xbox One base senza storage:** → `one-500gb`.
- **Regola Xbox 360 senza storage:** → `360`.
