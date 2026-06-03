# Catalogo Console Xbox — Tassonomia dal 3 giugno 2026

Dal 3 giugno 2026 la classificazione usa due livelli separati:

1. `console_family`: solo `original`, `360`, `one`, `series`, `other`.
2. `sub_model`: modello dentro la famiglia (`Base`, `S`, `X`, `E`, `Elite`, `Unknown`).

`canonical_model` resta lo slot tecnico usato per raggruppare prezzi, storage e revisioni.
Le edizioni limitate, speciali e bundle non cambiano lo slot tecnico: restano tracciate in
`edition_class` e, quando disponibile, in `edition_name`.

Il catalogo precedente è archiviato in
`STORICI3GIUGNO/console_catalog_pre_2026-06-03.md`.

---

## Famiglia `original`

| `sub_model` | Canonical slug | Label | Note |
|-------------|----------------|-------|------|
| `Base` | `original-base-8gb` | Xbox Original | Storage utile 8 GB; revisioni hardware 1.0-1.6b non sono SKU commerciali separate. |

## Famiglia `360`

| `sub_model` | Canonical slug | Label | Note |
|-------------|----------------|-------|------|
| `Base` | `360-base` | Xbox 360 | Storage non specificato. |
| `Base` | `360-base-4gb` | Xbox 360 4 GB | Arcade / flash interna. |
| `Base` | `360-base-20gb` | Xbox 360 20 GB | Pro/Premium lancio. |
| `Base` | `360-base-60gb` | Xbox 360 60 GB | Pro dal 2008. |
| `Base` | `360-base-120gb` | Xbox 360 120 GB | Fat / Elite quando il titolo non distingue Elite. |
| `Base` | `360-base-250gb` | Xbox 360 250 GB | Fat / bundle standard. |
| `Base` | `360-base-320gb` | Xbox 360 320 GB | Bundle e special edition. |
| `Base` | `360-base-500gb` | Xbox 360 500 GB | Slot fallback se il modello S/E non è chiaro. |
| `S` | `360-s` | Xbox 360 S | Slim senza storage specificato. |
| `S` | `360-s-4gb` | Xbox 360 S 4 GB | Slim con flash interna. |
| `S` | `360-s-250gb` | Xbox 360 S 250 GB | Slim standard. |
| `S` | `360-s-320gb` | Xbox 360 S 320 GB | Special edition principali. |
| `S` | `360-s-500gb` | Xbox 360 S 500 GB | Ultime revisioni Slim. |
| `E` | `360-e` | Xbox 360 E | Revisione 2013 senza storage specificato. |
| `E` | `360-e-4gb` | Xbox 360 E 4 GB | E entry-level. |
| `E` | `360-e-250gb` | Xbox 360 E 250 GB | E standard. |
| `E` | `360-e-500gb` | Xbox 360 E 500 GB | E late bundle. |
| `Elite` | `360-elite` | Xbox 360 Elite | Elite senza storage specificato. |
| `Elite` | `360-elite-120gb` | Xbox 360 Elite 120 GB | Elite 2007. |
| `Elite` | `360-elite-250gb` | Xbox 360 Elite 250 GB | Super Elite / bundle. |

## Famiglia `one`

| `sub_model` | Canonical slug | Label | Note |
|-------------|----------------|-------|------|
| `Base` | `one-base-500gb` | Xbox One 500 GB | Modello 2013; fallback quando lo storage non è chiaro. |
| `Base` | `one-base-1tb` | Xbox One 1 TB | Bundle e refresh storage. |
| `S` | `one-s-500gb` | Xbox One S 500 GB | Revisione 2016. |
| `S` | `one-s-1tb` | Xbox One S 1 TB | Configurazione più comune. |
| `S` | `one-s-2tb` | Xbox One S 2 TB | Launch Edition 2016. |
| `S` | `one-s-digital-1tb` | Xbox One S All-Digital 1 TB | Senza lettore ottico, 2019. |
| `X` | `one-x-1tb` | Xbox One X 1 TB | Unica configurazione consumer standard. |

## Famiglia `series`

| `sub_model` | Canonical slug | Label | Note |
|-------------|----------------|-------|------|
| `S` | `series-s-512gb` | Xbox Series S 512 GB | Bianco, 2020. |
| `S` | `series-s-1tb` | Xbox Series S 1 TB | Carbon Black 2023 e Robot White 2024. |
| `X` | `series-x-1tb` | Xbox Series X 1 TB | Carbon Black con lettore. |
| `X` | `series-x-digital-1tb` | Xbox Series X Digital 1 TB | Robot White, senza lettore, 2024. |
| `X` | `series-x-2tb` | Xbox Series X 2 TB | Galaxy Black Special Edition, 2024. |

## Valori speciali

| Campo | Valore | Significato |
|-------|--------|-------------|
| `console_family` | `other` | Non è una console Xbox o non è classificabile. |
| `sub_model` | `Unknown` | Famiglia nota ma modello non determinato. |
| `canonical_model` | `unknown` | Slot tecnico non determinato. |
| `canonical_model` | `other` | Record esplicitamente non-console. |

## Regole operative

- Non combinare mai due modelli della stessa famiglia nello stesso record: un annuncio non può essere sia `one` + `S` sia `one` + `X`.
- Se nel titolo compaiono più console, vince la prima console citata.
- Colore, bundle, giochi inclusi e limited edition non cambiano `console_family` o `sub_model`.
- Per Xbox 360, `S` è il modello Slim: il viewer può mostrare "S", non "Slim", per restare coerente con il DB.
- I vecchi slug `series-x`, `series-s`, `one-x`, `one-s` restano alias di lettura e vengono normalizzati dalle migrazioni.
