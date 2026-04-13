from __future__ import annotations

import asyncio
import argparse
import json
import logging
import os
import sqlite3
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("ai_classifier")

from db_subito import DB_PATH, _connect  # noqa: E402

try:
    from anthropic import AsyncAnthropic
except ImportError:
    log.error("Il modulo 'anthropic' non è installato. Esegui: pip install anthropic")
    sys.exit(1)

HARDWARE_MODEL = "claude-haiku-4-5-20251001"
SELECTED_MODEL = os.environ.get("ANTHROPIC_MODEL") or HARDWARE_MODEL

DEFAULT_BATCH_SIZE = 50
DEFAULT_MAX_CONCURRENT_BATCHES = 5

# Valori canonici ammessi — devono corrispondere esattamente a model_rules.py
_VALID_CANONICALS = {
    "series-x-1tb", "series-x-2tb", "series-x-digital",
    "series-s-512gb", "series-s-1tb",
    "one-x-1tb",
    "one-s-500gb", "one-s-1tb", "one-s-2tb",
    "one-500gb", "one-1tb",
    "360-120gb", "360-250gb", "360-500gb", "360",
    "original", "other",
}
_VALID_FAMILIES  = {"series-x", "series-s", "one-x", "one-s", "one", "360", "original", "other"}
_VALID_EDITIONS  = {"standard", "limited", "special", "bundle"}

SYSTEM_PROMPT = """Analizza ogni annuncio Subito.it e determina: (1) se vende hardware console Xbox, (2) il modello esatto.

Per ogni annuncio restituisci questi campi:
- console_confidence: intero 0-100 (100=sicuramente console Xbox hardware, 0=non è console, 50=ambiguo)
- family: "series-x" | "series-s" | "one-x" | "one-s" | "one" | "360" | "original" | "other"
- canonical: ESATTAMENTE uno tra: "series-x-1tb" | "series-x-2tb" | "series-x-digital" | "series-s-512gb" | "series-s-1tb" | "one-x-1tb" | "one-s-500gb" | "one-s-1tb" | "one-s-2tb" | "one-500gb" | "one-1tb" | "360-120gb" | "360-250gb" | "360-500gb" | "360" | "original" | "other"
- storage_gb: capacità storage come intero (es. 512, 1024, 2048) oppure null se non specificata
- edition: "standard" | "limited" | "special" | "bundle"

Regole:
- Se console_confidence < 75 (non è console Xbox): family="other", canonical="other", storage_gb=null, edition="standard"
- "bundle" = console + giochi o accessori inclusi nel prezzo
- "limited" = edizione con nome di gioco/brand (Halo, Forza, Cyberpunk, Starfield, ecc.)
- "special" = edizione speciale senza nome specifico (Anniversary, Galaxy Black, ecc.)
- "digital" = console senza lettore ottico (Xbox Series X Digital Edition)
- Per storage: 500GB→500, 512GB→512, 1TB→1024, 2TB→2048
- Se non è chiaro se è 500GB o 512GB per One S, usa 500
- Xbox One base senza storage specificato → canonical="one-500gb"
- Xbox 360 senza storage specificato → canonical="360"

Rispondi ESCLUSIVAMENTE con un array JSON (nessun testo prima o dopo):
[{"id": int, "console_confidence": int, "family": "...", "canonical": "...", "storage_gb": int_o_null, "edition": "..."}]"""


def _normalize_text(raw: str | None) -> str:
    if not raw:
        return ""
    return " ".join(str(raw).split()).strip()


def _shorten(text: str, max_len: int = 1200) -> str:
    value = _normalize_text(text)
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


async def classify_batch(client: AsyncAnthropic, batch: list) -> list[dict]:
    """Invia un batch di annunci a Claude e restituisce validazione + classificazione."""
    import re as _re

    ads_text = []
    for row in batch:
        title = _shorten(row["name"], max_len=240)
        body = _shorten(row["body_text"] or "", max_len=960)
        _price = row["last_price"]
        price_str = str(_price) if _price is not None else "null"
        ads_text.append(
            f'{{"id": {row["id"]}, "title": {json.dumps(title)}, '
            f'"body": {json.dumps(body)}, "price": {price_str}}}'
        )

    user_message = "Classifica i seguenti annunci:\n" + "\n".join(ads_text)

    try:
        response = await client.messages.create(
            model=SELECTED_MODEL,
            max_tokens=4096,
            temperature=0.0,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}]
        )

        reply_text = response.content[0].text.strip()
        # Rimuovi eventuale markdown code block
        if "```json" in reply_text:
            reply_text = reply_text.split("```json")[1].split("```")[0].strip()
        elif "```" in reply_text:
            reply_text = reply_text.split("```")[1].split("```")[0].strip()

        try:
            data = json.loads(reply_text)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "ads" in data:
                return data["ads"]
            return []
        except Exception as e:
            # Fallback: estrai almeno id + console_confidence via regex
            matches = _re.finditer(
                r'["\']?id["\']?\s*:\s*(\d+).*?["\']?console_confidence["\']?\s*:\s*(\d+)',
                reply_text, _re.IGNORECASE | _re.DOTALL,
            )
            results = []
            for m in matches:
                results.append({"id": int(m.group(1)), "console_confidence": int(m.group(2))})
            if results:
                log.info("Fallback regex: recuperati %d risultati parziali (senza classificazione).", len(results))
                return results
            log.error("Classificazione AI fallita e fallback regex vuoto: %s\nText: %s", e, reply_text[:300])
            return []

    except Exception as e:
        log.error("Errore di rete/API durante la classificazione: %s", e)
        return []

def _reset_ai_state(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        """
        UPDATE ads
        SET ai_status = 'pending',
            ai_confidence = NULL
        """
    )
    return int(cur.rowcount or 0)


def _load_rows(
    conn: sqlite3.Connection,
    *,
    classify_all: bool,
    limit: int | None,
) -> list[sqlite3.Row]:
    if classify_all:
        sql = (
            "SELECT id, urn_id, name, body_text, last_price, url "
            "FROM ads ORDER BY id"
        )
        params: tuple = ()
    else:
        sql = (
            "SELECT id, urn_id, name, body_text, last_price, url "
            "FROM ads "
            "WHERE ai_status = 'pending' AND ai_confidence IS NULL "
            "ORDER BY id"
        )
        params = ()
    if limit is not None and limit > 0:
        sql += " LIMIT ?"
        params = params + (int(limit),)
    return conn.execute(sql, params).fetchall()


def _validate_model(model_name: str) -> str:
    if model_name != HARDWARE_MODEL:
        log.warning(
            "ANTHROPIC_MODEL=%s non consentito per questa pipeline. Uso forzato: %s",
            model_name,
            HARDWARE_MODEL,
        )
    return HARDWARE_MODEL


async def run_ai_classifier(
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    concurrency: int = DEFAULT_MAX_CONCURRENT_BATCHES,
    classify_all: bool = False,
    reset_first: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("La variabile d'ambiente ANTHROPIC_API_KEY non è impostata.")
        log.info("Esegui: export ANTHROPIC_API_KEY='la-tua-chiave'")
        return {"total": 0, "updated": 0, "errors": 0}

    client = AsyncAnthropic(api_key=api_key)
    
    global SELECTED_MODEL
    SELECTED_MODEL = _validate_model(SELECTED_MODEL)

    conn = _connect(DB_PATH)
    if reset_first:
        reset_rows = _reset_ai_state(conn)
        conn.commit()
        log.info("Reset classificazione AI completato su %d annunci.", reset_rows)

    rows = _load_rows(
        conn,
        classify_all=classify_all,
        limit=limit,
    )
    
    if not rows:
        log.info("Nessun annuncio da classificare. Il DB è aggiornato!")
        conn.close()
        return {"total": 0, "updated": 0, "errors": 0}
        
    log.info(
        "Trovati %d annunci da classificare con %s (all=%s).",
        len(rows),
        SELECTED_MODEL,
        classify_all,
    )

    total_updated = 0
    errors = 0
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    eff_batch = max(1, int(batch_size))
    tot_batches = (len(rows) + eff_batch - 1) // eff_batch

    async def _process_one_batch(batch_idx: int, batch: list) -> list[dict]:
        """Processa un singolo batch con semaphore per limitare la concorrenza."""
        num_batch = batch_idx + 1
        async with sem:
            log.info(
                "Elaborazione batch %d/%d (annunci %d-%d)...",
                num_batch, tot_batches,
                batch_idx * eff_batch + 1,
                min((batch_idx + 1) * eff_batch, len(rows)),
            )
            results = await classify_batch(client, batch)
            if not results:
                log.warning(
                    "Nessun risultato valido restituito dall'API per il batch %d. "
                    "Riproverà al prossimo avvio.", num_batch,
                )
            return results or []

    # Lancia tutti i batch in parallelo (il semaphore limita a MAX_CONCURRENT_BATCHES)
    batches = [rows[i:i + eff_batch] for i in range(0, len(rows), eff_batch)]
    all_results = await asyncio.gather(
        *[_process_one_batch(idx, batch) for idx, batch in enumerate(batches)]
    )

    # Applica i risultati al DB (sequenziale — SQLite non supporta write concorrenti)
    for batch_results in all_results:
        for res in batch_results:
            ad_id = res.get("id")
            conf  = res.get("console_confidence")

            if ad_id is None or conf is None:
                continue
            try:
                conf = int(conf)
            except (ValueError, TypeError):
                continue

            # Determina lo status
            if conf >= 75:
                status = "approved"
            elif conf <= 25:
                status = "rejected"
            else:
                status = "pending"

            # Campi classificazione (presenti solo nelle risposte del nuovo prompt)
            family   = res.get("family")
            canonical = res.get("canonical")
            edition  = res.get("edition")
            storage  = res.get("storage_gb")

            # Valida i valori restituiti dall'AI
            if family and family not in _VALID_FAMILIES:
                log.warning("family non valida '%s' per id %s — ignorata.", family, ad_id)
                family = None
            if canonical and canonical not in _VALID_CANONICALS:
                log.warning("canonical non valido '%s' per id %s — ignorato.", canonical, ad_id)
                canonical = None
            if edition and edition not in _VALID_EDITIONS:
                edition = None
            if storage is not None:
                try:
                    storage = int(storage)
                except (ValueError, TypeError):
                    storage = None

            try:
                if status == "approved" and family and canonical:
                    # Console confermata: aggiorna anche la classificazione modello
                    conn.execute(
                        """UPDATE ads
                           SET ai_confidence     = ?,
                               ai_status         = ?,
                               console_family    = ?,
                               canonical_model   = ?,
                               edition_class     = COALESCE(?, edition_class),
                               classify_method   = 'ai:v1',
                               classify_confidence = ?
                           WHERE id = ?""",
                        (conf, status, family, canonical, edition,
                         round(conf / 100.0, 3), ad_id),
                    )
                elif status == "rejected":
                    # Non è una console: azzera la classificazione modello
                    conn.execute(
                        """UPDATE ads
                           SET ai_confidence     = ?,
                               ai_status         = ?,
                               console_family    = 'other',
                               canonical_model   = 'other',
                               classify_method   = 'ai:v1',
                               classify_confidence = ?
                           WHERE id = ?""",
                        (conf, status, round(conf / 100.0, 3), ad_id),
                    )
                else:
                    # Ambiguo (pending) o approvato senza campi classificazione (fallback regex):
                    # aggiorna solo ai_status/ai_confidence, lascia la classificazione rules intatta
                    conn.execute(
                        "UPDATE ads SET ai_confidence = ?, ai_status = ? WHERE id = ?",
                        (conf, status, ad_id),
                    )
                total_updated += 1
            except Exception as e:
                log.error("Errore aggiornamento db per id %s: %s", ad_id, e)
                errors += 1

    try:
        conn.commit()
    except sqlite3.OperationalError:
        pass  # auto-commit potrebbe essere già attivo
    finally:
        conn.close()

    log.info(
        "Classificazione completata. %d annunci analizzati e aggiornati, %d errori "
        "(concorrenza: %d batch paralleli).",
        total_updated, errors, max(1, int(concurrency)),
    )
    return {"total": len(rows), "updated": total_updated, "errors": errors}


# ---------------------------------------------------------------------------
# eBay classification
# ---------------------------------------------------------------------------

def _load_ebay_rows(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    reclassify_all: bool = False,
) -> list[dict]:
    """Carica eBay sold_items che necessitano classificazione AI.

    Criteri di selezione (default):
    - canonical_model IN ('other','unknown',NULL) oppure classify_confidence < 0.6
    - E non ancora classificati con AI (classify_method NOT LIKE 'ai:%')

    Restituisce dicts con chiavi compatibili con classify_batch:
    {id, name, body_text, last_price}.
    """
    if reclassify_all:
        sql = "SELECT id, name, sold_price FROM sold_items ORDER BY id"
        params: tuple = ()
    else:
        sql = """
            SELECT id, name, sold_price FROM sold_items
            WHERE (
                canonical_model IN ('other', 'unknown')
                OR canonical_model IS NULL
                OR classify_confidence IS NULL
                OR classify_confidence < 0.6
            )
            AND (classify_method IS NULL OR classify_method NOT LIKE 'ai:%')
            ORDER BY id
        """
        params = ()
    if limit is not None and limit > 0:
        sql += " LIMIT ?"
        params = params + (int(limit),)

    raw = conn.execute(sql, params).fetchall()
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "body_text": None,       # sold_items non ha body_text
            "last_price": r["sold_price"],
        }
        for r in raw
    ]


async def run_ebay_classifier(
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    concurrency: int = DEFAULT_MAX_CONCURRENT_BATCHES,
    limit: int | None = None,
    reclassify_all: bool = False,
) -> dict[str, int]:
    """Classifica eBay sold_items con canonical_model non risolto o confidence < 0.6.

    A differenza di Subito, sold_items non ha ai_status: lo stato è tracciato
    via classify_method ('ai:v1' = già classificato con AI).
    Il SYSTEM_PROMPT è lo stesso di Subito: stessi canonical bucket, stesso modello.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("La variabile d'ambiente ANTHROPIC_API_KEY non è impostata.")
        return {"total": 0, "updated": 0, "errors": 0}

    client = AsyncAnthropic(api_key=api_key)
    global SELECTED_MODEL
    SELECTED_MODEL = _validate_model(SELECTED_MODEL)

    conn = _connect(DB_PATH)
    rows = _load_ebay_rows(conn, limit=limit, reclassify_all=reclassify_all)

    if not rows:
        log.info("eBay: nessun item da classificare. DB aggiornato.")
        conn.close()
        return {"total": 0, "updated": 0, "errors": 0}

    log.info(
        "eBay: trovati %d item da classificare con %s.",
        len(rows), SELECTED_MODEL,
    )

    total_updated = 0
    errors = 0
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    eff_batch = max(1, int(batch_size))
    batches = [rows[i : i + eff_batch] for i in range(0, len(rows), eff_batch)]
    tot_batches = len(batches)

    async def _process_one_batch(batch_idx: int, batch: list) -> list[dict]:
        async with sem:
            log.info(
                "eBay batch %d/%d (item %d-%d)...",
                batch_idx + 1, tot_batches,
                batch_idx * eff_batch + 1,
                min((batch_idx + 1) * eff_batch, len(rows)),
            )
            return await classify_batch(client, batch) or []

    all_results = await asyncio.gather(
        *[_process_one_batch(idx, batch) for idx, batch in enumerate(batches)]
    )

    for batch_results in all_results:
        for res in batch_results:
            item_id = res.get("id")
            conf = res.get("console_confidence")
            if item_id is None or conf is None:
                continue
            try:
                conf = int(conf)
            except (ValueError, TypeError):
                continue

            family   = res.get("family")
            canonical = res.get("canonical")
            edition  = res.get("edition")

            if family and family not in _VALID_FAMILIES:
                family = None
            if canonical and canonical not in _VALID_CANONICALS:
                canonical = None
            if edition and edition not in _VALID_EDITIONS:
                edition = None

            try:
                if conf >= 75 and family and canonical:
                    conn.execute(
                        """UPDATE sold_items
                           SET console_family      = ?,
                               canonical_model     = ?,
                               edition_class       = COALESCE(?, edition_class),
                               classify_confidence = ?,
                               classify_method     = 'ai:v1'
                           WHERE id = ?""",
                        (family, canonical, edition, round(conf / 100.0, 3), item_id),
                    )
                elif conf <= 25:
                    conn.execute(
                        """UPDATE sold_items
                           SET console_family      = 'other',
                               canonical_model     = 'other',
                               classify_confidence = ?,
                               classify_method     = 'ai:v1'
                           WHERE id = ?""",
                        (round(conf / 100.0, 3), item_id),
                    )
                else:
                    # Ambiguo: aggiorna confidence e method, lascia canonical intatto
                    conn.execute(
                        """UPDATE sold_items
                           SET classify_confidence = ?,
                               classify_method     = 'ai:v1'
                           WHERE id = ?""",
                        (round(conf / 100.0, 3), item_id),
                    )
                total_updated += 1
            except Exception as e:
                log.error("Errore update eBay id %s: %s", item_id, e)
                errors += 1

    try:
        conn.commit()
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()

    log.info(
        "eBay classificazione completata. %d item aggiornati, %d errori "
        "(concorrenza: %d batch paralleli).",
        total_updated, errors, max(1, int(concurrency)),
    )
    return {"total": len(rows), "updated": total_updated, "errors": errors}


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Classifica annunci Subito e/o eBay con Claude Haiku: valida hardware console Xbox "
            "E assegna family/canonical/edition direttamente nel DB (classify_method=ai:v1)."
        )
    )
    parser.add_argument(
        "--source",
        choices=["subito", "ebay", "all"],
        default="subito",
        help="Sorgente da classificare: subito (default), ebay, all",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Annunci/item per batch API (default {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_BATCHES,
        metavar="N",
        help=f"Batch concorrenti (default {DEFAULT_MAX_CONCURRENT_BATCHES})",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="classify_all",
        help="(Subito) Classifica tutti gli annunci, ignora filtro pending+ai_confidence NULL.",
    )
    parser.add_argument(
        "--reset-first",
        action="store_true",
        help="(Subito) Resetta ai_status/ai_confidence prima della classificazione.",
    )
    parser.add_argument(
        "--ebay-reclassify-all",
        action="store_true",
        help="(eBay) Riclassifica tutti gli item, non solo quelli con canonical non risolto.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite massimo annunci/item da processare (applicato a ciascuna sorgente).",
    )
    return parser


async def main():
    args = _build_arg_parser().parse_args()

    if args.source in ("subito", "all"):
        await run_ai_classifier(
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            classify_all=args.classify_all,
            reset_first=args.reset_first,
            limit=args.limit,
        )

    if args.source in ("ebay", "all"):
        await run_ebay_classifier(
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            limit=args.limit,
            reclassify_all=args.ebay_reclassify_all,
        )


if __name__ == "__main__":
    asyncio.run(main())
