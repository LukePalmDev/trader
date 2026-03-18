"""
Pipeline classificazione Subito:
1) regole locali deterministiche (model_rules)
2) match con catalogo CEX base (canonical_model)
3) fallback AI (Claude) solo per casi ambigui

Usage:
  python3 classifier.py
  python3 classifier.py --dry-run
  python3 classifier.py --limit 100
  python3 classifier.py --rules-only
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
from pathlib import Path

from model_rules import VALID_EDITIONS, VALID_SEGMENTS, classify_title

log = logging.getLogger("classifier")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

ROOT = Path(__file__).parent
DB_SUBITO = ROOT / "subito.db"
DB_TRADER = ROOT / "trader.db"

VALID_FAMILIES = {"series-x", "series-s", "one-x", "one-s", "one", "360", "original", "other"}

SYSTEM_PROMPT = """\
Sei un esperto di console Xbox. Ricevi titoli annunci Subito e devi classificare:
- family: series-x|series-s|one-x|one-s|one|360|original|other
- segment: base|premium|unknown
- edition_class: standard|limited|special|bundle
- canonical_model: slug breve (es: series-x-1tb, series-s-512gb, one-s-1tb, 360-250gb, original, unknown)
- confidence: numero 0..1

Rispondi SOLO con JSON:
{
  "classifications": [
    {
      "id": <int>,
      "family": "<value>",
      "segment": "<value>",
      "edition_class": "<value>",
      "canonical_model": "<slug>",
      "confidence": <float>
    }
  ]
}
"""

BATCH_SIZE = 25


def _connect_subito() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_SUBITO))
    conn.row_factory = sqlite3.Row
    return conn


def _connect_trader() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_TRADER))
    conn.row_factory = sqlite3.Row
    return conn


def _get_candidates(limit: int | None = None) -> list[dict]:
    with _connect_subito() as conn:
        q = """
            SELECT
                id, urn_id, name,
                console_family, model_segment, edition_class, canonical_model,
                classify_confidence, classify_method
            FROM ads
            WHERE
                console_family = 'other'
                OR model_segment = 'unknown'
                OR canonical_model IS NULL
                OR TRIM(canonical_model) = ''
                OR classify_method IS NULL
                OR TRIM(classify_method) = ''
            ORDER BY id
        """
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = conn.execute(q).fetchall()
    return [dict(r) for r in rows]


def _norm_tokens(text: str) -> set[str]:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in (text or ""))
    tokens = [t for t in cleaned.split() if len(t) >= 3]
    stop = {"xbox", "console", "con", "the", "and", "per", "for", "edition"}
    return {t for t in tokens if t not in stop}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _load_cex_anchors() -> dict[str, list[dict]]:
    if not DB_TRADER.exists():
        return {}

    with _connect_trader() as conn:
        rows = conn.execute(
            """
            SELECT name, console_family, canonical_model
            FROM products
            WHERE source = 'cex'
              AND model_segment = 'base'
              AND edition_class = 'standard'
              AND canonical_model IS NOT NULL
              AND TRIM(canonical_model) <> ''
            """
        ).fetchall()

    anchors: dict[str, list[dict]] = {}
    for r in rows:
        family = r["console_family"] or "other"
        entry = {
            "name": r["name"],
            "canonical_model": r["canonical_model"],
            "tokens": _norm_tokens(r["name"]),
        }
        anchors.setdefault(family, []).append(entry)
    return anchors


def _best_cex_match(name: str, family: str, anchors: dict[str, list[dict]]) -> dict | None:
    options = anchors.get(family) or []
    if not options:
        return None

    needle = _norm_tokens(name)
    best: dict | None = None
    best_score = 0.0
    for opt in options:
        score = _jaccard(needle, opt["tokens"])
        if score > best_score:
            best_score = score
            best = {
                "canonical_model": opt["canonical_model"],
                "score": round(score, 3),
            }

    if best and best["score"] >= 0.45:
        return best
    return None


def _apply_classifications(updates: list[dict], dry_run: bool = False) -> int:
    if dry_run:
        for u in updates:
            log.info(
                "[DRY-RUN] ID %s -> fam=%s seg=%s ed=%s canon=%s (%.2f via %s)",
                u["id"],
                u["family"],
                u["segment"],
                u["edition_class"],
                u["canonical_model"],
                u["confidence"],
                u["method"],
            )
        return len(updates)

    with _connect_subito() as conn:
        for u in updates:
            conn.execute(
                """
                UPDATE ads
                SET console_family = ?,
                    model_segment = ?,
                    edition_class = ?,
                    canonical_model = ?,
                    classify_confidence = ?,
                    classify_method = ?
                WHERE id = ?
                """,
                (
                    u["family"],
                    u["segment"],
                    u["edition_class"],
                    u["canonical_model"],
                    float(u["confidence"]),
                    u["method"],
                    int(u["id"]),
                ),
            )
    return len(updates)


def _rule_and_cex_pass(candidates: list[dict], anchors: dict[str, list[dict]]) -> tuple[list[dict], list[dict]]:
    updates: list[dict] = []
    unresolved: list[dict] = []

    for ad in candidates:
        classified = classify_title(ad["name"], family_hint=ad.get("console_family"))
        family = classified.console_family
        segment = classified.model_segment
        edition_class = classified.edition_class
        canonical_model = classified.canonical_model
        confidence = float(classified.classify_confidence)
        method = classified.classify_method

        if family != "other" and segment == "base" and edition_class == "standard":
            cex_match = _best_cex_match(ad["name"], family, anchors)
            if cex_match:
                canonical_model = cex_match["canonical_model"]
                confidence = max(confidence, 0.65 + (cex_match["score"] * 0.3))
                method = "cex-match:v1"

        update = {
            "id": int(ad["id"]),
            "family": family,
            "segment": segment,
            "edition_class": edition_class,
            "canonical_model": canonical_model,
            "confidence": round(min(max(confidence, 0.0), 1.0), 3),
            "method": method,
        }
        updates.append(update)

        if family == "other" or segment == "unknown" or update["confidence"] < 0.6:
            unresolved.append({"id": int(ad["id"]), "name": ad["name"]})

    return updates, unresolved


def classify_batch(ads: list[dict], client) -> list[dict]:
    items_text = "\n".join(
        f'{{"id": {ad["id"]}, "title": {json.dumps(ad["name"])}}}'
        for ad in ads
    )
    user_msg = f"Classifica i seguenti annunci:\n{items_text}"

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=900,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.rsplit("```", 1)[0].strip()

        data = json.loads(raw)
        result: list[dict] = []
        for c in data.get("classifications", []):
            family = c.get("family", "other")
            segment = c.get("segment", "unknown")
            edition_class = c.get("edition_class", "standard")
            canonical = (c.get("canonical_model") or "unknown").strip() or "unknown"
            confidence = c.get("confidence", 0.5)

            if family not in VALID_FAMILIES:
                family = "other"
            if segment not in VALID_SEGMENTS:
                segment = "unknown"
            if edition_class not in VALID_EDITIONS:
                edition_class = "standard"
            try:
                confidence_f = float(confidence)
            except (TypeError, ValueError):
                confidence_f = 0.5

            result.append(
                {
                    "id": int(c["id"]),
                    "family": family,
                    "segment": segment,
                    "edition_class": edition_class,
                    "canonical_model": canonical,
                    "confidence": round(min(max(confidence_f, 0.0), 1.0), 3),
                    "method": "ai:claude-haiku-4-5",
                }
            )
        return result

    except Exception as exc:  # noqa: BLE001
        log.error("Errore classificazione AI: %s", exc)
        return []


def run_classifier(
    limit: int | None = None,
    dry_run: bool = False,
    rules_only: bool = False,
) -> dict[str, int]:
    candidates = _get_candidates(limit)
    log.info("Annunci da arricchire: %d", len(candidates))
    if not candidates:
        return {
            "total_candidates": 0,
            "rule_updates": 0,
            "ai_updates": 0,
            "unresolved": 0,
            "errors": 0,
        }

    anchors = _load_cex_anchors()
    rule_updates_payload, unresolved = _rule_and_cex_pass(candidates, anchors)
    rule_updates = _apply_classifications(rule_updates_payload, dry_run=dry_run)

    ai_updates = 0
    errors = 0

    if unresolved and not rules_only:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            log.warning("ANTHROPIC_API_KEY non impostata: fallback AI saltato (%d unresolved)", len(unresolved))
        else:
            try:
                import anthropic

                client = anthropic.Anthropic(api_key=api_key)
            except Exception as exc:  # noqa: BLE001
                log.error("Client Anthropic non disponibile: %s", exc)
                client = None

            if client is not None:
                for i in range(0, len(unresolved), BATCH_SIZE):
                    batch = unresolved[i : i + BATCH_SIZE]
                    log.info(
                        "Batch AI %d/%d (%d annunci)",
                        i // BATCH_SIZE + 1,
                        (len(unresolved) - 1) // BATCH_SIZE + 1,
                        len(batch),
                    )
                    updates = classify_batch(batch, client)
                    if not updates:
                        errors += len(batch)
                        continue
                    ai_updates += _apply_classifications(updates, dry_run=dry_run)

    unresolved_final = max(0, len(unresolved) - ai_updates)

    log.info(
        "Classificazione completata: candidates=%d rules=%d ai=%d unresolved=%d errors=%d",
        len(candidates),
        rule_updates,
        ai_updates,
        unresolved_final,
        errors,
    )

    return {
        "total_candidates": len(candidates),
        "rule_updates": rule_updates,
        "ai_updates": ai_updates,
        "unresolved": unresolved_final,
        "errors": errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Classifica/arricchisce annunci Subito")
    parser.add_argument("--dry-run", action="store_true", help="Mostra senza salvare")
    parser.add_argument("--limit", type=int, default=None, help="Limite annunci")
    parser.add_argument("--rules-only", action="store_true", help="Salta fallback AI")
    args = parser.parse_args()

    result = run_classifier(limit=args.limit, dry_run=args.dry_run, rules_only=args.rules_only)
    print("\nRisultati:")
    print(f"  Candidati:      {result['total_candidates']}")
    print(f"  Agg. rules/CEX: {result['rule_updates']}")
    print(f"  Agg. AI:        {result['ai_updates']}")
    print(f"  Unresolved:     {result['unresolved']}")
    print(f"  Errori:         {result['errors']}")


if __name__ == "__main__":
    main()
