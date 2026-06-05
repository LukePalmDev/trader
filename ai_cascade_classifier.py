from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from db_subito import DB_PATH, _connect, init_db as init_subito_db
from model_rules import canonical_taxonomy_ids, fields_from_canonical_id

log = logging.getLogger("ai_cascade_classifier")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

PROMPT_VERSION = "ai-cascade:v1:taxonomy+price:2026-06-05"
TAXONOMY_VERSION = "xbox-taxonomy:2026-06-03"
DEFAULT_MODELS = ("gpt-4o-mini", "gpt-4.1-mini", "gpt-5.1-mini")
DEFAULT_THRESHOLD = 80
DEFAULT_LIMIT = 200
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"

OBJECT_TYPES = (
    "console",
    "bundle",
    "accessory",
    "game",
    "parts",
    "empty_box",
    "service",
    "unknown",
)
PRICE_SIGNALS = ("compatible", "suspicious_low", "suspicious_high", "missing", "irrelevant")

_PRICE_FLOORS = {
    "series-x": 80.0,
    "series-s": 50.0,
    "one-x": 40.0,
    "one-s": 35.0,
    "one-base": 30.0,
    "360": 10.0,
    "original": 8.0,
}

_PRICE_CEILINGS = {
    "series-x": 900.0,
    "series-s": 450.0,
    "one-x": 400.0,
    "one-s": 300.0,
    "one-base": 250.0,
    "360": 250.0,
    "original": 250.0,
}


@dataclass(frozen=True)
class CascadeResult:
    ad_id: int
    taxonomy_id: str
    confidence: int
    object_type: str
    price_signal: str
    decision_reason: str
    status: str
    selected_model: str
    attempts: list[dict[str, Any]]
    input_hash: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(raw: str | None) -> str:
    if not raw:
        return ""
    return " ".join(str(raw).split()).strip()


def _shorten(text: str | None, max_len: int) -> str:
    value = _normalize_text(text)
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


def _models_from_env() -> tuple[str, ...]:
    raw = os.environ.get("OPENAI_CASCADE_MODELS", "").strip()
    if not raw:
        return DEFAULT_MODELS
    models = tuple(part.strip() for part in raw.split(",") if part.strip())
    return models or DEFAULT_MODELS


def _taxonomy_payload() -> list[dict[str, str]]:
    payload: list[dict[str, str]] = []
    for taxonomy_id in canonical_taxonomy_ids(include_other=False):
        fields = fields_from_canonical_id(taxonomy_id)
        payload.append(
            {
                "id": taxonomy_id,
                "family": fields.console_family,
                "model": fields.sub_model,
                "label": taxonomy_id.replace("-", " "),
            }
        )
    payload.append(
        {
            "id": "other",
            "family": "other",
            "model": "Unknown",
            "label": "non-target item: controller, games, accessories, parts, empty boxes, services",
        }
    )
    return payload


def _schema() -> dict[str, Any]:
    return {
        "name": "xbox_listing_classification",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "taxonomy_id": {"type": "string", "enum": canonical_taxonomy_ids()},
                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                "object_type": {"type": "string", "enum": list(OBJECT_TYPES)},
                "price_signal": {"type": "string", "enum": list(PRICE_SIGNALS)},
                "decision_reason": {"type": "string"},
            },
            "required": [
                "taxonomy_id",
                "confidence",
                "object_type",
                "price_signal",
                "decision_reason",
            ],
        },
    }


def _system_prompt() -> str:
    taxonomy = json.dumps(_taxonomy_payload(), ensure_ascii=False, separators=(",", ":"))
    return (
        "Classifica annunci marketplace italiani di console Xbox. "
        "Ricevi titolo, descrizione e prezzo. Devi scegliere esattamente un taxonomy_id "
        "dalla tassonomia fornita oppure other. Non inventare ID. "
        "Usa other per controller, giochi, accessori, ricambi, scatole vuote, account, servizi "
        "o qualunque oggetto non presente nella tassonomia. "
        "Il prezzo è un segnale di plausibilità: non basta da solo, ma un prezzo incompatibile "
        "deve ridurre la confidence o produrre other se il testo indica accessori/parti. "
        "Esempi: 'controller Xbox Series' -> other; 'Xbox One con giochi e pad' -> modello Xbox One più probabile; "
        "'scatola Xbox Series X' -> other; 'Xbox 360 E 250GB' -> 360-e-250gb. "
        f"Tassonomia: {taxonomy}"
    )


def _input_hash(row: sqlite3.Row | dict[str, Any]) -> str:
    payload = {
        "title": _normalize_text(row["name"]).lower(),
        "body": _normalize_text(row["body_text"] if "body_text" in row.keys() else "").lower(),
        "price": row["last_price"],
        "prompt_version": PROMPT_VERSION,
        "taxonomy_version": TAXONOMY_VERSION,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _row_to_user_message(row: sqlite3.Row | dict[str, Any]) -> str:
    body = ""
    if hasattr(row, "keys") and "body_text" in row.keys():
        body = row["body_text"] or ""
    price = row["last_price"]
    price_text = "missing" if price is None else f"{float(price):.2f} EUR"
    payload = {
        "ad_id": int(row["id"]),
        "title": _shorten(row["name"], 240),
        "body": _shorten(body, 1200),
        "price": price_text,
    }
    return json.dumps(payload, ensure_ascii=False)


def _post_openai(model: str, row: sqlite3.Row | dict[str, Any], api_key: str) -> tuple[dict[str, Any], dict[str, Any]]:
    started = time.monotonic()
    response = requests.post(
        OPENAI_CHAT_COMPLETIONS_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": _system_prompt()},
                {"role": "user", "content": _row_to_user_message(row)},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": _schema(),
            },
        },
        timeout=60,
    )
    latency_ms = int((time.monotonic() - started) * 1000)
    response.raise_for_status()
    data = response.json()
    content = data["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    usage = data.get("usage") or {}
    meta = {
        "raw_response": content,
        "input_tokens": usage.get("prompt_tokens"),
        "output_tokens": usage.get("completion_tokens"),
        "latency_ms": latency_ms,
    }
    return parsed, meta


def _normalize_attempt(payload: dict[str, Any]) -> dict[str, Any]:
    taxonomy_id = str(payload.get("taxonomy_id") or "other").strip()
    if taxonomy_id not in set(canonical_taxonomy_ids()):
        taxonomy_id = "other"

    try:
        confidence = int(payload.get("confidence", 0))
    except (TypeError, ValueError):
        confidence = 0
    confidence = max(0, min(confidence, 100))

    object_type = str(payload.get("object_type") or "unknown").strip()
    if object_type not in OBJECT_TYPES:
        object_type = "unknown"

    price_signal = str(payload.get("price_signal") or "missing").strip()
    if price_signal not in PRICE_SIGNALS:
        price_signal = "missing"

    if object_type in {"accessory", "game", "parts", "empty_box", "service"}:
        taxonomy_id = "other"

    return {
        "taxonomy_id": taxonomy_id,
        "confidence": confidence,
        "object_type": object_type,
        "price_signal": price_signal,
        "decision_reason": _shorten(str(payload.get("decision_reason") or ""), 600),
    }


def _taxonomy_price_family(taxonomy_id: str) -> str:
    if taxonomy_id.startswith("series-x"):
        return "series-x"
    if taxonomy_id.startswith("series-s"):
        return "series-s"
    if taxonomy_id.startswith("one-x"):
        return "one-x"
    if taxonomy_id.startswith("one-s"):
        return "one-s"
    if taxonomy_id.startswith("one"):
        return "one-base"
    if taxonomy_id.startswith("360"):
        return "360"
    if taxonomy_id.startswith("original"):
        return "original"
    return "other"


def _has_price_conflict(taxonomy_id: str, price: float | None, price_signal: str) -> bool:
    if taxonomy_id == "other" or price is None:
        return False
    family = _taxonomy_price_family(taxonomy_id)
    if family == "other":
        return False
    floor = _PRICE_FLOORS.get(family)
    ceiling = _PRICE_CEILINGS.get(family)
    if floor is not None and price < floor:
        return True
    if ceiling is not None and price > ceiling:
        return True
    return price_signal in {"suspicious_low", "suspicious_high"}


def _final_status(taxonomy_id: str, confidence: int, threshold: int, price_conflict: bool) -> str:
    if confidence < threshold or price_conflict:
        return "pending_review"
    if taxonomy_id == "other":
        return "rejected_auto"
    return "approved_auto"


def _load_rows(
    conn: sqlite3.Connection,
    *,
    limit: int | None,
    classify_all: bool,
) -> list[sqlite3.Row]:
    where = ""
    if not classify_all:
        where = """
            WHERE (
                ai_status IN ('pending', 'pending_review')
                OR ai_confidence IS NULL
                OR ai_prompt_version IS NULL
                OR ai_prompt_version <> ?
            )
        """
        params: tuple[Any, ...] = (PROMPT_VERSION,)
    else:
        params = ()
    sql = (
        "SELECT id, urn_id, name, body_text, last_price, ai_input_hash "
        f"FROM ads {where} ORDER BY id"
    )
    if limit is not None and limit > 0:
        sql += " LIMIT ?"
        params = params + (int(limit),)
    return conn.execute(sql, params).fetchall()


def _find_reusable_result(conn: sqlite3.Connection, input_hash: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT taxonomy_id_final, confidence_final, status_final, selected_model
        FROM classification_runs
        WHERE input_hash = ?
          AND status_final IN ('approved_auto', 'rejected_auto', 'pending_review')
        ORDER BY id DESC
        LIMIT 1
        """,
        (input_hash,),
    ).fetchone()


def _insert_run(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    result: CascadeResult,
    *,
    dry_run: bool,
) -> int | None:
    if dry_run:
        return None
    cur = conn.execute(
        """
        INSERT INTO classification_runs (
            ad_id, input_hash, title, body_text, price, taxonomy_version, prompt_version,
            status_final, taxonomy_id_final, confidence_final, selected_model, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["id"],
            result.input_hash,
            row["name"],
            row["body_text"],
            row["last_price"],
            TAXONOMY_VERSION,
            PROMPT_VERSION,
            result.status,
            result.taxonomy_id,
            result.confidence,
            result.selected_model,
            _utc_now(),
        ),
    )
    run_id = int(cur.lastrowid)
    for idx, attempt in enumerate(result.attempts, start=1):
        conn.execute(
            """
            INSERT INTO classification_attempts (
                run_id, ad_id, step_number, model, taxonomy_id, confidence,
                object_type, price_signal, decision_reason, raw_response,
                input_tokens, output_tokens, cost_estimate, latency_ms, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                row["id"],
                idx,
                attempt["model"],
                attempt["taxonomy_id"],
                attempt["confidence"],
                attempt["object_type"],
                attempt["price_signal"],
                attempt["decision_reason"],
                attempt.get("raw_response"),
                attempt.get("input_tokens"),
                attempt.get("output_tokens"),
                attempt.get("cost_estimate"),
                attempt.get("latency_ms"),
                _utc_now(),
            ),
        )
    return run_id


def _apply_result_to_ad(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    result: CascadeResult,
    *,
    dry_run: bool,
) -> None:
    if dry_run:
        log.info(
            "[DRY-RUN] ad=%s taxonomy=%s confidence=%s status=%s model=%s",
            row["id"],
            result.taxonomy_id,
            result.confidence,
            result.status,
            result.selected_model,
        )
        return

    mapped = fields_from_canonical_id(result.taxonomy_id)
    classify_confidence = round(result.confidence / 100.0, 3)
    conn.execute(
        """
        UPDATE ads
        SET ai_status = ?,
            ai_confidence = ?,
            ai_taxonomy_id = ?,
            ai_object_type = ?,
            ai_price_signal = ?,
            ai_prompt_version = ?,
            ai_input_hash = ?,
            console_family = ?,
            sub_model = ?,
            model_segment = ?,
            edition_class = ?,
            canonical_model = ?,
            classify_method = ?,
            classify_confidence = ?,
            classify_version = ?
        WHERE id = ?
        """,
        (
            result.status,
            result.confidence,
            result.taxonomy_id,
            result.object_type,
            result.price_signal,
            PROMPT_VERSION,
            result.input_hash,
            mapped.console_family,
            mapped.sub_model,
            mapped.model_segment,
            mapped.edition_class,
            mapped.canonical_model,
            f"ai-cascade:{result.selected_model}",
            classify_confidence,
            PROMPT_VERSION,
            row["id"],
        ),
    )


def classify_row(
    row: sqlite3.Row,
    *,
    api_key: str,
    models: tuple[str, ...] | None = None,
    threshold: int = DEFAULT_THRESHOLD,
) -> CascadeResult:
    selected_models = models or _models_from_env()
    input_hash = _input_hash(row)
    attempts: list[dict[str, Any]] = []
    last: dict[str, Any] | None = None
    last_model = selected_models[-1]

    for model in selected_models:
        parsed, meta = _post_openai(model, row, api_key)
        attempt = _normalize_attempt(parsed)
        attempt.update(
            {
                "model": model,
                "raw_response": meta.get("raw_response"),
                "input_tokens": meta.get("input_tokens"),
                "output_tokens": meta.get("output_tokens"),
                "cost_estimate": None,
                "latency_ms": meta.get("latency_ms"),
            }
        )
        attempts.append(attempt)
        last = attempt
        last_model = model
        if int(attempt["confidence"]) >= threshold:
            break

    assert last is not None
    price = row["last_price"]
    price_float = None if price is None else float(price)
    price_conflict = _has_price_conflict(last["taxonomy_id"], price_float, last["price_signal"])
    status = _final_status(last["taxonomy_id"], int(last["confidence"]), threshold, price_conflict)
    if price_conflict and status == "pending_review":
        last["decision_reason"] = _shorten(
            f"{last['decision_reason']} Price conflict detected by local validation.",
            600,
        )

    return CascadeResult(
        ad_id=int(row["id"]),
        taxonomy_id=last["taxonomy_id"],
        confidence=int(last["confidence"]),
        object_type=last["object_type"],
        price_signal=last["price_signal"],
        decision_reason=last["decision_reason"],
        status=status,
        selected_model=last_model,
        attempts=attempts,
        input_hash=input_hash,
    )


def _result_from_reuse(row: sqlite3.Row, reusable: sqlite3.Row, input_hash: str) -> CascadeResult:
    taxonomy_id = reusable["taxonomy_id_final"] or "other"
    confidence = int(reusable["confidence_final"] or 0)
    status = reusable["status_final"] or "pending_review"
    return CascadeResult(
        ad_id=int(row["id"]),
        taxonomy_id=taxonomy_id,
        confidence=confidence,
        object_type=("unknown" if taxonomy_id == "other" else "console"),
        price_signal="compatible",
        decision_reason="Reused previous cascade classification for identical input hash.",
        status=status,
        selected_model=reusable["selected_model"] or "reuse",
        attempts=[
            {
                "model": "reuse",
                "taxonomy_id": taxonomy_id,
                "confidence": confidence,
                "object_type": "unknown" if taxonomy_id == "other" else "console",
                "price_signal": "compatible",
                "decision_reason": "Reused previous cascade classification for identical input hash.",
            }
        ],
        input_hash=input_hash,
    )


def run_ai_cascade_classifier(
    *,
    limit: int | None = DEFAULT_LIMIT,
    classify_all: bool = False,
    threshold: int = DEFAULT_THRESHOLD,
    dry_run: bool = False,
    reuse: bool = True,
    models: tuple[str, ...] | None = None,
) -> dict[str, int]:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key and not dry_run:
        log.error("OPENAI_API_KEY non impostata.")
        return {"total": 0, "updated": 0, "errors": 0, "reused": 0, "pending_review": 0}

    init_subito_db(DB_PATH)
    conn = _connect(DB_PATH)
    rows = _load_rows(conn, limit=limit, classify_all=classify_all)
    log.info("AI cascade: %d annunci candidati.", len(rows))

    updated = 0
    errors = 0
    reused_count = 0
    pending_review = 0
    selected_models = models or _models_from_env()
    threshold = max(0, min(int(threshold), 100))

    for row in rows:
        try:
            input_hash = _input_hash(row)
            reusable = _find_reusable_result(conn, input_hash) if reuse and not dry_run else None
            if reusable is not None:
                result = _result_from_reuse(row, reusable, input_hash)
                reused_count += 1
            else:
                result = classify_row(
                    row,
                    api_key=api_key,
                    models=selected_models,
                    threshold=threshold,
                )

            _insert_run(conn, row, result, dry_run=dry_run)
            _apply_result_to_ad(conn, row, result, dry_run=dry_run)
            updated += 1
            if result.status == "pending_review":
                pending_review += 1

            if not dry_run and updated % 25 == 0:
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("Errore classificazione cascade ad %s: %s", row["id"], exc)
            errors += 1

    if not dry_run:
        conn.commit()
    conn.close()
    log.info(
        "AI cascade completata: total=%d updated=%d reused=%d pending_review=%d errors=%d",
        len(rows),
        updated,
        reused_count,
        pending_review,
        errors,
    )
    return {
        "total": len(rows),
        "updated": updated,
        "errors": errors,
        "reused": reused_count,
        "pending_review": pending_review,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Classifica annunci Subito con cascata OpenAI.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--all", action="store_true", dest="classify_all")
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-reuse", action="store_true")
    parser.add_argument(
        "--models",
        default="",
        help="Lista modelli separata da virgola. Default: OPENAI_CASCADE_MODELS o gpt-4o-mini,gpt-4.1-mini,gpt-5.1-mini.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    models = tuple(part.strip() for part in args.models.split(",") if part.strip()) or None
    result = run_ai_cascade_classifier(
        limit=args.limit,
        classify_all=args.classify_all,
        threshold=args.threshold,
        dry_run=args.dry_run,
        reuse=not args.no_reuse,
        models=models,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
