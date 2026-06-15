from __future__ import annotations

import sqlite3
from pathlib import Path

import ai_cascade_classifier as cascade
import db_ebay
import db_subito
from model_rules import fields_from_canonical_id


def _row() -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE ads (
            id INTEGER, urn_id TEXT, name TEXT, body_text TEXT,
            last_price REAL, ai_input_hash TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO ads VALUES (
            1, 'SUBITO-1', 'Xbox Series S 512GB',
            'Console funzionante con controller incluso', 180.0, NULL
        )
        """
    )
    row = conn.execute("SELECT * FROM ads").fetchone()
    assert row is not None
    return row


def test_fields_from_canonical_id_maps_taxonomy_to_db_fields() -> None:
    fields = fields_from_canonical_id("12622")

    assert fields.console_family == "360"
    assert fields.sub_model == "E"
    assert fields.model_segment == "base"
    assert fields.canonical_model == "12622"


def test_default_ai_cascade_models_follow_configured_steps(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_CASCADE_MODELS", raising=False)

    assert cascade._models_from_env() == (
        "openai/gpt-5-nano",
        "openai/gpt-4.1-nano",
        "openai/gpt-5.4-nano",
    )


def test_legacy_env_default_is_upgraded_to_configured_steps(monkeypatch) -> None:
    monkeypatch.setenv(
        "OPENAI_CASCADE_MODELS",
        "openai/gpt-4o-mini,openai/gpt-4.1-mini,openai/gpt-5-mini",
    )

    assert cascade._models_from_env() == (
        "openai/gpt-5-nano",
        "openai/gpt-4.1-nano",
        "openai/gpt-5.4-nano",
    )


def test_classify_row_escalates_until_threshold(monkeypatch) -> None:
    calls: list[str] = []

    def fake_post(model, row, api_key):
        calls.append(model)
        if model == "m1":
            return (
                {
                    "taxonomy_id": "14111",
                    "confidence": 61,
                    "object_type": "console",
                    "price_signal": "compatible",
                    "decision_reason": "probabile Series S",
                },
                {"raw_response": "{}", "input_tokens": 10, "output_tokens": 5, "latency_ms": 1},
            )
        return (
                {
                    "taxonomy_id": "14111",
                "confidence": 88,
                "object_type": "console",
                "price_signal": "compatible",
                "decision_reason": "Series S confermata",
            },
            {"raw_response": "{}", "input_tokens": 12, "output_tokens": 6, "latency_ms": 1},
        )

    monkeypatch.setattr(cascade, "_post_openai", fake_post)

    result = cascade.classify_row(_row(), api_key="test", models=("m1", "m2", "m3"), threshold=80)

    assert calls == ["m1", "m2"]
    assert result.taxonomy_id == "14111"
    assert result.confidence == 88
    assert result.status == "approved_auto"


def test_ai_cascade_load_rows_skips_sold_ads() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE ads (
            id INTEGER, urn_id TEXT, name TEXT, body_text TEXT,
            last_price REAL, last_available INTEGER, sold_at TEXT,
            ai_status TEXT, ai_confidence INTEGER, ai_prompt_version TEXT,
            ai_input_hash TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO ads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1,
                "SUBITO-ACTIVE",
                "Xbox Series S",
                "Console disponibile",
                180.0,
                1,
                None,
                "pending_review",
                70,
                cascade.PROMPT_VERSION,
                None,
            ),
            (
                2,
                "SUBITO-SOLD",
                "Xbox Series X",
                "Annuncio gia venduto",
                330.0,
                0,
                "2026-06-15T10:00:00+00:00",
                "pending_review",
                68,
                cascade.PROMPT_VERSION,
                None,
            ),
        ],
    )

    rows = cascade._load_rows(conn, limit=None, classify_all=True)

    assert [r["urn_id"] for r in rows] == ["SUBITO-ACTIVE"]


def test_pending_reviews_skip_sold_ads(tmp_path: Path) -> None:
    db_path = tmp_path / "tracker.db"
    db_subito.init_db(db_path)
    db_subito.process_ads(
        [
            {
                "sku": "SUBITO-ACTIVE",
                "name": "Xbox Series S",
                "body_text": "Console disponibile",
                "price": 180.0,
                "available": True,
                "url": "https://example.test/active",
            },
            {
                "sku": "SUBITO-SOLD",
                "name": "Xbox Series X",
                "body_text": "Annuncio venduto subito",
                "price": 330.0,
                "available": False,
                "url": "https://example.test/sold",
            },
        ],
        db_path=db_path,
    )
    con = sqlite3.connect(db_path)
    con.execute("UPDATE ads SET ai_status='pending_review', ai_confidence=70")
    con.commit()
    con.close()

    rows = db_subito.get_pending_reviews(db_path=db_path)

    assert [r["urn_id"] for r in rows] == ["SUBITO-ACTIVE"]


def test_ebay_cascade_updates_sold_item_and_saves_audit(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "tracker.db"
    monkeypatch.setattr(cascade, "DB_PATH", db_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    db_ebay.init_db(db_path)

    con = sqlite3.connect(db_path)
    con.execute(
        """
        INSERT INTO sold_items (
            item_id, name, console_family, sub_model, model_segment, edition_class,
            canonical_model, classify_confidence, classify_method, sold_price,
            sold_date, url, query_label, first_seen, last_seen
        ) VALUES (
            'EBAY-1', 'Xbox Series S 512GB Console', 'series', 'S', 'base', 'standard',
            'series-s-512gb', 0.8, 'rules:v1', 180, '', 'https://example.test/1',
            'Xbox Series S', 'now', 'now'
        )
        """
    )
    con.commit()
    con.close()

    def fake_post(model, row, api_key):
        return (
            {
                "taxonomy_id": "14111",
                "confidence": 91,
                "object_type": "console",
                "price_signal": "compatible",
                "decision_reason": "Series S 512GB",
            },
            {"raw_response": "{}", "input_tokens": 10, "output_tokens": 5, "latency_ms": 1},
        )

    monkeypatch.setattr(cascade, "_post_openai", fake_post)

    result = cascade.run_ebay_cascade_classifier(
        limit=None,
        classify_all=True,
        models=("m1", "m2", "m3"),
    )

    assert result["updated"] == 1
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    item = con.execute("SELECT canonical_model, classify_method FROM sold_items").fetchone()
    runs = con.execute("SELECT COUNT(*) AS n FROM ebay_classification_runs").fetchone()
    attempts = con.execute("SELECT COUNT(*) AS n FROM ebay_classification_attempts").fetchone()
    con.close()

    assert item["canonical_model"] == "14111"
    assert item["classify_method"] == "ai-cascade:m1:approved_auto"
    assert runs["n"] == 1
    assert attempts["n"] == 1


def test_human_review_updates_ad_and_saves_audit(tmp_path: Path) -> None:
    db_path = tmp_path / "tracker.db"
    db_subito.init_db(db_path)
    db_subito.process_ads(
        [
            {
                "sku": "SUBITO-1",
                "name": "Controller Xbox Series X",
                "body_text": "Controller wireless",
                "price": 35.0,
                "available": True,
                "url": "https://example.test/ad",
            }
        ],
        db_path=db_path,
    )

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ad_id = con.execute("SELECT id FROM ads WHERE urn_id='SUBITO-1'").fetchone()["id"]
    con.execute(
        """
        INSERT INTO classification_runs (
            ad_id, input_hash, title, body_text, price, taxonomy_version, prompt_version,
            status_final, taxonomy_id_final, confidence_final, selected_model, created_at
        ) VALUES (?, 'h', 'Controller Xbox Series X', 'Controller wireless', 35,
                  'tax', 'prompt', 'pending_review', 'other', 67, 'm3', 'now')
        """,
        (ad_id,),
    )
    run_id = con.execute("SELECT id FROM classification_runs").fetchone()["id"]
    con.execute(
        """
        UPDATE ads
        SET ai_status='pending_review', ai_taxonomy_id='other', ai_confidence=67
        WHERE id=?
        """,
        (ad_id,),
    )
    con.commit()
    con.close()

    result = db_subito.save_human_review(
        ad_id=ad_id,
        human_taxonomy_id="other",
        human_status="rejected_manual",
        review_reason="controller only",
        run_id=run_id,
        db_path=db_path,
    )

    assert result["ok"] is True

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ad = con.execute("SELECT ai_status, canonical_model FROM ads WHERE id=?", (ad_id,)).fetchone()
    review = con.execute("SELECT human_taxonomy_id, human_status FROM human_reviews").fetchone()
    con.close()

    assert ad["ai_status"] == "rejected_manual"
    assert ad["canonical_model"] == "other"
    assert review["human_taxonomy_id"] == "other"
    assert review["human_status"] == "rejected_manual"
