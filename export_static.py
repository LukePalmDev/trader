"""
Genera i file JSON statici in viewer/data/ per GitHub Pages.
Eseguito automaticamente dal workflow scrape-subito dopo ogni run.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError as exc:
        raise ImportError("Python < 3.11 richiede 'tomli': pip install tomli") from exc

from paths import DB_PATH

ROOT = Path(__file__).parent
OUT_DIR = ROOT / "viewer" / "data"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _load_cfg() -> tuple[dict, Path, list[str]]:
    with open(ROOT / "config.toml", "rb") as f:
        cfg = tomllib.load(f)
    data_dir = ROOT / cfg["data"]["output_dir"]
    sources_cfg = cfg.get("sources", {})
    enabled = [k for k, v in sources_cfg.items() if v.get("enabled", True)]
    return sources_cfg, data_dir, enabled


def _latest_snapshot(source: str, data_dir: Path) -> Path | None:
    snaps = sorted(data_dir.glob(f"{source}_*.json"), key=lambda p: p.name)
    if not snaps:
        return None
    for snap in reversed(snaps):
        try:
            raw = json.loads(snap.read_text(encoding="utf-8"))
            if raw.get("products"):
                return snap
        except (OSError, json.JSONDecodeError):
            continue
    return snaps[-1]


def _write(name: str, data) -> None:
    path = OUT_DIR / name
    path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    size_kb = path.stat().st_size // 1024
    log.info("  %-40s %d KB", name, size_kb)


def export_all() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sources_cfg, data_dir, enabled = _load_cfg()

    # --- Piccoli endpoint ---
    import db
    import db_subito
    import db_ebay

    log.info("Esportazione endpoint DB...")
    _write("db-products.json",      db.get_all_products())
    _write("db-base-models.json",   db.get_base_models())
    _write("db-storage-sizes.json", db.get_storage_sizes())
    _write("db-standard-groups.json", db.get_standard_groups())
    _write("db-price-history.json", db.get_price_history())

    log.info("Esportazione endpoint Subito...")
    _write("subito-stats.json",      db_subito.get_stats())
    _write("subito-sold-stats.json", db_subito.get_sold_stats())

    log.info("Esportazione endpoint eBay...")
    _write("ebay-stats.json", db_ebay.get_stats())

    # --- Endpoint grandi con limiti ---
    log.info("Esportazione ads attivi (ultimi 7gg)...")
    _write("subito-ads.json", _get_active_ads())

    log.info("Esportazione sold Subito...")
    _write("subito-sold.json", db_subito.get_sold_ads())

    log.info("Esportazione eBay sold (top 5000)...")
    _write("ebay-sold.json", _get_ebay_sold_limited(5000))

    # --- Valuation ---
    log.info("Esportazione valuation opportunities...")
    try:
        import valuation
        _write("valuation-opportunities.json", valuation.score_subito_opportunities(limit=500))
    except Exception as exc:
        log.warning("valuation skip: %s", exc)
        _write("valuation-opportunities.json", [])

    # --- combined/latest (snapshot JSON) ---
    log.info("Esportazione combined/latest...")
    _write("combined-latest.json", _get_combined_latest(data_dir, sources_cfg, enabled))

    # --- sources meta ---
    log.info("Esportazione sources meta...")
    _write("sources.json", _get_sources_meta(data_dir, sources_cfg, enabled))

    # --- verifica consistenza Catalogo (DB) vs Riepilogo (snapshot) ---
    _verify_consistency(data_dir, sources_cfg, enabled)

    log.info("Esportazione completata in %s", OUT_DIR)


def _verify_consistency(data_dir: Path, sources_cfg: dict, enabled: list[str]) -> bool:
    """Verifica che il conteggio prodotti del Catalogo (DB) coincida con quello
    del Riepilogo (ultimo snapshot) per ogni fonte store. Logga le discrepanze.
    """
    import db
    from collections import Counter

    db_counts: Counter = Counter()
    for p in db.get_all_products():
        src = (p.get("source") or "").lower()
        if src and src not in {"subito", "ebay"}:
            db_counts[src] += 1

    snap_counts: Counter = Counter()
    combined = _get_combined_latest(data_dir, sources_cfg, enabled)
    for p in combined["products"]:
        src = (p.get("source") or "").lower()
        if src and src not in {"subito", "ebay"}:
            snap_counts[src] += 1

    ok = True
    for src in sorted(set(db_counts) | set(snap_counts)):
        if db_counts[src] != snap_counts[src]:
            ok = False
            log.warning(
                "CONSISTENZA: %s Catalogo(DB)=%d != Riepilogo(snapshot)=%d",
                src, db_counts[src], snap_counts[src],
            )
    if ok:
        log.info(
            "Consistenza Catalogo/Riepilogo OK (%d prodotti store).",
            sum(db_counts.values()),
        )
    else:
        log.warning(
            "Consistenza Catalogo/Riepilogo NON allineata — esegui "
            "'python3 run.py --cleanup' per potare i prodotti delistati."
        )
    return ok


def _get_active_ads() -> list[dict]:
    import sqlite3
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT id, urn_id, name, console_family, sub_model, model_segment, edition_class,
                   canonical_model,
                   url, city, region, seller_type, published_at,
                   last_price, last_available, ai_status, ai_confidence,
                   verify_status, sold_at_estimated, sold_window_hours
            FROM ads
            WHERE last_available = 1
              AND last_seen >= date('now', '-7 days')
            ORDER BY console_family, last_price ASC NULLS LAST
        """).fetchall()
    return [dict(r) for r in rows]


def _get_ebay_sold_limited(limit: int) -> list[dict]:
    import sqlite3
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT id, item_id, name, console_family, sub_model, model_segment, edition_class,
                   canonical_model, sold_price, sold_date, url, query_label, first_seen
            FROM sold_items
            ORDER BY first_seen DESC NULLS LAST
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def _get_combined_latest(data_dir: Path, sources_cfg: dict, enabled: list[str]) -> dict:
    all_products = []
    for source in sorted(enabled):
        if source == "subito":
            continue
        snap = _latest_snapshot(source, data_dir)
        if not snap:
            continue
        try:
            raw = json.loads(snap.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        prods = raw.get("products", [])
        for prod in prods:
            if not prod.get("source"):
                prod["source"] = source
        all_products.extend(prods)
    return {"products": all_products, "total": len(all_products)}


def _get_sources_meta(data_dir: Path, sources_cfg: dict, enabled: list[str]) -> list[dict]:
    result = []
    snaps_by_source: dict[str, list[Path]] = {}
    for fpath in data_dir.glob("*.json"):
        idx = fpath.stem.find("_20")
        if idx < 0:
            continue
        src = fpath.stem[:idx]
        if src in enabled:
            snaps_by_source.setdefault(src, []).append(fpath)

    for source in sorted(snaps_by_source):
        snaps = sorted(snaps_by_source[source], key=lambda p: p.name)
        snap = _latest_snapshot(source, data_dir)
        cfg = sources_cfg.get(source, {})
        entry = {
            "id": source,
            "label": cfg.get("label", source.title()),
            "color": cfg.get("color", "#888"),
            "enabled": cfg.get("enabled", True),
            "snapshots": len(snaps),
            "last_scraped": "",
            "last_total": 0,
        }
        if snap:
            try:
                meta = json.loads(snap.read_text(encoding="utf-8"))
                entry["last_scraped"] = meta.get("scraped_at", "")
                entry["last_total"] = meta.get("total", 0)
            except (OSError, json.JSONDecodeError):
                pass
        result.append(entry)
    return result


if __name__ == "__main__":
    try:
        export_all()
    except Exception as exc:
        log.error("Export fallito: %s", exc)
        sys.exit(1)
