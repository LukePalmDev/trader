#!/usr/bin/env python3
"""
Trader — Entry point multi-fonte

Usage:
  python3 run.py                          # scrape tutte le fonti abilitate
  python3 run.py --source gamelife        # scrape sorgente specifica
  python3 run.py --source gamelife,cex    # scrape sorgenti multiple
  python3 run.py --source subito --subito-region lombardia
                                          # scrape Subito solo su una regione
  python3 run.py --subito-dedup --subito-dedup-latest 5
                                          # deduplica gli ultimi snapshot Subito
  python3 run.py --view                   # avvia il viewer web
  python3 run.py --all                    # scrape tutte + viewer
  python3 run.py --full                   # scrape Subito + eBay + AI + viewer
  python3 run.py --cleanup                # retention + archiviazione + VACUUM DB
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import importlib
import json
import logging
import os
import shlex
import sqlite3
import subprocess
import sys
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path

import db as _db
import db_ebay as _db_ebay
import db_subito as _db_subito
from run_report import RunReport
from settings import ConfigError, load_default_config

_ROOT = Path(__file__).parent
_CFG = load_default_config(_ROOT)

DATA_DIR = _ROOT / _CFG["data"]["output_dir"]
VIEWER_DIR = _ROOT / "viewer"
LOGS_DIR = _ROOT / "logs"
DEFAULT_PORT = _CFG["viewer"]["port"]
DEFAULT_HOST = _CFG["viewer"]["host"]
DEFAULT_OPEN_BROWSER = _CFG["viewer"]["open_browser"]
DEFAULT_API_TOKEN = _CFG["viewer"]["api_token"]
RETENTION = _CFG["data"]["retention_keep"]
ARCHIVE_AFTER_DAYS = _CFG["data"]["archive_after_days"]
SOURCES_CFG = _CFG.get("sources", {})

log = logging.getLogger("trader")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

# Mappa nome_sorgente → modulo Python da importare
_SCRAPER_MODULES = {
    "gamelife": "scrapers.gamelife",
    "gamepeople": "scrapers.gamepeople",
    "jollyrogerbay": "scrapers.jollyrogerbay",
    "gameshock": "scrapers.gameshock",
    "rebuy": "scrapers.rebuy",
    "cex": "scrapers.cex",
    "subito": "scrapers.subito",
    "ebay": "scrapers.ebay",
}


# --------------------------------------------------------------------------- #
# Helpers snapshot
# --------------------------------------------------------------------------- #

def _snapshots(source: str) -> list[Path]:
    """Tutti gli snapshot di una sorgente, ordinati dal più vecchio."""
    return sorted(DATA_DIR.glob(f"{source}_*.json"))


def _latest_snapshot(source: str) -> Path | None:
    snaps = _snapshots(source)
    return snaps[-1] if snaps else None


def _split_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [tok.strip() for tok in raw.split(",") if tok.strip()]


def _all_sources_with_data() -> list[str]:
    """Sorgenti abilitate che hanno almeno uno snapshot salvato."""
    enabled = set(_enabled_sources())
    sources: set[str] = set()
    for fpath in DATA_DIR.glob("*.json"):
        stem = fpath.stem
        idx = stem.find("_20")
        if idx < 0:
            continue
        src = stem[:idx]
        if src in enabled:
            sources.add(src)
    return sorted(sources)


def _enabled_sources() -> list[str]:
    """Sorgenti abilitate in config.toml."""
    return [k for k, v in SOURCES_CFG.items() if v.get("enabled", False)]


# --------------------------------------------------------------------------- #
# Scrape
# --------------------------------------------------------------------------- #

def _run_scraper(
    source: str,
    report: RunReport | None = None,
    *,
    subito_options: dict | None = None,
) -> Path | None:
    """Esegue lo scraper per la sorgente indicata."""
    if source not in _SCRAPER_MODULES:
        msg = f"Sorgente sconosciuta: {source!r}. Disponibili: {list(_SCRAPER_MODULES.keys())}"
        log.error(msg)
        if report:
            report.note_error("scrape", msg, source=source)
        return None

    if source in SOURCES_CFG and not SOURCES_CFG[source].get("enabled", True):
        log.info("Sorgente %r disabilitata in config.toml — skip.", source)
        return None

    try:
        mod = importlib.import_module(_SCRAPER_MODULES[source])
        if source == "subito" and subito_options:
            result = mod.main(**subito_options)
        else:
            result = mod.main()
    except Exception as exc:  # noqa: BLE001
        log.exception("Errore scraper %r", source)
        if report:
            report.note_error("scrape", str(exc), source=source)
        return None

    if not result:
        log.warning("Scraper %s ha terminato senza snapshot", source)
        return None

    path = Path(result)
    if not path.exists():
        msg = f"Snapshot dichiarato ma non trovato: {path}"
        log.error(msg)
        if report:
            report.note_error("scrape", msg, source=source)
        return None

    return path


def cmd_scrape(
    sources: list[str],
    report: RunReport | None = None,
    *,
    subito_options: dict | None = None,
) -> tuple[list[Path], dict[str, dict]]:
    """Esegue lo scrape per le sorgenti indicate, poi retention e update DB.

    Returns:
        (paths, stats_by_source) dove stats_by_source è {source: {total, new, ...}}
    """
    results: list[Path] = []
    stats_by_source: dict[str, dict] = {}
    for source in sources:
        ctx = report.step("scrape_source", {"source": source}) if report else nullcontext({})
        with ctx:
            log.info("=" * 60)
            log.info("Avvio scraper: %s", source)
            path = _run_scraper(
                source,
                report=report,
                subito_options=subito_options if source == "subito" else None,
            )
            if path:
                results.append(path)
                s = _update_db_from_snapshot(path, report=report)
                if s:
                    stats_by_source[source] = s
            _apply_retention(source)
    return results, stats_by_source


def _update_db_from_snapshot(
    snapshot_path: Path, report: RunReport | None = None
) -> dict:
    """Legge uno snapshot JSON e aggiorna il DB con change detection.

    Returns:
        dict con chiavi: source, total, new, price_changes, avail_changes, unchanged
    """
    empty: dict = {}
    try:
        data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        msg = f"Errore lettura snapshot {snapshot_path.name}: {exc}"
        log.error(msg)
        if report:
            report.note_error("update_db", msg, snapshot=str(snapshot_path))
        return empty

    products = data.get("products", [])
    source = data.get("source", "")
    if not products:
        return empty

    try:
        if source == "ebay":
            stats = _db_ebay.process_sold_items(products)
            log.info(
                "eBay DB aggiornato — nuovi: %d | prezzi cambiati: %d | invariati: %d",
                stats["new"],
                stats["price_changes"],
                stats["unchanged"],
            )
        elif source == "subito":
            stats = _db_subito.process_ads(products)
            log.info(
                "Subito DB aggiornato — nuovi: %d | prezzi cambiati: %d | "
                "disponibilita' cambiata: %d | invariati: %d",
                stats["new"],
                stats["price_changes"],
                stats["avail_changes"],
                stats["unchanged"],
            )

            import alerts as _alerts

            _alerts.check_alerts()
        else:
            stats = _db.process_products(products)
            log.info(
                "DB aggiornato — nuovi: %d | prezzi cambiati: %d | "
                "disponibilita' cambiata: %d | invariati: %d",
                stats["new"],
                stats["price_changes"],
                stats["avail_changes"],
                stats["unchanged"],
            )
        return {"source": source, "total": len(products), **stats}
    except Exception as exc:  # noqa: BLE001
        msg = f"Errore aggiornamento DB da {snapshot_path.name}: {exc}"
        log.exception(msg)
        if report:
            report.note_error("update_db", msg, snapshot=str(snapshot_path), source=source)
        return empty


def _apply_retention(source: str) -> None:
    if RETENTION <= 0:
        return
    snaps = _snapshots(source)
    to_delete = snaps[:-RETENTION] if len(snaps) > RETENTION else []
    for fpath in to_delete:
        fpath.unlink(missing_ok=True)
        log.info("Retention: eliminato %s", fpath.name)


def cmd_subito_dedup(
    *,
    files: list[Path] | None = None,
    latest_n: int = 0,
) -> Path:
    """Unisce e deduplica snapshot Subito in un nuovo snapshot."""
    from scrapers.base import deduplicate, save_snapshot

    if files:
        input_paths = [Path(f).expanduser().resolve() for f in files]
    else:
        snaps = _snapshots("subito")
        if latest_n > 0:
            snaps = snaps[-latest_n:]
        input_paths = snaps

    if not input_paths:
        raise RuntimeError("Nessuno snapshot Subito disponibile per la deduplica.")

    all_products: list[dict] = []
    for path in input_paths:
        if not path.exists():
            raise FileNotFoundError(f"Snapshot non trovato: {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        products = data.get("products") or []
        if not isinstance(products, list):
            continue
        all_products.extend(products)

    unique = deduplicate(all_products)
    out = save_snapshot(
        source="subito",
        products=unique,
        url="merged://subito-dedup",
        data_dir=DATA_DIR,
    )
    log.info(
        "Deduplica Subito completata: %d -> %d annunci (snapshot input: %d).",
        len(all_products),
        len(unique),
        len(input_paths),
    )
    return out


def _archive_old_snapshots(days: int = ARCHIVE_AFTER_DAYS) -> int:
    """Comprime in data/archive gli snapshot JSON più vecchi di N giorni."""
    threshold = datetime.now(timezone.utc) - timedelta(days=days)
    archive_dir = DATA_DIR / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    archived = 0
    for snap in DATA_DIR.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(snap.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if mtime >= threshold:
            continue

        gz_path = archive_dir / f"{snap.name}.gz"
        if gz_path.exists():
            snap.unlink(missing_ok=True)
            continue

        with snap.open("rb") as src, gzip.open(gz_path, "wb") as dst:
            dst.write(src.read())
        snap.unlink(missing_ok=True)
        archived += 1

    if archived:
        log.info("Archivio snapshot: compressi %d file", archived)
    return archived


def _vacuum_databases() -> dict[str, int]:
    counts: dict[str, int] = {}
    db_path = _ROOT / "tracker.db"
    if db_path.exists():
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("VACUUM")
        counts["tracker.db"] = 1
    else:
        counts["tracker.db"] = 0
    return counts


def cmd_cleanup() -> dict:
    """Esegue manutenzione storage: retention, archiviazione, VACUUM DB."""
    for source in _enabled_sources():
        _apply_retention(source)

    archived = _archive_old_snapshots()
    db_clean = _db.clean_db()
    vacuumed = _vacuum_databases()

    return {
        "archived_snapshots": archived,
        "db_clean": db_clean,
        "vacuumed": vacuumed,
    }


# --------------------------------------------------------------------------- #
# Crontab setup
# --------------------------------------------------------------------------- #

def cmd_setup_cron(
    *,
    scrape_schedule: str = "0 */6 * * *",   # ogni 6 ore
    sold_schedule: str = "30 0 * * *",      # ogni giorno alle 00:30
    source: str = "subito",
    sold_batch: int = 1200,
    sold_concurrency: int = 5,
) -> None:
    """Configura cron per scraping + verifica venduti incrementale giornaliera."""
    python = sys.executable
    script = str(_ROOT / "run.py")
    LOGS_DIR.mkdir(exist_ok=True)
    log_file = LOGS_DIR / "cron.log"

    safe_source = shlex.quote(source)
    marker_scrape = f"# xbox-tracker-cron:scrape:{source}"
    marker_sold = "# xbox-tracker-cron:verify-sold:daily"

    scrape_cmd = (
        f"cd \"{_ROOT}\" && "
        f"{python} \"{script}\" --source {safe_source} "
        f">> \"{log_file}\" 2>&1"
    )
    sold_cmd = (
        f"cd \"{_ROOT}\" && "
        f"{python} \"{script}\" --verify-sold {int(sold_batch)} "
        f"--verify-chunk-size 300 --verify-max-runtime-minutes 50 "
        f"--verify-browser-restart-every 3 --concurrency {int(sold_concurrency)} "
        f">> \"{log_file}\" 2>&1"
    )

    line_scrape = f"{scrape_schedule} {scrape_cmd}  {marker_scrape}"
    line_sold = f"{sold_schedule} {sold_cmd}  {marker_sold}"

    res = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = res.stdout if res.returncode == 0 else ""

    lines = [
        line
        for line in existing.splitlines()
        if marker_scrape not in line and marker_sold not in line
    ]
    lines.extend([line_scrape, line_sold])
    new_crontab = "\n".join(lines) + "\n"

    subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)

    print("✅  Crontab configurato (scrape + verify sold incrementale)")
    print(f"   Scrape: {scrape_schedule} -> {scrape_cmd}")
    print(f"   Sold:   {sold_schedule} -> {sold_cmd}")
    print(f"   Log:    {log_file}")
    print()
    print("   Verifica:  crontab -l")
    print("   Rimuovi:   crontab -e  (elimina le righe manualmente)")


# --------------------------------------------------------------------------- #
# Viewer (delegato a server.py)
# --------------------------------------------------------------------------- #

def cmd_view(
    port: int = DEFAULT_PORT,
    host: str = DEFAULT_HOST,
    *,
    open_browser: bool = DEFAULT_OPEN_BROWSER,
    api_token: str = DEFAULT_API_TOKEN,
) -> None:
    import server as _server

    _server.start_server(
        port=port,
        host=host,
        open_browser=open_browser,
        api_token=api_token,
        data_dir=DATA_DIR,
        sources_cfg=SOURCES_CFG,
        enabled_sources=_enabled_sources(),
    )


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def _build_command_string() -> str:
    return "python3 run.py " + " ".join(shlex.quote(arg) for arg in sys.argv[1:])


def main() -> None:
    parser = argparse.ArgumentParser(description="Xbox Price Tracker — Multi-fonte")
    parser.add_argument(
        "--source",
        default="all",
        help="Sorgente: gamelife, gamepeople, gameshock, rebuy, cex, subito, ebay, all",
    )
    parser.add_argument("--view", action="store_true", help="Avvia solo il viewer web")
    parser.add_argument("--all", action="store_true", help="Scrape + avvia viewer")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Tutto: scrape Subito + eBay + classifica AI + viewer",
    )
    parser.add_argument("--cleanup", action="store_true", help="Retention + archiviazione + VACUUM DB")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Porta web (default {DEFAULT_PORT})")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Host web (default {DEFAULT_HOST})")
    parser.add_argument("--no-browser", action="store_true", help="Non aprire automaticamente il browser")
    parser.add_argument("--setup-cron", action="store_true", help="Configura crontab per scraping automatico")
    parser.add_argument(
        "--cron-scrape-schedule",
        default="0 */6 * * *",
        help="Schedule cron scraping (default: ogni 6 ore)",
    )
    parser.add_argument(
        "--cron-sold-schedule",
        default="30 0 * * *",
        help="Schedule cron verifica venduti (default: ogni giorno 00:30)",
    )
    parser.add_argument(
        "--cron-sold-batch",
        type=int,
        default=1200,
        help="Batch giornaliero verify_sold nella configurazione cron",
    )
    parser.add_argument(
        "--classify",
        action="store_true",
        help="Classifica attributi annunci Subito (family/segment/edition/canonical)",
    )
    parser.add_argument("--classify-limit", type=int, default=None, help="Limite annunci da classificare")
    parser.add_argument("--classify-dry-run", action="store_true", help="Classificazione AI senza salvare")
    parser.add_argument(
        "--classify-rebuild-all",
        action="store_true",
        help="Riclassifica tutti gli annunci Subito (title+body).",
    )
    parser.add_argument(
        "--ai-classify",
        action="store_true",
        help="Classifica ai_status/ai_confidence con Haiku 4.5 (titolo+descrizione).",
    )
    parser.add_argument("--ai-limit", type=int, default=None, help="Limite annunci per ai_classifier")
    parser.add_argument("--ai-batch-size", type=int, default=50, help="Batch size ai_classifier")
    parser.add_argument("--ai-concurrency", type=int, default=5, help="Concorrenza ai_classifier")
    parser.add_argument(
        "--ai-all",
        action="store_true",
        help="Classifica tutti gli annunci in ai_classifier (non solo pending+NULL).",
    )
    parser.add_argument(
        "--ai-reset-first",
        action="store_true",
        help="Resetta ai_status/ai_confidence prima di ai_classifier.",
    )
    parser.add_argument(
        "--subito-rebuild-all",
        action="store_true",
        help="Pipeline completa Subito: scrape -> verify sold -> reset AI -> ai_classifier all -> classifier rebuild.",
    )
    parser.add_argument(
        "--subito-region",
        default=None,
        help="Regione/i Subito (slug o nome, separati da virgola) per scrape mirato.",
    )
    parser.add_argument(
        "--subito-keyword",
        default="xbox",
        help="Keyword scrape Subito (default: xbox).",
    )
    parser.add_argument(
        "--subito-max-pages",
        type=int,
        default=300,
        help="Pagine max per regione nello scrape Subito (default: 300).",
    )
    parser.add_argument(
        "--subito-region-concurrency",
        type=int,
        default=1,
        help="Numero regioni Subito processate in parallelo (default: 1).",
    )
    parser.add_argument(
        "--subito-no-strict-xbox",
        action="store_true",
        help="Disabilita filtro locale anti-risultati non Xbox nello scrape Subito.",
    )
    parser.add_argument(
        "--subito-no-dedup",
        action="store_true",
        help="Disabilita deduplica finale nello scrape Subito.",
    )
    parser.add_argument(
        "--subito-dedup",
        action="store_true",
        help="Unisce e deduplica snapshot Subito esistenti senza riscrapare.",
    )
    parser.add_argument(
        "--subito-dedup-files",
        default=None,
        help="Lista file snapshot Subito (separati da virgola) da deduplicare.",
    )
    parser.add_argument(
        "--subito-dedup-latest",
        type=int,
        default=0,
        help="Usa solo gli ultimi N snapshot Subito in dedup (0=tutti).",
    )
    parser.add_argument(
        "--subito-dedup-update-db",
        action="store_true",
        help="Dopo dedup aggiorna il DB Subito con lo snapshot risultante.",
    )
    parser.add_argument(
        "--valuation-report",
        action="store_true",
        help="Stampa fair value + spiegazioni + backtest",
    )
    parser.add_argument(
        "--tune-valuation",
        action="store_true",
        help="Esegue tuning pesi fair value e salva logs/valuation_tuning_latest.json",
    )
    parser.add_argument("--api-token", default=DEFAULT_API_TOKEN, help="Token bearer per endpoint POST viewer")
    parser.add_argument("--test-telegram", action="store_true", help="Invia un messaggio di test su Telegram")
    parser.add_argument(
        "--verify-sold",
        type=int,
        nargs="?",
        const=200,
        default=None,
        metavar="N",
        help="Verifica N annunci Subito (default 200) per segnare i venduti",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Concorrenza per la verifica annunci (default 3)",
    )
    parser.add_argument(
        "--verify-chunk-size",
        type=int,
        default=350,
        help="Chunk size operativo per verify_sold (default 350)",
    )
    parser.add_argument(
        "--verify-max-runtime-minutes",
        type=int,
        default=45,
        help="Durata massima run verify_sold in minuti (0 = senza limite)",
    )
    parser.add_argument(
        "--verify-browser-restart-every",
        type=int,
        default=3,
        help="Riavvio browser ogni N chunk in verify_sold (default 3)",
    )
    parser.add_argument(
        "--verify-include-rejected",
        action="store_true",
        help="Include annunci ai_status=rejected durante verify_sold",
    )
    parser.add_argument(
        "--verify-xbox-only",
        dest="verify_xbox_only",
        action="store_true",
        help="Verifica solo annunci con segnali testuali Xbox (default: attivo).",
    )
    parser.add_argument(
        "--no-verify-xbox-only",
        dest="verify_xbox_only",
        action="store_false",
        help="Disabilita filtro verify xbox-only.",
    )
    parser.set_defaults(verify_xbox_only=True)
    parser.add_argument(
        "--verify-all",
        action="store_true",
        help="Verifica TUTTI gli annunci Subito nel DB (ignora batch)",
    )
    args = parser.parse_args()

    report = RunReport(command=_build_command_string())
    ok = False

    try:
        with report.step("init_db"):
            _db.init_db()
            _db_subito.init_db()
            _db_ebay.init_db()

        if args.source == "all":
            sources = _enabled_sources()
        else:
            sources = [src.strip() for src in args.source.split(",") if src.strip()]

        if args.setup_cron:
            with report.step("setup_cron", {"source": "subito"}):
                cmd_setup_cron(
                    scrape_schedule=args.cron_scrape_schedule,
                    sold_schedule=args.cron_sold_schedule,
                    source="subito",
                    sold_batch=args.cron_sold_batch,
                    sold_concurrency=args.concurrency,
                )
            ok = True
            return

        if args.subito_dedup:
            dedup_files = [Path(p) for p in _split_csv(args.subito_dedup_files)] if args.subito_dedup_files else None
            with report.step(
                "subito_dedup",
                {
                    "latest_n": args.subito_dedup_latest,
                    "files": [str(p) for p in dedup_files] if dedup_files else [],
                },
            ):
                out = cmd_subito_dedup(files=dedup_files, latest_n=int(args.subito_dedup_latest or 0))
                if args.subito_dedup_update_db:
                    _update_db_from_snapshot(out, report=report)
            ok = True
            return

        custom_subito_scrape = any(
            [
                bool(args.subito_region),
                (args.subito_keyword or "xbox").strip().lower() != "xbox",
                int(args.subito_max_pages) != 300,
                int(args.subito_region_concurrency) != 1,
                bool(args.subito_no_strict_xbox),
                bool(args.subito_no_dedup),
            ]
        )
        if custom_subito_scrape and not args.subito_rebuild_all:
            subito_opts = {
                "regions": _split_csv(args.subito_region),
                "keyword": args.subito_keyword,
                "max_pages": args.subito_max_pages,
                "strict_xbox": not args.subito_no_strict_xbox,
                "dedup_results": not args.subito_no_dedup,
                "region_concurrency": args.subito_region_concurrency,
            }
            with report.step("scrape_source", {"source": "subito-custom", **subito_opts}):
                _, _stats = cmd_scrape(["subito"], report=report, subito_options=subito_opts)
            ok = True
            return

        if args.verify_sold is not None or args.verify_all:
            import verify_sold as _vs
            batch = 999999 if args.verify_all else (200 if args.verify_sold is None else int(args.verify_sold))
            max_runtime = (
                None
                if int(args.verify_max_runtime_minutes) <= 0
                else int(args.verify_max_runtime_minutes)
            )
            log.info(
                "Avvio verifica annunci Subito (batch=%d, concurrency=%d, chunk=%d, include_rejected=%s, xbox_only=%s)…",
                batch,
                args.concurrency,
                args.verify_chunk_size,
                args.verify_include_rejected,
                args.verify_xbox_only,
            )
            with report.step(
                "verify_sold",
                {
                    "batch_size": batch,
                    "concurrency": args.concurrency,
                    "chunk_size": args.verify_chunk_size,
                    "include_rejected": args.verify_include_rejected,
                },
            ):
                asyncio.run(
                    _vs.verify_batch(
                        batch_size=batch,
                        verify_all=args.verify_all,
                        include_rejected=args.verify_include_rejected,
                        xbox_only=args.verify_xbox_only,
                        max_runtime_minutes=max_runtime,
                        cfg=_vs.VerifyConfig(
                            concurrency=args.concurrency,
                            chunk_size=args.verify_chunk_size,
                            browser_restart_every=args.verify_browser_restart_every,
                        ),
                    )
                )
            ok = True
            return

        if args.cleanup:
            with report.step("cleanup"):
                summary = cmd_cleanup()
                log.info("Cleanup completato: %s", summary)
            ok = True
            return

        if args.classify:
            import classifier as _classifier

            with report.step("classify"):
                _classifier.run_classifier(
                    limit=args.classify_limit,
                    dry_run=args.classify_dry_run,
                    rebuild_all=args.classify_rebuild_all,
                )
            ok = True
            return

        if args.ai_classify:
            import ai_classifier as _aic

            with report.step("ai_classify_subito"):
                asyncio.run(
                    _aic.run_ai_classifier(
                        batch_size=args.ai_batch_size,
                        concurrency=args.ai_concurrency,
                        classify_all=args.ai_all,
                        reset_first=args.ai_reset_first,
                        limit=args.ai_limit,
                    )
                )
            ok = True
            return

        if args.subito_rebuild_all:
            subito_opts = {
                "regions": _split_csv(args.subito_region),
                "keyword": args.subito_keyword,
                "max_pages": args.subito_max_pages,
                "strict_xbox": not args.subito_no_strict_xbox,
                "dedup_results": not args.subito_no_dedup,
                "region_concurrency": args.subito_region_concurrency,
            }
            log.info("[ 1/5 ] Scrape completo Subito…")
            _, _s1 = cmd_scrape(["subito"], report=report, subito_options=subito_opts)

            log.info("[ 2/5 ] Verify sold incrementale (approved+pending)…")
            import verify_sold as _vs
            with report.step("verify_sold"):
                asyncio.run(
                    _vs.verify_batch(
                        batch_size=2000,
                        verify_all=False,
                        include_rejected=False,
                        xbox_only=args.verify_xbox_only,
                        max_runtime_minutes=50,
                        cfg=_vs.VerifyConfig(
                            concurrency=max(args.concurrency, 5),
                            chunk_size=max(120, min(int(args.verify_chunk_size), 240)),
                            browser_restart_every=max(args.verify_browser_restart_every, 3),
                        ),
                    )
                )

            log.info("[ 3/5 ] Reset + AI classify completo (Haiku 4.5)…")
            import ai_classifier as _aic
            with report.step("ai_classify_subito"):
                asyncio.run(
                    _aic.run_ai_classifier(
                        batch_size=max(args.ai_batch_size, 50),
                        concurrency=max(args.ai_concurrency, 5),
                        classify_all=True,
                        reset_first=True,
                        limit=args.ai_limit,
                    )
                )

            log.info("[ 4/5 ] Ricatalogazione completa attributi (title+body)…")
            import classifier as _classifier
            with report.step("classify"):
                _classifier.run_classifier(
                    dry_run=False,
                    rebuild_all=True,
                    limit=args.classify_limit,
                )

            log.info("[ 5/5 ] Completato. Apri viewer con --view per validare.")
            ok = True
            return

        if args.valuation_report:
            import valuation as _valuation

            with report.step("valuation_report"):
                fair = _valuation.compute_fair_values()
                explain = _valuation.explain_fair_values(limit=20)
                backtest = _valuation.backtest_fair_values()
                log.info(
                    "Fair values: %d modelli | Backtest count=%s MAPE=%s MAE=%s",
                    fair.get("total_models", 0),
                    backtest.get("count"),
                    backtest.get("mape"),
                    backtest.get("mae"),
                )
                for row in explain.get("items", [])[:10]:
                    log.info(
                        "  %s -> FV €%.2f | conf %.2f | %s",
                        row.get("key"),
                        row.get("fair_value", 0.0),
                        row.get("confidence", 0.0),
                        row.get("explanation"),
                    )
            ok = True
            return

        if args.tune_valuation:
            import valuation as _valuation

            with report.step("tune_valuation"):
                tuned = _valuation.tune_weights()
                log.info("Tuning completato: %s", tuned.get("best"))
            ok = True
            return

        if args.test_telegram:
            import alerts as _alerts

            log.info("Invio messaggio di test Telegram…")
            success = _alerts._send_telegram(
                "🎮 Xbox Tracker — Test",
                "Connessione Telegram funzionante! Le notifiche deal arriveranno qui.",
            )
            if success:
                log.info("✅ Messaggio Telegram inviato con successo!")
            else:
                log.error("❌ Invio Telegram fallito. Controlla bot_token e chat_id in config.toml")
            ok = success
            return

        if args.full:
            log.info("[ 1/6 ] Scrape Subito…")
            _, _s1 = cmd_scrape(["subito"], report=report)

            log.info("[ 2/6 ] Scrape eBay sold…")
            _, _s2 = cmd_scrape(["ebay"], report=report)

            log.info("[ 3/6 ] Verifica venduti Subito (verify_sold)…")
            import verify_sold as _vs
            with report.step("verify_sold"):
                asyncio.run(
                    _vs.verify_batch(
                        include_rejected=args.verify_include_rejected,
                        xbox_only=args.verify_xbox_only,
                        max_runtime_minutes=(
                            None
                            if int(args.verify_max_runtime_minutes) <= 0
                            else int(args.verify_max_runtime_minutes)
                        ),
                        cfg=_vs.VerifyConfig(
                            concurrency=args.concurrency,
                            chunk_size=args.verify_chunk_size,
                            browser_restart_every=args.verify_browser_restart_every,
                        ),
                    )
                )

            log.info("[ 4/6 ] Filtro AI su Subito (ai_classifier)…")
            if os.environ.get("ANTHROPIC_API_KEY"):
                import ai_classifier as _aic
                with report.step("ai_classify_subito"):
                    asyncio.run(
                        _aic.run_ai_classifier(
                            batch_size=args.ai_batch_size,
                            concurrency=args.ai_concurrency,
                            classify_all=args.ai_all,
                            reset_first=args.ai_reset_first,
                            limit=args.ai_limit,
                        )
                    )
            else:
                log.warning("ANTHROPIC_API_KEY non trovata — filtro AI Subito saltato.")

            log.info("[ 5/6 ] Classificazione attributi Globale (classifier)…")
            if os.environ.get("ANTHROPIC_API_KEY"):
                import classifier as _classifier
                with report.step("classify"):
                    _classifier.run_classifier(rebuild_all=args.classify_rebuild_all)
            else:
                log.warning("ANTHROPIC_API_KEY non trovata — classificazione AI saltata.")

            log.info("[ 6/6 ] Avvio viewer…")
            with report.step("view"):
                cmd_view(
                    port=args.port,
                    host=args.host,
                    open_browser=(not args.no_browser) and DEFAULT_OPEN_BROWSER,
                    api_token=args.api_token,
                )
        elif args.view:
            with report.step("view"):
                cmd_view(
                    port=args.port,
                    host=args.host,
                    open_browser=(not args.no_browser) and DEFAULT_OPEN_BROWSER,
                    api_token=args.api_token,
                )
        elif args.all:
            _, _scrape_stats = cmd_scrape(sources, report=report)
            with report.step("view"):
                cmd_view(
                    port=args.port,
                    host=args.host,
                    open_browser=(not args.no_browser) and DEFAULT_OPEN_BROWSER,
                    api_token=args.api_token,
                )
        else:
            _, _scrape_stats = cmd_scrape(sources, report=report)
            _archive_old_snapshots()
            if _scrape_stats:
                import alerts as _alerts
                _alerts.send_run_summary(_scrape_stats)

        ok = True

    except ConfigError as exc:
        report.note_error("config", str(exc))
        log.error("Configurazione non valida: %s", exc)
        raise
    except Exception as exc:  # noqa: BLE001
        report.note_error("main", str(exc))
        log.exception("Esecuzione fallita")
        raise
    finally:
        report.finalize(ok=ok)
        path = report.write(LOGS_DIR)
        log.info("Run report scritto in: %s", path)


if __name__ == "__main__":
    main()
