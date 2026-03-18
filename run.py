#!/usr/bin/env python3
"""
Trader — Entry point multi-fonte

Usage:
  python3 run.py                          # scrape tutte le fonti abilitate
  python3 run.py --source gamelife        # scrape sorgente specifica
  python3 run.py --source gamelife,cex    # scrape sorgenti multiple
  python3 run.py --view                   # avvia il viewer web
  python3 run.py --all                    # scrape tutte + viewer
  python3 run.py --full                   # scrape Subito + eBay + AI + viewer
  python3 run.py --cleanup                # retention + archiviazione + VACUUM DB
"""

from __future__ import annotations

import argparse
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

def _run_scraper(source: str, report: RunReport | None = None) -> Path | None:
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


def cmd_scrape(sources: list[str], report: RunReport | None = None) -> list[Path]:
    """Esegue lo scrape per le sorgenti indicate, poi retention e update DB."""
    results: list[Path] = []
    for source in sources:
        ctx = report.step("scrape_source", {"source": source}) if report else nullcontext({})
        with ctx:
            log.info("=" * 60)
            log.info("Avvio scraper: %s", source)
            path = _run_scraper(source, report=report)
            if path:
                results.append(path)
                _update_db_from_snapshot(path, report=report)
            _apply_retention(source)
    return results


def _update_db_from_snapshot(snapshot_path: Path, report: RunReport | None = None) -> None:
    """Legge uno snapshot JSON e aggiorna il DB con change detection."""
    try:
        data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        msg = f"Errore lettura snapshot {snapshot_path.name}: {exc}"
        log.error(msg)
        if report:
            report.note_error("update_db", msg, snapshot=str(snapshot_path))
        return

    products = data.get("products", [])
    source = data.get("source", "")
    if not products:
        return

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
            from scrapers.subito import _BLOCKLIST, _IS_CONSOLE

            removed = _db_subito.refilter_ads(_BLOCKLIST, _IS_CONSOLE)
            if removed:
                log.info("Subito: rimossi %d annunci da filtri aggiornati", removed)

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
    except Exception as exc:  # noqa: BLE001
        msg = f"Errore aggiornamento DB da {snapshot_path.name}: {exc}"
        log.exception(msg)
        if report:
            report.note_error("update_db", msg, snapshot=str(snapshot_path), source=source)


def _apply_retention(source: str) -> None:
    if RETENTION <= 0:
        return
    snaps = _snapshots(source)
    to_delete = snaps[:-RETENTION] if len(snaps) > RETENTION else []
    for fpath in to_delete:
        fpath.unlink(missing_ok=True)
        log.info("Retention: eliminato %s", fpath.name)


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
    for db_name in ("trader.db", "subito.db", "ebay.db"):
        db_path = _ROOT / db_name
        if not db_path.exists():
            counts[db_name] = 0
            continue
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("VACUUM")
        counts[db_name] = 1
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
    schedule: str = "0 */6 * * *",  # ogni 6 ore
    source: str = "subito",
) -> None:
    """Aggiunge una voce crontab per lo scraping automatico."""
    python = sys.executable
    script = str(_ROOT / "run.py")
    LOGS_DIR.mkdir(exist_ok=True)
    log_file = LOGS_DIR / "cron.log"

    marker = f"# xbox-tracker-cron:{source}"
    cron_cmd = (
        f"cd \"{_ROOT}\" && "
        f"{python} \"{script}\" --source {source} "
        f">> \"{log_file}\" 2>&1"
    )
    new_line = f"{schedule} {cron_cmd}  {marker}"

    res = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = res.stdout if res.returncode == 0 else ""

    lines = [line for line in existing.splitlines() if marker not in line]
    lines.append(new_line)
    new_crontab = "\n".join(lines) + "\n"

    subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)

    print(f"✅  Crontab configurato — source: {source}  schedule: {schedule}")
    print(f"   Esegue: {cron_cmd}")
    print(f"   Log:    {log_file}")
    print()
    print("   Verifica:  crontab -l")
    print("   Rimuovi:   crontab -e  (elimina la riga manualmente)")


# --------------------------------------------------------------------------- #
# Viewer
# --------------------------------------------------------------------------- #

def cmd_view(
    port: int = DEFAULT_PORT,
    host: str = DEFAULT_HOST,
    *,
    open_browser: bool = DEFAULT_OPEN_BROWSER,
    api_token: str = DEFAULT_API_TOKEN,
) -> None:
    import http.server
    import webbrowser

    os.chdir(_ROOT)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, fmt, *args):
            log.debug("HTTP: " + fmt, *args)

        def do_GET(self):
            path = self.path.split("?")[0]
            query = self._parse_query()

            if self.path == "/api/sources":
                self._json(self._build_sources_meta())
            elif path == "/api/latest":
                source = query.get("source", "gamelife")
                snap = _latest_snapshot(source)
                if snap is None:
                    self._404(f"Nessun dato per sorgente: {source}")
                else:
                    self._json_file(snap)
            elif path == "/api/history":
                source = query.get("source", "gamelife")
                snaps = _snapshots(source)
                result = []
                for snap in snaps:
                    try:
                        meta = json.loads(snap.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError) as exc:
                        log.warning("Snapshot non valido %s: %s", snap.name, exc)
                        continue

                    prods = meta.get("products", [])
                    for prod in prods:
                        if not prod.get("source"):
                            prod["source"] = source
                    result.append(
                        {
                            "filename": snap.name,
                            "scraped_at": meta.get("scraped_at", ""),
                            "total": meta.get("total", 0),
                            "products": prods,
                        }
                    )
                self._json(result)
            elif self.path == "/api/combined/latest":
                all_products = []
                for source in _all_sources_with_data():
                    if source == "subito":
                        continue
                    snap = _latest_snapshot(source)
                    if not snap:
                        continue
                    try:
                        data = json.loads(snap.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError) as exc:
                        log.warning("Snapshot non valido %s: %s", snap.name, exc)
                        continue
                    prods = data.get("products", [])
                    for prod in prods:
                        if not prod.get("source"):
                            prod["source"] = source
                    all_products.extend(prods)
                self._json({"products": all_products, "total": len(all_products)})
            elif self.path == "/api/db/products":
                self._json(_db.get_all_products())
            elif self.path == "/api/db/base-models":
                self._json(_db.get_base_models())
            elif self.path == "/api/db/standard-groups":
                self._json(_db.get_standard_groups())
            elif path == "/api/db/changes":
                days = int(query.get("days", 30))
                self._json(_db.get_recent_changes(days))
            elif self.path == "/api/subito/ads":
                self._json(_db_subito.get_all_ads())
            elif self.path == "/api/subito/stats":
                self._json(_db_subito.get_stats())
            elif path == "/api/subito/ad-history":
                urn_id = query.get("urn_id", "")
                self._json(_db_subito.get_ad_history(urn_id))
            elif path == "/api/subito/changes":
                days = int(query.get("days", 30))
                self._json(_db_subito.get_recent_changes(days))
            elif self.path == "/api/ebay/sold":
                self._json(_db_ebay.get_all_sold())
            elif self.path == "/api/ebay/stats":
                self._json(_db_ebay.get_stats())
            elif self.path == "/api/valuation/fair-values":
                import valuation as _valuation

                self._json(_valuation.compute_fair_values())
            elif path == "/api/valuation/subito-opportunities":
                limit = int(query.get("limit", 300))
                import valuation as _valuation

                self._json(_valuation.score_subito_opportunities(limit=limit))
            elif path == "/api/valuation/explain":
                limit = int(query.get("limit", 100))
                import valuation as _valuation

                self._json(_valuation.explain_fair_values(limit=limit))
            elif self.path == "/api/valuation/backtest":
                import valuation as _valuation

                self._json(_valuation.backtest_fair_values())
            elif self.path == "/api/db/storage-sizes":
                self._json(_db.get_storage_sizes())
            elif self.path == "/api/db/categories":
                self._json(_db.get_categories())
            elif path == "/api/db/search":
                kinect_raw = query.get("has_kinect", "")
                results = _db.search_products(
                    base_family=query.get("base_family") or None,
                    sub_model=query.get("sub_model") or None,
                    edition_name=query.get("edition_name") or None,
                    color=query.get("color") or None,
                    storage_label=query.get("storage_label") or None,
                    has_kinect=(int(kinect_raw) if kinect_raw in ("0", "1") else None),
                    available_only=query.get("available_only", "") == "1",
                )
                self._json(results)
            else:
                super().do_GET()

        def do_POST(self):
            if not self._authorize_post(api_token):
                self._json({"ok": False, "error": "unauthorized"}, status=401)
                return

            path = self.path.split("?")[0]
            if path != "/api/db/set-base":
                self._json({"ok": False, "error": "not-found"}, status=404)
                return

            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                payload = json.loads(body)
                product_id = int(payload["id"])
                value = bool(payload["value"])
                ok = _db.set_base_model(product_id, value)
                self._json({"ok": ok})
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                self._json({"ok": False, "error": str(exc)}, status=400)

        def _parse_query(self) -> dict[str, str]:
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            return {k: v[0] for k, v in qs.items()}

        def _authorize_post(self, token: str) -> bool:
            if not token:
                return True
            auth = (self.headers.get("Authorization") or "").strip()
            expected = f"Bearer {token}"
            return auth == expected

        def _build_sources_meta(self) -> list[dict]:
            result = []
            for source in _all_sources_with_data():
                snap = _latest_snapshot(source)
                cfg = SOURCES_CFG.get(source, {})
                entry = {
                    "id": source,
                    "label": cfg.get("label", source.title()),
                    "color": cfg.get("color", "#888"),
                    "enabled": cfg.get("enabled", True),
                    "snapshots": len(_snapshots(source)),
                    "last_scraped": "",
                    "last_total": 0,
                }
                if snap:
                    try:
                        meta = json.loads(snap.read_text(encoding="utf-8"))
                        entry["last_scraped"] = meta.get("scraped_at", "")
                        entry["last_total"] = meta.get("total", 0)
                    except (OSError, json.JSONDecodeError) as exc:
                        log.warning("Snapshot non valido %s: %s", snap.name, exc)
                result.append(entry)
            return result

        def _json_file(self, path: Path) -> None:
            body = path.read_bytes()
            self._send_json(body, 200)

        def _json(self, data, *, status: int = 200) -> None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self._send_json(body, status)

        def _send_json(self, body: bytes, status: int) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _404(self, msg: str) -> None:
            self._json({"error": msg}, status=404)

    url = f"http://{host}:{port}/viewer/index.html"
    log.info("Viewer avviato su: %s", url)
    log.info("Premi Ctrl+C per fermare.")

    if open_browser:
        webbrowser.open(url)

    class _Server(http.server.ThreadingHTTPServer):
        allow_reuse_address = True

    with _Server((host, port), Handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            log.info("Server fermato.")


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
        "--classify",
        action="store_true",
        help="Classifica annunci 'other' con AI (richiede ANTHROPIC_API_KEY)",
    )
    parser.add_argument("--classify-limit", type=int, default=None, help="Limite annunci da classificare")
    parser.add_argument("--classify-dry-run", action="store_true", help="Classificazione AI senza salvare")
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
                cmd_setup_cron()
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
                _classifier.run_classifier(limit=args.classify_limit, dry_run=args.classify_dry_run)
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

        if args.full:
            log.info("[ 1/4 ] Scrape Subito…")
            cmd_scrape(["subito"], report=report)

            log.info("[ 2/4 ] Scrape eBay sold…")
            cmd_scrape(["ebay"], report=report)

            log.info("[ 3/4 ] Classificazione AI…")
            if os.environ.get("ANTHROPIC_API_KEY"):
                import classifier as _classifier

                with report.step("classify"):
                    _classifier.run_classifier()
            else:
                log.warning("ANTHROPIC_API_KEY non trovata — classificazione AI saltata.")

            log.info("[ 4/4 ] Avvio viewer…")
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
            cmd_scrape(sources, report=report)
            with report.step("view"):
                cmd_view(
                    port=args.port,
                    host=args.host,
                    open_browser=(not args.no_browser) and DEFAULT_OPEN_BROWSER,
                    api_token=args.api_token,
                )
        else:
            cmd_scrape(sources, report=report)
            _archive_old_snapshots()

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
