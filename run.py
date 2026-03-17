#!/usr/bin/env python3
"""
Trader — Entry point multi-fonte

Usage:
  python3 run.py                          # scrape tutte le fonti abilitate
  python3 run.py --source gamelife        # scrape sorgente specifica
  python3 run.py --source gamelife,cex    # scrape sorgenti multiple
  python3 run.py --view                   # avvia il viewer web
  python3 run.py --all                    # scrape tutte + viewer
  python3 run.py --all --source gamelife  # scrape gamelife + viewer
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        raise ImportError("Python < 3.11 richiede 'tomli': pip install tomli")

import db as _db

_ROOT = Path(__file__).parent
with open(_ROOT / "config.toml", "rb") as _f:
    _CFG = tomllib.load(_f)

DATA_DIR     = _ROOT / _CFG["data"]["output_dir"]
VIEWER_DIR   = _ROOT / "viewer"
DEFAULT_PORT = _CFG["viewer"]["port"]
RETENTION    = _CFG["data"]["retention_keep"]
SOURCES_CFG  = _CFG.get("sources", {})

log = logging.getLogger("trader")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

# Mappa nome_sorgente → modulo Python da importare
_SCRAPER_MODULES = {
    "gamelife":     "scrapers.gamelife",
    "gamepeople":   "scrapers.gamepeople",
    "jollyrogerbay":"scrapers.jollyrogerbay",
    "gameshock":    "scrapers.gameshock",
    "rebuy":        "scrapers.rebuy",
    "cex":          "scrapers.cex",
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
    for f in DATA_DIR.glob("*.json"):
        stem = f.stem
        idx  = stem.find("_20")
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

def _run_scraper(source: str) -> Path | None:
    """Esegue lo scraper per la sorgente indicata."""
    if source not in _SCRAPER_MODULES:
        log.error("Sorgente sconosciuta: %r. Disponibili: %s",
                  source, list(_SCRAPER_MODULES.keys()))
        return None
    if source in SOURCES_CFG and not SOURCES_CFG[source].get("enabled", True):
        log.info("Sorgente %r disabilitata in config.toml — skip.", source)
        return None

    import importlib
    mod = importlib.import_module(_SCRAPER_MODULES[source])
    try:
        return mod.main()
    except Exception as exc:
        log.error("Errore scraper %r: %s", source, exc)
        return None


def cmd_scrape(sources: list[str]) -> list[Path]:
    """Esegue lo scrape per le sorgenti indicate, poi applica retention e aggiorna DB."""
    results = []
    for source in sources:
        log.info("=" * 60)
        log.info("Avvio scraper: %s", source)
        path = _run_scraper(source)
        if path:
            results.append(path)
            _update_db_from_snapshot(path)
        _apply_retention(source)
    return results


def _update_db_from_snapshot(snapshot_path: Path) -> None:
    """Legge uno snapshot JSON e aggiorna il DB con change detection."""
    try:
        data     = json.loads(snapshot_path.read_text(encoding="utf-8"))
        products = data.get("products", [])
        if not products:
            return
        stats = _db.process_products(products)
        log.info(
            "DB aggiornato — nuovi: %d | prezzi cambiati: %d | "
            "disponibilità cambiata: %d | invariati: %d",
            stats["new"], stats["price_changes"],
            stats["avail_changes"], stats["unchanged"],
        )
    except Exception as exc:
        log.error("Errore aggiornamento DB da %s: %s", snapshot_path.name, exc)


def _apply_retention(source: str) -> None:
    if RETENTION <= 0:
        return
    snaps     = _snapshots(source)
    to_delete = snaps[:-RETENTION] if len(snaps) > RETENTION else []
    for f in to_delete:
        f.unlink()
        log.info("Retention: eliminato %s", f.name)


# --------------------------------------------------------------------------- #
# Viewer
# --------------------------------------------------------------------------- #

def cmd_view(port: int = DEFAULT_PORT) -> None:
    import http.server
    import webbrowser

    os.chdir(_ROOT)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # sopprime log HTTP

        def do_GET(self):
            path  = self.path.split("?")[0]
            query = self._parse_query()

            # --- /api/sources ---
            if self.path == "/api/sources":
                self._json(self._build_sources_meta())

            # --- /api/latest?source=X ---
            elif path == "/api/latest":
                source = query.get("source", "gamelife")
                snap   = _latest_snapshot(source)
                if snap is None:
                    self._404(f"Nessun dato per sorgente: {source}")
                else:
                    self._json_file(snap)

            # --- /api/history?source=X ---
            elif path == "/api/history":
                source = query.get("source", "gamelife")
                snaps  = _snapshots(source)
                result = []
                for s in snaps:
                    try:
                        meta  = json.loads(s.read_text(encoding="utf-8"))
                        prods = meta.get("products", [])
                        for p in prods:
                            if not p.get("source"):
                                p["source"] = source
                        result.append({
                            "filename":   s.name,
                            "scraped_at": meta.get("scraped_at", ""),
                            "total":      meta.get("total", 0),
                            "products":   prods,
                        })
                    except Exception:
                        pass
                self._json(result)

            # --- /api/combined/latest ---
            elif self.path == "/api/combined/latest":
                all_products = []
                for source in _all_sources_with_data():
                    snap = _latest_snapshot(source)
                    if snap:
                        try:
                            data  = json.loads(snap.read_text(encoding="utf-8"))
                            prods = data.get("products", [])
                            for p in prods:
                                if not p.get("source"):
                                    p["source"] = source
                            all_products.extend(prods)
                        except Exception:
                            pass
                self._json({"products": all_products, "total": len(all_products)})

            # --- /api/db/products ---
            elif self.path == "/api/db/products":
                self._json(_db.get_all_products())

            # --- /api/db/base-models ---
            elif self.path == "/api/db/base-models":
                self._json(_db.get_base_models())

            # --- /api/db/changes?days=N ---
            elif path == "/api/db/changes":
                days = int(query.get("days", 30))
                self._json(_db.get_recent_changes(days))

            # --- /api/db/storage-sizes ---
            elif self.path == "/api/db/storage-sizes":
                self._json(_db.get_storage_sizes())

            # --- /api/db/categories ---
            elif self.path == "/api/db/categories":
                self._json(_db.get_categories())

            else:
                super().do_GET()

        def do_POST(self):
            path = self.path.split("?")[0]

            # --- /api/db/set-base  body: {"id": N, "value": true/false} ---
            if path == "/api/db/set-base":
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                try:
                    payload    = json.loads(body)
                    product_id = int(payload["id"])
                    value      = bool(payload["value"])
                    ok         = _db.set_base_model(product_id, value)
                    self._json({"ok": ok})
                except Exception as exc:
                    self._json({"ok": False, "error": str(exc)})
            else:
                self.send_response(404)
                self.end_headers()

        def _parse_query(self) -> dict:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.path)
            qs     = parse_qs(parsed.query)
            return {k: v[0] for k, v in qs.items()}

        def _build_sources_meta(self) -> list[dict]:
            result = []
            for source in _all_sources_with_data():
                snap = _latest_snapshot(source)
                cfg  = SOURCES_CFG.get(source, {})
                entry = {
                    "id":      source,
                    "label":   cfg.get("label", source.title()),
                    "color":   cfg.get("color", "#888"),
                    "enabled": cfg.get("enabled", True),
                    "snapshots": len(_snapshots(source)),
                    "last_scraped": "",
                    "last_total": 0,
                }
                if snap:
                    try:
                        meta = json.loads(snap.read_text(encoding="utf-8"))
                        entry["last_scraped"] = meta.get("scraped_at", "")
                        entry["last_total"]   = meta.get("total", 0)
                    except Exception:
                        pass
                result.append(entry)
            return result

        def _json_file(self, path: Path) -> None:
            body = path.read_bytes()
            self._send_json(body)

        def _json(self, data) -> None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self._send_json(body)

        def _send_json(self, body: bytes) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _404(self, msg: str) -> None:
            body = json.dumps({"error": msg}).encode("utf-8")
            self.send_response(404)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    url = f"http://localhost:{port}/viewer/index.html"
    log.info("Viewer avviato su: %s", url)
    log.info("Premi Ctrl+C per fermare.")
    webbrowser.open(url)

    with http.server.HTTPServer(("", port), Handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            log.info("Server fermato.")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(description="Xbox Price Tracker — Multi-fonte")
    parser.add_argument(
        "--source",
        default="all",
        help="Sorgente: gamelife, gamepeople, gameshock, rebuy, cex, all (default: all)",
    )
    parser.add_argument("--view", action="store_true", help="Avvia solo il viewer web")
    parser.add_argument("--all",  action="store_true", help="Scrape + avvia viewer")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"Porta web (default {DEFAULT_PORT})")
    args = parser.parse_args()

    # Inizializza DB (crea tabelle se non esistono)
    _db.init_db()

    # Risolvi lista sorgenti
    if args.source == "all":
        sources = _enabled_sources()
    else:
        sources = [s.strip() for s in args.source.split(",")]

    if args.view:
        cmd_view(args.port)
    elif args.all:
        cmd_scrape(sources)
        cmd_view(args.port)
    else:
        cmd_scrape(sources)


if __name__ == "__main__":
    main()
