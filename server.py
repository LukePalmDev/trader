"""
Server HTTP — Xbox Tracker Viewer API

Modulo estratto da run.py per separazione responsabilità.
Espone le API REST JSON (GET + POST) e serve i file statici del viewer.
"""

from __future__ import annotations

import http.server
import json
import logging
import os
import secrets
import webbrowser
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import db as _db
import db_subito as _db_subito
import db_ebay as _db_ebay

log = logging.getLogger("trader.server")

_ROOT = Path(__file__).parent


# ---------------------------------------------------------------------------
# Helpers snapshot (usati dalle route API)
# ---------------------------------------------------------------------------

def _snapshots(source: str, data_dir: Path) -> list[Path]:
    return sorted(data_dir.glob(f"{source}_*.json"))


def _latest_snapshot(source: str, data_dir: Path) -> Path | None:
    snaps = _snapshots(source, data_dir)
    if not snaps:
        return None
    # Prova l'ultimo snapshot; se è vuoto, cerca il più recente con prodotti
    for snap in reversed(snaps):
        try:
            raw = json.loads(snap.read_text(encoding="utf-8"))
            if raw.get("products"):
                return snap
        except (OSError, json.JSONDecodeError):
            continue
    return snaps[-1]  # fallback: ultimo anche se vuoto


def _all_sources_with_data(enabled_sources: list[str], data_dir: Path) -> list[str]:
    enabled = set(enabled_sources)
    sources: set[str] = set()
    for fpath in data_dir.glob("*.json"):
        stem = fpath.stem
        idx = stem.find("_20")
        if idx < 0:
            continue
        src = stem[:idx]
        if src in enabled:
            sources.add(src)
    return sorted(sources)


# ---------------------------------------------------------------------------
# Handler HTTP con routing API
# ---------------------------------------------------------------------------

def _make_handler(
    api_token: str,
    data_dir: Path,
    sources_cfg: dict,
    enabled_sources: list[str],
):
    """Factory che crea la classe Handler con le dipendenze iniettate."""

    class Handler(http.server.SimpleHTTPRequestHandler):

        def log_message(self, fmt, *args):
            log.debug("HTTP: " + fmt, *args)

        # --- Utilità ---

        def _is_api_path(self, path: str) -> bool:
            return path.startswith("/api/")

        def _safe_int(self, raw: str, default: int, lo: int = 1, hi: int = 100000) -> int:
            try:
                val = int(raw)
                return max(lo, min(val, hi))
            except (ValueError, TypeError):
                return default

        def _parse_query(self) -> dict[str, str]:
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            return {k: v[0] for k, v in qs.items()}

        def _authorize_request(self) -> bool:
            auth = (self.headers.get("Authorization") or "").strip()
            expected = f"Bearer {api_token}"
            return secrets.compare_digest(auth, expected)

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

        def _build_sources_meta(self) -> list[dict]:
            result = []
            for source in _all_sources_with_data(enabled_sources, data_dir):
                snap = _latest_snapshot(source, data_dir)
                cfg = sources_cfg.get(source, {})
                entry = {
                    "id": source,
                    "label": cfg.get("label", source.title()),
                    "color": cfg.get("color", "#888"),
                    "enabled": cfg.get("enabled", True),
                    "snapshots": len(_snapshots(source, data_dir)),
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

        # --- GET routing ---

        def do_GET(self):
            path = self.path.split("?")[0]
            query = self._parse_query()

            # Auth su tutte le API (tranne bootstrap token)
            if self._is_api_path(path) and path != "/api/token":
                if not self._authorize_request():
                    self._json({"ok": False, "error": "unauthorized"}, status=401)
                    return

            # Bootstrap token (solo localhost)
            if path == "/api/token":
                self._json({"token": api_token})
                return

            if self.path == "/api/sources":
                self._json(self._build_sources_meta())
            elif path == "/api/latest":
                source = query.get("source", "gamelife")
                snap = _latest_snapshot(source, data_dir)
                if snap is None:
                    self._404(f"Nessun dato per sorgente: {source}")
                else:
                    self._json_file(snap)
            elif path == "/api/history":
                source = query.get("source", "gamelife")
                snaps = _snapshots(source, data_dir)
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
                    result.append({
                        "filename": snap.name,
                        "scraped_at": meta.get("scraped_at", ""),
                        "total": meta.get("total", 0),
                        "products": prods,
                    })
                self._json(result)
            elif self.path == "/api/combined/latest":
                all_products = []
                for source in _all_sources_with_data(enabled_sources, data_dir):
                    if source == "subito":
                        continue
                    snap = _latest_snapshot(source, data_dir)
                    if not snap:
                        continue
                    try:
                        raw = json.loads(snap.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError) as exc:
                        log.warning("Snapshot non valido %s: %s", snap.name, exc)
                        continue
                    prods = raw.get("products", [])
                    for prod in prods:
                        if not prod.get("source"):
                            prod["source"] = source
                    all_products.extend(prods)
                self._json({"products": all_products, "total": len(all_products)})
            elif path == "/api/db/products":
                self._json(_db.get_all_products())
            elif path == "/api/db/base-models":
                self._json(_db.get_base_models())
            elif path == "/api/db/standard-groups":
                self._json(_db.get_standard_groups())
            elif path == "/api/db/changes":
                days = self._safe_int(query.get("days", "30"), 30, lo=1, hi=365)
                self._json(_db.get_recent_changes(days))
            elif path == "/api/subito/ads":
                self._json(_db_subito.get_all_ads())
            elif path == "/api/subito/stats":
                self._json(_db_subito.get_stats())
            elif path == "/api/subito/ad-history":
                urn_id = query.get("urn_id", "")
                self._json(_db_subito.get_ad_history(urn_id))
            elif path == "/api/db/price-history":
                self._json(_db.get_price_history())
            elif path == "/api/subito/changes":
                days = self._safe_int(query.get("days", "30"), 30, lo=1, hi=365)
                self._json(_db_subito.get_recent_changes(days))
            elif path == "/api/subito/sold":
                self._json(_db_subito.get_sold_ads())
            elif path == "/api/subito/sold-stats":
                self._json(_db_subito.get_sold_stats())
            elif path == "/api/ebay/sold":
                self._json(_db_ebay.get_all_sold())
            elif path == "/api/ebay/stats":
                self._json(_db_ebay.get_stats())
            elif path == "/api/valuation/fair-values":
                import valuation as _valuation
                self._json(_valuation.compute_fair_values())
            elif path == "/api/valuation/subito-opportunities":
                limit = self._safe_int(query.get("limit", "300"), 300, lo=1, hi=5000)
                import valuation as _valuation
                self._json(_valuation.score_subito_opportunities(limit=limit))
            elif path == "/api/valuation/explain":
                limit = self._safe_int(query.get("limit", "100"), 100, lo=1, hi=1000)
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

        # --- POST routing ---

        def do_POST(self):
            path = self.path.split("?")[0]

            if self._is_api_path(path):
                if not self._authorize_request():
                    self._json({"ok": False, "error": "unauthorized"}, status=401)
                    return

            if path == "/api/db/set-base":
                pass
            elif path == "/api/subito/update-ai":
                pass
            else:
                self._json({"ok": False, "error": "not-found"}, status=404)
                return

            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length > 1048576:
                    self._json({"ok": False, "error": "payload-too-large"}, status=413)
                    return
                body = self.rfile.read(length)
                payload = json.loads(body)

                if path == "/api/db/set-base":
                    product_id = int(payload["id"])
                    value = payload.get("value")
                    if not isinstance(value, bool):
                        self._json({"ok": False, "error": "invalid-value-type"}, status=400)
                        return
                    ok = _db.set_base_model(product_id, value)
                    self._json({"ok": ok})

                elif path == "/api/subito/update-ai":
                    ad_id = int(payload["id"])
                    status = str(payload["status"])
                    if status not in ("approved", "pending", "rejected"):
                        self._json({"ok": False, "error": "invalid-status"}, status=400)
                        return
                    _db_subito.update_ai_status(ad_id, status)
                    self._json({"ok": True})

            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                self._json({"ok": False, "error": str(exc)}, status=400)

    return Handler


# ---------------------------------------------------------------------------
# Avvio server
# ---------------------------------------------------------------------------

def start_server(
    port: int = 8080,
    host: str = "127.0.0.1",
    *,
    open_browser: bool = True,
    api_token: str = "",
    data_dir: Path | None = None,
    sources_cfg: dict | None = None,
    enabled_sources: list[str] | None = None,
) -> None:
    """Avvia il server HTTP per il viewer."""

    if data_dir is None:
        data_dir = _ROOT / "data"
    if sources_cfg is None:
        sources_cfg = {}
    if enabled_sources is None:
        enabled_sources = []

    if not api_token:
        api_token = secrets.token_hex(16)
        log.warning("NESSUN API TOKEN CONFIGURATO! Token auto-generato per la sessione: %s", api_token)

    os.chdir(_ROOT)

    Handler = _make_handler(api_token, data_dir, sources_cfg, enabled_sources)

    class _Server(http.server.ThreadingHTTPServer):
        allow_reuse_address = True

    actual_port = port
    httpd = None
    for _attempt in range(10):
        try:
            httpd = _Server((host, actual_port), Handler)
            break
        except OSError as exc:
            if exc.errno in (48, 98):  # Address already in use
                log.warning("Porta %d occupata, provo %d…", actual_port, actual_port + 1)
                actual_port += 1
            else:
                raise

    if httpd is None:
        raise OSError(f"Nessuna porta libera trovata nell'intervallo {port}–{port + 9}")

    url = f"http://{host}:{actual_port}/viewer/index.html"
    log.info("Viewer avviato su: %s", url)
    log.info("API Token: %s  (il viewer lo legge automaticamente dall'header di bootstrap)", api_token)
    log.info("Premi Ctrl+C per fermare.")

    if open_browser:
        webbrowser.open(url)

    with httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            log.info("Server fermato.")
