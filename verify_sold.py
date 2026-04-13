from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("verify_sold")

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from curl_cffi.requests import AsyncSession as CffiAsyncSession
except ImportError:  # pragma: no cover - fallback runtime
    CffiAsyncSession = None

from scrapers.base import launch_chromium  # noqa: E402
from settings import load_config  # noqa: E402
from db_subito import DB_PATH, _connect, _estimate_sold_window, init_db  # noqa: E402
from patchright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError  # noqa: E402
from playwright_stealth import Stealth  # noqa: E402

_CONFIG_PATH = Path("config.toml")
_CFG = load_config(_CONFIG_PATH)
_COMMON = _CFG["common"]

# Istanza stealth riusabile — si limita ad add_init_script, nessuno stato mutabile.
_STEALTH = Stealth(
    navigator_languages_override=("it-IT", "it"),
    navigator_platform_override="MacIntel",
    navigator_webdriver=False,  # False = nasconde il flag webdriver (era True: bug)
)

DEFAULT_BATCH_SIZE = 200
DEFAULT_RECHECK_DAYS = 7
DEFAULT_CHUNK_SIZE = 350
DEFAULT_MAX_RUNTIME_MINUTES = 45
DEFAULT_BROWSER_RESTART_EVERY = 3
DEFAULT_CONCURRENCY = 3
DEFAULT_CFFI_CONCURRENCY = 12
DEFAULT_MIN_CFFI_CONCURRENCY = 4
DEFAULT_NAV_TIMEOUT_MS = 10_000
DEFAULT_MAX_HTTP403_RATIO = 0.40
DEFAULT_FAIL_FAST_MIN_ATTEMPTS = 150
DEFAULT_FAIL_FAST_BLOCKED_RATIO = 0.85
DEFAULT_FAIL_FAST_403_RATIO = 0.60
DEFAULT_CFFI_BLOCK_UNKNOWN_RATIO = 0.85
DEFAULT_MIN_COVERAGE_RATIO = 0.0


@dataclass
class VerifyConfig:
    """Parametri di tuning per verify_batch(). Separati dai parametri semantici."""

    concurrency: int = DEFAULT_CONCURRENCY
    cffi_concurrency: int = DEFAULT_CFFI_CONCURRENCY
    chunk_size: int = DEFAULT_CHUNK_SIZE
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS
    fail_fast_min_attempts: int = DEFAULT_FAIL_FAST_MIN_ATTEMPTS
    fail_fast_blocked_ratio: float = DEFAULT_FAIL_FAST_BLOCKED_RATIO
    fail_fast_403_ratio: float = DEFAULT_FAIL_FAST_403_RATIO
    max_http403_ratio: float = DEFAULT_MAX_HTTP403_RATIO
    min_coverage_ratio: float = DEFAULT_MIN_COVERAGE_RATIO
    cffi_block_unknown_ratio: float = DEFAULT_CFFI_BLOCK_UNKNOWN_RATIO
    browser_restart_every: int = DEFAULT_BROWSER_RESTART_EVERY
    selection_sample: int = 0


def _compute_cffi_backoff_seconds(consecutive_blocks: int) -> int:
    if consecutive_blocks <= 0:
        return 0
    return min(300, 30 * (2 ** max(0, consecutive_blocks - 1)))


def _count_unstable_hits(reason_counts: dict[str, int]) -> int:
    return sum(
        int(count)
        for reason, count in reason_counts.items()
        if (
            str(reason).startswith("blocked:")
            or "http-403" in str(reason)
            or "http-429" in str(reason)
            or str(reason) == "skipped:cffi-block"
        )
    )


_XBOX_MATCH_CONDITION = """
(
    lower(name) LIKE '%xbox%'
    OR lower(body_text) LIKE '%xbox%'
    OR lower(url) LIKE '%xbox%'
    OR (
        (
            lower(name) LIKE '%series x%'
            OR lower(name) LIKE '%series s%'
            OR lower(name) LIKE '%serie x%'
            OR lower(name) LIKE '%serie s%'
            OR lower(body_text) LIKE '%series x%'
            OR lower(body_text) LIKE '%series s%'
            OR lower(body_text) LIKE '%serie x%'
            OR lower(body_text) LIKE '%serie s%'
        )
        AND (
            lower(name) LIKE '%console%'
            OR lower(body_text) LIKE '%console%'
            OR lower(name) LIKE '%microsoft%'
            OR lower(body_text) LIKE '%microsoft%'
        )
    )
)
"""

_XBOX_SQL_FILTER = f"\nAND {_XBOX_MATCH_CONDITION}\n"

_SOLD_MARKERS = (
    "non più disponibile",
    "annuncio non più disponibile",
    "questo annuncio non è più disponibile",
)


def _is_sold_redirect(url: str) -> bool:
    final_url = (url or "").rstrip("/")
    return final_url == "https://www.subito.it" or (
        "annunci-italia/vendita" in final_url and "q=" in final_url
    )


def _contains_sold_marker(text: str) -> bool:
    normalized = (text or "").lower()
    return any(marker in normalized for marker in _SOLD_MARKERS)
def _check_db_integrity(conn: sqlite3.Connection) -> bool:
    """Verifica integrità del database SQLite tramite PRAGMA integrity_check.

    Args:
        conn: Connessione SQLite attiva.

    Returns:
        True se il DB è integro ("ok"), False altrimenti.
    """
    try:
        result = conn.execute("PRAGMA integrity_check(1)").fetchone()
        return result is not None and result[0] == "ok"
    except Exception as e:
        log.error(f"Errore durante check integrità DB: {e}")
        return False


def _classify_navigation_exception(exc: Exception) -> str:
    msg = str(exc).lower()
    if "err_connection_timed_out" in msg or "timeout" in msg or "timed out" in msg:
        return "blocked:timeout"
    if any(token in msg for token in ("err_name_not_resolved", "err_connection_reset", "err_network_changed")):
        return "blocked:network"
    # Chrome reindirizza a chrome-error://chromewebdata/ quando Akamai chiude la TCP
    # prima che la pagina si carichi; l'eccezione Playwright contiene "interrupted by
    # another navigation" oppure "chrome-error". Non è un bug del codice: classificare
    # silenziosamente come blocked:network (non loggare WARNING).
    if "chrome-error" in msg or "interrupted by another navigation" in msg:
        return "blocked:network"
    return "blocked:error"


def _build_selection_breakdown(
    conn,
    statuses: list[str],
    *,
    xbox_only: bool,
) -> dict[str, int]:
    placeholders = ",".join("?" * len(statuses))
    base_where = f"ai_status IN ({placeholders})"
    status_ok = conn.execute(
        f"SELECT COUNT(*) FROM ads WHERE {base_where}",
        statuses,
    ).fetchone()[0]
    active_unsold = conn.execute(
        (
            f"SELECT COUNT(*) FROM ads WHERE {base_where} "
            "AND last_available = 1 AND sold_at IS NULL"
        ),
        statuses,
    ).fetchone()[0]
    if xbox_only:
        xbox_subset = conn.execute(
            (
                f"SELECT COUNT(*) FROM ads WHERE {base_where} "
                "AND last_available = 1 AND sold_at IS NULL "
                f"AND {_XBOX_MATCH_CONDITION}"
            ),
            statuses,
        ).fetchone()[0]
    else:
        xbox_subset = active_unsold
    return {
        "total_ads": int(conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]),
        "status_ok": int(status_ok),
        "active_unsold": int(active_unsold),
        "xbox_subset": int(xbox_subset),
        "excluded_by_xbox": int(active_unsold - xbox_subset),
    }


def _pick_excluded_by_xbox_sample(
    conn,
    statuses: list[str],
    *,
    limit: int,
) -> list[tuple]:
    if limit <= 0:
        return []
    placeholders = ",".join("?" * len(statuses))
    query = (
        "SELECT id, url, name FROM ads "
        f"WHERE ai_status IN ({placeholders}) "
        "AND last_available = 1 AND sold_at IS NULL "
        f"AND NOT {_XBOX_MATCH_CONDITION} "
        "ORDER BY COALESCE(last_seen, first_seen) DESC "
        "LIMIT ?"
    )
    return conn.execute(query, list(statuses) + [int(limit)]).fetchall()


async def _new_context(browser):
    """Context fresco con stealth completo per bypassare i controlli bot Akamai di Subito."""
    ctx = await browser.new_context(
        user_agent=_COMMON["user_agent"],
        viewport={
            "width": _COMMON["viewport_width"],
            "height": _COMMON["viewport_height"],
        },
        locale=_COMMON["locale"],
    )
    await _STEALTH.apply_stealth_async(ctx)
    return ctx


async def _cffi_precheck(url: str, session: "CffiAsyncSession") -> str:
    """Pre-check leggero con curl-cffi: impersona Chrome a livello TLS, nessun JS.

    Subito.it risponde 410 per annunci venduti/scaduti e 200 per annunci attivi.
    Questo consente di filtrare i venduti certi senza aprire Playwright.

    Returns:
        "sold"    — 404/410 HTTP o redirect a homepage/ricerca (definitivo)
        "active"  — 200 con URL finale invariato (alta confidenza = attivo)
        "unknown" — 403/errore/altro → demanda a Playwright
    """
    try:
        resp = await session.get(url, timeout=10, allow_redirects=True)
        if resp.status_code in (404, 410):
            return "sold"
        if resp.status_code == 200:
            final = str(resp.url).rstrip("/")
            if final == "https://www.subito.it" or (
                "annunci-italia/vendita" in final and "q=" in final
            ):
                return "sold"
            return "active"
        # 403 = bloccato da Akamai, 5xx = errore server → Playwright
        return "unknown"
    except Exception:
        return "unknown"


async def check_url(
    worker: dict,
    url: str,
    *,
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS,
) -> tuple[bool, str]:
    """Verifica URL con strategia strict (accuratezza > velocità)."""
    ctx = worker["ctx"]
    page = worker.get("page")
    if page is None or page.is_closed():
        page = await ctx.new_page()
        worker["page"] = page

    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=int(nav_timeout_ms))
    except PlaywrightTimeoutError:
        return True, "blocked:timeout"
    except Exception as exc:
        reason = _classify_navigation_exception(exc)
        if reason == "blocked:error":
            log.warning("Errore navigazione %s: %s", url, exc)
        return True, reason

    if not resp:
        return True, "blocked:no-response"

    if _is_sold_redirect(page.url):
        log.debug("  Redirect rilevato: %s -> %s", url, page.url)
        return False, "sold:redirect"

    if resp.status in (404, 410):
        return False, "sold:http-status"

    if resp.status == 403:
        return True, "blocked:http-403"
    if resp.status == 429:
        return True, "blocked:http-429"
    if resp.status in (500, 502, 503, 504):
        return True, f"blocked:http-{resp.status}"

    sold_count = await page.locator("text=/non più disponibile/i").count()
    if sold_count > 0:
        return False, "sold:dom-marker"

    return True, "active:dom-ok"


async def _reset_worker_session(worker: dict) -> None:
    """Ricrea context+page del worker dopo blocchi/timeout, per isolare la sessione."""
    page = worker.get("page")
    if page is not None:
        try:
            if not page.is_closed():
                await page.close()
        except Exception:
            pass
    ctx = worker.get("ctx")
    if ctx is not None:
        try:
            await ctx.close()
        except Exception:
            pass
    browser = worker["browser"]
    new_ctx = await _new_context(browser)
    new_page = await new_ctx.new_page()
    worker["ctx"] = new_ctx
    worker["page"] = new_page


_SUBITO_HOME = "https://www.subito.it"


async def _warmup_worker(worker: dict, timeout_ms: int = 8_000) -> None:
    """Carica la homepage di Subito per acquisire cookie Akamai prima di verificare annunci."""
    page = worker.get("page")
    if page is None or page.is_closed():
        return
    try:
        await page.goto(_SUBITO_HOME, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass  # warmup best-effort: un fallimento non blocca la verifica


async def _create_worker(browser, idx: int) -> dict:
    """Crea un singolo worker (context + page) per il pool Playwright."""
    ctx = await _new_context(browser)
    page = await ctx.new_page()
    return {"id": idx, "browser": browser, "ctx": ctx, "page": page}


def _deadline_reached(deadline_utc: datetime | None) -> bool:
    return deadline_utc is not None and datetime.now(timezone.utc) >= deadline_utc


async def _process_rows(
    worker_pool: asyncio.Queue,
    rows: list,
    *,
    deadline_utc: datetime | None = None,
    stagger_secs: float = 0.0,
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS,
    cffi_concurrency: int = DEFAULT_CFFI_CONCURRENCY,
    cffi_block_unknown_ratio: float = DEFAULT_CFFI_BLOCK_UNKNOWN_RATIO,
) -> dict:
    """Verifica concorrente con Playwright (+ precheck curl-cffi).

    stagger_secs > 0 scagliona le prime pool_size navigazioni di questo chunk nel tempo:
    task 0 parte subito, task 1 dopo stagger_secs, task 2 dopo 2×stagger_secs, ecc.
    Va usato solo sul primo chunk dopo un browser restart per evitare che 20 contesti
    freschi aprano 20 TCP connection simultanee verso Subito (trigger ban Akamai).

    La concorrenza Playwright è controllata dal pool di worker (worker_pool.qsize()).

    Returns:
        dict con chiavi: verified, active, sold, avg_price_sold, avg_hours_active
    """
    now = datetime.now(timezone.utc).isoformat()

    if _deadline_reached(deadline_utc):
        return {
            "verified": 0,
            "active": 0,
            "sold": 0,
            "already_sold": 0,
            "recovered": 0,
            "sold_price_sum": 0.0,
            "sold_price_count": 0,
            "sold_hour_sum": 0.0,
            "sold_hour_count": 0,
            "avg_price_sold": None,
            "avg_hours_active": None,
            "skipped": len(rows),
            "time_limit_hit": True,
        }

    time_limit_hit = False
    skipped_rows = 0

    # ---------- Fase 1: curl-cffi precheck (sempre attivo se disponibile) ----------
    # curl-cffi impersona Chrome a livello TLS (JA3/JA4), bypassando Akamai senza browser.
    # Subito.it risponde 410 per venduti e 200 per attivi: segnale affidabile e verificato.
    # I "sold" certi (410/404/redirect) saltano Playwright completamente.
    # I "active" (200) vengono inviati a Playwright per conferma DOM.
    # I "unknown" (403/errore) vengono inviati a Playwright come fallback.
    cffi_sold_ids: set[int] = set()
    cffi_active_ids: set[int] = set()
    _cffi_block = False  # True se cffi segnala blocco IP massivo (≥80% unknown)

    if CffiAsyncSession is not None and not _deadline_reached(deadline_utc):
        cffi_results: dict[int, str] = {}
        cffi_sem = asyncio.Semaphore(max(1, int(cffi_concurrency)))

        async def _cffi_one(session, row) -> None:
            async with cffi_sem:
                await asyncio.sleep(random.uniform(0.03, 0.12))
                cffi_results[row["id"]] = await _cffi_precheck(row["url"], session)

        async with CffiAsyncSession(impersonate="chrome136") as cffi_session:
            await asyncio.gather(*[_cffi_one(cffi_session, r) for r in rows])

        cffi_sold_ids = {r["id"] for r in rows if cffi_results.get(r["id"]) == "sold"}
        cffi_active_ids = {r["id"] for r in rows if cffi_results.get(r["id"]) == "active"}
        cffi_unknown_count = len(rows) - len(cffi_sold_ids) - len(cffi_active_ids)
        log.info(
            "cffi precheck: sold=%d | active=%d | unknown(→Playwright)=%d",
            len(cffi_sold_ids),
            len(cffi_active_ids),
            cffi_unknown_count,
        )
        # Blocco IP rilevato: se ≥80% degli URL sono "unknown" (403/errori),
        # Akamai ha bannato l'IP. Playwright otterrebbe gli stessi blocchi
        # bruciando 100+ secondi per 0 verifiche. Salta e marca tutto pending.
        _cffi_block_ratio = cffi_unknown_count / max(1, len(rows))
        if _cffi_block_ratio >= float(cffi_block_unknown_ratio):
            _cffi_block = True
            log.warning(
                "cffi blocco massivo (%.0f%% unknown, sem=%d): skip Playwright, chunk → pending",
                _cffi_block_ratio * 100,
                max(1, int(cffi_concurrency)),
            )
    else:
        if CffiAsyncSession is None:
            log.warning("curl-cffi non disponibile: precheck saltato, tutto a Playwright.")

    # Annunci già classificati da cffi → escludi da Playwright.
    # Se cffi ha segnalato blocco IP massivo, forza needs_playwright vuoto:
    # tutti i non-sold vengono marcati pending (vedi skipped_ids più sotto).
    skip_playwright_ids = cffi_sold_ids | cffi_active_ids
    needs_playwright = (
        []
        if _cffi_block
        else [r for r in rows if r["id"] not in skip_playwright_ids]
    )

    # ---------- Fase 2: Playwright solo per gli "unknown" ----------
    # La concorrenza è governata dal pool: solo worker_pool.qsize() check girano in parallelo.
    playwright_results: dict[int, tuple[bool | None, str]] = {}  # ad_id → (is_active|None, reason)
    reason_counts: dict[str, int] = {}

    # Numero di worker disponibili all'inizio del chunk (tutti in coda dopo un restart).
    pool_size = worker_pool.qsize()

    async def _check_one_pw(row, stagger: float = 0.0) -> None:
        if stagger > 0.0:
            await asyncio.sleep(stagger)
        await asyncio.sleep(random.uniform(0.3, 0.9))
        worker = await worker_pool.get()
        try:
            is_active, reason = await check_url(
                worker,
                row["url"],
                nav_timeout_ms=nav_timeout_ms,
            )
            if reason.startswith("blocked:http-403"):
                # Nessun retry: l'IP è bannato a livello Akamai e un secondo tentativo
                # sullo stesso IP dopo 0.35s non cambia nulla, bruciando 7s di timeout.
                # Reset del context per lasciare il worker in stato pulito.
                await _reset_worker_session(worker)
        finally:
            await worker_pool.put(worker)
        if reason.startswith("blocked:"):
            playwright_results[row["id"]] = (None, reason)
        else:
            playwright_results[row["id"]] = (is_active, reason)
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    if needs_playwright:
        for batch_idx, start in enumerate(range(0, len(needs_playwright), 200)):
            if _deadline_reached(deadline_utc):
                time_limit_hit = True
                break
            batch = needs_playwright[start : start + 200]
            # Stagger solo sul primo batch del chunk post-restart:
            # task i attende i * stagger_secs prima di aprire la connessione.
            # Questo distribuisce le N TCP connection su N*stagger_secs secondi
            # invece di spikarle tutte nello stesso momento (→ ERR_CONNECTION_TIMED_OUT).
            if batch_idx == 0 and stagger_secs > 0.0:
                tasks = [
                    _check_one_pw(r, i * stagger_secs if i < pool_size else 0.0)
                    for i, r in enumerate(batch)
                ]
            else:
                tasks = [_check_one_pw(r) for r in batch]
            await asyncio.gather(*tasks)

    # Aggiungi conteggi cffi a reason_counts per il reporting chunk
    if cffi_sold_ids:
        reason_counts["sold:cffi-410"] = reason_counts.get("sold:cffi-410", 0) + len(cffi_sold_ids)
    if cffi_active_ids:
        reason_counts["active:cffi-200"] = reason_counts.get("active:cffi-200", 0) + len(cffi_active_ids)
    if _cffi_block:
        _block_pending_count = len(rows) - len(cffi_sold_ids)
        reason_counts["skipped:cffi-block"] = reason_counts.get("skipped:cffi-block", 0) + _block_pending_count

    # ---------- Unifica risultati ----------
    results: list[tuple] = []
    # Annunci venduti da cffi precheck (is_active=False certi: 410/404/redirect)
    for row in rows:
        if row["id"] in cffi_sold_ids:
            results.append((row, False))
    # Annunci attivi da cffi precheck (is_active=True ad alta confidenza: HTTP 200)
    for row in rows:
        if row["id"] in cffi_active_ids:
            results.append((row, True))
    # Se cffi ha segnalato blocco IP, pre-popola skipped_ids con tutti i non-sold.
    # needs_playwright è già vuoto, quindi il loop sottostante non aggiunge altri ID.
    skipped_ids: list[int] = (
        [r["id"] for r in rows if r["id"] not in cffi_sold_ids]
        if _cffi_block
        else []
    )
    skipped_rows += len(skipped_ids)
    # Annunci verificati da Playwright
    for row in needs_playwright:
        ad_id = row["id"]
        if ad_id in playwright_results:
            is_active, _ = playwright_results[ad_id]
            if is_active is None:
                skipped_rows += 1
                skipped_ids.append(ad_id)
            else:
                results.append((row, is_active))
        else:
            # deadline raggiunta prima del check Playwright
            skipped_rows += 1
            skipped_ids.append(ad_id)

    conn = _connect(DB_PATH)
    conn.isolation_level = None
    sold_count = 0
    already_sold_count = 0
    recovered_count = 0
    active_count = 0
    sold_prices: list[float] = []
    sold_hours: list[float] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        for row, is_active in results:
            ad_id = row["id"]
            url = row["url"]
            price = row["last_price"]
            first_seen = row["first_seen"]
            last_seen = row["last_seen"]
            last_active_seen = row["last_active_seen"]
            first_inactive_seen = row["first_inactive_seen"]
            sold_at = row["sold_at"]
            sold_at_estimated = row["sold_at_estimated"]
            sold_window_hours = row["sold_window_hours"]
            was_available = int(row["last_available"] or 0) == 1

            try:
                if is_active:
                    if not was_available:
                        recovered_count += 1
                        log.info("  -> RIATTIVATO: %s", url)
                    conn.execute(
                        """
                        UPDATE ads
                        SET last_seen = ?,
                            last_available = 1,
                            last_active_seen = ?,
                            first_inactive_seen = NULL,
                            sold_at = NULL,
                            sold_at_estimated = NULL,
                            sold_window_hours = NULL,
                            verify_status = 'buyable'
                        WHERE id = ?
                        """,
                        (now, now, ad_id),
                    )
                    active_count += 1
                else:
                    if not first_inactive_seen:
                        first_inactive_seen = now
                    if not sold_at:
                        sold_at = now

                    if not sold_at_estimated:
                        sold_at_estimated, sold_window_hours = _estimate_sold_window(
                            last_active_seen=(last_active_seen or last_seen),
                            first_inactive_seen=first_inactive_seen,
                        )
                        if sold_at_estimated is None:
                            sold_at_estimated = sold_at

                    conn.execute(
                        """
                        UPDATE ads
                        SET last_available = 0,
                            sold_at = ?,
                            last_seen = ?,
                            first_inactive_seen = ?,
                            sold_at_estimated = ?,
                            sold_window_hours = ?,
                            verify_status = 'sold'
                        WHERE id = ?
                        """,
                        (
                            sold_at,
                            now,
                            first_inactive_seen,
                            sold_at_estimated,
                            sold_window_hours,
                            ad_id,
                        ),
                    )
                    if was_available:
                        conn.execute(
                            """
                            INSERT INTO ad_changes
                                (ad_id, changed_at, price_old, price_new, available_old, available_new, change_type)
                            VALUES (?, ?, ?, ?, 1, 0, 'availability')
                            """,
                            (ad_id, now, price, price),
                        )
                        sold_count += 1
                        log.info("  -> VENDUTO: %s", url)

                        if price and price > 0:
                            sold_prices.append(price)
                        if first_seen:
                            try:
                                fs = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
                                sold_dt = datetime.fromisoformat((sold_at_estimated or sold_at or now).replace("Z", "+00:00"))
                                hours = (sold_dt - fs).total_seconds() / 3600.0
                                if hours >= 0:
                                    sold_hours.append(hours)
                            except (ValueError, TypeError):
                                pass
                    else:
                        already_sold_count += 1
            except Exception as e:
                log.error("DB error for ad %s: %s", ad_id, e)

        conn.execute("COMMIT")
    except Exception as e:
        log.error("Errore transazione batch DB: %s", e)
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        skipped_ids = []  # non aggiornare pending se il batch è fallito
    finally:
        conn.close()

    # Aggiorna gli annunci bloccati/non verificati → verify_status = 'pending'
    # Saranno prioritizzati nel prossimo run (ORDER BY pending first).
    if skipped_ids:
        try:
            _pending_conn = _connect(DB_PATH)
            with _pending_conn:
                placeholders_p = ",".join("?" * len(skipped_ids))
                _pending_conn.execute(
                    f"UPDATE ads SET verify_status = 'pending' WHERE id IN ({placeholders_p})",
                    skipped_ids,
                )
            _pending_conn.close()
        except Exception as e:
            log.error("Errore aggiornamento verify_status pending: %s", e)

    sold_price_sum = sum(sold_prices)
    sold_price_count = len(sold_prices)
    sold_hour_sum = sum(sold_hours)
    sold_hour_count = len(sold_hours)
    avg_price_sold = (sold_price_sum / sold_price_count) if sold_price_count else None
    avg_hours_active = (sold_hour_sum / sold_hour_count) if sold_hour_count else None

    return {
        "verified": len(results),
        "active": active_count,
        "sold": sold_count,
        "already_sold": already_sold_count,
        "recovered": recovered_count,
        "sold_price_sum": sold_price_sum,
        "sold_price_count": sold_price_count,
        "sold_hour_sum": sold_hour_sum,
        "sold_hour_count": sold_hour_count,
        "avg_price_sold": avg_price_sold,
        "avg_hours_active": avg_hours_active,
        "skipped": skipped_rows,
        "time_limit_hit": time_limit_hit,
        "reason_counts": reason_counts,
    }


async def verify_batch(
    batch_size: int = DEFAULT_BATCH_SIZE,
    verify_all: bool = False,
    recheck_days=None,  # int or None
    include_rejected: bool = False,
    xbox_only: bool = True,
    max_runtime_minutes: int | None = DEFAULT_MAX_RUNTIME_MINUTES,
    cfg: VerifyConfig = None,
) -> dict:
    """Recupera un batch di annunci e li verifica online con concorrenza.

    Args:
        batch_size:          Numero massimo di annunci da verificare (ignorato se verify_all=True).
        verify_all:          Se True, verifica tutti gli annunci attivi non verificati.
        recheck_days:        Se impostato, ri-verifica anche i venduti degli ultimi N giorni.
        include_rejected:    Include anche annunci ai_status='rejected' (default False).
        xbox_only:           Verifica solo annunci rilevanti Xbox (default True).
        max_runtime_minutes: Stop soft oltre la durata indicata.
        cfg:                 Parametri di tuning (VerifyConfig). Se None usa i default.

    Returns:
        Statistiche della sessione di verifica.
    """
    if cfg is None:
        cfg = VerifyConfig()
    conn = _connect(DB_PATH)

    # Pre-run DB integrity check
    if not _check_db_integrity(conn):
        conn.close()
        log.error("Database corrotto (PRAGMA integrity_check failed). Aborting.")
        sys.exit(1)

    statuses = ["approved", "pending"]
    if include_rejected:
        statuses.append("rejected")
    placeholders = ",".join("?" * len(statuses))

    select_cols = (
        "id, url, last_price, first_seen, last_seen, last_available, "
        "last_active_seen, first_inactive_seen, sold_at, sold_at_estimated, sold_window_hours, "
        "verify_status"
    )
    base_query = (
        f"SELECT {select_cols} FROM ads "
        f"WHERE ai_status IN ({placeholders}) "
        "AND last_available = 1 AND sold_at IS NULL "
        + (_XBOX_SQL_FILTER if xbox_only else "")
        + " ORDER BY CASE verify_status WHEN 'pending' THEN 0 ELSE 1 END,"
        " COALESCE(last_seen, first_seen) ASC"
    )

    active_params: list = list(statuses)
    if verify_all:
        rows = conn.execute(base_query, active_params).fetchall()
    else:
        rows = conn.execute(base_query + " LIMIT ?", active_params + [int(batch_size)]).fetchall()

    recheck_rows: list = []
    if recheck_days is not None and recheck_days > 0:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=recheck_days)
        ).isoformat()
        recheck_query = (
            f"SELECT {select_cols} FROM ads "
            f"WHERE ai_status IN ({placeholders}) "
            "AND sold_at IS NOT NULL AND sold_at >= ? "
            + (_XBOX_SQL_FILTER if xbox_only else "")
            + " ORDER BY sold_at DESC"
        )
        recheck_params = list(statuses) + [cutoff]
        recheck_rows = conn.execute(recheck_query, recheck_params).fetchall()

    selection_breakdown = _build_selection_breakdown(
        conn,
        statuses,
        xbox_only=xbox_only,
    )
    excluded_sample = _pick_excluded_by_xbox_sample(
        conn,
        statuses,
        limit=max(0, int(cfg.selection_sample)),
    ) if xbox_only and cfg.selection_sample else []

    conn.close()

    all_rows = list(rows) + list(recheck_rows)
    if not all_rows:
        log.info("Nessun annuncio da verificare.")
        return {
            "verified": 0,
            "active": 0,
            "sold": 0,
            "already_sold": 0,
            "recovered": 0,
            "avg_price_sold": None,
            "avg_hours_active": None,
            "skipped": 0,
            "time_limit_hit": False,
        }

    log.info(
        (
            "Selezione verify-sold: totale=%d | status_ok=%d | attivi_non_venduti=%d "
            "| subset_finale=%d%s"
        ),
        selection_breakdown["total_ads"],
        selection_breakdown["status_ok"],
        selection_breakdown["active_unsold"],
        selection_breakdown["xbox_subset"] if xbox_only else selection_breakdown["active_unsold"],
        (
            f" | esclusi_da_xbox_only={selection_breakdown['excluded_by_xbox']}"
            if xbox_only
            else ""
        ),
    )
    if excluded_sample:
        for row in excluded_sample:
            log.info(
                "  escluso xbox-only id=%s | %s | %s",
                row["id"],
                row["name"],
                row["url"],
            )

    log.info(
        "Inizio verifica di %d annunci (concorrenza=%d, cffi=%d, chunk=%d, stati=%s)%s…",
        len(all_rows),
        cfg.concurrency,
        max(1, int(cfg.cffi_concurrency)),
        max(1, int(cfg.chunk_size)),
        ",".join(statuses),
        (
            (f" + {len(recheck_rows)} ri-verifiche venduti" if recheck_rows else "")
            + (" | filtro xbox-only ON" if xbox_only else " | filtro xbox-only OFF")
        ),
    )

    start_ts = datetime.now(timezone.utc)
    max_runtime = (
        timedelta(minutes=max(1, int(max_runtime_minutes)))
        if max_runtime_minutes is not None
        else None
    )
    deadline_utc = (start_ts + max_runtime) if max_runtime is not None else None

    total_stats = {
        "verified": 0,
        "active": 0,
        "sold": 0,
        "already_sold": 0,
        "recovered": 0,
        "avg_price_sold": None,
        "avg_hours_active": None,
        "skipped": 0,
        "blocked_403": 0,
        "blocked_cffi": 0,
        "blocked_total": 0,
        "blocked_timeout": 0,
        "blocked_network": 0,
        "blocked_error": 0,
        "time_limit_hit": False,
    }
    session_reason_counts: dict[str, int] = {}
    sold_price_sum = 0.0
    sold_price_count = 0
    sold_hour_sum = 0.0
    sold_hour_count = 0
    chunk_len = max(1, int(cfg.chunk_size))
    restart_every = max(1, int(cfg.browser_restart_every))
    target_concurrency = max(1, int(cfg.concurrency))
    min_concurrency = max(1, target_concurrency // 3)
    current_concurrency = target_concurrency
    initial_cffi_concurrency = max(1, int(cfg.cffi_concurrency))
    min_cffi_concurrency = max(1, min(DEFAULT_MIN_CFFI_CONCURRENCY, initial_cffi_concurrency))
    current_cffi_concurrency = initial_cffi_concurrency
    force_restart_next = False
    consecutive_cffi_blocks = 0  # Track consecutive chunks with massive cffi blocking

    async def _close_pool(pool: asyncio.Queue) -> None:
        """Chiude worker pool (page + context)."""
        while not pool.empty():
            worker = pool.get_nowait()
            page = worker.get("page")
            if page is not None:
                try:
                    if not page.is_closed():
                        await page.close()
                except Exception:
                    pass
            ctx = worker.get("ctx")
            if ctx is not None:
                try:
                    await ctx.close()
                except Exception:
                    pass

    async with async_playwright() as p:
        browser = None
        worker_pool: asyncio.Queue | None = None
        try:
            for idx in range(0, len(all_rows), chunk_len):
                if max_runtime is not None and datetime.now(timezone.utc) - start_ts >= max_runtime:
                    log.warning(
                        "Raggiunto max runtime (%s min). Stop dopo %d/%d annunci.",
                        int(max_runtime.total_seconds() // 60),
                        total_stats["verified"],
                        len(all_rows),
                    )
                    break

                chunk = all_rows[idx : idx + chunk_len]
                chunk_no = idx // chunk_len
                is_restart = (
                    force_restart_next
                    or browser is None
                    or (chunk_no > 0 and chunk_no % restart_every == 0)
                )
                if is_restart:
                    force_restart_next = False
                    # Chiudi pool e browser esistenti prima del restart
                    if worker_pool is not None:
                        await _close_pool(worker_pool)
                        worker_pool = None
                    if browser is not None:
                        await browser.close()
                    # Avvia nuovo browser e pre-crea pool di worker (context + page).
                    browser = await launch_chromium(
                        p,
                        headless=True,
                        preferred_channel=_COMMON.get("playwright_channel", "chrome"),
                    )
                    worker_pool = asyncio.Queue()
                    # Crea context+page di tutti i worker in parallelo (era sequenziale).
                    workers_created: list[dict] = list(
                        await asyncio.gather(
                            *[_create_worker(browser, i) for i in range(current_concurrency)]
                        )
                    )
                    # Warmup: carica homepage Subito in parallelo per acquisire cookie Akamai.
                    await asyncio.gather(*[_warmup_worker(w) for w in workers_created])
                    for w in workers_created:
                        worker_pool.put_nowait(w)
                    log.info(
                        "Browser session restart (chunk %d), pool=%d worker.",
                        chunk_no + 1,
                        current_concurrency,
                    )

                log.info(
                    "Chunk %d: verifica annunci %d-%d / %d",
                    chunk_no + 1,
                    idx + 1,
                    min(idx + len(chunk), len(all_rows)),
                    len(all_rows),
                )
                if worker_pool is None:
                    raise RuntimeError("worker_pool non inizializzato prima della verifica chunk")
                # Dopo ogni browser restart scagliona le prime N navigazioni di 0.15s l'una
                # per evitare spike di connessioni simultanee verso Subito.
                chunk_t0 = time.perf_counter()
                stats = await _process_rows(
                    worker_pool,
                    chunk,
                    deadline_utc=deadline_utc,
                    stagger_secs=1.5 if is_restart else 0.0,
                    nav_timeout_ms=cfg.nav_timeout_ms,
                    cffi_concurrency=current_cffi_concurrency,
                    cffi_block_unknown_ratio=cfg.cffi_block_unknown_ratio,
                )
                chunk_elapsed = max(0.001, time.perf_counter() - chunk_t0)
                reason_counts = stats.get("reason_counts") or {}
                attempted_in_chunk = sum(int(v) for v in reason_counts.values())
                if attempted_in_chunk <= 0:
                    attempted_in_chunk = int(stats.get("verified", 0) or 0) + int(stats.get("skipped", 0) or 0)
                chunk_rate = attempted_in_chunk / chunk_elapsed
                for reason, count in reason_counts.items():
                    session_reason_counts[reason] = session_reason_counts.get(reason, 0) + int(count)
                top_reasons = ", ".join(
                    f"{k}={v}"
                    for k, v in sorted(reason_counts.items(), key=lambda item: item[1], reverse=True)[:4]
                ) if reason_counts else "n/a"
                log.info(
                    "Chunk %d completato in %.1fs (%.2f tentativi/s, verificati=%d/%d) | reason: %s",
                    chunk_no + 1,
                    chunk_elapsed,
                    chunk_rate,
                    int(stats.get("verified", 0) or 0),
                    attempted_in_chunk,
                    top_reasons,
                )
                unstable_hits = _count_unstable_hits(reason_counts)
                unstable_ratio = unstable_hits / max(1, attempted_in_chunk)

                # Anti-burst throttling: scala sia backoff sia concorrenza cffi
                # quando il chunk finisce quasi interamente in cffi-block.
                cffi_block_count = int(reason_counts.get("skipped:cffi-block", 0) or 0)
                cffi_block_ratio = cffi_block_count / max(1, attempted_in_chunk)
                if cffi_block_ratio >= 0.70:
                    consecutive_cffi_blocks += 1
                    new_cffi_concurrency = max(
                        min_cffi_concurrency,
                        current_cffi_concurrency - 4,
                    )
                    if new_cffi_concurrency != current_cffi_concurrency:
                        log.warning(
                            "Chunk %d in cffi-block (%.1f%%): cffi %d -> %d",
                            chunk_no + 1,
                            cffi_block_ratio * 100,
                            current_cffi_concurrency,
                            new_cffi_concurrency,
                        )
                        current_cffi_concurrency = new_cffi_concurrency
                    force_restart_next = True
                    if idx + chunk_len < len(all_rows):
                        burst_delay = _compute_cffi_backoff_seconds(consecutive_cffi_blocks)
                        log.warning(
                            "Cloudflare burst rilevato (%d chunk consecutivi con cffi-block): pausa %ds",
                            consecutive_cffi_blocks,
                            burst_delay,
                        )
                        await asyncio.sleep(burst_delay)
                else:
                    if consecutive_cffi_blocks > 0 and cffi_block_count == 0:
                        log.info(
                            "cffi recovery dopo %d chunk bloccati consecutivi.",
                            consecutive_cffi_blocks,
                        )
                    if (
                        cffi_block_count == 0
                        and unstable_ratio <= 0.05
                        and current_cffi_concurrency < initial_cffi_concurrency
                    ):
                        new_cffi_concurrency = min(
                            initial_cffi_concurrency,
                            current_cffi_concurrency + 2,
                        )
                        if new_cffi_concurrency != current_cffi_concurrency:
                            log.info(
                                "Chunk %d stabile: cffi %d -> %d",
                                chunk_no + 1,
                                current_cffi_concurrency,
                                new_cffi_concurrency,
                            )
                            current_cffi_concurrency = new_cffi_concurrency
                    consecutive_cffi_blocks = 0

                if unstable_ratio >= 0.25 and current_concurrency > min_concurrency:
                    new_concurrency = max(min_concurrency, current_concurrency - 4)
                    if new_concurrency != current_concurrency:
                        log.warning(
                            "Chunk %d instabile (%.1f%% error/ban): concorrenza %d -> %d",
                            chunk_no + 1,
                            unstable_ratio * 100,
                            current_concurrency,
                            new_concurrency,
                        )
                        current_concurrency = new_concurrency
                        force_restart_next = True
                elif unstable_ratio <= 0.05 and current_concurrency < target_concurrency:
                    new_concurrency = min(target_concurrency, current_concurrency + 2)
                    if new_concurrency != current_concurrency:
                        log.info(
                            "Chunk %d stabile (%.1f%% error/ban): concorrenza %d -> %d",
                            chunk_no + 1,
                            unstable_ratio * 100,
                            current_concurrency,
                            new_concurrency,
                        )
                        current_concurrency = new_concurrency
                        force_restart_next = True

                # Cooldown inter-chunk adattivo: scala proporzionale all'intensità del blocco.
                # Ban totale (>95%) richiede più recovery del ban moderato (60-70%).
                if cffi_block_count == 0 and unstable_ratio >= 0.60 and idx + chunk_len < len(all_rows):
                    if unstable_ratio >= 0.95:
                        cooldown_secs = 75
                    elif unstable_ratio >= 0.85:
                        cooldown_secs = 45
                    elif unstable_ratio >= 0.70:
                        cooldown_secs = 25
                    else:
                        cooldown_secs = 15
                    log.info(
                        "Cooldown inter-chunk %d: %.1f%% blocked → pausa %ds",
                        chunk_no + 1,
                        unstable_ratio * 100,
                        cooldown_secs,
                    )
                    await asyncio.sleep(cooldown_secs)

                total_stats["verified"] += stats["verified"]
                total_stats["active"] += stats["active"]
                total_stats["sold"] += stats["sold"]
                total_stats["already_sold"] += stats["already_sold"]
                total_stats["recovered"] += stats["recovered"]
                total_stats["skipped"] += int(stats.get("skipped", 0) or 0)
                total_stats["blocked_403"] += int(
                    sum(v for k, v in reason_counts.items() if "http-403" in k)
                )
                total_stats["blocked_cffi"] += cffi_block_count
                total_stats["blocked_total"] += int(
                    cffi_block_count
                    + sum(v for k, v in reason_counts.items() if str(k).startswith("blocked:"))
                )
                total_stats["blocked_timeout"] += int(
                    sum(v for k, v in reason_counts.items() if "blocked:timeout" in str(k))
                )
                total_stats["blocked_network"] += int(
                    sum(v for k, v in reason_counts.items() if "blocked:network" in str(k))
                )
                total_stats["blocked_error"] += int(
                    sum(v for k, v in reason_counts.items() if "blocked:error" in str(k))
                )
                sold_price_sum += float(stats["sold_price_sum"] or 0.0)
                sold_price_count += int(stats["sold_price_count"] or 0)
                sold_hour_sum += float(stats["sold_hour_sum"] or 0.0)
                sold_hour_count += int(stats["sold_hour_count"] or 0)
                cumulative_attempted = int(total_stats["verified"]) + int(total_stats["skipped"])
                if cumulative_attempted >= max(1, int(cfg.fail_fast_min_attempts)):
                    cumulative_blocked_ratio = float(total_stats["blocked_total"]) / float(cumulative_attempted)
                    cumulative_403_ratio = float(total_stats["blocked_403"]) / float(cumulative_attempted)
                    if (
                        cumulative_blocked_ratio >= float(cfg.fail_fast_blocked_ratio)
                        and cumulative_403_ratio >= float(cfg.fail_fast_403_ratio)
                    ):
                        raise RuntimeError(
                            "Fail-fast: runner probabilmente bloccato (Akamai/network). "
                            f"blocked={cumulative_blocked_ratio * 100:.2f}% "
                            f"403={cumulative_403_ratio * 100:.2f}% "
                            f"su {cumulative_attempted} tentativi"
                        )
                if stats.get("time_limit_hit"):
                    total_stats["time_limit_hit"] = True
                    log.warning(
                        "Raggiunto max runtime durante chunk %d. Stop su %d/%d annunci.",
                        chunk_no + 1,
                        total_stats["verified"],
                        len(all_rows),
                    )
                    break
        finally:
            if worker_pool is not None:
                await _close_pool(worker_pool)
            if browser is not None:
                await browser.close()

    if sold_price_count:
        total_stats["avg_price_sold"] = sold_price_sum / sold_price_count
    if sold_hour_count:
        total_stats["avg_hours_active"] = sold_hour_sum / sold_hour_count

    attempted_total = int(total_stats["verified"]) + int(total_stats["skipped"])
    if attempted_total <= 0:
        attempted_total = 1
    blocked_403_ratio = float(total_stats["blocked_403"]) / float(attempted_total)
    blocked_cffi_ratio = float(total_stats["blocked_cffi"]) / float(attempted_total)
    blocked_total_ratio = float(total_stats["blocked_total"]) / float(attempted_total)
    coverage_ratio = float(total_stats["verified"]) / float(max(1, attempted_total))
    total_stats["blocked_403_ratio"] = blocked_403_ratio
    total_stats["blocked_cffi_ratio"] = blocked_cffi_ratio
    total_stats["blocked_total_ratio"] = blocked_total_ratio
    total_stats["coverage_ratio"] = coverage_ratio

    top_session_reasons = ", ".join(
        f"{k}={v}"
        for k, v in sorted(session_reason_counts.items(), key=lambda item: item[1], reverse=True)[:8]
    ) if session_reason_counts else "n/a"
    log.info(
        "Reason summary sessione: %s",
        top_session_reasons,
    )
    log.info(
        (
            "HTTP block summary: 403=%d/%d (%.2f%%) | cffi-block=%d/%d (%.2f%%) "
            "| blocked_totali=%d/%d (%.2f%%) | timeout=%d | network=%d | error=%d"
        ),
        total_stats["blocked_403"],
        attempted_total,
        blocked_403_ratio * 100,
        total_stats["blocked_cffi"],
        attempted_total,
        blocked_cffi_ratio * 100,
        total_stats["blocked_total"],
        attempted_total,
        blocked_total_ratio * 100,
        total_stats["blocked_timeout"],
        total_stats["blocked_network"],
        total_stats["blocked_error"],
    )

    avg_p = (
        f"€ {total_stats['avg_price_sold']:.2f}"
        if total_stats["avg_price_sold"] is not None
        else "—"
    )
    avg_h_raw = total_stats["avg_hours_active"]
    if avg_h_raw is not None:
        avg_h = (
            f"{round(avg_h_raw)} ore"
            if avg_h_raw < 48
            else f"{round(avg_h_raw / 24)} giorni"
        )
    else:
        avg_h = "—"

    log.info(
        "Verifica completata. %d verificati, %d skipped (coverage %.1f%%): %d attivi, %d venduti nuovi, "
        "%d venduti confermati, %d riattivati | prezzo medio venduto %s | tempo attivo medio %s%s",
        total_stats["verified"],
        total_stats["skipped"],
        coverage_ratio * 100,
        total_stats["active"],
        total_stats["sold"],
        total_stats["already_sold"],
        total_stats["recovered"],
        avg_p,
        avg_h,
        " | STOP per runtime" if total_stats.get("time_limit_hit") else "",
    )

    # Warning se coverage bassa
    if coverage_ratio < 0.70:
        log.warning(
            "Coverage bassa (%.1f%% < 70%%): verifica interrotta da blocchi massivi o errori",
            coverage_ratio * 100,
        )
    if blocked_403_ratio > cfg.max_http403_ratio:
        log.warning(
            "HTTP 403 alto: %.2f%% > %.2f%% (soglia) — dati parziali disponibili",
            blocked_403_ratio * 100,
            cfg.max_http403_ratio * 100,
        )
        total_stats["high_block_rate"] = True
    if cfg.min_coverage_ratio > 0 and coverage_ratio < cfg.min_coverage_ratio:
        log.error(
            "Coverage finale %.1f%% sotto soglia minima %.1f%%",
            coverage_ratio * 100,
            cfg.min_coverage_ratio * 100,
        )
        total_stats["coverage_below_min"] = True
    return total_stats


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verifica se gli annunci Subito sono ancora attivi o sono stati venduti."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Numero di annunci da verificare per esecuzione (default {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="verify_all",
        help="Verifica tutti gli annunci non-verificati (ignora --batch-size)",
    )
    parser.add_argument(
        "--re-check",
        type=int,
        default=None,
        metavar="DAYS",
        dest="recheck_days",
        help=(
            "Ri-verifica i venduti degli ultimi N giorni per conferma "
            f"(default disabilitato; suggerito: {DEFAULT_RECHECK_DAYS})"
        ),
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        metavar="N",
        help=f"Numero di URL verificati in parallelo (default {DEFAULT_CONCURRENCY})",
    )
    parser.add_argument(
        "--cffi-concurrency",
        type=int,
        default=DEFAULT_CFFI_CONCURRENCY,
        metavar="N",
        help=(
            "Numero massimo di precheck curl-cffi in parallelo "
            f"(default {DEFAULT_CFFI_CONCURRENCY})"
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        metavar="N",
        help=f"Numero di annunci per chunk operativo (default {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=DEFAULT_MAX_RUNTIME_MINUTES,
        metavar="N",
        help=(
            "Durata massima run in minuti (default "
            f"{DEFAULT_MAX_RUNTIME_MINUTES}; usa 0 per disabilitare)"
        ),
    )
    parser.add_argument(
        "--browser-restart-every",
        type=int,
        default=DEFAULT_BROWSER_RESTART_EVERY,
        metavar="N",
        help=(
            "Riavvia browser ogni N chunk per stabilità anti-bot "
            f"(default {DEFAULT_BROWSER_RESTART_EVERY})"
        ),
    )
    parser.add_argument(
        "--include-rejected",
        action="store_true",
        help="Include anche annunci AI rejected (default: solo approved+pending).",
    )
    parser.add_argument(
        "--xbox-only",
        dest="xbox_only",
        action="store_true",
        help="Verifica solo annunci con segnali testuali Xbox (default: attivo).",
    )
    parser.add_argument(
        "--no-xbox-only",
        dest="xbox_only",
        action="store_false",
        help="Disabilita filtro xbox-only.",
    )
    parser.set_defaults(xbox_only=True)
    parser.add_argument(
        "--nav-timeout-ms",
        type=int,
        default=DEFAULT_NAV_TIMEOUT_MS,
        metavar="MS",
        help=(
            "Timeout goto(wait_until='domcontentloaded') in millisecondi "
            f"(default {DEFAULT_NAV_TIMEOUT_MS})"
        ),
    )
    parser.add_argument(
        "--selection-sample",
        type=int,
        default=0,
        metavar="N",
        help="Logga N annunci esclusi dal filtro xbox-only (default 0).",
    )
    parser.add_argument(
        "--max-http403-ratio",
        type=float,
        default=DEFAULT_MAX_HTTP403_RATIO,
        metavar="R",
        help=(
            "Soglia massima tollerata di 403 (0.02 = 2%). "
            f"Default {DEFAULT_MAX_HTTP403_RATIO:.2f}"
        ),
    )
    parser.add_argument(
        "--fail-fast-min-attempts",
        type=int,
        default=DEFAULT_FAIL_FAST_MIN_ATTEMPTS,
        metavar="N",
        help=(
            "Abilita stop anticipato dopo almeno N tentativi "
            f"(default {DEFAULT_FAIL_FAST_MIN_ATTEMPTS})"
        ),
    )
    parser.add_argument(
        "--fail-fast-blocked-ratio",
        type=float,
        default=DEFAULT_FAIL_FAST_BLOCKED_RATIO,
        metavar="R",
        help=(
            "Soglia blocked totale per fail-fast (0.85 = 85%). "
            f"Default {DEFAULT_FAIL_FAST_BLOCKED_RATIO:.2f}"
        ),
    )
    parser.add_argument(
        "--fail-fast-403-ratio",
        type=float,
        default=DEFAULT_FAIL_FAST_403_RATIO,
        metavar="R",
        help=(
            "Soglia 403 per fail-fast (0.60 = 60%). "
            f"Default {DEFAULT_FAIL_FAST_403_RATIO:.2f}"
        ),
    )
    parser.add_argument(
        "--cffi-block-unknown-ratio",
        type=float,
        default=DEFAULT_CFFI_BLOCK_UNKNOWN_RATIO,
        metavar="R",
        help=(
            "Soglia unknown oltre cui il chunk entra in cffi-block "
            f"(default {DEFAULT_CFFI_BLOCK_UNKNOWN_RATIO:.2f})"
        ),
    )
    parser.add_argument(
        "--min-coverage-ratio",
        type=float,
        default=DEFAULT_MIN_COVERAGE_RATIO,
        metavar="R",
        help=(
            "Soglia minima opzionale di coverage finale; exit code 2 se non rispettata "
            f"(default {DEFAULT_MIN_COVERAGE_RATIO:.2f} = disabilitata)"
        ),
    )
    return parser


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    init_db()
    max_runtime = args.max_runtime_minutes if args.max_runtime_minutes and args.max_runtime_minutes > 0 else None
    cfg = VerifyConfig(
        concurrency=args.concurrency,
        cffi_concurrency=args.cffi_concurrency,
        chunk_size=args.chunk_size,
        nav_timeout_ms=args.nav_timeout_ms,
        max_http403_ratio=args.max_http403_ratio,
        fail_fast_min_attempts=args.fail_fast_min_attempts,
        fail_fast_blocked_ratio=args.fail_fast_blocked_ratio,
        fail_fast_403_ratio=args.fail_fast_403_ratio,
        cffi_block_unknown_ratio=args.cffi_block_unknown_ratio,
        min_coverage_ratio=args.min_coverage_ratio,
        browser_restart_every=args.browser_restart_every,
        selection_sample=args.selection_sample,
    )
    stats = asyncio.run(
        verify_batch(
            batch_size=args.batch_size,
            verify_all=args.verify_all,
            recheck_days=args.recheck_days,
            include_rejected=args.include_rejected,
            xbox_only=args.xbox_only,
            max_runtime_minutes=max_runtime,
            cfg=cfg,
        )
    )
    if args.min_coverage_ratio and stats.get("coverage_below_min"):
        raise SystemExit(2)
