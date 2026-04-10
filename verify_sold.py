from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import sys
import time
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
    import aiohttp
except ImportError:  # pragma: no cover - fallback runtime
    aiohttp = None

from scrapers.base import launch_chromium  # noqa: E402
from settings import load_config  # noqa: E402
from db_subito import DB_PATH, _connect, _estimate_sold_window  # noqa: E402
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError  # noqa: E402
from playwright_stealth import Stealth  # noqa: E402

_CONFIG_PATH = Path("config.toml")
_CFG = load_config(_CONFIG_PATH)
_COMMON = _CFG["common"]

# Istanza stealth riusabile — si limita ad add_init_script, nessuno stato mutabile.
_STEALTH = Stealth(
    navigator_languages_override=("it-IT", "it"),
    navigator_platform_override="MacIntel",
    navigator_webdriver=True,
)

DEFAULT_BATCH_SIZE = 200
DEFAULT_RECHECK_DAYS = 7
DEFAULT_CHUNK_SIZE = 350
DEFAULT_MAX_RUNTIME_MINUTES = 45
DEFAULT_BROWSER_RESTART_EVERY = 3
DEFAULT_CONCURRENCY = 3
DEFAULT_NAV_TIMEOUT_MS = 10_000
DEFAULT_BODY_TIMEOUT_SECONDS = 2.5
DEFAULT_DOM_FALLBACK_TIMEOUT_MS = 1_200
DEFAULT_MAX_HTTP403_RATIO = 0.40
DEFAULT_FAIL_FAST_MIN_ATTEMPTS = 150
DEFAULT_FAIL_FAST_BLOCKED_RATIO = 0.85
DEFAULT_FAIL_FAST_403_RATIO = 0.60
_WARNED_NO_AIOHTTP = False

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


def _classify_navigation_exception(exc: Exception) -> str:
    msg = str(exc).lower()
    if "err_connection_timed_out" in msg or "timeout" in msg or "timed out" in msg:
        return "blocked:timeout"
    if any(token in msg for token in ("err_name_not_resolved", "err_connection_reset", "err_network_changed")):
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


async def _http_precheck(session: aiohttp.ClientSession, url: str) -> str:
    """Pre-check HTTP leggero (senza browser) per rilevare 404/410/redirect.

    Segue i redirect per catturare catene 301→302→home che con allow_redirects=False
    sarebbero visibili solo al primo hop.

    Returns:
        "sold"     — sicuramente non disponibile (404, 410, redirect a home/ricerca)
        "unknown"  — serve verifica Playwright (200 ma potrebbe avere testo "non disponibile")
    """
    _timeout = aiohttp.ClientTimeout(total=8)

    async def _check(method: str) -> str:
        requester = session.head if method == "HEAD" else session.get
        async with requester(url, allow_redirects=True, timeout=_timeout) as resp:
            if resp.status in (404, 410):
                return "sold"
            if resp.status == 405:
                return "retry_get"
            final_url = str(resp.url)
            if (
                final_url.rstrip("/") == "https://www.subito.it"
                or ("annunci-italia/vendita" in final_url and "q=" in final_url)
            ):
                return "sold"
            return "unknown"

    try:
        if aiohttp is None:
            return "unknown"
        result = await _check("HEAD")
        if result == "retry_get":
            result = await _check("GET")
        return result
    except (aiohttp.ClientError, asyncio.TimeoutError):
        # Errore di rete → serve Playwright per conferma
        return "unknown"


async def check_url(
    worker: dict,
    url: str,
    *,
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS,
    body_timeout_seconds: float = DEFAULT_BODY_TIMEOUT_SECONDS,
    dom_fallback_timeout_ms: int = DEFAULT_DOM_FALLBACK_TIMEOUT_MS,
) -> tuple[bool, str]:
    """Verifica URL con strategia strict (accuratezza > velocità).

    Nota: gli argomenti body_timeout_seconds/dom_fallback_timeout_ms restano per
    compatibilità CLI, ma la verifica usa una singola navigazione domcontentloaded.
    """
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


def _deadline_reached(deadline_utc: datetime | None) -> bool:
    return deadline_utc is not None and datetime.now(timezone.utc) >= deadline_utc


async def _process_rows(
    worker_pool: asyncio.Queue,
    rows: list,
    *,
    deadline_utc: datetime | None = None,
    http_precheck: bool = False,
    http_batch_size: int = 120,
    stagger_secs: float = 0.0,
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS,
    body_timeout_seconds: float = DEFAULT_BODY_TIMEOUT_SECONDS,
    dom_fallback_timeout_ms: int = DEFAULT_DOM_FALLBACK_TIMEOUT_MS,
) -> dict:
    """Verifica concorrente con Playwright (+ pre-check HTTP opzionale).

    Subito restituisce sempre HTTP 200 per gli annunci, anche i venduti (che mostrano
    "non più disponibile" nel DOM via JS). Il pre-check HTTP è disabilitato per default
    perché aggiunge overhead senza filtrare nulla (~6 min su 7658 annunci, 0 catturati).
    Riabilitare con http_precheck=True se Subito modificasse il comportamento.

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

    # ---------- Fase 1 (opzionale): HTTP pre-check ----------
    # Disabilitato per default: Subito risponde 200 per tutto, 0 annunci filtrati.
    http_sold_ids: set[int] = set()

    if http_precheck and aiohttp is not None:
        http_results: dict[int, str] = {}
        http_sem = asyncio.Semaphore(50)
        total_rows = len(rows)
        processed_count = 0

        async def _precheck_one(session, row):
            nonlocal processed_count
            async with http_sem:
                status = await _http_precheck(session, row["url"])
                http_results[row["id"]] = status
                processed_count += 1
                if processed_count % 500 == 0:
                    log.info("  Pre-check HTTP: %d/%d…", processed_count, total_rows)

        effective_http_batch = max(1, int(http_batch_size))
        headers = {"User-Agent": _COMMON["user_agent"]}
        async with aiohttp.ClientSession(headers=headers) as session:
            for idx in range(0, len(rows), effective_http_batch):
                if _deadline_reached(deadline_utc):
                    time_limit_hit = True
                    break
                batch = rows[idx : idx + effective_http_batch]
                await asyncio.gather(*[_precheck_one(session, r) for r in batch])

        http_sold_ids = {r["id"] for r in rows if http_results.get(r["id"]) == "sold"}
        log.info(
            "Pre-check HTTP: %d venduti subito, %d a Playwright.",
            len(http_sold_ids), len(rows) - len(http_sold_ids),
        )
    elif http_precheck:
        log.warning("aiohttp non disponibile: pre-check HTTP saltato.")

    needs_playwright = [r for r in rows if r["id"] not in http_sold_ids]

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
                body_timeout_seconds=body_timeout_seconds,
                dom_fallback_timeout_ms=dom_fallback_timeout_ms,
            )
            if reason.startswith("blocked:http-403"):
                # Retry hard su sessione fresca: alcuni blocchi Akamai sono session-bound.
                await _reset_worker_session(worker)
                await asyncio.sleep(0.35)
                is_active, reason = await check_url(
                    worker,
                    row["url"],
                    nav_timeout_ms=nav_timeout_ms,
                    body_timeout_seconds=body_timeout_seconds,
                    dom_fallback_timeout_ms=dom_fallback_timeout_ms,
                )
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

    # ---------- Unifica risultati ----------
    results: list[tuple] = []
    # Annunci venduti dal pre-check HTTP (is_active=False certi)
    for row in rows:
        if row["id"] in http_sold_ids:
            results.append((row, False))
    # Annunci verificati da Playwright
    for row in needs_playwright:
        ad_id = row["id"]
        if ad_id in playwright_results:
            is_active, _ = playwright_results[ad_id]
            if is_active is None:
                skipped_rows += 1
            else:
                results.append((row, is_active))
        else:
            # deadline raggiunta prima del check Playwright
            skipped_rows += 1

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
                            sold_window_hours = NULL
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
                            sold_window_hours = ?
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
    finally:
        conn.close()

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
    concurrency: int = DEFAULT_CONCURRENCY,
    include_rejected: bool = False,
    xbox_only: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_runtime_minutes: int | None = DEFAULT_MAX_RUNTIME_MINUTES,
    browser_restart_every: int = DEFAULT_BROWSER_RESTART_EVERY,
    http_precheck: bool = False,
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS,
    body_timeout_seconds: float = DEFAULT_BODY_TIMEOUT_SECONDS,
    dom_fallback_timeout_ms: int = DEFAULT_DOM_FALLBACK_TIMEOUT_MS,
    max_http403_ratio: float = DEFAULT_MAX_HTTP403_RATIO,
    fail_fast_min_attempts: int = DEFAULT_FAIL_FAST_MIN_ATTEMPTS,
    fail_fast_blocked_ratio: float = DEFAULT_FAIL_FAST_BLOCKED_RATIO,
    fail_fast_403_ratio: float = DEFAULT_FAIL_FAST_403_RATIO,
    selection_sample: int = 0,
) -> dict:
    """Recupera un batch di annunci e li verifica online con concorrenza.

    Args:
        batch_size:    Numero massimo di annunci da verificare (ignorato se verify_all=True).
        verify_all:    Se True, verifica tutti gli annunci attivi non verificati.
        recheck_days:  Se impostato, ri-verifica anche i venduti degli ultimi N giorni.
        concurrency:   Numero massimo di URL verificati in parallelo (default 30).
        include_rejected: include anche annunci ai_status='rejected' (default False).
        xbox_only: verifica solo annunci rilevanti Xbox (default True).
        chunk_size: numero record per chunk operativo.
        max_runtime_minutes: stop soft oltre la durata indicata.
        browser_restart_every: riavvia browser ogni N chunk.
        http_precheck: pre-check HTTP prima di Playwright (default False).
                       Subito risponde 200 per tutto; non filtra nulla di utile.
        nav_timeout_ms: timeout navigazione `goto(..., wait_until="domcontentloaded")`.
        body_timeout_seconds: parametro legacy (compatibilità CLI).
        dom_fallback_timeout_ms: parametro legacy (compatibilità CLI).
        max_http403_ratio: soglia massima tollerata di 403 sul totale tentativi.
        fail_fast_min_attempts: minimo tentativi prima di abilitare stop anticipato.
        fail_fast_blocked_ratio: soglia blocked totale per stop anticipato.
        fail_fast_403_ratio: soglia 403 per stop anticipato.
        selection_sample: numero di annunci esclusi da xbox-only da mostrare a log.

    Returns:
        Statistiche della sessione di verifica.
    """
    conn = _connect(DB_PATH)

    statuses = ["approved", "pending"]
    if include_rejected:
        statuses.append("rejected")
    placeholders = ",".join("?" * len(statuses))

    select_cols = (
        "id, url, last_price, first_seen, last_seen, last_available, "
        "last_active_seen, first_inactive_seen, sold_at, sold_at_estimated, sold_window_hours"
    )
    base_query = (
        f"SELECT {select_cols} FROM ads "
        f"WHERE ai_status IN ({placeholders}) "
        "AND last_available = 1 AND sold_at IS NULL "
        + (_XBOX_SQL_FILTER if xbox_only else "")
        + " ORDER BY COALESCE(last_seen, first_seen) ASC"
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
        limit=max(0, int(selection_sample)),
    ) if xbox_only and selection_sample else []

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
        "Inizio verifica di %d annunci (concorrenza=%d, chunk=%d, stati=%s)%s…",
        len(all_rows),
        concurrency,
        max(1, int(chunk_size)),
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
    chunk_len = max(1, int(chunk_size))
    restart_every = max(1, int(browser_restart_every))
    target_concurrency = max(1, int(concurrency))
    min_concurrency = max(1, target_concurrency // 3)
    current_concurrency = target_concurrency
    force_restart_next = False

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
                    workers_created: list[dict] = []
                    for idx_worker in range(current_concurrency):
                        ctx = await _new_context(browser)
                        page = await ctx.new_page()
                        w = {"id": idx_worker, "browser": browser, "ctx": ctx, "page": page}
                        workers_created.append(w)
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
                    http_precheck=http_precheck,
                    stagger_secs=1.5 if is_restart else 0.0,
                    nav_timeout_ms=nav_timeout_ms,
                    body_timeout_seconds=body_timeout_seconds,
                    dom_fallback_timeout_ms=dom_fallback_timeout_ms,
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
                unstable_hits = sum(
                    count
                    for reason, count in reason_counts.items()
                    if (
                        str(reason).startswith("blocked:")
                        or "http-403" in reason
                        or "http-429" in reason
                    )
                )
                unstable_ratio = unstable_hits / max(1, attempted_in_chunk)
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

                total_stats["verified"] += stats["verified"]
                total_stats["active"] += stats["active"]
                total_stats["sold"] += stats["sold"]
                total_stats["already_sold"] += stats["already_sold"]
                total_stats["recovered"] += stats["recovered"]
                total_stats["skipped"] += int(stats.get("skipped", 0) or 0)
                total_stats["blocked_403"] += int(
                    sum(v for k, v in reason_counts.items() if "http-403" in k)
                )
                total_stats["blocked_total"] += int(
                    sum(v for k, v in reason_counts.items() if str(k).startswith("blocked:"))
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
                if cumulative_attempted >= max(1, int(fail_fast_min_attempts)):
                    cumulative_blocked_ratio = float(total_stats["blocked_total"]) / float(cumulative_attempted)
                    cumulative_403_ratio = float(total_stats["blocked_403"]) / float(cumulative_attempted)
                    if (
                        cumulative_blocked_ratio >= float(fail_fast_blocked_ratio)
                        and cumulative_403_ratio >= float(fail_fast_403_ratio)
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
    blocked_total_ratio = float(total_stats["blocked_total"]) / float(attempted_total)
    total_stats["blocked_403_ratio"] = blocked_403_ratio
    total_stats["blocked_total_ratio"] = blocked_total_ratio

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
            "HTTP block summary: 403=%d/%d (%.2f%%) | blocked_totali=%d/%d (%.2f%%) "
            "| timeout=%d | network=%d | error=%d"
        ),
        total_stats["blocked_403"],
        attempted_total,
        blocked_403_ratio * 100,
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
        "Verifica completata. %d verificati, %d skipped: %d attivi, %d venduti nuovi, "
        "%d venduti confermati, %d riattivati | prezzo medio venduto %s | tempo attivo medio %s%s",
        total_stats["verified"],
        total_stats["skipped"],
        total_stats["active"],
        total_stats["sold"],
        total_stats["already_sold"],
        total_stats["recovered"],
        avg_p,
        avg_h,
        " | STOP per runtime" if total_stats.get("time_limit_hit") else "",
    )
    if blocked_403_ratio > float(max_http403_ratio):
        log.warning(
            "HTTP 403 alto: %.2f%% > %.2f%% (soglia) — dati parziali disponibili",
            blocked_403_ratio * 100,
            float(max_http403_ratio) * 100,
        )
        total_stats["high_block_rate"] = True
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
        "--http-precheck",
        action="store_true",
        dest="http_precheck",
        default=False,
        help=(
            "Abilita pre-check HTTP prima di Playwright. "
            "Disabilitato per default: Subito risponde 200 per tutto, nessun filtro utile."
        ),
    )
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
        "--body-timeout-seconds",
        type=float,
        default=DEFAULT_BODY_TIMEOUT_SECONDS,
        metavar="S",
        help=(
            "Timeout probe resp.text() in secondi "
            f"(default {DEFAULT_BODY_TIMEOUT_SECONDS})"
        ),
    )
    parser.add_argument(
        "--dom-fallback-timeout-ms",
        type=int,
        default=DEFAULT_DOM_FALLBACK_TIMEOUT_MS,
        metavar="MS",
        help=(
            "Timeout fallback DOM (domcontentloaded + locator) in millisecondi "
            f"(default {DEFAULT_DOM_FALLBACK_TIMEOUT_MS})"
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
    return parser


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    max_runtime = args.max_runtime_minutes if args.max_runtime_minutes and args.max_runtime_minutes > 0 else None
    asyncio.run(
        verify_batch(
            batch_size=args.batch_size,
            verify_all=args.verify_all,
            recheck_days=args.recheck_days,
            concurrency=args.concurrency,
            include_rejected=args.include_rejected,
            xbox_only=args.xbox_only,
            chunk_size=args.chunk_size,
            max_runtime_minutes=max_runtime,
            browser_restart_every=args.browser_restart_every,
            http_precheck=args.http_precheck,
            nav_timeout_ms=args.nav_timeout_ms,
            body_timeout_seconds=args.body_timeout_seconds,
            dom_fallback_timeout_ms=args.dom_fallback_timeout_ms,
            max_http403_ratio=args.max_http403_ratio,
            fail_fast_min_attempts=args.fail_fast_min_attempts,
            fail_fast_blocked_ratio=args.fail_fast_blocked_ratio,
            fail_fast_403_ratio=args.fail_fast_403_ratio,
            selection_sample=args.selection_sample,
        )
    )
