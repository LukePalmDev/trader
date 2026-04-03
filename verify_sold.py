from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
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

from scrapers.base import launch_chromium, retry  # noqa: E402
from settings import load_config  # noqa: E402
from db_subito import DB_PATH, _connect, _estimate_sold_window  # noqa: E402
from playwright.async_api import async_playwright  # noqa: E402

_CONFIG_PATH = Path("config.toml")
_CFG = load_config(_CONFIG_PATH)
_COMMON = _CFG["common"]

DEFAULT_BATCH_SIZE = 200
DEFAULT_RECHECK_DAYS = 7
DEFAULT_CHUNK_SIZE = 350
DEFAULT_MAX_RUNTIME_MINUTES = 45
DEFAULT_BROWSER_RESTART_EVERY = 3
_WARNED_NO_AIOHTTP = False

_XBOX_SQL_FILTER = """
AND (
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


async def _new_context(browser):
    """Context fresco per bypassare i controlli bot Akamai di Subito."""
    return await browser.new_context(
        user_agent=_COMMON["user_agent"],
        viewport={
            "width": _COMMON["viewport_width"],
            "height": _COMMON["viewport_height"],
        },
        locale=_COMMON["locale"],
    )


async def _http_precheck(session: aiohttp.ClientSession, url: str) -> str:
    """Pre-check HTTP leggero (senza browser) per rilevare 404/410/redirect.

    Returns:
        "sold"     — sicuramente non disponibile (404, 410, redirect a home)
        "unknown"  — serve verifica Playwright (200 ma potrebbe avere testo "non disponibile")
    """
    try:
        if aiohttp is None:
            return "unknown"
        async with session.get(
            url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            # 404 / 410 → sicuramente rimosso
            if resp.status in (404, 410):
                return "sold"

            # Redirect 301/302 → controlla se va verso la home (annuncio eliminato)
            if resp.status in (301, 302, 303, 307, 308):
                location = resp.headers.get("Location", "")
                if (
                    location.rstrip("/") == "https://www.subito.it"
                    or "annunci-italia/vendita" in location
                ):
                    return "sold"

            # 200 → potrebbe avere "non più disponibile" nel body, serve Playwright
            return "unknown"
    except (aiohttp.ClientError, asyncio.TimeoutError):
        # Errore di rete → serve Playwright per conferma
        return "unknown"


async def check_url(browser, semaphore: asyncio.Semaphore, url: str) -> bool:
    """Visita l'URL e controlla se è ancora in vendita.
    Restituisce True se ancora attivo, False se venduto/eliminato.
    """
    async with semaphore:
        ctx = await _new_context(browser)
        page = await ctx.new_page()
        res = False
        try:
            async def _do() -> bool:
                resp = await page.goto(url, wait_until="load", timeout=30000)
                if not resp:
                    return False

                # Verifica redirect (Subito a volte reindirizza alla home o ricerca se assente)
                if page.url == "https://www.subito.it/" or (
                    "annunci-italia/vendita" in page.url and "q=" in page.url
                ):
                    log.debug("  Redirect rilevato: %s -> %s", url, page.url)
                    return False

                if resp.status == 404 or resp.status == 410:
                    return False

                # Verifica testo in pagina per annunci "venduti" ma con URL mantenuto
                content = await page.content()
                if (
                    "Annuncio non più disponibile" in content
                    or "Questo annuncio non è più disponibile" in content
                ):
                    return False

                return True

            res = bool(await retry(_do, retries=2, delay=5.0, label=url))
        except Exception as exc:
            log.warning("Errore navigazione %s: %s", url, exc)
            res = True  # In caso di errore strano assumiamo sia ancora vivo per prudenza
        finally:
            try:
                await ctx.close()
            except Exception:
                pass

    return res


def _deadline_reached(deadline_utc: datetime | None) -> bool:
    return deadline_utc is not None and datetime.now(timezone.utc) >= deadline_utc


async def _process_rows(
    browser,
    rows: list,
    semaphore: asyncio.Semaphore,
    *,
    deadline_utc: datetime | None = None,
    http_batch_size: int = 120,
    pw_batch_size: int = 40,
) -> dict:
    """Verifica concorrente con pre-check HTTP + fallback Playwright.

    Fase 1: HTTP GET leggero per tutti gli URL (molto veloce, ~50 concorrenti).
             Rileva subito i 404/410/redirect → "sold" senza aprire un browser.
    Fase 2: Solo gli URL "unknown" vengono verificati con Playwright (lento ma accurato).

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

    # ---------- Fase 1: HTTP pre-check in micro-batch ----------
    http_results: dict[int, str] = {}  # ad_id → "sold" | "unknown"
    http_sem = asyncio.Semaphore(50)   # concorrenza HTTP più alta (leggero)
    total_rows = len(rows)
    processed_count = 0

    async def _precheck_one(session, row):
        nonlocal processed_count
        async with http_sem:
            status = await _http_precheck(session, row["url"])
            http_results[row["id"]] = status
            processed_count += 1
            if processed_count % 500 == 0:
                log.info("  Pre-check HTTP in corso: %d/%d annunci verificati…", processed_count, total_rows)

    effective_http_batch = max(1, int(http_batch_size))
    effective_pw_batch = max(1, int(pw_batch_size))
    time_limit_hit = False

    if aiohttp is not None:
        headers = {"User-Agent": _COMMON["user_agent"]}
        async with aiohttp.ClientSession(headers=headers) as session:
            for idx in range(0, len(rows), effective_http_batch):
                if _deadline_reached(deadline_utc):
                    time_limit_hit = True
                    break
                batch = rows[idx : idx + effective_http_batch]
                await asyncio.gather(*[_precheck_one(session, r) for r in batch])
    else:
        global _WARNED_NO_AIOHTTP
        if not _WARNED_NO_AIOHTTP:
            log.warning("aiohttp non disponibile: pre-check HTTP saltato, uso solo Playwright.")
            _WARNED_NO_AIOHTTP = True
        for row in rows:
            if _deadline_reached(deadline_utc):
                time_limit_hit = True
                break
            http_results[row["id"]] = "unknown"

    processable_rows = [r for r in rows if r["id"] in http_results]
    skipped_rows = len(rows) - len(processable_rows)

    # Separa i risultati certi dai dubbi
    sold_fast = [r for r in processable_rows if http_results.get(r["id"]) == "sold"]
    needs_playwright = [r for r in processable_rows if http_results.get(r["id"]) == "unknown"]

    log.info(
        "Pre-check HTTP: %d venduti subito, %d da verificare con browser.",
        len(sold_fast), len(needs_playwright),
    )

    # ---------- Fase 2: Playwright solo per gli "unknown" ----------
    playwright_results: dict[int, bool] = {}  # ad_id → is_active

    async def _check_one_pw(row) -> None:
        is_active = await check_url(browser, semaphore, row["url"])
        playwright_results[row["id"]] = is_active

    if needs_playwright:
        for idx in range(0, len(needs_playwright), effective_pw_batch):
            if _deadline_reached(deadline_utc):
                time_limit_hit = True
                break
            batch = needs_playwright[idx : idx + effective_pw_batch]
            await asyncio.gather(*[_check_one_pw(r) for r in batch])

    # ---------- Unifica risultati ----------
    results: list[tuple] = []
    for row in processable_rows:
        ad_id = row["id"]
        if ad_id in playwright_results:
            is_active = playwright_results[ad_id]
        elif http_results.get(ad_id) == "sold":
            # pre-check HTTP ha detto "sold"
            is_active = False
        else:
            # deadline raggiunta prima del check browser: riga non processata
            skipped_rows += 1
            continue
        results.append((row, is_active))

    conn = _connect(DB_PATH)
    sold_count = 0
    already_sold_count = 0
    recovered_count = 0
    active_count = 0
    sold_prices: list[float] = []
    sold_hours: list[float] = []

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

        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
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

            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            log.error("DB error for ad %s: %s", ad_id, e)

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
    }


async def verify_batch(
    batch_size: int = DEFAULT_BATCH_SIZE,
    verify_all: bool = False,
    recheck_days=None,  # int or None
    concurrency: int = 5,
    include_rejected: bool = False,
    xbox_only: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_runtime_minutes: int | None = DEFAULT_MAX_RUNTIME_MINUTES,
    browser_restart_every: int = DEFAULT_BROWSER_RESTART_EVERY,
) -> dict:
    """Recupera un batch di annunci e li verifica online con concorrenza.

    Args:
        batch_size:    Numero massimo di annunci da verificare (ignorato se verify_all=True).
        verify_all:    Se True, verifica tutti gli annunci attivi non verificati.
        recheck_days:  Se impostato, ri-verifica anche i venduti degli ultimi N giorni.
        concurrency:   Numero massimo di URL verificati in parallelo (default 5).
        include_rejected: include anche annunci ai_status='rejected' (default False).
        xbox_only: verifica solo annunci rilevanti Xbox (default True).
        chunk_size: numero record per chunk operativo.
        max_runtime_minutes: stop soft oltre la durata indicata.
        browser_restart_every: riavvia browser ogni N chunk.

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

    semaphore = asyncio.Semaphore(concurrency)
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
        "time_limit_hit": False,
    }
    sold_price_sum = 0.0
    sold_price_count = 0
    sold_hour_sum = 0.0
    sold_hour_count = 0
    chunk_len = max(1, int(chunk_size))
    restart_every = max(1, int(browser_restart_every))

    async with async_playwright() as p:
        browser = None
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
                if browser is None or (chunk_no > 0 and chunk_no % restart_every == 0):
                    if browser is not None:
                        await browser.close()
                    browser = await launch_chromium(
                        p,
                        headless=True,
                        preferred_channel=_COMMON.get("playwright_channel", "chrome"),
                    )
                    log.info("Browser session restart (chunk %d).", chunk_no + 1)

                log.info(
                    "Chunk %d: verifica annunci %d-%d / %d",
                    chunk_no + 1,
                    idx + 1,
                    min(idx + len(chunk), len(all_rows)),
                    len(all_rows),
                )
                stats = await _process_rows(
                    browser,
                    chunk,
                    semaphore,
                    deadline_utc=deadline_utc,
                )

                total_stats["verified"] += stats["verified"]
                total_stats["active"] += stats["active"]
                total_stats["sold"] += stats["sold"]
                total_stats["already_sold"] += stats["already_sold"]
                total_stats["recovered"] += stats["recovered"]
                total_stats["skipped"] += int(stats.get("skipped", 0) or 0)
                sold_price_sum += float(stats["sold_price_sum"] or 0.0)
                sold_price_count += int(stats["sold_price_count"] or 0)
                sold_hour_sum += float(stats["sold_hour_sum"] or 0.0)
                sold_hour_count += int(stats["sold_hour_count"] or 0)
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
            if browser is not None:
                await browser.close()

    if sold_price_count:
        total_stats["avg_price_sold"] = sold_price_sum / sold_price_count
    if sold_hour_count:
        total_stats["avg_hours_active"] = sold_hour_sum / sold_hour_count

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
        default=5,
        metavar="N",
        help="Numero di URL verificati in parallelo (default 5)",
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
        )
    )
