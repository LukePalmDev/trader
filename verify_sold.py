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

import aiohttp

from scrapers.base import launch_chromium, retry
from settings import load_config
from db_subito import DB_PATH, _connect
from playwright.async_api import async_playwright

_CONFIG_PATH = Path("config.toml")
_CFG = load_config(_CONFIG_PATH)
_COMMON = _CFG["common"]

DEFAULT_BATCH_SIZE = 200
DEFAULT_RECHECK_DAYS = 7


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
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
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

            res = bool(await retry(_do, retries=2, delay=2.0, label=url))
        except Exception as exc:
            log.warning("Errore navigazione %s: %s", url, exc)
            res = True  # In caso di errore strano assumiamo sia ancora vivo per prudenza
        finally:
            try:
                await ctx.close()
            except Exception:
                pass

    return res


async def _process_rows(browser, rows: list, semaphore: asyncio.Semaphore) -> dict:
    """Verifica concorrente con pre-check HTTP + fallback Playwright.

    Fase 1: HTTP GET leggero per tutti gli URL (molto veloce, ~50 concorrenti).
             Rileva subito i 404/410/redirect → "sold" senza aprire un browser.
    Fase 2: Solo gli URL "unknown" vengono verificati con Playwright (lento ma accurato).

    Returns:
        dict con chiavi: verified, active, sold, avg_price_sold, avg_hours_active
    """
    now = datetime.now(timezone.utc).isoformat()

    # ---------- Fase 1: HTTP pre-check in batch ----------
    http_results: dict[int, str] = {}  # ad_id → "sold" | "unknown"
    http_sem = asyncio.Semaphore(30)   # concorrenza HTTP più alta (leggero)

    async def _precheck_one(session, row):
        async with http_sem:
            status = await _http_precheck(session, row["url"])
            http_results[row["id"]] = status

    headers = {"User-Agent": _COMMON["user_agent"]}
    async with aiohttp.ClientSession(headers=headers) as session:
        await asyncio.gather(*[_precheck_one(session, r) for r in rows])

    # Separa i risultati certi dai dubbi
    sold_fast = [r for r in rows if http_results.get(r["id"]) == "sold"]
    needs_playwright = [r for r in rows if http_results.get(r["id"]) == "unknown"]

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
        await asyncio.gather(*[_check_one_pw(r) for r in needs_playwright])

    # ---------- Unifica risultati ----------
    results: list[tuple] = []
    for row in rows:
        ad_id = row["id"]
        if ad_id in playwright_results:
            is_active = playwright_results[ad_id]
        else:
            # pre-check HTTP ha detto "sold"
            is_active = False
        results.append((ad_id, row["url"], row["last_price"], row["first_seen"], is_active))

    conn = _connect(DB_PATH)
    sold_count = 0
    active_count = 0
    sold_prices: list[float] = []
    sold_hours: list[float] = []

    for ad_id, url, price, first_seen, is_active in results:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        try:
            if is_active:
                conn.execute("UPDATE ads SET last_seen = ? WHERE id = ?", (now, ad_id))
                active_count += 1
            else:
                conn.execute(
                    "UPDATE ads SET last_available = 0, sold_at = ?, last_seen = ? WHERE id = ?",
                    (now, now, ad_id),
                )
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
                        now_dt = datetime.fromisoformat(now)
                        hours = (now_dt - fs).total_seconds() / 3600.0
                        if hours >= 0:
                            sold_hours.append(hours)
                    except (ValueError, TypeError):
                        pass

            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            log.error("DB error for ad %s: %s", ad_id, e)

    conn.close()

    avg_price_sold = (sum(sold_prices) / len(sold_prices)) if sold_prices else None
    avg_hours_active = (sum(sold_hours) / len(sold_hours)) if sold_hours else None

    return {
        "verified": len(results),
        "active": active_count,
        "sold": sold_count,
        "avg_price_sold": avg_price_sold,
        "avg_hours_active": avg_hours_active,
    }


async def verify_batch(
    batch_size: int = DEFAULT_BATCH_SIZE,
    verify_all: bool = False,
    recheck_days=None,  # int or None
    concurrency: int = 5,
) -> dict:
    """Recupera un batch di annunci e li verifica online con concorrenza.

    Args:
        batch_size:    Numero massimo di annunci da verificare (ignorato se verify_all=True).
        verify_all:    Se True, verifica tutti gli annunci attivi non verificati.
        recheck_days:  Se impostato, ri-verifica anche i venduti degli ultimi N giorni.
        concurrency:   Numero massimo di URL verificati in parallelo (default 5).

    Returns:
        Statistiche della sessione di verifica.
    """
    conn = _connect(DB_PATH)

    # Query base: annunci che crediamo attivi, ordinando dai NON VISTI da più tempo
    base_query = (
        "SELECT id, url, last_price, first_seen FROM ads "
        "WHERE last_available = 1 AND sold_at IS NULL ORDER BY last_seen ASC"
    )
    if verify_all:
        rows = conn.execute(base_query).fetchall()
    else:
        rows = conn.execute(base_query + " LIMIT ?", (batch_size,)).fetchall()

    recheck_rows: list = []
    if recheck_days is not None and recheck_days > 0:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=recheck_days)
        ).isoformat()
        recheck_rows = conn.execute(
            "SELECT id, url, last_price, first_seen FROM ads "
            "WHERE sold_at IS NOT NULL AND sold_at >= ? ORDER BY sold_at DESC",
            (cutoff,),
        ).fetchall()

    conn.close()

    all_rows = list(rows) + list(recheck_rows)
    if not all_rows:
        log.info("Nessun annuncio da verificare.")
        return {"verified": 0, "active": 0, "sold": 0, "avg_price_sold": None, "avg_hours_active": None}

    log.info(
        "Inizio verifica di %d annunci (concorrenza=%d)%s…",
        len(all_rows),
        concurrency,
        f" + {len(recheck_rows)} ri-verifiche venduti" if recheck_rows else "",
    )

    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )
        try:
            stats = await _process_rows(browser, all_rows, semaphore)
        finally:
            await browser.close()

    avg_p = f"€ {stats['avg_price_sold']:.2f}" if stats["avg_price_sold"] is not None else "—"
    avg_h_raw = stats["avg_hours_active"]
    if avg_h_raw is not None:
        avg_h = (
            f"{round(avg_h_raw)} ore"
            if avg_h_raw < 48
            else f"{round(avg_h_raw / 24)} giorni"
        )
    else:
        avg_h = "—"

    log.info(
        "Verifica completata. %d verificati: %d attivi, %d VENDUTI | "
        "prezzo medio venduto %s | tempo attivo medio %s",
        stats["verified"],
        stats["active"],
        stats["sold"],
        avg_p,
        avg_h,
    )
    return stats


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
    return parser


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    asyncio.run(
        verify_batch(
            batch_size=args.batch_size,
            verify_all=args.verify_all,
            recheck_days=args.recheck_days,
            concurrency=args.concurrency,
        )
    )
