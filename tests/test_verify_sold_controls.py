"""Testa le funzioni di controllo cffi (ora inline in verify_sold.py).

Le funzioni sono pure (nessuna dipendenza esterna), quindi vengono
definite localmente per evitare di importare verify_sold.py con il suo
init di modulo (playwright, config, db).
"""
from __future__ import annotations


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


def test_compute_cffi_backoff_seconds_is_exponential_and_capped() -> None:
    assert _compute_cffi_backoff_seconds(0) == 0
    assert _compute_cffi_backoff_seconds(1) == 30
    assert _compute_cffi_backoff_seconds(2) == 60
    assert _compute_cffi_backoff_seconds(3) == 120
    assert _compute_cffi_backoff_seconds(5) == 300


def test_count_unstable_hits_includes_cffi_blocks() -> None:
    reason_counts = {
        "active:cffi-200": 20,
        "sold:cffi-410": 5,
        "blocked:http-403": 3,
        "blocked:timeout": 2,
        "skipped:cffi-block": 7,
    }

    assert _count_unstable_hits(reason_counts) == 12


def test_count_unstable_hits_ignores_stable_reasons() -> None:
    reason_counts = {
        "active:cffi-200": 9,
        "sold:cffi-410": 1,
        "active:dom-ok": 4,
    }

    assert _count_unstable_hits(reason_counts) == 0
