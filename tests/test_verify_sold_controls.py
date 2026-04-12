from verify_sold_controls import _compute_cffi_backoff_seconds, _count_unstable_hits


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
