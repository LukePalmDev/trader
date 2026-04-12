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
