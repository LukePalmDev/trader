from __future__ import annotations

import hashlib
from urllib.parse import urlsplit, urlunsplit


def normalize_url(url: str) -> str:
    """Normalizza URL per ottenere ID stabili cross-run."""
    if not url:
        return ""
    parts = urlsplit(url.strip())
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    query = "&".join(sorted(filter(None, parts.query.split("&"))))
    return urlunsplit((scheme, netloc, path, query, ""))


def stable_item_id(prefix: str, url: str, *, length: int = 16) -> str:
    normalized = normalize_url(url)
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()  # noqa: S324
    return f"{prefix}-{digest[:length].upper()}"
