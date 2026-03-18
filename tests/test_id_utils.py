from id_utils import normalize_url, stable_item_id


def test_normalize_url_sorts_query_and_removes_fragment() -> None:
    raw = "HTTPS://Example.com/path?b=2&a=1#frag"
    assert normalize_url(raw) == "https://example.com/path?a=1&b=2"


def test_stable_item_id_is_deterministic() -> None:
    a = stable_item_id("SUBITO", "https://example.com/itm?id=1")
    b = stable_item_id("SUBITO", "https://example.com/itm?id=1")
    c = stable_item_id("SUBITO", "https://example.com/itm?id=2")
    assert a == b
    assert a != c
    assert a.startswith("SUBITO-")
