from scrapers.base import clean_price, deduplicate


def test_clean_price_supports_it_and_int_formats() -> None:
    assert clean_price("349,99 €") == 349.99
    assert clean_price("1.349,99") == 1349.99
    assert clean_price("350") == 350.0
    assert clean_price("") is None


def test_deduplicate_by_sku_then_url() -> None:
    products = [
        {"sku": "A1", "url": "https://a.test/1", "name": "one"},
        {"sku": "A1", "url": "https://a.test/2", "name": "two"},
        {"sku": "", "url": "https://a.test/3", "name": "three"},
        {"sku": "", "url": "https://a.test/3", "name": "four"},
    ]
    out = deduplicate(products)
    assert len(out) == 2
    assert out[0]["name"] == "one"
    assert out[1]["name"] == "three"
