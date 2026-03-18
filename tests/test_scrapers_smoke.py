import re

from scrapers.ebay import _parse_item
from scrapers.subito import _parse_ad


def test_parse_subito_ad_extracts_expected_fields() -> None:
    item = {
        "subject": "Xbox Series X console in ottime condizioni",
        "urls": {"default": "https://www.subito.it/annuncio/123"},
        "urn": "id:ad:abc:list:123456789",
        "features": {
            "/price": {"values": [{"key": "350", "value": "350 €"}]},
            "/item_condition": {"values": [{"key": "20"}]},
        },
        "geo": {"town": {"value": "Roma"}, "region": {"value": "Lazio"}},
        "images": [{"cdnBaseUrl": "https://img.test/x.jpg"}],
        "date": "2026-03-18 08:21:53",
        "advertiser": {"company": False},
        "type": {"key": "s"},
    }

    ad = _parse_ad(item)
    assert ad is not None
    assert ad["sku"] == "SUBITO-123456789"
    assert ad["available"] is True
    assert ad["price"] == 350.0
    assert ad["city"] == "Roma"


def test_parse_ebay_item_fallback_id_is_stable_format() -> None:
    raw = {
        "title": "Microsoft Xbox One console completa",
        "price_text": "EUR 120,00",
        "sold_date": "Venduto il 12 mar 2026",
        "url": "https://www.ebay.it/itm/no-numeric-id-here",
    }

    item = _parse_item(raw, "Xbox One")
    assert item is not None
    assert re.match(r"^EBAY-[A-F0-9]{16}$", item["sku"]) is not None
    assert item["price"] == 120.0
