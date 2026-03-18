from scrapers.rebuy import _is_console_candidate, _parse_variant_options


def test_rebuy_console_filter_keeps_bundle_and_excludes_controller_only() -> None:
    assert _is_console_candidate("Microsoft Xbox One S 1TB [incl. Wireless Controller] bianco")
    assert not _is_console_candidate("Microsoft Xbox One S Wireless Controller bianco")
    assert not _is_console_candidate("Controller wireless Microsoft Elite Series 2 nero")


def test_rebuy_parse_variant_options_with_price_deltas() -> None:
    html = """
    <html>
      <body>
        <span data-cy="product-price">218,99 €</span>
        <button data-cy="select-variant-A1" disabled="disabled">
          <div class="choice-tile__title">Eccellente</div>
          <span>Non disponibile</span>
        </button>
        <button data-cy="select-variant-A2">
          <div class="choice-tile__title">Molto buono</div>
          <span>+ 11 €</span>
        </button>
        <button data-cy="select-variant-A3" class="active">
          <div class="choice-tile__title">Buono</div>
          <span>Selezionato</span>
        </button>
        <button data-cy="select-variant-A4">
          <div class="choice-tile__title">Accettabile</div>
          <span>- 7 €</span>
        </button>
      </body>
    </html>
    """
    variants = _parse_variant_options(html, fallback_price=None)

    assert len(variants) == 4

    by_code = {v["code"]: v for v in variants}
    assert by_code["A1"]["available"] is False
    assert by_code["A1"]["price"] is None

    assert by_code["A2"]["available"] is True
    assert by_code["A2"]["price"] == 229.99
    assert by_code["A2"]["price_display"] == "229,99 €"

    assert by_code["A3"]["available"] is True
    assert by_code["A3"]["price"] == 218.99

    assert by_code["A4"]["available"] is True
    assert by_code["A4"]["price"] == 211.99
