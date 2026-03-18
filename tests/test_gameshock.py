from scrapers.gameshock import _parse_page


def test_gameshock_defaults_to_nuovo_when_no_used_marker() -> None:
    html = '''
    <div class="ajax_block_product">
      <h3><a href="https://www.gameshock.it/console-xbox-one/6940-console-xbox-one-s.html" title="Console Xbox One S (1 tb)">Console Xbox One S (1 tb)</a></h3>
      <span class="price">199,00 €</span>
      <div class="availability">Disponibile</div>
      <img src="/img/p/1/2/3.jpg" />
    </div>
    '''
    products = _parse_page(html, "Xbox One")
    assert len(products) == 1
    assert products[0]["condition"] == "Nuovo"


def test_gameshock_marks_usato_with_explicit_used_marker() -> None:
    html = '''
    <div class="ajax_block_product">
      <h3><a href="https://www.gameshock.it/console-xbox-one/9999-xbox-one-usata.html" title="Console Xbox One Usata">Console Xbox One Usata</a></h3>
      <span class="price">249,00 €</span>
      <div class="availability">Disponibile</div>
      <img src="/img/p/4/5/6.jpg" />
    </div>
    '''
    products = _parse_page(html, "Xbox One")
    assert len(products) == 1
    assert products[0]["condition"] == "Usato"
