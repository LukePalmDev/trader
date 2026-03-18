from pathlib import Path

import sqlite3

import db
from model_rules import classify_title, standardize_title


def test_standardize_title_groups_equivalent_console_names() -> None:
    a = "Microsoft Xbox One X 1TB [controller wireless incluso] nero"
    b = "Console Xbox One X 1 TB - Nero"

    ca = classify_title(a)
    cb = classify_title(b)
    sa = standardize_title(a, classification=ca)
    sb = standardize_title(b, classification=cb)

    assert sa.standard_name == "Xbox One X 1 TB - Nero"
    assert sb.standard_name == "Xbox One X 1 TB - Nero"
    assert sa.standard_key == sb.standard_key


def test_standardize_title_keeps_limited_separate() -> None:
    base = "Xbox One X 1 TB nero"
    limited = "Xbox One X 1 TB edizione Project Scorpio nero"

    s_base = standardize_title(base, classification=classify_title(base))
    s_limited = standardize_title(limited, classification=classify_title(limited))

    assert s_base.standard_name == "Xbox One X 1 TB - Nero"
    assert s_limited.standard_name == "Xbox One X 1 TB [Project Scorpio] - Nero"
    assert s_base.standard_key != s_limited.standard_key


def test_process_products_stores_original_and_standardized_name(tmp_path: Path) -> None:
    db_path = tmp_path / "trader.db"
    db.init_db(db_path)

    db.process_products(
        [
            {
                "name": "Microsoft Xbox One X 1TB [controller wireless incluso] nero",
                "source": "rebuy",
                "condition": "Usato",
                "price": 219.0,
                "available": True,
                "url": "https://www.rebuy.it/i,10796034/xbox-one/microsoft-xbox-one-x-1tb-controller-wireless-incluso-nero",
            }
        ],
        db_path,
    )

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT name, standard_name, standard_key, packaging_state FROM products WHERE source='rebuy' LIMIT 1"
    ).fetchone()
    con.close()

    assert row is not None
    assert row["name"] == "Microsoft Xbox One X 1TB [controller wireless incluso] nero"
    assert row["standard_name"] == "Xbox One X 1 TB - Nero"
    assert row["standard_key"] is not None and row["standard_key"] != ""
    assert row["packaging_state"] == "Imballata"


def test_standardize_title_keeps_xbox_360_e_distinct() -> None:
    base = "Console Xbox 360 250 GB nero"
    e_model = "Console Xbox 360 E 250 GB nero"

    s_base = standardize_title(base, classification=classify_title(base))
    s_e = standardize_title(e_model, classification=classify_title(e_model))

    assert s_base.standard_name == "Xbox 360 250 GB - Nero"
    assert s_e.standard_name == "Xbox 360 E 250 GB - Nero"
    assert s_base.standard_key != s_e.standard_key


def test_classify_title_prefers_first_family_occurrence() -> None:
    title = "Microsoft Xbox Series S 512GB [controller wireless per Xbox Series X]"
    classified = classify_title(title)
    standardized = standardize_title(title, classification=classified)

    assert classified.console_family == "series-s"
    assert classified.canonical_model == "series-s-512gb"
    assert standardized.standard_name.startswith("Xbox Series S 512 GB")


def test_xbox360_no_space_recognized_as_360() -> None:
    """'Xbox360' (senza spazio) deve essere riconosciuto come family 360."""
    cases = [
        ("Xbox360 250GB HaloR + 1 Pad Pad, Imballata", "Xbox 360 250 GB [Halo]"),
        ("Xbox360 320GB Halo4 + 1 Pad Pad, Non Imballata", "Xbox 360 320 GB [Halo]"),
    ]
    for name, expected in cases:
        c = classify_title(name)
        assert c.console_family == "360", f"family errata per: {name!r}"
        s = standardize_title(name, classification=c)
        assert s.standard_name == expected, f"nome errato per: {name!r}\nGOT: {s.standard_name}"


def test_xbox_360s_recognized_as_360_slim() -> None:
    """'Xbox 360S' (abbreviazione CEX per Slim) deve essere family 360 e sub-model Slim."""
    cases = [
        ("Xbox 360S 320GB MW3 + 2 Pads, Non Imballata", "Xbox 360 Slim 320 GB"),
        ("Xbox 360S Gears3 Ed+1 Pad (No Gioco), Imballata", "Xbox 360 Slim [Gears]"),
        ("Xbox 360S Halo Ed +2Casa, Imballata", "Xbox 360 Slim [Halo]"),
    ]
    for name, expected in cases:
        c = classify_title(name)
        assert c.console_family == "360", f"family errata per: {name!r}"
        s = standardize_title(name, classification=c)
        assert s.standard_name == expected, f"nome errato per: {name!r}\nGOT: {s.standard_name}"


def test_xbox_360_elite_sub_model() -> None:
    """'Xbox 360 Elite' deve essere riconosciuto come sotto-modello separato da Slim e E."""
    elite = "Xbox 360 Elite 120GB, Imballata"
    elite_color = "Xbox 360 Elite 120GB Rosso, Non Imballata"
    elite_no_storage = "Xbox 360 Elite, Resident Evil 5 Ltd. Ed. (No Gioco)"
    base = "Console Xbox 360 250 GB nero"

    s_elite = standardize_title(elite, classification=classify_title(elite))
    s_elite_color = standardize_title(elite_color, classification=classify_title(elite_color))
    s_elite_ns = standardize_title(elite_no_storage, classification=classify_title(elite_no_storage))
    s_base = standardize_title(base, classification=classify_title(base))

    assert s_elite.standard_name == "Xbox 360 Elite 120 GB"
    assert s_elite_color.standard_name == "Xbox 360 Elite 120 GB - Rosso"
    assert s_elite_ns.standard_name == "Xbox 360 Elite [LIMITED]"
    # Elite deve avere standard_key diverso da base 360
    assert s_elite.standard_key != s_base.standard_key


def test_xbox_360_sub_models_are_distinct() -> None:
    """E / Slim / Elite / base 360 devono avere standard_key diversi."""
    names = {
        "base":  "Xbox 360 250GB",
        "E":     "Xbox 360 E 250GB",
        "Slim":  "Xbox 360 Slim 250GB",
        "Elite": "Xbox 360 Elite 250GB",
    }
    keys = {label: standardize_title(n, classification=classify_title(n)).standard_key
            for label, n in names.items()}
    assert len(set(keys.values())) == 4, f"Chiavi non tutte distinte: {keys}"


def test_edizione_digitale_recognized_as_digital() -> None:
    """'Edizione Digitale' (italiano) deve essere riconosciuta come variante Digital."""
    name = "Xbox Series X Edizione Digitale, 1TB, Robot White, Imballata"
    c = classify_title(name)
    s = standardize_title(name, classification=c)
    assert "Digital" in s.standard_name, f"'Digital' mancante in: {s.standard_name!r}"
    assert s.standard_name == "Xbox Series X Digital 1 TB - Bianco"


def test_ltd_abbreviation_recognized_as_limited() -> None:
    """'Ltd.' (abbreviazione di Limited) deve essere riconosciuta come edizione limitata."""
    name = "Xbox 360 Elite, Resident Evil 5 Ltd. Ed. (No Gioco)"
    c = classify_title(name)
    assert c.edition_class == "limited", f"edition_class errata: {c.edition_class!r}"


def test_halo_forza_gears_with_trailing_chars() -> None:
    """Halo4, Gears3, HaloR ecc. devono essere riconosciuti come edition descriptor."""
    cases = [
        ("Xbox360 250GB HaloR + 1 Pad Pad", "Halo"),
        ("Xbox360 320GB Halo4 + 1 Pad Pad", "Halo"),
        ("Xbox 360S Gears3 Ed+1 Pad (No Gioco)", "Gears"),
        ("Xbox 360 Halo 3 Special Edizione", "Halo"),
    ]
    for name, expected_descriptor in cases:
        c = classify_title(name)
        s = standardize_title(name, classification=c)
        assert f"[{expected_descriptor}]" in s.standard_name, (
            f"[{expected_descriptor}] mancante per {name!r}\nGOT: {s.standard_name}"
        )


def test_process_products_sets_cex_packaging_from_name(tmp_path: Path) -> None:
    db_path = tmp_path / "trader.db"
    db.init_db(db_path)

    db.process_products(
        [
            {
                "name": "Xbox One S 1 TB, Non Imballata",
                "source": "cex",
                "condition": "Usato",
                "price": 149.0,
                "available": True,
                "url": "https://it.webuy.com/product-detail/?id=S123",
            }
        ],
        db_path,
    )

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT packaging_state FROM products WHERE source='cex' LIMIT 1"
    ).fetchone()
    con.close()

    assert row is not None
    assert row["packaging_state"] == "Non Imballata"
