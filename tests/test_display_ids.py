from pathlib import Path

import sqlite3

import db


def test_display_ids_prefix_and_gamelife_pairing(tmp_path: Path) -> None:
    db_path = tmp_path / "trader.db"
    db.init_db(db_path)

    products = [
        {
            "name": "Xbox Series X 1TB",
            "source": "cex",
            "condition": "Usato",
            "price": 420.0,
            "available": True,
            "url": "https://example.test/cex-x",
        },
        {
            "name": "Xbox One S 1TB",
            "source": "gamelife",
            "condition": "Nuovo",
            "price": 210.0,
            "available": True,
            "url": "https://example.test/gl-one-s-n",
        },
        {
            "name": "Xbox One S 1TB",
            "source": "gamelife",
            "condition": "Usato",
            "price": 160.0,
            "available": True,
            "url": "https://example.test/gl-one-s-u",
        },
        {
            "name": "Xbox 360 250GB",
            "source": "gamelife",
            "condition": "Nuovo",
            "price": 90.0,
            "available": True,
            "url": "https://example.test/gl-360-n",
        },
    ]
    db.process_products(products, db_path)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    cex_row = con.execute(
        "SELECT display_id FROM products WHERE source='cex'"
    ).fetchone()
    assert cex_row is not None
    assert str(cex_row["display_id"]).startswith("1")

    rows = con.execute(
        """
        SELECT condition, display_id
        FROM products
        WHERE source='gamelife' AND name='Xbox One S 1TB'
        ORDER BY display_id ASC
        """
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["condition"] == "Nuovo"
    assert rows[1]["condition"] == "Usato"
    assert rows[1]["display_id"] == rows[0]["display_id"] + 1

    con.close()
