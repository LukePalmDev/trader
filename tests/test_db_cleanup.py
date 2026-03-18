from pathlib import Path

import sqlite3

import db


def test_clean_db_removes_legacy_sources_and_normalizes_data(tmp_path: Path) -> None:
    db_path = tmp_path / "trader.db"
    db.init_db(db_path)

    db.process_products(
        [
            {
                "name": "Console Xbox One S (1 tb)",
                "source": "gameshock",
                "condition": "Nuovo",
                "price": 190.0,
                "available": True,
                "url": "https://www.gameshock.it/console-xbox-one/6940-console-xbox-one-s.html",
            },
            {
                "name": "Console Xbox One S",
                "source": "gameshock",
                "condition": "Usato",
                "price": 189.0,
                "available": True,
                "url": "https://www.gameshock.it/console-xbox-one/6940-console-xbox-one-s.html",
            },
            {
                "name": "Console Xbox One S (USATA)",
                "source": "gameshock",
                "condition": "Nuovo",
                "price": 180.0,
                "available": True,
                "url": "https://www.gameshock.it/console-xbox-one/7424-console-xbox-one-s-usata.html",
            },
            {
                "name": "Xbox One usata",
                "source": "cex",
                "condition": "Nuovo",
                "price": 99.0,
                "available": True,
                "url": "https://it.webuy.com/product-detail/?id=123",
            },
            {
                "name": "Microsoft Xbox One X 1TB [Buono]",
                "source": "rebuy",
                "condition": "Usato",
                "price": 218.99,
                "available": True,
                "url": "https://www.rebuy.it/i,10796034/xbox-one/microsoft-xbox-one-x-1tb-controller-wireless-incluso-nero",
            },
            {
                "name": "Microsoft Xbox One X 1TB [Molto buono]",
                "source": "rebuy",
                "condition": "Usato",
                "price": 229.99,
                "available": True,
                "url": "https://www.rebuy.it/i,10796034/xbox-one/microsoft-xbox-one-x-1tb-controller-wireless-incluso-nero",
            },
            {
                "name": "Annuncio legacy",
                "source": "subito",
                "condition": "Usato",
                "price": 50.0,
                "available": True,
                "url": "https://www.subito.it/x/legacy",
            },
        ],
        db_path,
    )

    # Inserisce una riga invalida legacy
    con = sqlite3.connect(db_path)
    con.execute(
        """
        INSERT INTO products (
            name, source, condition, first_seen, last_seen, last_available
        ) VALUES ('', 'gameshock', 'Usato', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', 1)
        """
    )
    con.commit()
    con.close()

    summary = db.clean_db(db_path)

    assert summary["removed_separate_db_sources"] >= 1
    assert summary["removed_invalid_rows"] >= 1
    assert summary["conditions_normalized"] >= 1
    assert summary["url_merged_rows"] >= 0

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # subito deve stare nel DB dedicato, non in trader.db
    assert con.execute("SELECT COUNT(*) FROM products WHERE source='subito'").fetchone()[0] == 0

    # gameshock deduplicato per URL e condizione basata su marker usato/usata/used
    gs = con.execute(
        "SELECT condition, COUNT(*) c FROM products WHERE source='gameshock' GROUP BY condition"
    ).fetchall()
    by_cond = {row["condition"]: row["c"] for row in gs}
    assert by_cond.get("Nuovo", 0) >= 1
    assert by_cond.get("Usato", 0) >= 1

    gs_rows = con.execute("SELECT name, condition FROM products WHERE source='gameshock'").fetchall()
    assert gs_rows
    for row in gs_rows:
        lname = row["name"].lower()
        if "usata" in lname or "usato" in lname or "used" in lname:
            assert row["condition"] == "Usato"
        else:
            assert row["condition"] == "Nuovo"
    assert con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT lower(rtrim(url,'/')) u, COUNT(*) c
            FROM products
            WHERE source='gameshock' AND url IS NOT NULL AND trim(url)<>'' 
            GROUP BY u HAVING c>1
        )
        """
    ).fetchone()[0] == 0

    # cex forzato Usato
    cex_cond = con.execute("SELECT condition FROM products WHERE source='cex'").fetchone()[0]
    assert cex_cond == "Usato"

    # rebuy: stesso URL ma varianti diverse devono rimanere separate
    rebuy_count = con.execute("SELECT COUNT(*) FROM products WHERE source='rebuy'").fetchone()[0]
    assert rebuy_count == 2

    # nessun nome vuoto
    assert con.execute("SELECT COUNT(*) FROM products WHERE name IS NULL OR trim(name)='' ").fetchone()[0] == 0
    con.close()
