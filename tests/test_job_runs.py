from pathlib import Path

import job_runs


def test_record_and_status(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    job_runs.record("scrape-cex", "ok", db_path=db, host="server",
                    source="cex", counts={"total": 168, "new": 2})
    job_runs.record("verify-sold", "error", db_path=db, error="boom")
    res = job_runs.status(db)
    by = {j["id"]: j for j in res["jobs"]}
    assert by["scrape-cex"]["status"] == "ok"
    assert by["verify-sold"]["status"] == "error"
    assert by["scrape-rebuy"]["status"] == "unknown"
    assert res["overall"] == "error"


def test_status_invalido_diventa_error(tmp_path: Path) -> None:
    db = tmp_path / "t.db"
    job_runs.record("backup", "bogus", db_path=db)
    by = {j["id"]: j for j in job_runs.status(db)["jobs"]}
    assert by["backup"]["status"] == "error"


def test_history(tmp_path):
    db = tmp_path / "t.db"
    job_runs.record("scrape-cex", "ok", db_path=db, counts={"total": 1})
    job_runs.record("scrape-cex", "warn", db_path=db, error="x")
    h = job_runs.history("scrape-cex", db_path=db)
    assert len(h) == 2 and h[0]["status"] == "warn"
    assert job_runs.history("inesistente", db_path=db) == []
