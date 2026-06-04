from pathlib import Path

import log_status


def test_collect_classifies_server_jobs(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    app_dir = tmp_path / "app"
    app_dir.mkdir()

    # Job con run pulito.
    (log_dir / "scrape-fonti.log").write_text(
        "[trader] job scrape-fonti started at 2026-06-04T07:00:00Z\n"
        "INFO DB aggiornato — nuovi: 3\n"
        "INFO Run report scritto\n",
        encoding="utf-8",
    )
    # Job con errore.
    (log_dir / "scrape-subito.log").write_text(
        "[trader] job scrape-subito started at 2026-06-04T06:00:00Z\n"
        "Traceback (most recent call last):\n"
        "RuntimeError: boom\n",
        encoding="utf-8",
    )

    res = log_status.collect(app_dir, log_dir)
    by_id = {j["id"]: j for j in res["jobs"]}

    assert by_id["scrape-fonti"]["status"] == "ok"
    assert by_id["scrape-subito"]["status"] == "error"
    # Job senza file => unknown.
    assert by_id["scrape-ebay"]["status"] == "unknown"
    # overall riflette lo stato peggiore presente (error).
    assert res["overall"] == "error"


def test_github_archive_parsed(tmp_path: Path) -> None:
    app_dir = tmp_path / "app"
    wf = app_dir / "LogGitHub" / "Subito.it" / "#5"
    wf.mkdir(parents=True)
    (wf / "run.log").write_text("step ok\nProcess completed\n", encoding="utf-8")
    older = app_dir / "LogGitHub" / "Subito.it" / "#4"
    older.mkdir(parents=True)
    (older / "run.log").write_text("error: failed\n", encoding="utf-8")

    res = log_status.collect(app_dir, tmp_path / "nolog")
    arch = {a["id"]: a for a in res["archive"]}
    assert arch["Subito.it"]["runs"] == 2
    assert arch["Subito.it"]["last_run"] == "#5"
    assert arch["Subito.it"]["status"] == "ok"  # #5 è l'ultimo, pulito
