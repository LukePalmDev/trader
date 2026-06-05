import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "gen_workflows", Path(__file__).resolve().parent.parent / "deploy" / "gen_workflows.py")
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)


def test_manifest_loads_and_has_server_jobs() -> None:
    jobs = gen._load()
    assert "scrape-fonti" in jobs
    server = [j for j, s in jobs.items() if s.get("host") == "server"]
    assert "verify-sold" in server and "backup" in server


def test_render_calendar_and_interval() -> None:
    svc, tmr = gen._render("verify-sold", {
        "description": "x", "user": "trader",
        "exec": "deploy/server_job.sh verify-sold", "oncalendar": "*-*-* 11:00:00"})
    assert "ExecStart=/opt/trader/app/deploy/server_job.sh verify-sold" in svc
    assert "OnCalendar=*-*-* 11:00:00" in tmr
    svc2, tmr2 = gen._render("watchdog", {
        "description": "x", "user": "trader", "exec": "job_runs.py check", "interval": "3h"})
    assert "/opt/trader/venv/bin/python /opt/trader/app/job_runs.py check" in svc2
    assert "OnUnitActiveSec=3h" in tmr2
