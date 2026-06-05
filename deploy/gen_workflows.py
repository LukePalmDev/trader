#!/usr/bin/env python3
"""Generatore/verifica dei workflow dal manifest unico workflows.toml.

Uso:
    python3 deploy/gen_workflows.py --check     # drift: manifest vs unit deployate (default)
    python3 deploy/gen_workflows.py --write     # (ri)genera deploy/systemd/*.service|*.timer

Genera solo i job host="server" (systemd). I job github/mac sono documentati
nel manifest ma gestiti dai rispettivi sistemi (Actions / launchd).
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

ROOT = Path(__file__).resolve().parent.parent
SYSTEMD_DIR = ROOT / "deploy" / "systemd"
APP_DIR = "/opt/trader/app"
VENV_PY = "/opt/trader/venv/bin/python"

SERVICE_TMPL = """[Unit]
Description=Trader: {description}
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User={user}
{group}EnvironmentFile=-/etc/trader/trader.env
WorkingDirectory={app}
ExecStart={execstart}
"""

TIMER_CAL = """[Unit]
Description=Timer: {description}

[Timer]
OnCalendar={oncalendar}
Persistent=true
RandomizedDelaySec=120

[Install]
WantedBy=timers.target
"""

TIMER_INT = """[Unit]
Description=Timer: {description}

[Timer]
OnBootSec=2min
OnUnitActiveSec={interval}
Persistent=true

[Install]
WantedBy=timers.target
"""


def _load() -> dict:
    with (ROOT / "workflows.toml").open("rb") as fh:
        return tomllib.load(fh).get("jobs", {})


def _execstart(spec: dict) -> str:
    exe = spec["exec"]
    if exe.endswith(".py"):
        return f"{VENV_PY} {APP_DIR}/{exe.split()[0]} {' '.join(exe.split()[1:])}".strip()
    return f"{APP_DIR}/{exe}"


def _render(name: str, spec: dict) -> tuple[str, str]:
    group = f"Group={spec['user']}\n" if spec.get("user") != "root" else ""
    service = SERVICE_TMPL.format(
        description=spec["description"], user=spec.get("user", "trader"),
        group=group, app=APP_DIR, execstart=_execstart(spec),
    )
    if "oncalendar" in spec:
        timer = TIMER_CAL.format(description=spec["description"], oncalendar=spec["oncalendar"])
    else:
        timer = TIMER_INT.format(description=spec["description"], interval=spec["interval"])
    return service, timer


def cmd_write(jobs: dict) -> int:
    SYSTEMD_DIR.mkdir(parents=True, exist_ok=True)
    n = 0
    for name, spec in jobs.items():
        if spec.get("host") != "server":
            continue
        service, timer = _render(name, spec)
        (SYSTEMD_DIR / f"trader-{name}.service").write_text(service, encoding="utf-8")
        (SYSTEMD_DIR / f"trader-{name}.timer").write_text(timer, encoding="utf-8")
        n += 1
    print(f"Generati {n} servizi+timer in {SYSTEMD_DIR}")
    return 0


def _schedule_of_timer(path: Path) -> str:
    if not path.exists():
        return "(timer mancante)"
    txt = path.read_text(encoding="utf-8")
    m = re.search(r"OnCalendar=(.+)", txt)
    if m:
        return m.group(1).strip()
    m = re.search(r"OnUnitActiveSec=(.+)", txt)
    return f"ogni {m.group(1).strip()}" if m else "(nessuno schedule)"


def cmd_check(jobs: dict) -> int:
    drift = 0
    for name, spec in jobs.items():
        if spec.get("host") != "server":
            print(f"  [{spec.get('host')}] {name}: {spec.get('schedule', '-')}")
            continue
        want = spec.get("oncalendar") or f"ogni {spec['interval']}"
        got = _schedule_of_timer(SYSTEMD_DIR / f"trader-{name}.timer")
        ok = want == got
        if not ok:
            drift += 1
        print(f"  [server] {name}: manifest={want!r} deployato={got!r} {'OK' if ok else 'DRIFT!'}")
    print("Nessun drift." if not drift else f"{drift} job in drift rispetto al manifest.")
    return 1 if drift else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", action="store_true", help="(ri)genera le unit systemd")
    ap.add_argument("--check", action="store_true", help="verifica drift (default)")
    args = ap.parse_args()
    jobs = _load()
    return cmd_write(jobs) if args.write else cmd_check(jobs)


if __name__ == "__main__":
    raise SystemExit(main())
