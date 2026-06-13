from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import argparse
import json
import sqlite3
import sys

from .config import BotConfig, ConfigError, load_config


BUSY_STREAM_STATUSES = frozenset({"downloading", "checking_after_exit", "waiting_retry"})
BUSY_JOB_STATUSES = frozenset({"queued", "running"})


@dataclass(frozen=True, slots=True)
class IdleCheckResult:
    known: bool
    idle: bool
    reasons: tuple[str, ...] = ()

    @property
    def exit_code(self) -> int:
        if not self.known:
            return 2
        return 0 if self.idle else 1

    def message(self) -> str:
        if self.idle and self.known:
            return "idle"
        prefix = "unknown" if not self.known else "busy"
        if not self.reasons:
            return prefix
        return f"{prefix}: {'; '.join(self.reasons)}"


class IdleStatusUnknown(RuntimeError):
    """Raised when the updater cannot safely determine service idleness."""


def idle_result_from_status_snapshot(snapshot: Mapping[str, Any]) -> IdleCheckResult:
    counts = snapshot.get("counts")
    jobs = snapshot.get("jobs")
    if not isinstance(counts, Mapping):
        return IdleCheckResult(False, False, ("status snapshot missing counts",))
    if not isinstance(jobs, list):
        return IdleCheckResult(False, False, ("status snapshot missing jobs",))

    reasons: list[str] = []
    for status in sorted(BUSY_STREAM_STATUSES):
        raw_count = counts.get(status, 0)
        try:
            count = int(raw_count)
        except (TypeError, ValueError):
            return IdleCheckResult(False, False, (f"invalid count for {status}",))
        if count > 0:
            reasons.append(f"{status} streams={count}")

    busy_jobs = 0
    for job in jobs:
        if not isinstance(job, Mapping):
            return IdleCheckResult(False, False, ("status snapshot has invalid job entry",))
        if str(job.get("status", "")) in BUSY_JOB_STATUSES:
            busy_jobs += 1
    if busy_jobs:
        reasons.append(f"active jobs={busy_jobs}")

    return IdleCheckResult(True, not reasons, tuple(reasons))


def idle_result_from_state(db_path: str | Path) -> IdleCheckResult:
    path = Path(db_path)
    if not path.is_file():
        return IdleCheckResult(False, False, (f"state database not found: {path}",))

    try:
        conn = sqlite3.connect(path)
        try:
            reasons = _busy_reasons_from_state_connection(conn)
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return IdleCheckResult(False, False, (f"state database unreadable: {exc}",))

    return IdleCheckResult(True, not reasons, tuple(reasons))


def _busy_reasons_from_state_connection(conn: sqlite3.Connection) -> list[str]:
    reasons: list[str] = []
    stream_rows = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM streams
        WHERE status IN ('downloading', 'checking_after_exit', 'waiting_retry')
        GROUP BY status
        """
    ).fetchall()
    for status, count in stream_rows:
        reasons.append(f"{status} streams={count}")

    watermark_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM watermark_copies
        WHERE status IN ('queued', 'running')
        """
    ).fetchone()[0]
    if int(watermark_count) > 0:
        reasons.append(f"active watermark jobs={watermark_count}")
    return reasons


def status_url_for_config(config: BotConfig) -> str:
    host = config.web_host.strip() or "127.0.0.1"
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    if ":" in host and not (host.startswith("[") and host.endswith("]")):
        host = f"[{host}]"
    return f"http://{host}:{config.web_port}/status.json"


def fetch_status_snapshot(url: str, *, timeout: float = 5.0) -> Mapping[str, Any]:
    try:
        with urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise IdleStatusUnknown(str(exc)) from exc
    if not isinstance(payload, Mapping):
        raise IdleStatusUnknown("status endpoint returned a non-object payload")
    return payload


def check_active_service_idle(config_path: str | Path, *, timeout: float = 5.0) -> IdleCheckResult:
    config = load_config(config_path)
    if not config.web_enabled:
        return IdleCheckResult(False, False, ("status web interface is disabled",))
    url = status_url_for_config(config)
    try:
        snapshot = fetch_status_snapshot(url, timeout=timeout)
    except IdleStatusUnknown as exc:
        return IdleCheckResult(False, False, (f"status endpoint unavailable: {exc}",))
    return idle_result_from_status_snapshot(snapshot)


def config_enables_transcription(config_path: str | Path) -> bool:
    return load_config(config_path).transcribe_subtitles


def render_python_update_service_unit(
    *,
    install_dir: str,
    app_dir: str,
    venv_dir: str,
    config_file: str,
    main_service_name: str,
    update_script: str | None = None,
) -> str:
    script = update_script or f"{app_dir}/scripts/update-python-deps.sh"
    return "\n".join(
        [
            "[Unit]",
            "Description=ONLYSAVEmeVODS Python dependency updater",
            "Wants=network-online.target",
            "After=network-online.target",
            f"ConditionPathExists={venv_dir}/bin/python",
            f"ConditionPathExists={script}",
            "",
            "[Service]",
            "Type=oneshot",
            systemd_environment_line("ONLYSAVEMEVODS_INSTALL_DIR", install_dir),
            systemd_environment_line("ONLYSAVEMEVODS_APP_DIR", app_dir),
            systemd_environment_line("ONLYSAVEMEVODS_VENV_DIR", venv_dir),
            systemd_environment_line("ONLYSAVEMEVODS_CONFIG_FILE", config_file),
            systemd_environment_line("ONLYSAVEMEVODS_SERVICE_NAME", main_service_name),
            f"ExecStart={script}",
            "",
        ]
    )


def render_python_update_timer_unit(
    *,
    update_service_name: str,
    calendar: str,
    random_delay: str,
) -> str:
    return "\n".join(
        [
            "[Unit]",
            "Description=Nightly ONLYSAVEmeVODS Python dependency updater",
            "",
            "[Timer]",
            f"OnCalendar={calendar}",
            f"RandomizedDelaySec={random_delay}",
            "Persistent=true",
            f"Unit={update_service_name}",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )


def systemd_environment_line(name: str, value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'Environment="{name}={escaped}"'


def write_systemd_units(args: argparse.Namespace) -> int:
    service_unit = render_python_update_service_unit(
        install_dir=args.install_dir,
        app_dir=args.app_dir,
        venv_dir=args.venv_dir,
        config_file=args.config,
        main_service_name=args.main_service_name,
    )
    timer_unit = render_python_update_timer_unit(
        update_service_name=args.update_service_name,
        calendar=args.calendar,
        random_delay=args.random_delay,
    )
    Path(args.service_unit).write_text(service_unit, encoding="utf-8")
    Path(args.timer_unit).write_text(timer_unit, encoding="utf-8")
    return 0


def check_idle_command(args: argparse.Namespace) -> int:
    try:
        if args.state_only:
            config = load_config(args.config)
            result = idle_result_from_state(config.db_path)
        else:
            result = check_active_service_idle(args.config, timeout=args.timeout)
    except ConfigError as exc:
        result = IdleCheckResult(False, False, (f"config unreadable: {exc}",))
    print(result.message())
    return result.exit_code


def config_enables_transcription_command(args: argparse.Namespace) -> int:
    try:
        return 0 if config_enables_transcription(args.config) else 1
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m onlysavemevods.python_update",
        description="Helpers for ONLYSAVEmeVODS unattended Python dependency updates.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_idle = subparsers.add_parser("check-idle")
    check_idle.add_argument("--config", required=True)
    check_idle.add_argument("--timeout", type=float, default=5.0)
    check_idle.add_argument("--state-only", action="store_true")

    transcription = subparsers.add_parser("config-enables-transcription")
    transcription.add_argument("--config", required=True)

    write_units = subparsers.add_parser("write-systemd-units")
    write_units.add_argument("--service-unit", required=True)
    write_units.add_argument("--timer-unit", required=True)
    write_units.add_argument("--install-dir", required=True)
    write_units.add_argument("--app-dir", required=True)
    write_units.add_argument("--venv-dir", required=True)
    write_units.add_argument("--config", required=True)
    write_units.add_argument("--main-service-name", required=True)
    write_units.add_argument("--update-service-name", required=True)
    write_units.add_argument("--calendar", required=True)
    write_units.add_argument("--random-delay", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "check-idle":
        return check_idle_command(args)
    if args.command == "config-enables-transcription":
        return config_enables_transcription_command(args)
    if args.command == "write-systemd-units":
        return write_systemd_units(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
