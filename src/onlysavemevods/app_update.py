from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

from . import __version__ as APP_VERSION
from .config import BotConfig, ConfigError, load_config

try:
    from packaging.version import InvalidVersion, Version
except Exception:  # pragma: no cover - fallback is covered without packaging.
    InvalidVersion = ValueError  # type: ignore[assignment]
    Version = None  # type: ignore[assignment]


APP_UPDATE_REQUEST_FILENAME = "app-update-request.json"
APP_UPDATE_STATUS_FILENAME = "app-update-status.json"
APP_UPDATE_BACKUP_DIRNAME = "app-update-backups"
GITHUB_API_ROOT = "https://api.github.com"
UPDATE_USER_AGENT = "ONLYSAVEmeVODS updater"
CHECK_STATUSES = {"checked", "update_available", "up_to_date", "failed", "disabled"}


class AppUpdateError(RuntimeError):
    """Raised when the app updater cannot complete safely."""


@dataclass(frozen=True, slots=True)
class ReleaseAsset:
    name: str
    download_url: str
    size: int = 0


@dataclass(frozen=True, slots=True)
class GitHubRelease:
    tag: str
    version: str
    name: str
    html_url: str
    published_at: str
    prerelease: bool
    draft: bool
    tarball: ReleaseAsset
    checksum: ReleaseAsset


def request_path(config: BotConfig) -> Path:
    return config.state_dir / APP_UPDATE_REQUEST_FILENAME


def status_path(config: BotConfig) -> Path:
    return config.state_dir / APP_UPDATE_STATUS_FILENAME


def update_status(config: BotConfig, *, current_version: str = APP_VERSION) -> dict[str, Any]:
    status = _read_json_file(status_path(config))
    if not isinstance(status, dict):
        status = {}
    request = _read_json_file(request_path(config))
    if not isinstance(request, dict):
        request = None

    latest_version = str(status.get("latest_version") or "")
    available = bool(
        latest_version
        and is_newer_version(latest_version, current_version)
        and status.get("status") in CHECK_STATUSES
    )
    pending = request is not None
    result: dict[str, Any] = {
        "mode": config.app_update_mode,
        "enabled": config.app_update_mode != "disabled",
        "repository": config.app_update_repository,
        "include_prereleases": config.app_update_include_prereleases,
        "token_env": config.app_update_github_token_env,
        "token_configured": bool(
            config.app_update_github_token_env
            and os.environ.get(config.app_update_github_token_env)
        ),
        "current_version": current_version,
        "status": status.get("status") or ("disabled" if config.app_update_mode == "disabled" else "unknown"),
        "message": status.get("message") or "",
        "checked_at": status.get("checked_at"),
        "updated_at": status.get("updated_at"),
        "latest_tag": status.get("latest_tag") or "",
        "latest_version": latest_version,
        "latest_name": status.get("latest_name") or "",
        "latest_url": status.get("latest_url") or "",
        "release_url": status.get("release_url") or status.get("latest_url") or "",
        "archive_name": status.get("archive_name") or "",
        "archive_url": status.get("archive_url") or "",
        "archive_size": status.get("archive_size") or 0,
        "checksum_name": status.get("checksum_name") or "",
        "checksum_url": status.get("checksum_url") or "",
        "checksum_size": status.get("checksum_size") or 0,
        "available": available,
        "pending": pending,
        "pending_tag": request.get("tag") if request else "",
        "pending_version": request.get("version") if request else "",
        "pending_source": request.get("source") if request else "",
        "requested_at": request.get("requested_at") if request else None,
        "last_error": status.get("last_error") or "",
        "last_installed_version": status.get("last_installed_version") or "",
        "last_installed_tag": status.get("last_installed_tag") or "",
        "installed_at": status.get("installed_at"),
    }
    return result


def check_for_updates(
    config: BotConfig,
    *,
    current_version: str = APP_VERSION,
    fetcher: Callable[[BotConfig], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    if config.app_update_mode == "disabled":
        status = {
            "status": "disabled",
            "message": "App updater is disabled.",
            "checked_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
        write_update_status(config, status)
        return update_status(config, current_version=current_version)

    try:
        raw_releases = fetcher(config) if fetcher is not None else fetch_github_releases(config)
        release = select_latest_release(
            raw_releases,
            include_prereleases=config.app_update_include_prereleases,
        )
        if release is None:
            raise AppUpdateError("No eligible GitHub release with install bundle assets was found")
        available = is_newer_version(release.version, current_version)
        status = release_status_payload(
            release,
            current_version=current_version,
            status="update_available" if available else "up_to_date",
            message=(
                f"Update {release.tag} is available."
                if available
                else f"Already up to date at {current_version}."
            ),
        )
        write_update_status(config, status)
    except Exception as exc:
        status = {
            "status": "failed",
            "message": "Update check failed.",
            "last_error": str(exc),
            "checked_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
        write_update_status(config, status)
    return update_status(config, current_version=current_version)


def check_or_request_auto(
    config: BotConfig,
    *,
    current_version: str = APP_VERSION,
    fetcher: Callable[[BotConfig], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    if config.app_update_mode not in {"check_only", "auto_install"}:
        return update_status(config, current_version=current_version)
    status = check_for_updates(config, current_version=current_version, fetcher=fetcher)
    if config.app_update_mode == "auto_install" and status.get("available") and not status.get("pending"):
        try:
            request_update(config, source="auto", current_version=current_version)
        except Exception as exc:
            merged = dict(status)
            merged.update(
                status="failed",
                message="Unable to queue automatic update.",
                last_error=str(exc),
                updated_at=utc_now_iso(),
            )
            write_update_status(config, merged)
    return update_status(config, current_version=current_version)


def request_update(
    config: BotConfig,
    *,
    tag: str | None = None,
    source: str = "manual",
    current_version: str = APP_VERSION,
) -> dict[str, Any]:
    if config.app_update_mode == "disabled":
        raise ConfigError("App updater is disabled")
    if config.app_update_mode == "check_only":
        raise ConfigError("App updater is in check-only mode")

    status = update_status(config, current_version=current_version)
    if not status.get("latest_tag") or (tag and tag != status.get("latest_tag")):
        status = check_for_updates(config, current_version=current_version)
    if tag and tag != status.get("latest_tag"):
        raise ConfigError(f"Release {tag} is not the latest checked update")
    if not status.get("available"):
        raise ConfigError("No newer checked release is available to install")

    request = {
        "tag": status["latest_tag"],
        "version": status["latest_version"],
        "release_url": status.get("release_url") or status.get("latest_url") or "",
        "archive_url": status.get("archive_url") or "",
        "checksum_url": status.get("checksum_url") or "",
        "source": source,
        "requested_at": utc_now_iso(),
    }
    if not request["archive_url"] or not request["checksum_url"]:
        raise ConfigError("Checked release is missing release asset URLs")
    _atomic_write_json(request_path(config), request)
    merged = dict(status)
    merged.update(
        status="requested",
        message=f"Update {request['tag']} requested; installer will apply it when idle.",
        updated_at=utc_now_iso(),
    )
    write_update_status(config, merged)
    return update_status(config, current_version=current_version)


def apply_requested_update(
    config: BotConfig,
    *,
    install_dir: Path,
    app_dir: Path,
    venv_dir: Path,
) -> dict[str, Any]:
    request = _read_json_file(request_path(config))
    if not isinstance(request, dict):
        return update_status(config)

    tag = _required_str(request, "tag")
    version = _required_str(request, "version")
    archive_url = _required_str(request, "archive_url")
    checksum_url = _required_str(request, "checksum_url")
    backup_dir = install_dir / APP_UPDATE_BACKUP_DIRNAME / f"app-before-{safe_tag(tag)}-{int(time.time())}"

    status = dict(update_status(config))
    status.update(
        status="installing",
        message=f"Installing {tag}.",
        updated_at=utc_now_iso(),
        last_error="",
    )
    write_update_status(config, status)

    try:
        with tempfile.TemporaryDirectory(prefix="onlysavemevods-app-update-") as tmp:
            temp_dir = Path(tmp)
            archive = temp_dir / f"ONLYSAVEmeVODS-{tag}.tar.gz"
            checksum = temp_dir / f"ONLYSAVEmeVODS-{tag}.tar.gz.sha256"
            download_file(archive_url, archive, config=config)
            download_file(checksum_url, checksum, config=config)
            verify_checksum(archive, checksum)
            bundle_root = extract_and_validate_bundle(archive, temp_dir, tag)

            if backup_dir.exists():
                raise AppUpdateError(f"Backup directory already exists: {backup_dir}")
            backup_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(app_dir, backup_dir, symlinks=True)
            replace_app_dir(bundle_root, app_dir)
            try:
                repair_install(config, app_dir=app_dir, venv_dir=venv_dir)
            except Exception:
                restore_app_dir(backup_dir, app_dir)
                try:
                    repair_install(config, app_dir=app_dir, venv_dir=venv_dir)
                except Exception:
                    pass
                raise

        request_path(config).unlink(missing_ok=True)
        status = dict(update_status(config, current_version=version))
        status.update(
            status="installed",
            message=f"Installed {tag}.",
            last_installed_tag=tag,
            last_installed_version=version,
            installed_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            pending=False,
            pending_tag="",
            pending_version="",
        )
        write_update_status(config, status)
    except Exception as exc:
        request_path(config).unlink(missing_ok=True)
        status = dict(update_status(config))
        status.update(
            status="failed",
            message=f"Failed to install {tag}.",
            last_error=str(exc),
            updated_at=utc_now_iso(),
        )
        write_update_status(config, status)
        raise
    return update_status(config, current_version=version)


def fetch_github_releases(config: BotConfig) -> list[dict[str, Any]]:
    url = f"{GITHUB_API_ROOT}/repos/{config.app_update_repository}/releases?per_page=20"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": UPDATE_USER_AGENT,
    }
    token = os.environ.get(config.app_update_github_token_env or "")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise AppUpdateError(f"Unable to fetch GitHub releases: {exc}") from exc
    if not isinstance(payload, list):
        raise AppUpdateError("GitHub releases response was not a list")
    return payload


def select_latest_release(
    raw_releases: list[dict[str, Any]],
    *,
    include_prereleases: bool,
) -> GitHubRelease | None:
    for raw in raw_releases:
        release = parse_release(raw)
        if release is None:
            continue
        if release.draft:
            continue
        if release.prerelease and not include_prereleases:
            continue
        return release
    return None


def parse_release(raw: dict[str, Any]) -> GitHubRelease | None:
    tag = str(raw.get("tag_name") or "").strip()
    if not tag:
        return None
    assets = raw.get("assets")
    if not isinstance(assets, list):
        return None
    expected_archive = f"ONLYSAVEmeVODS-{tag}.tar.gz"
    expected_checksum = f"{expected_archive}.sha256"
    archive = asset_named(assets, expected_archive)
    checksum = asset_named(assets, expected_checksum)
    if archive is None or checksum is None:
        return None
    return GitHubRelease(
        tag=tag,
        version=version_from_tag(tag),
        name=str(raw.get("name") or tag),
        html_url=str(raw.get("html_url") or ""),
        published_at=str(raw.get("published_at") or ""),
        prerelease=bool(raw.get("prerelease")),
        draft=bool(raw.get("draft")),
        tarball=archive,
        checksum=checksum,
    )


def asset_named(assets: list[Any], name: str) -> ReleaseAsset | None:
    for raw_asset in assets:
        if not isinstance(raw_asset, dict):
            continue
        if str(raw_asset.get("name") or "") != name:
            continue
        download_url = str(raw_asset.get("browser_download_url") or "")
        if not download_url:
            continue
        size = raw_asset.get("size", 0)
        try:
            parsed_size = int(size)
        except (TypeError, ValueError):
            parsed_size = 0
        return ReleaseAsset(name=name, download_url=download_url, size=parsed_size)
    return None


def release_status_payload(
    release: GitHubRelease,
    *,
    current_version: str,
    status: str,
    message: str,
) -> dict[str, Any]:
    now = utc_now_iso()
    return {
        "status": status,
        "message": message,
        "current_version": current_version,
        "latest_tag": release.tag,
        "latest_version": release.version,
        "latest_name": release.name,
        "latest_url": release.html_url,
        "release_url": release.html_url,
        "published_at": release.published_at,
        "prerelease": release.prerelease,
        "archive_name": release.tarball.name,
        "archive_url": release.tarball.download_url,
        "archive_size": release.tarball.size,
        "checksum_name": release.checksum.name,
        "checksum_url": release.checksum.download_url,
        "checksum_size": release.checksum.size,
        "checked_at": now,
        "updated_at": now,
        "last_error": "",
    }


def download_file(url: str, target: Path, *, config: BotConfig) -> None:
    headers = {"User-Agent": UPDATE_USER_AGENT}
    token = os.environ.get(config.app_update_github_token_env or "")
    if token and "github.com" in url:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=60) as response, target.open("wb") as handle:
            shutil.copyfileobj(response, handle)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise AppUpdateError(f"Unable to download {url}: {exc}") from exc


def verify_checksum(archive: Path, checksum_file: Path) -> None:
    expected = parse_sha256_file(checksum_file)
    digest = hashlib.sha256()
    with archive.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual.casefold() != expected.casefold():
        raise AppUpdateError("Release tarball checksum verification failed")


def parse_sha256_file(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    match = re.search(r"\b([A-Fa-f0-9]{64})\b", text)
    if not match:
        raise AppUpdateError("Release checksum file does not contain a SHA256 digest")
    return match.group(1)


def extract_and_validate_bundle(archive: Path, temp_dir: Path, tag: str) -> Path:
    expected_root = f"ONLYSAVEmeVODS-{tag}"
    extract_dir = temp_dir / "extract"
    extract_dir.mkdir()
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            member_path = Path(member.name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise AppUpdateError("Release tarball contains unsafe paths")
        tar.extractall(extract_dir)
    root = extract_dir / expected_root
    required = [
        root / "pyproject.toml",
        root / "README.md",
        root / "config.example.toml",
        root / "src" / "onlysavemevods",
        root / "scripts" / "install-systemd.sh",
    ]
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    if missing:
        raise AppUpdateError("Release tarball is missing required paths: " + ", ".join(missing))
    return root


def replace_app_dir(bundle_root: Path, app_dir: Path) -> None:
    if app_dir.exists():
        shutil.rmtree(app_dir)
    app_dir.mkdir(parents=True)
    for item in bundle_root.iterdir():
        target = app_dir / item.name
        if item.is_dir():
            shutil.copytree(item, target, symlinks=True)
        else:
            shutil.copy2(item, target)
    chmod_tree_readable(app_dir)


def restore_app_dir(backup_dir: Path, app_dir: Path) -> None:
    if app_dir.exists():
        shutil.rmtree(app_dir)
    shutil.copytree(backup_dir, app_dir, symlinks=True)
    chmod_tree_readable(app_dir)


def repair_install(config: BotConfig, *, app_dir: Path, venv_dir: Path) -> None:
    python = venv_dir / "bin" / "python"
    if not python.exists():
        raise AppUpdateError(f"Python venv not found: {python}")
    run_command([str(python), "-m", "pip", "install", "--upgrade", "pip", "setuptools<82", "wheel"])
    run_command([str(python), "-m", "pip", "install", "--upgrade", "--editable", str(app_dir)])
    run_command(
        [
            str(python),
            "-m",
            "onlysavemevods",
            "update-config",
            "--config",
            str(config.config_path or "config.toml"),
            "--defaults",
            str(app_dir / "config.example.toml"),
        ]
    )
    run_command([str(python), "-m", "pip", "check"])


def run_command(command: list[str]) -> None:
    completed = subprocess.run(command, text=True, capture_output=True)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise AppUpdateError(f"Command failed: {command_for_log(command)} {detail}")


def command_for_log(command: list[str]) -> str:
    return " ".join(command)


def chmod_tree_readable(root: Path) -> None:
    for path in [root, *root.rglob("*")]:
        try:
            mode = path.stat().st_mode
            if path.is_dir():
                path.chmod((mode | 0o755) & ~0o022)
            else:
                path.chmod((mode | 0o644) & ~0o022)
        except OSError:
            pass


def is_newer_version(candidate: str, current: str) -> bool:
    candidate = version_from_tag(candidate)
    current = version_from_tag(current)
    if Version is not None:
        try:
            return Version(candidate) > Version(current)
        except InvalidVersion:
            pass
    return fallback_version_key(candidate) > fallback_version_key(current)


def fallback_version_key(value: str) -> tuple[tuple[int, ...], str]:
    normalized = version_from_tag(value)
    numbers = tuple(int(part) for part in re.findall(r"\d+", normalized))
    suffix = re.sub(r"[0-9.]+", "", normalized).casefold()
    return numbers, suffix


def version_from_tag(tag: str) -> str:
    value = str(tag or "").strip()
    if value[:1].casefold() == "v":
        value = value[1:]
    return value or "0"


def safe_tag(tag: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", tag).strip(".-") or "release"


def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_update_status(config: BotConfig, payload: dict[str, Any]) -> None:
    _atomic_write_json(status_path(config), payload)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _read_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise AppUpdateError(f"Update request is missing {key}")
    return value


def status_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(json.dumps(update_status(config, current_version=args.current_version), indent=2, sort_keys=True))
    return 0


def check_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(json.dumps(check_for_updates(config, current_version=args.current_version), indent=2, sort_keys=True))
    return 0


def request_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(
        json.dumps(
            request_update(
                config,
                tag=args.tag,
                source=args.source,
                current_version=args.current_version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def check_auto_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(json.dumps(check_or_request_auto(config, current_version=args.current_version), indent=2, sort_keys=True))
    return 0


def has_request_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    return 0 if request_path(config).is_file() else 1


def apply_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(
        json.dumps(
            apply_requested_update(
                config,
                install_dir=Path(args.install_dir),
                app_dir=Path(args.app_dir),
                venv_dir=Path(args.venv_dir),
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m onlysavemevods.app_update",
        description="ONLYSAVEmeVODS GitHub Release updater helpers.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--config", required=True)
        subparser.add_argument("--current-version", default=APP_VERSION)

    status = subparsers.add_parser("status")
    add_common(status)

    check = subparsers.add_parser("check")
    add_common(check)

    request = subparsers.add_parser("request")
    add_common(request)
    request.add_argument("--tag", default="")
    request.add_argument("--source", default="manual")

    check_auto = subparsers.add_parser("check-auto")
    add_common(check_auto)

    has_request = subparsers.add_parser("has-request")
    has_request.add_argument("--config", required=True)

    apply = subparsers.add_parser("apply")
    apply.add_argument("--config", required=True)
    apply.add_argument("--install-dir", required=True)
    apply.add_argument("--app-dir", required=True)
    apply.add_argument("--venv-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            return status_command(args)
        if args.command == "check":
            return check_command(args)
        if args.command == "request":
            return request_command(args)
        if args.command == "check-auto":
            return check_auto_command(args)
        if args.command == "has-request":
            return has_request_command(args)
        if args.command == "apply":
            return apply_command(args)
    except (AppUpdateError, ConfigError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
