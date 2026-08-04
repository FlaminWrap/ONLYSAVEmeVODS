from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
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
import tomllib

from . import __version__ as APP_VERSION
from .config import (
    DEFAULT_APP_UPDATE_GITHUB_TOKEN_ENV,
    DEFAULT_APP_UPDATE_REPOSITORY,
    BotConfig,
    ConfigError,
    load_config,
)

try:
    from packaging.version import InvalidVersion, Version
except Exception:  # pragma: no cover - fallback is covered without packaging.
    InvalidVersion = ValueError  # type: ignore[assignment]
    Version = None  # type: ignore[assignment]


APP_UPDATE_REQUEST_FILENAME = "app-update-request.json"
APP_UPDATE_STATUS_FILENAME = "app-update-status.json"
APP_UPDATE_BACKUP_DIRNAME = "app-update-backups"
APP_UPDATE_STATE_DIR_ENV = "ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR"
GITHUB_API_ROOT = "https://api.github.com"
UPDATE_USER_AGENT = "ONLYSAVEmeVODS updater"
TRUSTED_UPDATE_MODES = frozenset({"disabled", "check_only", "manual", "auto_install"})
UPDATE_MODE_CAPABILITY = {
    "disabled": 0,
    "check_only": 1,
    "manual": 2,
    "auto_install": 3,
}
INSTALLING_UPDATE_MODES = frozenset({"manual", "auto_install"})
REQUEST_SOURCES = frozenset({"manual", "auto"})
GITHUB_REPOSITORY_RE = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,99})/[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,99})"
)
RELEASE_TAG_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._+-]{0,127})")
FALLBACK_VERSION_RE = re.compile(
    r"""
    ^
    (?P<release>[0-9]+(?:\.[0-9]+)*)
    (?:(?P<pre_label>a|b|rc)(?P<pre_number>[0-9]+))?
    (?:\.post(?P<post_number>[0-9]+))?
    (?:\.dev(?P<dev_number>[0-9]+))?
    $
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)
CHECK_STATUSES = {"checked", "update_available", "up_to_date", "failed", "disabled"}
EXECUTABLE_SCRIPT_NAMES = (
    "app-update.sh",
    "install-almalinux.sh",
    "install-debian.sh",
    "install-systemd.sh",
    "install-ubuntu.sh",
    "uninstall-systemd.sh",
    "update-python-deps.sh",
)


class AppUpdateError(RuntimeError):
    """Raised when the app updater cannot complete safely."""


class TransientAppUpdateError(AppUpdateError):
    """Raised when a valid request should remain pending for a later retry."""


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


@dataclass(frozen=True, slots=True)
class TrustedUpdatePolicy:
    """Root-owned policy inputs which must never come from web-managed config."""

    repository: str = DEFAULT_APP_UPDATE_REPOSITORY
    mode: str = "manual"
    include_prereleases: bool = False
    token_env: str = DEFAULT_APP_UPDATE_GITHUB_TOKEN_ENV

    def validated(self) -> TrustedUpdatePolicy:
        repository = self.repository.strip()
        mode = self.mode.strip()
        token_env = self.token_env.strip()
        if GITHUB_REPOSITORY_RE.fullmatch(repository) is None:
            raise AppUpdateError("Trusted update repository must be an owner/repository name")
        if mode not in TRUSTED_UPDATE_MODES:
            raise AppUpdateError(f"Invalid trusted update mode: {mode}")
        if token_env and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token_env) is None:
            raise AppUpdateError("Trusted GitHub token environment name is invalid")
        return TrustedUpdatePolicy(
            repository=repository,
            mode=mode,
            include_prereleases=bool(self.include_prereleases),
            token_env=token_env,
        )

    def restricted_by(self, config: BotConfig) -> TrustedUpdatePolicy:
        desired_mode = str(config.app_update_mode).strip()
        if desired_mode not in TRUSTED_UPDATE_MODES:
            raise AppUpdateError(f"Invalid configured update mode: {desired_mode}")
        effective_mode = min(
            (self.mode, desired_mode),
            key=UPDATE_MODE_CAPABILITY.__getitem__,
        )
        return TrustedUpdatePolicy(
            repository=self.repository,
            mode=effective_mode,
            include_prereleases=(
                self.include_prereleases and bool(config.app_update_include_prereleases)
            ),
            token_env=self.token_env,
        )


def app_update_state_dir(
    config: BotConfig,
    *,
    state_dir: Path | None = None,
) -> Path:
    """Return the shared updater mailbox without letting config redirect root.

    Installed services receive the mailbox path from their root-owned systemd
    units. Source and other non-systemd runs fall back to the configured state
    directory. Privileged updater commands pass ``state_dir`` explicitly, so
    the root-owned launcher takes precedence over the generic process fallback.
    """

    if state_dir is not None:
        return state_dir
    installed_state_dir = os.environ.get(APP_UPDATE_STATE_DIR_ENV)
    if installed_state_dir:
        path = Path(installed_state_dir)
        if not path.is_absolute():
            raise ConfigError(f"{APP_UPDATE_STATE_DIR_ENV} must be an absolute path")
        return path
    return config.state_dir


def request_path(config: BotConfig, *, state_dir: Path | None = None) -> Path:
    return app_update_state_dir(config, state_dir=state_dir) / APP_UPDATE_REQUEST_FILENAME


def status_path(config: BotConfig, *, state_dir: Path | None = None) -> Path:
    return app_update_state_dir(config, state_dir=state_dir) / APP_UPDATE_STATUS_FILENAME


def update_status(
    config: BotConfig,
    *,
    current_version: str = APP_VERSION,
    state_dir: Path | None = None,
) -> dict[str, Any]:
    trusted_state_dir = app_update_state_dir(config, state_dir=state_dir)
    status = _read_json_file(trusted_state_dir / APP_UPDATE_STATUS_FILENAME)
    if not isinstance(status, dict):
        status = {}
    request = _read_json_file(trusted_state_dir / APP_UPDATE_REQUEST_FILENAME)
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
        "source": source,
        "requested_at": utc_now_iso(),
    }
    _atomic_write_json(request_path(config), request)
    merged = dict(status)
    merged.update(
        status="requested",
        message=f"Update {request['tag']} requested; installer will apply it when idle.",
        updated_at=utc_now_iso(),
    )
    write_update_status(config, merged)
    return update_status(config, current_version=current_version)


def check_or_request_trusted_update(
    config: BotConfig,
    *,
    trusted_policy: TrustedUpdatePolicy | None = None,
    state_dir: Path | None = None,
    current_version: str = APP_VERSION,
    release_fetcher: Callable[[str, str], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Check the root-pinned source while allowing web policy only to narrow it."""

    policy = (trusted_policy or TrustedUpdatePolicy()).validated().restricted_by(config)
    trusted_state_dir = app_update_state_dir(config, state_dir=state_dir)
    request_file = trusted_state_dir / APP_UPDATE_REQUEST_FILENAME
    if policy.mode == "disabled":
        status = {
            "status": "disabled",
            "message": "Privileged app updater policy is disabled.",
            "current_version": current_version,
            "checked_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
        write_update_status(config, status, state_dir=trusted_state_dir)
        return update_status(config, current_version=current_version, state_dir=trusted_state_dir)

    try:
        raw_releases = (
            release_fetcher(policy.repository, policy.token_env)
            if release_fetcher is not None
            else fetch_github_releases_from_repository(
                policy.repository,
                token_env=policy.token_env,
            )
        )
        release = select_latest_release(
            raw_releases,
            include_prereleases=policy.include_prereleases,
        )
        if release is None:
            raise AppUpdateError(
                "No eligible release with install bundle assets was found in the trusted repository"
            )
        validate_trusted_release(release, repository=policy.repository)
        available = is_newer_version(release.version, current_version)
        status = release_status_payload(
            release,
            current_version=current_version,
            status="update_available" if available else "up_to_date",
            message=(
                f"Update {release.tag} is available from the trusted repository."
                if available
                else f"Already up to date at {current_version}."
            ),
        )
        write_update_status(config, status, state_dir=trusted_state_dir)
        if policy.mode == "auto_install" and available and not request_file.exists():
            _atomic_write_json(
                request_file,
                {
                    "tag": release.tag,
                    "source": "auto",
                    "requested_at": utc_now_iso(),
                },
            )
    except Exception as exc:
        status = {
            "status": "failed",
            "message": "Trusted update check failed.",
            "last_error": str(exc),
            "current_version": current_version,
            "checked_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
        write_update_status(config, status, state_dir=trusted_state_dir)
    return update_status(config, current_version=current_version, state_dir=trusted_state_dir)


def apply_requested_update(
    config: BotConfig,
    *,
    install_dir: Path,
    app_dir: Path,
    venv_dir: Path,
    trusted_policy: TrustedUpdatePolicy | None = None,
    state_dir: Path | None = None,
    current_version: str = APP_VERSION,
    release_fetcher: Callable[[str, str], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    policy = (trusted_policy or TrustedUpdatePolicy()).validated().restricted_by(config)
    trusted_state_dir = app_update_state_dir(config, state_dir=state_dir)
    request_file = trusted_state_dir / APP_UPDATE_REQUEST_FILENAME
    if not request_file.exists():
        return update_status(config, current_version=current_version, state_dir=trusted_state_dir)

    tag = "requested release"
    try:
        request = _load_update_request(request_file)
        tag = _required_str(request, "tag")
        source = _required_str(request, "source")
        validate_requested_update(tag=tag, source=source, policy=policy)

        raw_releases = (
            release_fetcher(policy.repository, policy.token_env)
            if release_fetcher is not None
            else fetch_github_releases_from_repository(
                policy.repository,
                token_env=policy.token_env,
            )
        )
        release = select_latest_release(
            raw_releases,
            include_prereleases=policy.include_prereleases,
        )
        if release is None:
            raise AppUpdateError(
                "No eligible release with install bundle assets was found in the trusted repository"
            )
        validate_trusted_release(release, repository=policy.repository)
        if tag != release.tag:
            raise AppUpdateError(
                f"Requested release {tag} is not the latest trusted release ({release.tag})"
            )
        if not is_newer_version(release.version, current_version):
            raise AppUpdateError(
                f"Refusing to install {release.tag} over current version {current_version}"
            )

        status = release_status_payload(
            release,
            current_version=current_version,
            status="installing",
            message=f"Installing {release.tag} from trusted repository {policy.repository}.",
        )
        write_update_status(config, status, state_dir=trusted_state_dir)

        with tempfile.TemporaryDirectory(prefix="onlysavemevods-app-update-") as tmp:
            temp_dir = Path(tmp)
            archive = temp_dir / release.tarball.name
            checksum = temp_dir / release.checksum.name
            download_file(
                release.tarball.download_url,
                archive,
                token_env=policy.token_env,
            )
            download_file(
                release.checksum.download_url,
                checksum,
                token_env=policy.token_env,
            )
            verify_checksum(archive, checksum)
            bundle_root = extract_and_validate_bundle(
                archive,
                temp_dir,
                release.tag,
                expected_version=release.version,
            )

            staged_app = stage_app_dir(
                bundle_root,
                app_dir=app_dir,
                expected_version=release.version,
            )
            try:
                backup_dir = backup_app_dir(
                    app_dir,
                    install_dir=install_dir,
                    tag=release.tag,
                )
                displaced_app: Path | None = None
                try:
                    displaced_app = activate_staged_app(staged_app, app_dir)
                    repair_install(config, app_dir=app_dir, venv_dir=venv_dir)
                except BaseException:
                    if displaced_app is not None or not app_dir.exists():
                        rollback_app_dir(
                            app_dir=app_dir,
                            displaced_app=displaced_app,
                            backup_dir=backup_dir,
                        )
                    try:
                        repair_install(config, app_dir=app_dir, venv_dir=venv_dir)
                    except BaseException:
                        pass
                    raise
                else:
                    if displaced_app is not None:
                        shutil.rmtree(displaced_app, ignore_errors=True)
            finally:
                if staged_app.exists():
                    shutil.rmtree(staged_app, ignore_errors=True)

        request_file.unlink(missing_ok=True)
        status = release_status_payload(
            release,
            current_version=release.version,
            status="installed",
            message=f"Installed {release.tag}.",
        )
        status.update(
            last_installed_tag=release.tag,
            last_installed_version=release.version,
            installed_at=utc_now_iso(),
            updated_at=utc_now_iso(),
        )
        write_update_status(config, status, state_dir=trusted_state_dir)
    except BaseException as exc:
        if not isinstance(exc, TransientAppUpdateError):
            request_file.unlink(missing_ok=True)
        status = {
            "status": "failed",
            "message": f"Failed to install {tag}.",
            "last_error": str(exc),
            "current_version": current_version,
            "updated_at": utc_now_iso(),
        }
        try:
            write_update_status(config, status, state_dir=trusted_state_dir)
        except OSError:
            pass
        raise
    return update_status(
        config,
        current_version=release.version,
        state_dir=trusted_state_dir,
    )


def fetch_github_releases(config: BotConfig) -> list[dict[str, Any]]:
    return fetch_github_releases_from_repository(
        config.app_update_repository,
        token_env=config.app_update_github_token_env,
    )


def fetch_github_releases_from_repository(
    repository: str,
    *,
    token_env: str = DEFAULT_APP_UPDATE_GITHUB_TOKEN_ENV,
) -> list[dict[str, Any]]:
    if GITHUB_REPOSITORY_RE.fullmatch(repository) is None:
        raise AppUpdateError("GitHub repository must be an owner/repository name")
    url = f"{GITHUB_API_ROOT}/repos/{repository}/releases?per_page=100"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": UPDATE_USER_AGENT,
    }
    token = os.environ.get(token_env or "")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise TransientAppUpdateError(f"Unable to fetch GitHub releases: {exc}") from exc
    if not isinstance(payload, list):
        raise AppUpdateError("GitHub releases response was not a list")
    return payload


def validate_requested_update(
    *,
    tag: str,
    source: str,
    policy: TrustedUpdatePolicy,
) -> None:
    if policy.mode not in INSTALLING_UPDATE_MODES:
        raise AppUpdateError(
            f"Privileged app updater policy does not allow installs in {policy.mode} mode"
        )
    if RELEASE_TAG_RE.fullmatch(tag) is None:
        raise AppUpdateError("Update request contains an invalid release tag")
    if source not in REQUEST_SOURCES:
        raise AppUpdateError("Update request contains an invalid source")
    if source == "auto" and policy.mode != "auto_install":
        raise AppUpdateError("Automatic update request is not allowed by privileged policy")


def validate_trusted_release(release: GitHubRelease, *, repository: str) -> None:
    if RELEASE_TAG_RE.fullmatch(release.tag) is None:
        raise AppUpdateError("Trusted repository returned an invalid release tag")
    validate_release_asset_url(
        release.tarball.download_url,
        repository=repository,
        tag=release.tag,
        asset_name=release.tarball.name,
    )
    validate_release_asset_url(
        release.checksum.download_url,
        repository=repository,
        tag=release.tag,
        asset_name=release.checksum.name,
    )


def validate_release_asset_url(
    url: str,
    *,
    repository: str,
    tag: str,
    asset_name: str,
) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or (parsed.hostname or "").casefold() != "github.com":
        raise AppUpdateError("Trusted release asset URL is not hosted on github.com over HTTPS")
    if parsed.username or parsed.password:
        raise AppUpdateError("Trusted release asset URL contains credentials")
    decoded_path = unquote(parsed.path)
    expected_path = f"/{repository}/releases/download/{tag}/{asset_name}"
    if decoded_path.casefold() != expected_path.casefold():
        raise AppUpdateError("Trusted release asset URL does not match the pinned repository")


def select_latest_release(
    raw_releases: list[dict[str, Any]],
    *,
    include_prereleases: bool,
) -> GitHubRelease | None:
    latest: GitHubRelease | None = None
    for raw in raw_releases:
        release = parse_release(raw)
        if release is None:
            continue
        if release.draft:
            continue
        if release.prerelease and not include_prereleases:
            continue
        if latest is None or is_newer_version(release.version, latest.version):
            latest = release
    return latest


def parse_release(raw: dict[str, Any]) -> GitHubRelease | None:
    tag = str(raw.get("tag_name") or "").strip()
    if RELEASE_TAG_RE.fullmatch(tag) is None:
        return None
    version = version_from_tag(tag)
    if not is_valid_release_version(version):
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
        version=version,
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


def download_file(
    url: str,
    target: Path,
    *,
    config: BotConfig | None = None,
    token_env: str | None = None,
) -> None:
    headers = {"User-Agent": UPDATE_USER_AGENT}
    if token_env is None and config is not None:
        token_env = config.app_update_github_token_env
    token = os.environ.get(token_env or "")
    hostname = (urlsplit(url).hostname or "").casefold()
    if token and (hostname == "github.com" or hostname.endswith(".githubusercontent.com")):
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=60) as response, target.open("wb") as handle:
            shutil.copyfileobj(response, handle)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise TransientAppUpdateError(f"Unable to download {url}: {exc}") from exc


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


def extract_and_validate_bundle(
    archive: Path,
    temp_dir: Path,
    tag: str,
    *,
    expected_version: str | None = None,
) -> Path:
    if RELEASE_TAG_RE.fullmatch(tag) is None:
        raise AppUpdateError("Release tag is unsafe for bundle extraction")
    expected_root = f"ONLYSAVEmeVODS-{tag}"
    extract_dir = temp_dir / "extract"
    extract_dir.mkdir()
    try:
        with tarfile.open(archive, "r:gz") as tar:
            validated_members: list[tuple[tarfile.TarInfo, Path]] = []
            seen_destinations: set[Path] = set()
            for member in tar.getmembers():
                member_path = PurePosixPath(member.name)
                if (
                    not member.name
                    or "\\" in member.name
                    or member_path.is_absolute()
                    or ".." in member_path.parts
                    or not member_path.parts
                    or member_path.parts[0] != expected_root
                ):
                    raise AppUpdateError("Release tarball contains unsafe paths")
                if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                    raise AppUpdateError("Release tarball contains links or special files")
                if not (member.isdir() or member.isfile()):
                    raise AppUpdateError("Release tarball contains an unsupported member type")
                destination = (extract_dir / Path(*member_path.parts)).resolve()
                if destination != extract_dir.resolve() and extract_dir.resolve() not in destination.parents:
                    raise AppUpdateError("Release tarball member escapes the extraction directory")
                if destination in seen_destinations:
                    raise AppUpdateError("Release tarball contains duplicate paths")
                seen_destinations.add(destination)
                validated_members.append((member, destination))
            for member, destination in validated_members:
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                    destination.chmod(0o755)
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = tar.extractfile(member)
                if source is None:
                    raise AppUpdateError("Release tarball contains an unreadable file")
                with source, destination.open("xb") as target:
                    shutil.copyfileobj(source, target)
                destination.chmod(0o644)
    except AppUpdateError:
        raise
    except (tarfile.TarError, OSError) as exc:
        raise AppUpdateError(f"Unable to extract release tarball safely: {exc}") from exc
    root = extract_dir / expected_root
    validate_bundle_layout(root, expected_version=expected_version)
    return root


def validate_bundle_layout(root: Path, *, expected_version: str | None = None) -> None:
    required = [
        root / "pyproject.toml",
        root / "README.md",
        root / "LICENSE",
        root / "config.example.toml",
        root / "src" / "onlysavemevods",
        root / "scripts" / "install-systemd.sh",
    ]
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    if missing:
        raise AppUpdateError("Release tarball is missing required paths: " + ", ".join(missing))
    if root.is_symlink() or any(path.is_symlink() for path in root.rglob("*")):
        raise AppUpdateError("Release bundle contains symbolic links")
    if expected_version is not None:
        try:
            project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
            bundled_version = str(project["project"]["version"])
        except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
            raise AppUpdateError(f"Release bundle has no readable project version: {exc}") from exc
        if not versions_equal(bundled_version, expected_version):
            raise AppUpdateError(
                f"Release bundle version {bundled_version} does not match tag version {expected_version}"
            )


def stage_app_dir(bundle_root: Path, *, app_dir: Path, expected_version: str) -> Path:
    app_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(
        tempfile.mkdtemp(
            prefix=f".{app_dir.name}.update-stage-",
            dir=app_dir.parent,
        )
    )
    stage.rmdir()
    try:
        shutil.copytree(bundle_root, stage, symlinks=False)
        chmod_tree_readable(stage)
        chmod_packaged_scripts_executable(stage)
        validate_bundle_layout(stage, expected_version=expected_version)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    return stage


def backup_app_dir(app_dir: Path, *, install_dir: Path, tag: str) -> Path:
    if not app_dir.is_dir() or app_dir.is_symlink():
        raise AppUpdateError(f"Application directory is missing or unsafe: {app_dir}")
    backup_root = install_dir / APP_UPDATE_BACKUP_DIRNAME
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_dir = backup_root / f"app-before-{safe_tag(tag)}-{time.time_ns()}"
    try:
        shutil.copytree(app_dir, backup_dir, symlinks=True)
    except Exception:
        shutil.rmtree(backup_dir, ignore_errors=True)
        raise
    return backup_dir


def activate_staged_app(staged_app: Path, app_dir: Path) -> Path:
    if not staged_app.is_dir() or staged_app.is_symlink():
        raise AppUpdateError(f"Staged application directory is missing or unsafe: {staged_app}")
    if not app_dir.is_dir() or app_dir.is_symlink():
        raise AppUpdateError(f"Application directory is missing or unsafe: {app_dir}")
    displaced_app = app_dir.parent / f".{app_dir.name}.update-old-{time.time_ns()}"
    try:
        os.replace(app_dir, displaced_app)
        try:
            os.replace(staged_app, app_dir)
        except Exception as exc:
            try:
                os.replace(displaced_app, app_dir)
            except Exception as rollback_exc:
                raise AppUpdateError(
                    f"Unable to activate staged app and immediate rollback failed: {rollback_exc}"
                ) from exc
            raise
    except OSError as exc:
        raise AppUpdateError(f"Unable to atomically activate staged app: {exc}") from exc
    return displaced_app


def rollback_app_dir(
    *,
    app_dir: Path,
    displaced_app: Path | None,
    backup_dir: Path,
) -> None:
    failed_app = app_dir.parent / f".{app_dir.name}.update-failed-{time.time_ns()}"
    if app_dir.exists():
        os.replace(app_dir, failed_app)
    try:
        if displaced_app is not None and displaced_app.exists():
            os.replace(displaced_app, app_dir)
        else:
            restore_stage = stage_app_dir(
                backup_dir,
                app_dir=app_dir,
                expected_version=bundle_version(backup_dir),
            )
            os.replace(restore_stage, app_dir)
    except Exception as exc:
        if not app_dir.exists() and failed_app.exists():
            os.replace(failed_app, app_dir)
        raise AppUpdateError(f"Unable to restore application backup: {exc}") from exc
    finally:
        if failed_app.exists():
            shutil.rmtree(failed_app, ignore_errors=True)


def bundle_version(root: Path) -> str:
    try:
        project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        return str(project["project"]["version"])
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise AppUpdateError(f"Application backup has no readable project version: {exc}") from exc


def replace_app_dir(bundle_root: Path, app_dir: Path) -> None:
    staged_app = stage_app_dir(
        bundle_root,
        app_dir=app_dir,
        expected_version=bundle_version(bundle_root),
    )
    displaced_app = activate_staged_app(staged_app, app_dir)
    shutil.rmtree(displaced_app)


def restore_app_dir(backup_dir: Path, app_dir: Path) -> None:
    staged_app = stage_app_dir(
        backup_dir,
        app_dir=app_dir,
        expected_version=bundle_version(backup_dir),
    )
    displaced_app = activate_staged_app(staged_app, app_dir)
    shutil.rmtree(displaced_app)


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


def chmod_packaged_scripts_executable(app_dir: Path) -> None:
    scripts_dir = app_dir / "scripts"
    for name in EXECUTABLE_SCRIPT_NAMES:
        path = scripts_dir / name
        try:
            if path.is_file():
                path.chmod((path.stat().st_mode | 0o755) & ~0o022)
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
    candidate_key = fallback_version_key(candidate)
    current_key = fallback_version_key(current)
    return (
        candidate_key is not None
        and current_key is not None
        and candidate_key > current_key
    )


def is_valid_release_version(value: str) -> bool:
    normalized = version_from_tag(value)
    if Version is not None:
        try:
            Version(normalized)
        except InvalidVersion:
            return False
        return True
    return fallback_version_key(normalized) is not None


def versions_equal(left: str, right: str) -> bool:
    left = version_from_tag(left)
    right = version_from_tag(right)
    if Version is not None:
        try:
            return Version(left) == Version(right)
        except InvalidVersion:
            pass
    left_key = fallback_version_key(left)
    right_key = fallback_version_key(right)
    return left_key is not None and right_key is not None and left_key == right_key


def fallback_version_key(
    value: str,
) -> tuple[tuple[int, ...], int, int, int, int] | None:
    """Order the updater's supported PEP 440 subset, or fail closed.

    Release segments ignore trailing zeroes, development releases sort before
    prereleases, prereleases sort ``a`` < ``b`` < ``rc`` < final, and post
    releases sort after their base release. Unknown syntax is deliberately not
    guessed at when the optional import is unavailable.
    """

    normalized = version_from_tag(value)
    match = FALLBACK_VERSION_RE.fullmatch(normalized)
    if match is None:
        return None

    release = tuple(int(part) for part in match.group("release").split("."))
    while len(release) > 1 and release[-1] == 0:
        release = release[:-1]

    pre_label = (match.group("pre_label") or "").casefold()
    pre_number = int(match.group("pre_number") or 0)
    post_text = match.group("post_number")
    dev_text = match.group("dev_number")
    post_number = int(post_text or 0)
    dev_number = int(dev_text or 0)

    if pre_label:
        stage = {"a": 0, "b": 1, "rc": 2}[pre_label]
        if dev_text is not None:
            return release, stage, pre_number, 0, dev_number
        if post_text is not None:
            return release, stage, pre_number, 2, post_number
        return release, stage, pre_number, 1, 0

    if post_text is not None:
        # A development release of a post release precedes that post release,
        # but both remain newer than the final base release.
        return release, 4, post_number, 0 if dev_text is not None else 1, dev_number
    if dev_text is not None:
        return release, -1, dev_number, 0, 0
    return release, 3, 0, 0, 0


def version_from_tag(tag: str) -> str:
    value = str(tag or "").strip()
    if value[:1].casefold() == "v":
        value = value[1:]
    return value or "0"


def safe_tag(tag: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", tag).strip(".-") or "release"


def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_update_status(
    config: BotConfig,
    payload: dict[str, Any],
    *,
    state_dir: Path | None = None,
) -> None:
    trusted_state_dir = app_update_state_dir(config, state_dir=state_dir)
    _atomic_write_json(trusted_state_dir / APP_UPDATE_STATUS_FILENAME, payload)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        os.fchmod(handle.fileno(), 0o644)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _read_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _load_update_request(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise AppUpdateError(f"Unable to read update request: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise AppUpdateError(f"Update request is malformed JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise AppUpdateError("Update request must contain a JSON object")
    return payload


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


def check_trusted_auto_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    result = check_or_request_trusted_update(
        config,
        trusted_policy=trusted_policy_from_args(args),
        state_dir=Path(args.state_dir),
        current_version=args.current_version,
    )
    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )
    return 2 if result.get("status") == "failed" else 0


def has_request_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_dir = app_update_state_dir(
        config,
        state_dir=Path(args.state_dir) if args.state_dir else None,
    )
    return 0 if (state_dir / APP_UPDATE_REQUEST_FILENAME).is_file() else 1


def apply_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(
        json.dumps(
            apply_requested_update(
                config,
                install_dir=Path(args.install_dir),
                app_dir=Path(args.app_dir),
                venv_dir=Path(args.venv_dir),
                trusted_policy=trusted_policy_from_args(args),
                state_dir=Path(args.state_dir),
                current_version=args.current_version,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def trusted_policy_from_args(args: argparse.Namespace) -> TrustedUpdatePolicy:
    return TrustedUpdatePolicy(
        repository=args.trusted_repository,
        mode=args.trusted_mode,
        include_prereleases=parse_bool_arg(args.trusted_include_prereleases),
        token_env=args.trusted_token_env,
    ).validated()


def parse_bool_arg(value: str) -> bool:
    normalized = str(value).strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise AppUpdateError(f"Invalid boolean value: {value}")


def add_trusted_policy_args(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument("--state-dir", required=True)
    subparser.add_argument("--current-version", default=APP_VERSION)
    subparser.add_argument("--trusted-repository", default=DEFAULT_APP_UPDATE_REPOSITORY)
    subparser.add_argument("--trusted-mode", default="manual")
    subparser.add_argument("--trusted-include-prereleases", default="false")
    subparser.add_argument(
        "--trusted-token-env",
        default=DEFAULT_APP_UPDATE_GITHUB_TOKEN_ENV,
    )


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

    check_trusted_auto = subparsers.add_parser("check-trusted-auto")
    check_trusted_auto.add_argument("--config", required=True)
    add_trusted_policy_args(check_trusted_auto)

    has_request = subparsers.add_parser("has-request")
    has_request.add_argument("--config", required=True)
    has_request.add_argument("--state-dir")

    apply = subparsers.add_parser("apply")
    apply.add_argument("--config", required=True)
    apply.add_argument("--install-dir", required=True)
    apply.add_argument("--app-dir", required=True)
    apply.add_argument("--venv-dir", required=True)
    add_trusted_policy_args(apply)
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
        if args.command == "check-trusted-auto":
            return check_trusted_auto_command(args)
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
