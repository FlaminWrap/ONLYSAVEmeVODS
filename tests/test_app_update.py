from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
import hashlib
import json
import shutil
import tarfile
import tomllib
import unittest

from onlysavemevods.app_update import (
    APP_UPDATE_STATE_DIR_ENV,
    AppUpdateError,
    TransientAppUpdateError,
    TrustedUpdatePolicy,
    apply_requested_update,
    check_for_updates,
    check_or_request_auto,
    check_or_request_trusted_update,
    is_newer_version,
    extract_and_validate_bundle,
    parse_release,
    request_path,
    request_update,
    select_latest_release,
    status_path,
    update_status,
    versions_equal,
)
from onlysavemevods.config import BotConfig, ConfigError


def fake_release(
    tag: str = "v2.0.0",
    *,
    prerelease: bool = False,
    repository: str = "FlaminWrap/ONLYSAVEmeVODS",
) -> dict[str, object]:
    archive = f"ONLYSAVEmeVODS-{tag}.tar.gz"
    download_root = f"https://github.com/{repository}/releases/download/{tag}"
    return {
        "tag_name": tag,
        "name": tag,
        "html_url": f"https://github.com/FlaminWrap/ONLYSAVEmeVODS/releases/tag/{tag}",
        "published_at": "2026-07-06T00:00:00Z",
        "draft": False,
        "prerelease": prerelease,
        "assets": [
            {
                "name": archive,
                "browser_download_url": f"{download_root}/{archive}",
                "size": 123,
            },
            {
                "name": f"{archive}.sha256",
                "browser_download_url": f"{download_root}/{archive}.sha256",
                "size": 64,
            },
        ],
    }


def updater_config(root: Path, *, mode: str = "manual") -> BotConfig:
    state_dir = root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    config_path = root / "config.toml"
    config_path.write_text("", encoding="utf-8")
    return BotConfig(
        state_dir=state_dir,
        app_update_mode=mode,
        config_path=config_path,
    )


class AppUpdateReleaseTests(unittest.TestCase):
    def test_semantic_version_parser_is_a_runtime_dependency(self) -> None:
        project_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
        project = tomllib.loads(project_path.read_text(encoding="utf-8"))["project"]

        self.assertTrue(
            any(
                dependency.casefold().startswith("packaging>=")
                for dependency in project["dependencies"]
            )
        )

    def test_parse_release_requires_install_bundle_assets(self) -> None:
        release = parse_release(fake_release("v2.1.0"))

        self.assertIsNotNone(release)
        assert release is not None
        self.assertEqual(release.tag, "v2.1.0")
        self.assertEqual(release.version, "2.1.0")
        self.assertEqual(release.tarball.name, "ONLYSAVEmeVODS-v2.1.0.tar.gz")

    def test_select_latest_release_skips_prereleases_by_default(self) -> None:
        release = select_latest_release(
            [fake_release("v3.0.0b1", prerelease=True), fake_release("v2.0.0")],
            include_prereleases=False,
        )

        self.assertIsNotNone(release)
        assert release is not None
        self.assertEqual(release.tag, "v2.0.0")

    def test_select_latest_release_uses_semantic_version_not_api_order(self) -> None:
        release = select_latest_release(
            [fake_release("v2.0.0"), fake_release("v10.0.0"), fake_release("v3.0.0")],
            include_prereleases=False,
        )

        self.assertIsNotNone(release)
        assert release is not None
        self.assertEqual(release.tag, "v10.0.0")

    def test_version_comparison_accepts_v_prefix(self) -> None:
        self.assertTrue(is_newer_version("v2.0.0", "1.9.9"))
        self.assertFalse(is_newer_version("v1.0.0", "1.0.0"))

    def test_release_version_is_newer_than_dev_checkout(self) -> None:
        self.assertTrue(is_newer_version("v0.1.0", "0.1.0.dev0"))
        self.assertFalse(is_newer_version("v0.1.0", "0.1.0"))
        self.assertFalse(is_newer_version("v0.1.0", "0.1.1.dev0"))

    def test_fallback_version_comparison_preserves_release_semantics(self) -> None:
        with patch("onlysavemevods.app_update.Version", None):
            self.assertTrue(is_newer_version("1.0", "1.0rc1"))
            self.assertFalse(is_newer_version("1.0rc1", "1.0"))
            self.assertTrue(is_newer_version("1.0rc2", "1.0rc1"))
            self.assertTrue(is_newer_version("1.0", "1.0.dev9"))
            self.assertTrue(is_newer_version("1.0.post1", "1.0"))
            self.assertTrue(versions_equal("1.0", "1.0.0"))

    def test_fallback_version_comparison_fails_closed_for_unknown_syntax(self) -> None:
        with patch("onlysavemevods.app_update.Version", None):
            self.assertFalse(is_newer_version("newest", "1.0"))
            self.assertFalse(is_newer_version("2.0", "current"))
            self.assertFalse(versions_equal("nonsense1", "nonsense1"))


class AppUpdateModeTests(unittest.TestCase):
    def test_installed_mailbox_survives_config_state_dir_change(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = updater_config(root, mode="manual")
            original_state_dir = config.state_dir
            changed_state_dir = root / "moved-application-state"
            changed_state_dir.mkdir()
            installed_mailbox = root / "installed-updater-mailbox"
            installed_mailbox.mkdir()

            with patch.dict(
                "os.environ",
                {APP_UPDATE_STATE_DIR_ENV: str(installed_mailbox)},
            ):
                check_for_updates(
                    config,
                    current_version="1.0.0",
                    fetcher=lambda _config: [fake_release("v2.0.0")],
                )
                request_update(config, current_version="1.0.0")
                config.state_dir = changed_state_dir
                observed = update_status(config, current_version="1.0.0")

                self.assertEqual(request_path(config).parent, installed_mailbox)
                self.assertEqual(status_path(config).parent, installed_mailbox)
                self.assertTrue(observed["pending"])
                self.assertEqual(observed["pending_tag"], "v2.0.0")

            for application_state_dir in (original_state_dir, changed_state_dir):
                self.assertFalse(
                    (application_state_dir / "app-update-request.json").exists()
                )
                self.assertFalse(
                    (application_state_dir / "app-update-status.json").exists()
                )

    def test_privileged_state_dir_overrides_inherited_mailbox_environment(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = updater_config(root, mode="auto_install")
            untrusted_mailbox = root / "untrusted-mailbox"
            trusted_mailbox = root / "root-owned-mailbox"
            untrusted_mailbox.mkdir()
            trusted_mailbox.mkdir()

            with patch.dict(
                "os.environ",
                {APP_UPDATE_STATE_DIR_ENV: str(untrusted_mailbox)},
            ):
                status = check_or_request_trusted_update(
                    config,
                    trusted_policy=TrustedUpdatePolicy(mode="auto_install"),
                    state_dir=trusted_mailbox,
                    current_version="1.0.0",
                    release_fetcher=lambda _repository, _token_env: [
                        fake_release("v2.0.0")
                    ],
                )

            self.assertTrue(status["pending"])
            self.assertTrue((trusted_mailbox / "app-update-request.json").is_file())
            self.assertTrue((trusted_mailbox / "app-update-status.json").is_file())
            self.assertEqual(list(untrusted_mailbox.iterdir()), [])

    def test_installed_mailbox_environment_must_be_absolute(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="manual")
            with patch.dict(
                "os.environ",
                {APP_UPDATE_STATE_DIR_ENV: "relative-state"},
            ):
                with self.assertRaisesRegex(ConfigError, "must be an absolute path"):
                    request_path(config)

    def test_manual_mode_checks_and_requests_install(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="manual")
            check = check_for_updates(
                config,
                current_version="1.0.0",
                fetcher=lambda _config: [fake_release("v2.0.0")],
            )
            requested = request_update(config, current_version="1.0.0")

            self.assertTrue(check["available"])
            self.assertTrue(request_path(config).is_file())
            self.assertEqual(requested["pending_tag"], "v2.0.0")
            self.assertEqual(requested["pending_source"], "manual")
            request = json.loads(request_path(config).read_text(encoding="utf-8"))
            self.assertEqual(set(request), {"tag", "source", "requested_at"})

    def test_check_only_mode_never_creates_install_request(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="check_only")
            status = check_or_request_auto(
                config,
                current_version="1.0.0",
                fetcher=lambda _config: [fake_release("v2.0.0")],
            )

            self.assertTrue(status["available"])
            self.assertFalse(request_path(config).exists())
            with self.assertRaises(ConfigError):
                request_update(config, current_version="1.0.0")

    def test_auto_install_mode_creates_pending_request(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="auto_install")
            status = check_or_request_auto(
                config,
                current_version="1.0.0",
                fetcher=lambda _config: [fake_release("v2.0.0")],
            )

            self.assertTrue(status["pending"])
            request = json.loads(request_path(config).read_text(encoding="utf-8"))
            self.assertEqual(request["source"], "auto")

    def test_disabled_mode_reports_disabled(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="disabled")
            status = check_for_updates(config, current_version="1.0.0")

            self.assertEqual(status["status"], "disabled")
            self.assertFalse(status["enabled"])

    def test_root_auto_policy_is_reduced_by_manual_web_config(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="manual")
            status = check_or_request_trusted_update(
                config,
                current_version="1.0.0",
                trusted_policy=TrustedUpdatePolicy(mode="auto_install"),
                release_fetcher=lambda _repository, _token_env: [fake_release("v2.0.0")],
            )

            self.assertEqual(status["status"], "update_available")
            self.assertFalse(request_path(config).exists())
            self.assertEqual(status_path(config).stat().st_mode & 0o777, 0o644)

    def test_root_auto_policy_honors_disabled_web_config_without_fetching(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="disabled")
            fetcher = Mock()
            status = check_or_request_trusted_update(
                config,
                current_version="1.0.0",
                trusted_policy=TrustedUpdatePolicy(mode="auto_install"),
                release_fetcher=fetcher,
            )

            self.assertEqual(status["status"], "disabled")
            self.assertFalse(request_path(config).exists())
            fetcher.assert_not_called()

    def test_web_auto_mode_cannot_expand_manual_root_policy(self) -> None:
        with TemporaryDirectory() as tmp:
            config = updater_config(Path(tmp), mode="auto_install")
            check_or_request_trusted_update(
                config,
                current_version="1.0.0",
                trusted_policy=TrustedUpdatePolicy(mode="manual"),
                release_fetcher=lambda _repository, _token_env: [fake_release("v2.0.0")],
            )

            self.assertFalse(request_path(config).exists())


class AppUpdateApplyTests(unittest.TestCase):
    def test_apply_replaces_app_dir_and_clears_request(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, app_dir, venv_dir = prepare_install(root)
            archive, checksum = create_release_bundle(root, "v2.0.0")
            write_request(config, "v2.0.0")

            with (
                patch("onlysavemevods.app_update.download_file", local_downloader(archive, checksum)),
                patch("onlysavemevods.app_update.repair_install") as repair,
            ):
                status = apply_requested_update(
                    config,
                    install_dir=root,
                    app_dir=app_dir,
                    venv_dir=venv_dir,
                    current_version="1.0.0",
                    release_fetcher=lambda _repository, _token_env: [fake_release("v2.0.0")],
                )

            self.assertFalse((app_dir / "old.txt").exists())
            self.assertTrue((app_dir / "src" / "onlysavemevods").is_dir())
            self.assertExecutable(app_dir / "scripts" / "app-update.sh")
            self.assertExecutable(app_dir / "scripts" / "update-python-deps.sh")
            self.assertFalse(request_path(config).exists())
            self.assertEqual(status["status"], "installed")
            repair.assert_called_once()

    def test_failed_apply_restores_backup_and_marks_failed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, app_dir, venv_dir = prepare_install(root)
            archive, checksum = create_release_bundle(root, "v2.0.0")
            write_request(config, "v2.0.0")

            with (
                patch("onlysavemevods.app_update.download_file", local_downloader(archive, checksum)),
                patch(
                    "onlysavemevods.app_update.repair_install",
                    side_effect=[AppUpdateError("pip failed"), None],
                ),
            ):
                with self.assertRaises(AppUpdateError):
                    apply_requested_update(
                        config,
                        install_dir=root,
                        app_dir=app_dir,
                        venv_dir=venv_dir,
                        current_version="1.0.0",
                        release_fetcher=lambda _repository, _token_env: [fake_release("v2.0.0")],
                    )

            self.assertEqual((app_dir / "old.txt").read_text(encoding="utf-8"), "old")
            status = json.loads(status_path(config).read_text(encoding="utf-8"))
            self.assertEqual(status["status"], "failed")
            self.assertIn("pip failed", status["last_error"])
            backups = list((root / "app-update-backups").glob("app-before-v2.0.0-*"))
            self.assertEqual(len(backups), 1)
            self.assertEqual((backups[0] / "old.txt").read_text(encoding="utf-8"), "old")

    def test_staging_copy_failure_never_removes_live_app(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, app_dir, venv_dir = prepare_install(root)
            archive, checksum = create_release_bundle(root, "v2.0.0")
            write_request(config, "v2.0.0")

            with (
                patch("onlysavemevods.app_update.download_file", local_downloader(archive, checksum)),
                patch("onlysavemevods.app_update.shutil.copytree", side_effect=OSError("copy failed")),
            ):
                with self.assertRaisesRegex(OSError, "copy failed"):
                    apply_requested_update(
                        config,
                        install_dir=root,
                        app_dir=app_dir,
                        venv_dir=venv_dir,
                        current_version="1.0.0",
                        release_fetcher=lambda _repository, _token_env: [fake_release("v2.0.0")],
                    )

            self.assertEqual((app_dir / "old.txt").read_text(encoding="utf-8"), "old")
            self.assertFalse(request_path(config).exists())

    def test_root_apply_ignores_request_urls_and_web_repository(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, app_dir, venv_dir = prepare_install(root)
            config.app_update_repository = "Attacker/Evil"
            archive, checksum = create_release_bundle(root, "v2.0.0")
            write_request(
                config,
                "v2.0.0",
                extra={
                    "archive_url": "file:///tmp/attacker.tar.gz",
                    "checksum_url": "file:///tmp/attacker.sha256",
                    "version": "9999.0",
                    "repository": "Attacker/Evil",
                },
            )
            trusted_state = root / "root-pinned-state"
            trusted_state.mkdir()
            shutil.move(
                request_path(config),
                trusted_state / request_path(config).name,
            )
            status_path(config).write_text(
                json.dumps(
                    {
                        "latest_tag": "v9999.0",
                        "archive_url": "file:///tmp/status-controlled.tar.gz",
                        "checksum_url": "file:///tmp/status-controlled.sha256",
                    }
                ),
                encoding="utf-8",
            )
            fetcher = Mock(return_value=[fake_release("v2.0.0", repository="Trusted/Repo")])
            downloaded: list[str] = []

            def trusted_download(url: str, target: Path, **_kwargs: object) -> None:
                downloaded.append(url)
                local_downloader(archive, checksum)(url, target)

            with (
                patch("onlysavemevods.app_update.download_file", trusted_download),
                patch("onlysavemevods.app_update.repair_install"),
            ):
                apply_requested_update(
                    config,
                    install_dir=root,
                    app_dir=app_dir,
                    venv_dir=venv_dir,
                    current_version="1.0.0",
                    trusted_policy=TrustedUpdatePolicy(repository="Trusted/Repo", mode="manual"),
                    state_dir=trusted_state,
                    release_fetcher=fetcher,
                )

            fetcher.assert_called_once_with("Trusted/Repo", "GITHUB_TOKEN")
            self.assertTrue(downloaded)
            self.assertTrue(all(url.startswith("https://github.com/Trusted/Repo/") for url in downloaded))

    def test_malformed_request_is_removed_and_records_failure(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, app_dir, venv_dir = prepare_install(root)
            request_path(config).write_text("{not-json", encoding="utf-8")

            with self.assertRaisesRegex(AppUpdateError, "malformed JSON"):
                apply_requested_update(
                    config,
                    install_dir=root,
                    app_dir=app_dir,
                    venv_dir=venv_dir,
                )

            self.assertFalse(request_path(config).exists())
            status = json.loads(status_path(config).read_text(encoding="utf-8"))
            self.assertEqual(status["status"], "failed")
            self.assertIn("malformed JSON", status["last_error"])

    def test_transient_download_failure_keeps_valid_request(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, app_dir, venv_dir = prepare_install(root)
            write_request(config, "v2.0.0")

            with patch(
                "onlysavemevods.app_update.download_file",
                side_effect=TransientAppUpdateError("network unavailable"),
            ):
                with self.assertRaises(TransientAppUpdateError):
                    apply_requested_update(
                        config,
                        install_dir=root,
                        app_dir=app_dir,
                        venv_dir=venv_dir,
                        current_version="1.0.0",
                        release_fetcher=lambda _repository, _token_env: [fake_release("v2.0.0")],
                    )

            self.assertTrue(request_path(config).exists())

    def test_pending_auto_request_is_rejected_after_web_mode_reduces_to_manual(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config, app_dir, venv_dir = prepare_install(root)
            write_request(config, "v2.0.0", source="auto")
            fetcher = Mock(return_value=[fake_release("v2.0.0")])

            with self.assertRaisesRegex(AppUpdateError, "Automatic update request"):
                apply_requested_update(
                    config,
                    install_dir=root,
                    app_dir=app_dir,
                    venv_dir=venv_dir,
                    trusted_policy=TrustedUpdatePolicy(mode="auto_install"),
                    current_version="1.0.0",
                    release_fetcher=fetcher,
                )

            self.assertFalse(request_path(config).exists())
            fetcher.assert_not_called()

    def test_downgrade_and_mode_inappropriate_requests_are_rejected(self) -> None:
        for policy, release, current in (
            (TrustedUpdatePolicy(mode="check_only"), fake_release("v2.0.0"), "1.0.0"),
            (TrustedUpdatePolicy(mode="manual"), fake_release("v1.0.0"), "2.0.0"),
            (
                TrustedUpdatePolicy(repository="Trusted/Repo", mode="manual"),
                fake_release("v2.0.0", repository="Attacker/Evil"),
                "1.0.0",
            ),
        ):
            with self.subTest(mode=policy.mode, current=current), TemporaryDirectory() as tmp:
                root = Path(tmp)
                config, app_dir, venv_dir = prepare_install(root)
                write_request(config, str(release["tag_name"]))
                with self.assertRaises(AppUpdateError):
                    apply_requested_update(
                        config,
                        install_dir=root,
                        app_dir=app_dir,
                        venv_dir=venv_dir,
                        trusted_policy=policy,
                        current_version=current,
                        release_fetcher=lambda _repository, _token_env, item=release: [item],
                    )
                self.assertFalse(request_path(config).exists())

    def assertExecutable(self, path: Path) -> None:
        self.assertTrue(path.is_file())
        self.assertTrue(path.stat().st_mode & 0o111, f"{path} is not executable")


class AppUpdateArchiveSafetyTests(unittest.TestCase):
    def test_extraction_rejects_symlinks_and_hardlinks(self) -> None:
        for member_type in (tarfile.SYMTYPE, tarfile.LNKTYPE):
            with self.subTest(member_type=member_type), TemporaryDirectory() as tmp:
                root = Path(tmp)
                archive = root / "unsafe.tar.gz"
                with tarfile.open(archive, "w:gz") as tar:
                    member = tarfile.TarInfo("ONLYSAVEmeVODS-v2.0.0/src/onlysavemevods/escape")
                    member.type = member_type
                    member.linkname = "../../../../outside"
                    tar.addfile(member)

                with self.assertRaisesRegex(AppUpdateError, "links or special files"):
                    extract_and_validate_bundle(
                        archive,
                        root,
                        "v2.0.0",
                        expected_version="2.0.0",
                    )


def create_release_bundle(root: Path, tag: str) -> tuple[Path, Path]:
    bundle = root / f"ONLYSAVEmeVODS-{tag}"
    (bundle / "src" / "onlysavemevods").mkdir(parents=True)
    (bundle / "scripts").mkdir()
    (bundle / "tests").mkdir()
    version = tag.removeprefix("v")
    (bundle / "pyproject.toml").write_text(
        f'[project]\nversion = "{version}"\n',
        encoding="utf-8",
    )
    (bundle / "README.md").write_text("readme", encoding="utf-8")
    (bundle / "LICENSE").write_text("license", encoding="utf-8")
    (bundle / "config.example.toml").write_text("channels = []\n", encoding="utf-8")
    (bundle / "src" / "onlysavemevods" / "__init__.py").write_text("", encoding="utf-8")
    (bundle / "scripts" / "install-systemd.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    (bundle / "scripts" / "app-update.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    (bundle / "scripts" / "update-python-deps.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    archive = root / f"ONLYSAVEmeVODS-{tag}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(bundle, arcname=bundle.name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    checksum = root / f"{archive.name}.sha256"
    checksum.write_text(f"{digest}  {archive.name}\n", encoding="utf-8")
    return archive, checksum


def prepare_install(root: Path) -> tuple[BotConfig, Path, Path]:
    config = updater_config(root, mode="manual")
    app_dir = root / "app"
    venv_dir = root / ".venv"
    app_dir.mkdir()
    (app_dir / "old.txt").write_text("old", encoding="utf-8")
    (venv_dir / "bin").mkdir(parents=True)
    (venv_dir / "bin" / "python").write_text("python", encoding="utf-8")
    return config, app_dir, venv_dir


def local_downloader(archive: Path, checksum: Path):
    def download(url: str, target: Path, **_kwargs: object) -> None:
        source = checksum if url.endswith(".sha256") else archive
        shutil.copy2(source, target)

    return download


def write_request(
    config: BotConfig,
    tag: str,
    *,
    source: str = "manual",
    extra: dict[str, object] | None = None,
) -> None:
    payload = {
        "tag": tag,
        "source": source,
        "requested_at": "2026-07-06T00:00:00Z",
    }
    if extra:
        payload.update(extra)
    request_path(config).write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
