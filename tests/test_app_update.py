from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import hashlib
import json
import tarfile
import unittest

from onlysavemevods.app_update import (
    AppUpdateError,
    apply_requested_update,
    check_for_updates,
    check_or_request_auto,
    is_newer_version,
    parse_release,
    request_path,
    request_update,
    select_latest_release,
    status_path,
)
from onlysavemevods.config import BotConfig, ConfigError


def fake_release(tag: str = "v2.0.0", *, prerelease: bool = False) -> dict[str, object]:
    archive = f"ONLYSAVEmeVODS-{tag}.tar.gz"
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
                "browser_download_url": f"https://example.invalid/{archive}",
                "size": 123,
            },
            {
                "name": f"{archive}.sha256",
                "browser_download_url": f"https://example.invalid/{archive}.sha256",
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

    def test_version_comparison_accepts_v_prefix(self) -> None:
        self.assertTrue(is_newer_version("v2.0.0", "1.9.9"))
        self.assertFalse(is_newer_version("v1.0.0", "1.0.0"))

    def test_release_version_is_newer_than_dev_checkout(self) -> None:
        self.assertTrue(is_newer_version("v0.1.0", "0.1.0.dev0"))
        self.assertFalse(is_newer_version("v0.1.0", "0.1.0"))


class AppUpdateModeTests(unittest.TestCase):
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


class AppUpdateApplyTests(unittest.TestCase):
    def test_apply_replaces_app_dir_and_clears_request(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = updater_config(root, mode="manual")
            app_dir = root / "app"
            venv_dir = root / ".venv"
            app_dir.mkdir()
            (app_dir / "old.txt").write_text("old", encoding="utf-8")
            (venv_dir / "bin").mkdir(parents=True)
            (venv_dir / "bin" / "python").write_text("python", encoding="utf-8")
            archive, checksum = create_release_bundle(root, "v2.0.0")
            write_request(config, "v2.0.0", archive, checksum)

            with patch("onlysavemevods.app_update.repair_install") as repair:
                status = apply_requested_update(
                    config,
                    install_dir=root,
                    app_dir=app_dir,
                    venv_dir=venv_dir,
                )

            self.assertFalse((app_dir / "old.txt").exists())
            self.assertTrue((app_dir / "src" / "onlysavemevods").is_dir())
            self.assertFalse(request_path(config).exists())
            self.assertEqual(status["status"], "installed")
            repair.assert_called_once()

    def test_failed_apply_restores_backup_and_marks_failed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = updater_config(root, mode="manual")
            app_dir = root / "app"
            venv_dir = root / ".venv"
            app_dir.mkdir()
            (app_dir / "old.txt").write_text("old", encoding="utf-8")
            (venv_dir / "bin").mkdir(parents=True)
            (venv_dir / "bin" / "python").write_text("python", encoding="utf-8")
            archive, checksum = create_release_bundle(root, "v2.0.0")
            write_request(config, "v2.0.0", archive, checksum)

            with patch(
                "onlysavemevods.app_update.repair_install",
                side_effect=AppUpdateError("pip failed"),
            ):
                with self.assertRaises(AppUpdateError):
                    apply_requested_update(
                        config,
                        install_dir=root,
                        app_dir=app_dir,
                        venv_dir=venv_dir,
                    )

            self.assertEqual((app_dir / "old.txt").read_text(encoding="utf-8"), "old")
            status = json.loads(status_path(config).read_text(encoding="utf-8"))
            self.assertEqual(status["status"], "failed")
            self.assertIn("pip failed", status["last_error"])


def create_release_bundle(root: Path, tag: str) -> tuple[Path, Path]:
    bundle = root / f"ONLYSAVEmeVODS-{tag}"
    (bundle / "src" / "onlysavemevods").mkdir(parents=True)
    (bundle / "scripts").mkdir()
    (bundle / "tests").mkdir()
    (bundle / "pyproject.toml").write_text('[project]\nversion = "2.0.0"\n', encoding="utf-8")
    (bundle / "README.md").write_text("readme", encoding="utf-8")
    (bundle / "config.example.toml").write_text("channels = []\n", encoding="utf-8")
    (bundle / "src" / "onlysavemevods" / "__init__.py").write_text("", encoding="utf-8")
    (bundle / "scripts" / "install-systemd.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    archive = root / f"ONLYSAVEmeVODS-{tag}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(bundle, arcname=bundle.name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    checksum = root / f"{archive.name}.sha256"
    checksum.write_text(f"{digest}  {archive.name}\n", encoding="utf-8")
    return archive, checksum


def write_request(config: BotConfig, tag: str, archive: Path, checksum: Path) -> None:
    payload = {
        "tag": tag,
        "version": tag.removeprefix("v"),
        "archive_url": archive.as_uri(),
        "checksum_url": checksum.as_uri(),
        "source": "manual",
        "requested_at": "2026-07-06T00:00:00Z",
    }
    request_path(config).write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
