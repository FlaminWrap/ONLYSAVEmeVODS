from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from datetime import datetime, timezone
import subprocess
import unittest

from onlysavemevods.chat_refresh import (
    build_chat_replay_download_command,
    media_origin_from_exit,
    refresh_chat_from_replay,
    refresh_chat_sidecar,
    sync_recorded_live_chat,
)
from onlysavemevods.chat_render import parse_live_chat_file
from onlysavemevods.chat_timing import (
    ChatTiming,
    iso_timestamp_to_us,
    read_chat_timing,
    update_chat_timing,
    write_chat_timing,
)
from onlysavemevods.config import BotConfig


def iso_from_us(timestamp_us: int) -> str:
    return datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc).isoformat()


def live_chat_line(offset_ms: int, timestamp_us: int, message: str = "hello") -> str:
    return (
        '{"replayChatItemAction":{"actions":[{"addChatItemAction":{"item":'
        '{"liveChatTextMessageRenderer":{'
        f'"timestampUsec":"{timestamp_us}",'
        '"authorName":{"simpleText":"Alice"},'
        f'"message":{{"simpleText":"{message}"}}'
        "}}}}]},"
        f'"videoOffsetTimeMsec":"{offset_ms}","isLive":true}}'
    )


def replay_chat_line(offset_ms: int, timestamp_us: int, message: str = "hello") -> str:
    return (
        '{"replayChatItemAction":{'
        f'"videoOffsetTimeMsec":"{offset_ms}",'
        '"actions":[{"addChatItemAction":{"item":'
        '{"liveChatTextMessageRenderer":{'
        f'"timestampUsec":"{timestamp_us}",'
        '"authorName":{"simpleText":"Alice"},'
        f'"message":{{"simpleText":"{message}"}}'
        "}}}}]}}"
    )


class ChatRefreshTests(unittest.TestCase):
    def test_timing_retry_preserves_first_capture_anchors(self) -> None:
        with TemporaryDirectory() as tmp:
            timing_file = Path(tmp) / "Live [LIVEVIDEO01].timing.json"
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=1,
                    stream_started_at="2026-05-17T21:45:00+00:00",
                    media_started_at="2026-05-17T21:45:05+00:00",
                    chat_started_at="2026-05-17T21:45:08+00:00",
                    media_live_from_start=True,
                    last_exit_at="2026-05-17T21:50:00+00:00",
                ),
            )

            update_chat_timing(
                timing_file,
                stream_started_at="2026-05-17T22:00:00+00:00",
                media_started_at="2026-05-17T22:00:05+00:00",
                chat_started_at="2026-05-17T22:00:08+00:00",
                media_live_from_start=False,
                last_exit_at="2026-05-17T22:10:00+00:00",
            )
            updated = update_chat_timing(
                timing_file,
                stream_started_at=None,
                media_started_at=None,
                chat_started_at=None,
                last_exit_at=None,
            )
            persisted = read_chat_timing(timing_file)

        self.assertEqual(updated, persisted)
        self.assertEqual(updated.stream_started_at, "2026-05-17T21:45:00+00:00")
        self.assertEqual(updated.media_started_at, "2026-05-17T21:45:05+00:00")
        self.assertEqual(updated.chat_started_at, "2026-05-17T21:45:08+00:00")
        self.assertTrue(updated.media_live_from_start)
        self.assertEqual(updated.last_exit_at, "2026-05-17T22:10:00+00:00")

    def test_naive_timing_timestamps_are_interpreted_as_utc(self) -> None:
        timestamp = "2026-07-01T12:00:00"

        self.assertEqual(
            iso_timestamp_to_us(timestamp),
            round(datetime(2026, 7, 1, 12, tzinfo=timezone.utc).timestamp() * 1_000_000),
        )
        self.assertEqual(
            media_origin_from_exit(timestamp, 60.0),
            round(datetime(2026, 7, 1, 11, 59, tzinfo=timezone.utc).timestamp() * 1_000_000),
        )

    def test_replay_refresh_command_does_not_pass_live_from_start(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                yt_dlp_path="yt-dlp",
                live_from_start=True,
                extra_yt_dlp_args=["--live-from-start", "--cookies", "cookies.txt"],
            )

            command = build_chat_replay_download_command(
                config,
                "https://www.youtube.com/watch?v=LIVEVIDEO01",
                Path(tmp) / "chat.%(ext)s",
            )

        self.assertNotIn("--live-from-start", command)
        self.assertIn("--skip-download", command)
        self.assertIn("--write-subs", command)
        self.assertIn("--sub-langs", command)
        self.assertIn("live_chat", command)
        self.assertIn("--cookies", command)

    def test_replay_refresh_replaces_existing_live_sidecar(self) -> None:
        origin_us = 1_779_025_200_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            output_template = Path(command[command.index("-o") + 1])
            output_file = Path(str(output_template).replace("%(ext)s", "live_chat.json"))
            output_file.write_text(
                replay_chat_line(50_000, origin_us + 50_000_000, "replay"),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, b"", b"")

        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            chat_file.write_text(
                live_chat_line(0, origin_us + 50_000_000, "replay"),
                encoding="utf-8",
            )
            original_bytes = chat_file.read_bytes()
            config = BotConfig(download_dir=Path(tmp))

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_from_replay(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    chat_file=chat_file,
                )

            entries = parse_live_chat_file(chat_file)
            backup_bytes = result.backup_file.read_bytes() if result.backup_file else b""

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "replay")
        self.assertTrue(result.changed)
        self.assertEqual(entries[0].offset_seconds, 50.0)
        self.assertEqual(entries[0].message, "replay")
        self.assertEqual(backup_bytes, original_bytes)

    def test_replay_with_fewer_messages_falls_back_to_recorded_live_chat(self) -> None:
        origin_us = 1_779_025_200_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            output_template = Path(command[command.index("-o") + 1])
            output_file = Path(str(output_template).replace("%(ext)s", "live_chat.json"))
            output_file.write_text(
                replay_chat_line(10_000, origin_us + 10_000_000, "first"),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, b"", b"")

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media_file = root / "Live [LIVEVIDEO01].mp4"
            chat_file = root / "Live [LIVEVIDEO01].live_chat.json"
            timing_file = root / "Live [LIVEVIDEO01].timing.json"
            media_file.write_text("media", encoding="utf-8")
            raw_chat = "\n".join(
                [
                    live_chat_line(0, origin_us + 10_000_000, "first"),
                    live_chat_line(0, origin_us + 50_000_000, "second"),
                ]
            )
            chat_file.write_text(raw_chat, encoding="utf-8")
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=1,
                    stream_started_at=iso_from_us(origin_us),
                    media_live_from_start=True,
                ),
            )
            config = BotConfig(download_dir=root)

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_sidecar(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    media_file=media_file,
                    chat_file=chat_file,
                    timing_file=timing_file,
                )

            entries = parse_live_chat_file(chat_file)
            backup_bytes = result.backup_file.read_bytes() if result.backup_file else b""

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "sync")
        self.assertEqual([entry.message for entry in entries], ["first", "second"])
        self.assertEqual([entry.offset_seconds for entry in entries], [10.0, 50.0])
        self.assertEqual(backup_bytes, raw_chat.encode())

    def test_replay_cannot_regress_a_previously_synced_sidecar(self) -> None:
        origin_us = 1_779_025_200_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            output_template = Path(command[command.index("-o") + 1])
            output_file = Path(str(output_template).replace("%(ext)s", "live_chat.json"))
            output_file.write_text(
                replay_chat_line(10_000, origin_us + 10_000_000, "first"),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, b"", b"")

        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            original = "\n".join(
                [
                    replay_chat_line(10_000, origin_us + 10_000_000, "first"),
                    replay_chat_line(50_000, origin_us + 50_000_000, "second"),
                ]
            ).encode()
            chat_file.write_bytes(original)

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_from_replay(
                    BotConfig(download_dir=Path(tmp)),
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    chat_file=chat_file,
                )

            final_bytes = chat_file.read_bytes()
            backups = list(Path(tmp).glob("*.raw-live.json.bak"))

        self.assertFalse(result.ok)
        self.assertIn("drop recorded messages", result.message)
        self.assertEqual(final_bytes, original)
        self.assertFalse(backups)

    def test_replay_backs_up_an_existing_synced_sidecar_before_upgrade(self) -> None:
        origin_us = 1_779_025_200_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            output_template = Path(command[command.index("-o") + 1])
            output_file = Path(str(output_template).replace("%(ext)s", "live_chat.json"))
            output_file.write_text(
                "\n".join(
                    [
                        replay_chat_line(10_000, origin_us + 10_000_000, "first"),
                        replay_chat_line(50_000, origin_us + 50_000_000, "second"),
                    ]
                ),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, b"", b"")

        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            original = replay_chat_line(
                10_000,
                origin_us + 10_000_000,
                "first",
            ).encode()
            chat_file.write_bytes(original)

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_from_replay(
                    BotConfig(download_dir=Path(tmp)),
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    chat_file=chat_file,
                )

            backup_bytes = result.backup_file.read_bytes() if result.backup_file else b""

        self.assertTrue(result.ok)
        self.assertTrue(result.changed)
        self.assertEqual(backup_bytes, original)

    def test_replay_cannot_reduce_absolute_timestamp_coverage(self) -> None:
        origin_us = 1_779_025_200_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            output_template = Path(command[command.index("-o") + 1])
            output_file = Path(str(output_template).replace("%(ext)s", "live_chat.json"))
            output_file.write_text(
                "\n".join(
                    [
                        replay_chat_line(20_000, origin_us + 20_000_000, "first"),
                        replay_chat_line(90_000, origin_us + 90_000_000, "second"),
                    ]
                ),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, b"", b"")

        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            original = "\n".join(
                [
                    live_chat_line(0, origin_us + 10_000_000, "first"),
                    live_chat_line(0, origin_us + 100_000_000, "second"),
                ]
            ).encode()
            chat_file.write_bytes(original)
            config = BotConfig(download_dir=Path(tmp))

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_from_replay(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    chat_file=chat_file,
                )

            final_bytes = chat_file.read_bytes()
            backups = list(Path(tmp).glob("*.raw-live.json.bak"))

        self.assertFalse(result.ok)
        self.assertIn("reduce absolute timestamp coverage", result.message)
        self.assertEqual(final_bytes, original)
        self.assertFalse(backups)

    def test_replay_cannot_replace_local_messages_with_different_content(self) -> None:
        origin_us = 1_779_025_200_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            output_template = Path(command[command.index("-o") + 1])
            output_file = Path(str(output_template).replace("%(ext)s", "live_chat.json"))
            output_file.write_text(
                replay_chat_line(50_000, origin_us + 50_000_000, "different"),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, b"", b"")

        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            original = live_chat_line(
                0,
                origin_us + 50_000_000,
                "locally captured",
            ).encode()
            chat_file.write_bytes(original)
            config = BotConfig(download_dir=Path(tmp))

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_from_replay(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    chat_file=chat_file,
                )

            final_bytes = chat_file.read_bytes()
            backups = list(Path(tmp).glob("*.raw-live.json.bak"))

        self.assertFalse(result.ok)
        self.assertIn("omit 1 locally recorded message", result.message)
        self.assertEqual(final_bytes, original)
        self.assertFalse(backups)

    def test_live_capture_sync_shifts_messages_onto_media_timeline(self) -> None:
        origin_us = 1_779_054_300_000_000
        last_exit_at = "2026-05-17T21:46:40+00:00"

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(live_chat_line(0, origin_us + 50_000_000), encoding="utf-8")
            config = BotConfig(download_dir=Path(tmp))

            with patch("onlysavemevods.chat_refresh.probe_video_duration", return_value=100.0):
                result = sync_recorded_live_chat(
                    config,
                    media_file=media_file,
                    chat_file=chat_file,
                    last_exit_at=last_exit_at,
                )

            entries = parse_live_chat_file(chat_file)
            backups = list(Path(tmp).glob("*.raw-live.json.bak"))

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "sync")
        self.assertEqual(entries[0].offset_seconds, 50.0)
        self.assertEqual(len(backups), 1)

    def test_live_capture_sync_drops_messages_without_absolute_timestamp(self) -> None:
        origin_us = 1_779_054_300_000_000
        unsafe_line = live_chat_line(3_000, origin_us + 5_000_000, "unsafe").replace(
            f'"timestampUsec":"{origin_us + 5_000_000}",',
            "",
        )
        safe_line = live_chat_line(0, origin_us + 50_000_000, "safe")

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            raw_chat = f"{unsafe_line}\n{safe_line}"
            chat_file.write_text(raw_chat, encoding="utf-8")
            config = BotConfig(download_dir=Path(tmp))

            with patch("onlysavemevods.chat_refresh.probe_video_duration", return_value=100.0):
                result = sync_recorded_live_chat(
                    config,
                    media_file=media_file,
                    chat_file=chat_file,
                    last_exit_at="2026-05-17T21:46:40+00:00",
                )

            entries = parse_live_chat_file(chat_file)
            backup_bytes = result.backup_file.read_bytes() if result.backup_file else b""

        self.assertTrue(result.ok)
        self.assertEqual([entry.message for entry in entries], ["safe"])
        self.assertEqual(entries[0].offset_seconds, 50.0)
        self.assertEqual(backup_bytes, raw_chat.encode())

    def test_live_capture_sync_uses_timing_stream_start_for_live_from_start(self) -> None:
        origin_us = 1_779_054_300_000_000
        late_by_us = 65_700_000

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            timing_file = Path(tmp) / "Live [LIVEVIDEO01].timing.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                live_chat_line(0, origin_us + late_by_us),
                encoding="utf-8",
            )
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=1,
                    stream_started_at=iso_from_us(origin_us),
                    media_started_at=iso_from_us(origin_us + 10_000_000),
                    chat_started_at=iso_from_us(origin_us + late_by_us),
                    media_live_from_start=True,
                ),
            )
            config = BotConfig(download_dir=Path(tmp))

            with patch(
                "onlysavemevods.chat_refresh.probe_video_duration",
                side_effect=AssertionError("duration fallback should not be used"),
            ):
                result = sync_recorded_live_chat(
                    config,
                    media_file=media_file,
                    chat_file=chat_file,
                    timing_file=timing_file,
                )

            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertIn("timing stream start", result.message)
        self.assertIn("65.7s", result.message)
        self.assertAlmostEqual(entries[0].offset_seconds, 65.7)

    def test_live_capture_sync_prefers_segment_exit_when_not_live_from_start(self) -> None:
        stream_origin_us = 1_779_054_300_000_000
        media_origin_us = stream_origin_us + 20_000_000
        message_timestamp_us = stream_origin_us + 50_000_000
        segment_exit_us = media_origin_us + 100_000_000

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            timing_file = Path(tmp) / "Live [LIVEVIDEO01].timing.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                live_chat_line(0, message_timestamp_us),
                encoding="utf-8",
            )
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=1,
                    stream_started_at=iso_from_us(stream_origin_us),
                    media_started_at=iso_from_us(media_origin_us - 15_000_000),
                    chat_started_at=iso_from_us(message_timestamp_us),
                    media_live_from_start=False,
                    last_exit_at=iso_from_us(segment_exit_us),
                ),
            )
            config = BotConfig(download_dir=Path(tmp), live_from_start=False)

            with patch("onlysavemevods.chat_refresh.probe_video_duration", return_value=100.0):
                result = sync_recorded_live_chat(
                    config,
                    media_file=media_file,
                    chat_file=chat_file,
                    last_exit_at=iso_from_us(segment_exit_us + 60_000_000),
                    timing_file=timing_file,
                )

            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertIn("timing media exit", result.message)
        self.assertEqual(entries[0].offset_seconds, 30.0)

    def test_live_from_start_without_stream_anchor_prefers_segment_exit(self) -> None:
        media_origin_us = 1_779_054_300_000_000
        message_timestamp_us = media_origin_us + 30_000_000

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            timing_file = Path(tmp) / "Live [LIVEVIDEO01].timing.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                live_chat_line(0, message_timestamp_us),
                encoding="utf-8",
            )
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=1,
                    stream_started_at=None,
                    media_started_at=iso_from_us(media_origin_us - 20_000_000),
                    media_live_from_start=True,
                    last_exit_at=iso_from_us(media_origin_us + 100_000_000),
                ),
            )
            config = BotConfig(download_dir=Path(tmp), live_from_start=True)

            with patch("onlysavemevods.chat_refresh.probe_video_duration", return_value=100.0):
                result = sync_recorded_live_chat(
                    config,
                    media_file=media_file,
                    chat_file=chat_file,
                    timing_file=timing_file,
                )

            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertIn("timing media exit", result.message)
        self.assertEqual(entries[0].offset_seconds, 30.0)

    def test_media_start_is_used_only_when_duration_origin_is_unavailable(self) -> None:
        media_origin_us = 1_779_054_300_000_000

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            timing_file = Path(tmp) / "Live [LIVEVIDEO01].timing.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                live_chat_line(0, media_origin_us + 30_000_000),
                encoding="utf-8",
            )
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=1,
                    media_started_at=iso_from_us(media_origin_us),
                    media_live_from_start=False,
                ),
            )
            config = BotConfig(download_dir=Path(tmp), live_from_start=False)

            with patch(
                "onlysavemevods.chat_refresh.probe_video_duration",
                side_effect=RuntimeError("probe unavailable"),
            ):
                result = sync_recorded_live_chat(
                    config,
                    media_file=media_file,
                    chat_file=chat_file,
                    timing_file=timing_file,
                )

            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertIn("timing media start", result.message)
        self.assertEqual(entries[0].offset_seconds, 30.0)

    def test_replay_failure_falls_back_to_recorded_chat_sync(self) -> None:
        origin_us = 1_779_054_300_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            return subprocess.CompletedProcess(command, 1, b"", b"ERROR: private video\n")

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(live_chat_line(0, origin_us + 50_000_000), encoding="utf-8")
            config = BotConfig(download_dir=Path(tmp))

            with (
                patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run),
                patch("onlysavemevods.chat_refresh.probe_video_duration", return_value=100.0),
            ):
                result = refresh_chat_sidecar(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    media_file=media_file,
                    chat_file=chat_file,
                    last_exit_at="2026-05-17T21:46:40+00:00",
                )

            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "sync")
        self.assertEqual(entries[0].offset_seconds, 50.0)

    def test_segment_capture_skips_whole_replay_and_drops_pre_media_backlog(self) -> None:
        media_origin_us = 1_779_054_300_000_000

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media_file = root / "Live [LIVEVIDEO01].mp4"
            chat_file = root / "Live [LIVEVIDEO01].live_chat.json"
            timing_file = root / "Live [LIVEVIDEO01].timing.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                "\n".join(
                    [
                        live_chat_line(0, media_origin_us - 5_000_000, "backlog"),
                        live_chat_line(0, media_origin_us + 30_000_000, "in media"),
                    ]
                ),
                encoding="utf-8",
            )
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=2,
                    media_started_at=iso_from_us(media_origin_us - 10_000_000),
                    media_live_from_start=False,
                    last_exit_at=iso_from_us(media_origin_us + 100_000_000),
                ),
            )
            config = BotConfig(download_dir=root, live_from_start=False)

            with (
                patch(
                    "onlysavemevods.chat_refresh.subprocess.run",
                    side_effect=AssertionError("whole-stream replay must not be downloaded"),
                ),
                patch("onlysavemevods.chat_refresh.probe_video_duration", return_value=100.0),
            ):
                result = refresh_chat_sidecar(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    media_file=media_file,
                    chat_file=chat_file,
                    timing_file=timing_file,
                )
            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "sync")
        self.assertEqual([entry.message for entry in entries], ["in media"])
        self.assertEqual(entries[0].offset_seconds, 30.0)

    def test_terminal_video_uses_local_capture_without_replay_attempt(self) -> None:
        origin_us = 1_779_054_300_000_000

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media_file = root / "Live [LIVEVIDEO01].mp4"
            chat_file = root / "Live [LIVEVIDEO01].live_chat.json"
            timing_file = root / "Live [LIVEVIDEO01].timing.json"
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                live_chat_line(0, origin_us + 50_000_000),
                encoding="utf-8",
            )
            write_chat_timing(
                timing_file,
                ChatTiming(
                    video_id="LIVEVIDEO01",
                    segment_index=1,
                    stream_started_at=iso_from_us(origin_us),
                    media_live_from_start=True,
                ),
            )

            with patch(
                "onlysavemevods.chat_refresh.subprocess.run",
                side_effect=AssertionError("terminal video must not retry replay"),
            ):
                result = refresh_chat_sidecar(
                    BotConfig(download_dir=root),
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    media_file=media_file,
                    chat_file=chat_file,
                    timing_file=timing_file,
                    allow_replay=False,
                )
            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "sync")
        self.assertEqual(entries[0].offset_seconds, 50.0)

    def test_private_replay_failure_keeps_existing_valid_timeline(self) -> None:
        origin_us = 1_779_054_300_000_000

        def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            return subprocess.CompletedProcess(command, 1, b"", b"ERROR: private video\n")

        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            chat_file = Path(tmp) / "Live [LIVEVIDEO01].live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            original = replay_chat_line(50_000, origin_us + 50_000_000, "saved").encode()
            chat_file.write_bytes(original)
            config = BotConfig(download_dir=Path(tmp))

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_sidecar(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    media_file=media_file,
                    chat_file=chat_file,
                )

            final_bytes = chat_file.read_bytes()

        self.assertTrue(result.ok)
        self.assertFalse(result.changed)
        self.assertEqual(result.source, "existing")
        self.assertIn("valid timeline", result.message)
        self.assertEqual(final_bytes, original)


if __name__ == "__main__":
    unittest.main()
