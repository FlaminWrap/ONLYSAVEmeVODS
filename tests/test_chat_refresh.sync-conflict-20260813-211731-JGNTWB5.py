from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from datetime import datetime, timezone
import subprocess
import unittest

from onlysavemevods.chat_refresh import (
    build_chat_replay_download_command,
    refresh_chat_from_replay,
    refresh_chat_sidecar,
    sync_recorded_live_chat,
)
from onlysavemevods.chat_render import parse_live_chat_file
from onlysavemevods.chat_timing import ChatTiming, write_chat_timing
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
            chat_file.write_text(live_chat_line(0, origin_us + 50_000_000), encoding="utf-8")
            config = BotConfig(download_dir=Path(tmp))

            with patch("onlysavemevods.chat_refresh.subprocess.run", side_effect=fake_run):
                result = refresh_chat_from_replay(
                    config,
                    video_url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                    chat_file=chat_file,
                )

            entries = parse_live_chat_file(chat_file)

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "replay")
        self.assertEqual(entries[0].offset_seconds, 50.0)
        self.assertEqual(entries[0].message, "replay")

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

    def test_live_capture_sync_uses_media_start_when_not_live_from_start(self) -> None:
        stream_origin_us = 1_779_054_300_000_000
        media_origin_us = stream_origin_us + 20_000_000
        message_timestamp_us = stream_origin_us + 50_000_000

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
                    media_started_at=iso_from_us(media_origin_us),
                    chat_started_at=iso_from_us(message_timestamp_us),
                    media_live_from_start=False,
                ),
            )
            config = BotConfig(download_dir=Path(tmp), live_from_start=False)

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


if __name__ == "__main__":
    unittest.main()
