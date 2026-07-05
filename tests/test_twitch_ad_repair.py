from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json
import unittest

from onlysavemevods.config import BotConfig, load_config
from onlysavemevods.models import LiveStream
from onlysavemevods.twitch_ad_repair import (
    TwitchAdRepairResult,
    TwitchAdRepairSegmentResult,
    TwitchAdSegment,
    TwitchVodMetadata,
    format_section_time,
    is_twitch_commercial_break_text,
    merge_commercial_samples,
    repaired_media_path,
    repair_twitch_ads_for_media,
    twitch_ad_repair_sidecar_path,
    twitch_channel_from_stream,
)


class TwitchAdRepairTests(unittest.TestCase):
    def test_commercial_break_ocr_text_is_detected(self) -> None:
        self.assertTrue(
            is_twitch_commercial_break_text("Twitch\nCommercial break in progress")
        )
        self.assertFalse(is_twitch_commercial_break_text("starting soon"))

    def test_merge_commercial_samples_extends_to_next_content_sample(self) -> None:
        segments = merge_commercial_samples(
            [(0.0, "Commercial break in progress"), (2.0, "Commercial break in progress")],
            sample_seconds=2.0,
            media_duration=30.0,
            max_ad_seconds=20.0,
        )

        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0].start, 0.0)
        self.assertEqual(segments[0].end, 6.0)
        self.assertGreater(segments[0].confidence, 0.5)

    def test_twitch_channel_prefers_configured_source(self) -> None:
        stream = LiveStream(
            video_id="twitch:ignored",
            url="https://www.twitch.tv/videos/123",
            channel="Display Name",
            platform="twitch",
            source="twitch:stylishnoob4",
        )

        self.assertEqual(twitch_channel_from_stream(stream), "stylishnoob4")

    def test_format_section_time(self) -> None:
        self.assertEqual(format_section_time(65), "00:01:05")
        self.assertEqual(format_section_time(3661.5), "01:01:01.500")


    def test_repair_reports_unavailable_when_tesseract_is_missing(self) -> None:
        with TemporaryDirectory() as tmp:
            media = Path(tmp) / "segment-001.mp4"
            media.write_bytes(b"media")
            config = BotConfig(download_dir=Path(tmp), state_dir=Path(tmp) / "state")
            stream = LiveStream(
                video_id="twitch:Example",
                url="https://www.twitch.tv/Example",
                channel="Example",
                platform="twitch",
                source="twitch:Example",
            )

            with patch("onlysavemevods.twitch_ad_repair.executable_available", return_value=False):
                result = repair_twitch_ads_for_media(config, stream, media)

        self.assertFalse(result.repaired)
        self.assertIn("tesseract is not available", result.message)

    def test_repair_writes_sidecar_when_no_ad_is_detected(self) -> None:
        with TemporaryDirectory() as tmp:
            media = Path(tmp) / "segment-001.mp4"
            media.write_bytes(b"media")
            config = BotConfig(download_dir=Path(tmp), state_dir=Path(tmp) / "state")
            stream = LiveStream(
                video_id="twitch:Example",
                url="https://www.twitch.tv/Example",
                channel="Example",
                platform="twitch",
                source="twitch:Example",
            )

            with (
                patch("onlysavemevods.twitch_ad_repair.executable_available", return_value=True),
                patch("onlysavemevods.twitch_ad_repair.detect_twitch_commercial_breaks", return_value=[]),
            ):
                result = repair_twitch_ads_for_media(config, stream, media)

            sidecar = twitch_ad_repair_sidecar_path(media)
            payload = json.loads(sidecar.read_text(encoding="utf-8"))

        self.assertFalse(result.repaired)
        self.assertEqual(result.message, "No Twitch commercial break slate detected")
        self.assertFalse(payload["repaired"])

    def test_repair_uses_aligned_vod_slice_and_reports_repaired_copy(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media = root / "segment-001.mp4"
            media.write_bytes(b"media")
            vod_slice = root / "vod.mp4"
            vod_slice.write_bytes(b"vod")
            config = BotConfig(download_dir=root, state_dir=root / "state")
            stream = LiveStream(
                video_id="twitch:Example",
                url="https://www.twitch.tv/Example",
                channel="Example",
                platform="twitch",
                source="twitch:Example",
            )
            ad = TwitchAdSegment(0.0, 30.0, 0.9, "Commercial break in progress")
            rendered: list[tuple[Path, Path]] = []

            def fake_render(media_file, replacements, output_file, **_kwargs):
                rendered.append((media_file, output_file))
                output_file.write_bytes(b"repaired")

            with (
                patch("onlysavemevods.twitch_ad_repair.executable_available", return_value=True),
                patch("onlysavemevods.twitch_ad_repair.detect_twitch_commercial_breaks", return_value=[ad]),
                patch("onlysavemevods.twitch_ad_repair.find_recent_twitch_vod_url", return_value="https://www.twitch.tv/videos/1"),
                patch(
                    "onlysavemevods.twitch_ad_repair.probe_twitch_vod",
                    return_value=TwitchVodMetadata(
                        "https://www.twitch.tv/videos/1",
                        timestamp=1000.0,
                        duration=3600.0,
                    ),
                ),
                patch("onlysavemevods.twitch_ad_repair.probe_media_duration", side_effect=[120.0, 90.0]),
                patch("onlysavemevods.twitch_ad_repair.download_twitch_vod_slice", return_value=vod_slice),
                patch("onlysavemevods.twitch_ad_repair.find_best_alignment_time", return_value=(52.0, 3.0)),
                patch("onlysavemevods.twitch_ad_repair.render_repaired_media", fake_render),
            ):
                result = repair_twitch_ads_for_media(
                    config,
                    stream,
                    media,
                    started_at="1970-01-01T00:20:00+00:00",
                )

            sidecar = twitch_ad_repair_sidecar_path(media)
            payload = json.loads(sidecar.read_text(encoding="utf-8"))

        self.assertTrue(result.repaired)
        self.assertEqual(Path(result.output_file), repaired_media_path(media))
        self.assertEqual(rendered, [(media, repaired_media_path(media))])
        self.assertTrue(payload["repaired"])
        self.assertEqual(payload["segment_results"][0]["vod_replacement_start"], 22.0)
        self.assertEqual(payload["segment_results"][0]["vod_replacement_end"], 52.0)

    def test_config_parses_twitch_ad_repair_settings(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text(
                "twitch_ad_repair_enabled = false\n"
                'twitch_ad_repair_tesseract_path = "/usr/bin/tesseract"\n'
                "twitch_ad_repair_scan_seconds = 0\n"
                "twitch_ad_repair_sample_seconds = 5\n"
                "twitch_ad_repair_max_seconds = 120\n"
                "twitch_ad_repair_vod_search_limit = 3\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertFalse(config.twitch_ad_repair_enabled)
        self.assertEqual(config.twitch_ad_repair_tesseract_path, "/usr/bin/tesseract")
        self.assertEqual(config.twitch_ad_repair_scan_seconds, 0)
        self.assertEqual(config.twitch_ad_repair_sample_seconds, 5)
        self.assertEqual(config.twitch_ad_repair_max_seconds, 120)
        self.assertEqual(config.twitch_ad_repair_vod_search_limit, 3)


class TwitchAdRepairDataclassTests(unittest.TestCase):
    def test_result_dataclasses_are_importable_for_downloader_tests(self) -> None:
        ad = TwitchAdSegment(0.0, 10.0, 0.9)
        segment_result = TwitchAdRepairSegmentResult(ad, True, "ok")
        result = TwitchAdRepairResult(True, "out.mp4", "done", [ad], [segment_result])

        self.assertTrue(result.repaired)
        self.assertEqual(result.segment_results[0].ad.duration, 10.0)
