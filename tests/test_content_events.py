from pathlib import Path
from tempfile import TemporaryDirectory
import json
import sys
import unittest

from onlysavemevods.config import (
    BotConfig,
    StreamerConfig,
    StreamEventDetectionConfig,
    StreamEventRuleConfig,
)
from onlysavemevods.content_events import (
    content_event_file,
    detect_content_events_for_media,
    effective_content_event_settings,
    load_content_events,
)


class FakeBackend:
    def __init__(self, labels):
        self.labels = labels

    def classify(self, audio, sample_rate):
        return list(self.labels)


def write_fake_ffmpeg(root: Path, *, amplitude: float = 0.5, samples: int = 32_000) -> Path:
    ffmpeg = root / "ffmpeg"
    ffmpeg.write_text(
        "#!/bin/sh\n"
        f"exec {sys.executable!r} -c "
        + repr(
            "import array,sys; "
            f"samples=array.array('f',[{amplitude!r}])*{samples}; "
            "samples.tofile(sys.stdout.buffer)"
        )
        + "\n",
        encoding="utf-8",
    )
    ffmpeg.chmod(0o755)
    return ffmpeg


class ContentEventDetectionTests(unittest.TestCase):
    def test_fake_classifier_detects_label_event_and_writes_sidecar(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media_file = root / "stream.mp4"
            media_file.write_text("media", encoding="utf-8")
            config = BotConfig(
                ffmpeg_path=str(write_fake_ffmpeg(root)),
                stream_event_detection_enabled=True,
                stream_event_window_seconds=1.0,
                stream_event_hop_seconds=1.0,
                stream_event_min_confidence=0.5,
                stream_event_rules=[
                    StreamEventRuleConfig(
                        name="Funny",
                        labels=["Laughter"],
                        min_loudness_dbfs=-12.0,
                        severity="high",
                    )
                ],
            )
            phases = []

            detected = detect_content_events_for_media(
                config,
                media_file,
                backend=FakeBackend([{"label": "Laughter", "score": 0.91}]),
                progress_callback=lambda phase, progress: phases.append((phase, progress)),
            )
            events = load_content_events(media_file)
            payload = json.loads(content_event_file(media_file).read_text(encoding="utf-8"))

        self.assertTrue(detected)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["rule"], "Funny")
        self.assertEqual(events[0]["severity"], "high")
        self.assertEqual(events[0]["labels"][0]["label"], "Laughter")
        self.assertGreater(events[0]["duration"], 1.0)
        self.assertEqual(payload["model"], config.stream_event_model)
        self.assertIn(("Content event detection complete", 1.0), phases)

    def test_loudness_threshold_filters_quiet_windows(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media_file = root / "quiet.mp4"
            media_file.write_text("media", encoding="utf-8")
            config = BotConfig(
                ffmpeg_path=str(write_fake_ffmpeg(root, amplitude=0.5)),
                stream_event_detection_enabled=True,
                stream_event_window_seconds=1.0,
                stream_event_hop_seconds=1.0,
                stream_event_min_confidence=0.5,
                stream_event_rules=[
                    StreamEventRuleConfig(
                        name="Too loud",
                        labels=["Shout"],
                        min_loudness_dbfs=-3.0,
                    )
                ],
            )

            detect_content_events_for_media(
                config,
                media_file,
                backend=FakeBackend([{"label": "Shout", "score": 0.95}]),
            )
            events = load_content_events(media_file)

        self.assertEqual(events, [])

    def test_keyword_only_rule_uses_whisperx_transcript_json(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media_file = root / "stream.mp4"
            media_file.write_text("media", encoding="utf-8")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {"start": 12.0, "end": 15.5, "text": "that was a huge clutch"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config = BotConfig(
                stream_event_detection_enabled=True,
                stream_event_rules=[
                    StreamEventRuleConfig(
                        name="Clutch",
                        keywords=["clutch"],
                        severity="warning",
                    )
                ],
            )

            detect_content_events_for_media(config, media_file)
            events = load_content_events(media_file)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["start"], 12.0)
        self.assertEqual(events[0]["end"], 15.5)
        self.assertEqual(events[0]["keywords"], ["clutch"])
        self.assertIn("huge clutch", events[0]["text"])

    def test_streamer_settings_override_globals_and_replace_rules(self) -> None:
        config = BotConfig(
            stream_event_detection_enabled=True,
            stream_event_model="global/model",
            stream_event_rules=[StreamEventRuleConfig(name="Global", labels=["Laughter"])],
            streamers={
                "OUMB3rd": StreamerConfig(
                    sources=["@OUMB3rd"],
                    stream_event_detection=StreamEventDetectionConfig(
                        enabled=False,
                        model="streamer/model",
                        min_confidence=0.7,
                    ),
                    stream_event_rules=[
                        StreamEventRuleConfig(name="Streamer", keywords=["lets go"])
                    ],
                )
            },
        )

        settings = effective_content_event_settings(config, "OUMB3rd")

        self.assertFalse(settings.enabled)
        self.assertEqual(settings.model, "streamer/model")
        self.assertEqual(settings.min_confidence, 0.7)
        self.assertEqual([rule.name for rule in settings.rules], ["Streamer"])


if __name__ == "__main__":
    unittest.main()
