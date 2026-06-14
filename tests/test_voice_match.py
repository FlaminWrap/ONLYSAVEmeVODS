from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from onlysavemevods.config import BotConfig, StreamerConfig, VoiceProfileConfig, voice_sample_dir
from onlysavemevods.voice_match import (
    create_transcript_voice_sample,
    match_known_voices_for_media,
    update_voice_attribution_decision,
    voice_attribution_labels_for_media,
    voice_match_rows_for_media,
)


class FakeEmbeddingBackend:
    def __init__(self, sample_vectors: dict[str, list[float]], speaker_vector: list[float]) -> None:
        self.sample_vectors = sample_vectors
        self.speaker_vector = speaker_vector

    def embed(self, media_file: Path, ranges: list[tuple[float, float]] | None = None) -> list[float]:
        if ranges:
            return self.speaker_vector
        return self.sample_vectors.get(media_file.name, [0.0, 1.0])


class VoiceMatchTests(unittest.TestCase):
    def test_high_confidence_voice_match_writes_auto_attribution(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                state_dir=root / "state",
                streamers={
                    "OUMB3rd": StreamerConfig(
                        sources=["Example Channel"],
                        voices={"Host": VoiceProfileConfig(samples=["host.wav"])},
                    )
                },
                voice_match_threshold=0.2,
            )
            sample_dir = voice_sample_dir(config, "OUMB3rd", "Host")
            sample_dir.mkdir(parents=True)
            (sample_dir / "host.wav").write_bytes(b"sample")
            media_file = root / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_bytes(b"media")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 2.0,
                                "text": "hello",
                                "speaker": "SPEAKER_00",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            matched = match_known_voices_for_media(
                config,
                media_file,
                channel="Example Channel",
                backend=FakeEmbeddingBackend({"host.wav": [1.0, 0.0]}, [1.0, 0.0]),
            )
            labels = voice_attribution_labels_for_media(media_file)
            rows = voice_match_rows_for_media(media_file)

        self.assertTrue(matched)
        self.assertEqual(labels, {"SPEAKER_00": "Host"})
        self.assertEqual(rows[0]["status"], "auto")

    def test_low_confidence_voice_match_is_suggestion_until_approved(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(
                state_dir=root / "state",
                streamers={
                    "OUMB3rd": StreamerConfig(
                        sources=["Example Channel"],
                        voices={"Host": VoiceProfileConfig(samples=["host.wav"])},
                    )
                },
                voice_match_threshold=0.1,
            )
            sample_dir = voice_sample_dir(config, "OUMB3rd", "Host")
            sample_dir.mkdir(parents=True)
            (sample_dir / "host.wav").write_bytes(b"sample")
            media_file = root / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_bytes(b"media")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 2.0,
                                "text": "hello",
                                "speaker": "SPEAKER_00",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            match_known_voices_for_media(
                config,
                media_file,
                channel="Example Channel",
                backend=FakeEmbeddingBackend({"host.wav": [0.0, 1.0]}, [1.0, 0.0]),
            )
            labels_before = voice_attribution_labels_for_media(media_file)
            update_voice_attribution_decision(
                media_file,
                "SPEAKER_00",
                "approve",
                voice_name="Host",
            )
            labels_after = voice_attribution_labels_for_media(media_file)

        self.assertEqual(labels_before, {})
        self.assertEqual(labels_after, {"SPEAKER_00": "Host"})

    def test_transcript_sample_metadata_is_created_from_speaker_segments(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(state_dir=root / "state")
            media_file = root / "Live Status [LIVEVIDEO01].mp4"
            media_file.write_bytes(b"media")
            media_file.with_suffix(".json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {
                                "start": 0.0,
                                "end": 1.0,
                                "text": "hello",
                                "speaker": "SPEAKER_00",
                            },
                            {
                                "start": 2.0,
                                "end": 3.0,
                                "text": "bye",
                                "speaker": "SPEAKER_01",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            sample_name = create_transcript_voice_sample(
                config,
                "OUMB3rd",
                "Host",
                media_file,
                "SPEAKER_00",
            )
            sample_file = voice_sample_dir(config, "OUMB3rd", "Host") / sample_name
            payload = json.loads(sample_file.read_text(encoding="utf-8"))

        self.assertTrue(sample_name.endswith(".voice-sample.json"))
        self.assertEqual(payload["speaker_label"], "SPEAKER_00")
        self.assertEqual(payload["ranges"], [[0.0, 1.0]])


if __name__ == "__main__":
    unittest.main()
