import io
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from onlysavemevods import kick_archive as archive


class Response(io.BytesIO):
    def __init__(self, payload, url, length=None):
        super().__init__(payload)
        self.url = url
        self.status = 200
        self.headers = {"Content-Length": str(len(payload) if length is None else length)}

    def geturl(self):
        return self.url


@unittest.skipUnless(
    shutil.which("ffmpeg") and shutil.which("ffprobe"), "requires ffmpeg and ffprobe"
)
class ArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_dir = tempfile.TemporaryDirectory()
        path = Path(cls.fixture_dir.name)
        # Real muxed MPEG-TS packets, with a video keyframe per second.
        subprocess.run(
            [
                "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=80x48:rate=25",
                "-f",
                "lavfi",
                "-i",
                "sine=sample_rate=48000",
                "-t",
                "3",
                "-c:v",
                "mpeg2video",
                "-g",
                "25",
                "-bf",
                "0",
                "-sc_threshold",
                "1000000000",
                "-c:a",
                "aac",
                "-f",
                "hls",
                "-hls_time",
                "1",
                "-hls_list_size",
                "0",
                "-hls_segment_filename",
                str(path / "clip%d.ts"),
                str(path / "vod.m3u8"),
            ],
            check=True,
        )
        cls.samples = [(path / f"clip{i}.ts").read_bytes() for i in range(3)]

    @classmethod
    def tearDownClass(cls):
        cls.fixture_dir.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name) / "segment-001.ts.part"
        self.id = "kick:42"
        self.now = 100.0
        self.active = True
        self.count = 2
        self.start = 99.0
        self.supply = lambda i: self.samples[i]
        self.fetches = []
        self.discontinuous = False
        self.omit = set()
        self.lookup = SimpleNamespace(
            started_at=self.start,
            replay_id="replay",
            format_id="source",
            expected_duration=None,
            resolve=lambda: ("https://example.invalid/playlist", not self.active),
        )
        self.patcher = patch.object(archive, "_response", self.response)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        quiet = patch("builtins.print")
        quiet.start()
        self.addCleanup(quiet.stop)
        archive.initialize_checkpoint(self.out, self.id)
        self.make_worker()

    def make_worker(self, delay=0):
        self.worker = archive.ArchiveRecorder(
            self.out, self.id, self.lookup, delay=delay, clock=lambda: self.now
        )

    def response(self, url, headers):
        if url.endswith("playlist"):
            lines = ["#EXTM3U", "#EXT-X-TARGETDURATION:1", "#EXT-X-MEDIA-SEQUENCE:0"]
            for i in range(self.count):
                if self.discontinuous and i == 1:
                    lines.append("#EXT-X-DISCONTINUITY")
                if i in self.omit:
                    lines.append("#EXT-X-GAP")
                lines += ["#EXTINF:1,", f"clip{i}.ts"]
            return Response(("\n".join(lines) + "\n#EXT-X-ENDLIST\n").encode(), url)
        idx = int(url.rsplit("clip", 1)[1].split(".")[0])
        self.fetches.append(idx)
        return Response(self.supply(idx), url)

    def test_delayed_catchup_final_tail_and_restarts(self):
        self.make_worker(delay=60)
        self.assertFalse(self.worker.step())
        self.assertFalse(self.out.exists())
        self.now = self.start + 61
        self.assertFalse(self.worker.step())
        self.assertEqual(self.out.read_bytes(), self.samples[0])
        # Crash between output flush and checkpoint commit: resume truncates this tail.
        with self.out.open("ab") as f:
            f.write(b"uncommitted bytes")
        self.make_worker(delay=60)
        self.assertEqual(self.out.read_bytes(), self.samples[0])
        self.now = self.start + 62
        self.count = 3
        self.assertFalse(self.worker.step())
        self.assertEqual(self.fetches, [0, 1])
        self.active = False
        self.lookup.expected_duration = 5.0
        self.assertFalse(self.worker.step())  # final metadata precedes last snapshot
        self.now += 90
        self.lookup.expected_duration = 3.0
        self.assertFalse(self.worker.step())
        self.assertEqual(self.fetches, [0, 1])  # last segment can still change
        self.now += 61
        self.assertTrue(self.worker.step())
        self.assertEqual(self.out.read_bytes(), b"".join(self.samples))
        self.make_worker()
        self.assertTrue(self.worker.step())
        self.assertEqual(self.fetches, [0, 1, 2])
        self.assertTrue(self.worker.state["complete"])

    def test_outage_or_short_packet_aligned_segment_never_skips(self):
        self.count = 3
        self.now += 100

        def outage(index):
            if index == 1:
                raise ConnectionResetError()
            return self.samples[index]

        self.supply = outage
        with self.assertRaises(ConnectionResetError):
            self.worker.step()
        self.assertEqual(self.fetches, [0, 1])
        self.assertEqual(self.out.read_bytes(), self.samples[0])
        self.make_worker()
        self.supply = lambda i: self.samples[i][: len(self.samples[i]) // 376 * 188]
        with self.assertRaisesRegex(archive.ArchiveRetry, "incomplete_media"):
            self.worker.step()
        self.assertEqual(self.worker.state["saved_segments"], 1)
        self.omit = {1}
        with self.assertRaisesRegex(archive.ArchiveRetry, "missing_segment"):
            self.worker.step()
        self.omit.clear()
        self.supply = lambda i: self.samples[i]
        self.assertFalse(self.worker.step())
        self.assertEqual(self.out.read_bytes(), b"".join(self.samples[:2]))

    def test_discontinuity_rebases_reset_timestamps_and_preserves_frames(self):
        self.discontinuous = True
        self.supply = lambda i: self.samples[0]  # second TS restarts at previous origin
        self.active = False
        self.assertFalse(self.worker.step())
        self.now += 61
        self.assertTrue(self.worker.step())
        packets = json.loads(
            subprocess.check_output(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_packets",
                    "-show_entries",
                    "packet=dts_time",
                    "-of",
                    "json",
                    str(self.out),
                ]
            )
        )["packets"]
        dts = [float(p["dts_time"]) for p in packets]
        self.assertEqual(len(dts), 50)
        for left, right in zip(dts, dts[1:]):
            self.assertAlmostEqual(right - left, 0.04, places=2)
        self.assertEqual(self.out.stat().st_size, self.worker.state["bytes_written"])
        self.make_worker()
        self.assertTrue(self.worker.step())

    def test_regressed_snapshot_and_changed_prefix_wait(self):
        self.now += 100
        self.count = 3
        self.worker.step()
        self.count = 1
        with self.assertRaisesRegex(archive.ArchiveRetry, "snapshot_regressed"):
            self.worker.step()
        self.count = 3
        self.discontinuous = True
        with self.assertRaisesRegex(archive.ArchiveRetry, "archive_changed"):
            self.worker.step()
        self.assertEqual(self.fetches, [0, 1])

    def test_real_video_with_short_audio_is_never_committed(self):
        sample = Path(self.tmp.name) / "audio-short.ts"
        subprocess.run(
            [
                "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=80x48:rate=25:duration=1",
                "-f",
                "lavfi",
                "-i",
                "sine=sample_rate=48000:duration=0.12",
                "-c:v",
                "mpeg2video",
                "-bf",
                "0",
                "-c:a",
                "aac",
                "-f",
                "mpegts",
                str(sample),
            ],
            check=True,
        )
        self.supply = lambda index: sample.read_bytes()
        with self.assertRaisesRegex(archive.ArchiveRetry, "incomplete_media"):
            self.worker.step()
        self.assertEqual(self.worker.state["saved_segments"], 0)
        self.assertFalse(self.out.exists())

    def test_unmarked_forward_timestamp_gap_is_never_flattened(self):
        self.now += 100
        self.count = 3
        # Each segment is valid; their DTS ranges reveal a missing full second.
        self.supply = lambda i: self.samples[0 if i == 0 else 2]
        with self.assertRaisesRegex(archive.ArchiveRetry, "source_timestamp_gap"):
            self.worker.step()
        self.assertEqual(self.out.read_bytes(), self.samples[0])
        self.assertEqual(self.worker.state["saved_segments"], 1)
        self.assertFalse(self.worker.epochs.exists())

    def test_normalization_integrity_failure_preserves_committed_media(self):
        self.discontinuous = True
        self.supply = lambda i: self.samples[0]
        self.active = False
        self.worker.step()
        self.now += 61
        with patch.object(
            archive,
            "measure_file",
            return_value={"video": ("mpeg2video", 25, 2.0), "audio": ("aac", 25, 2.0)},
        ):
            with self.assertRaisesRegex(archive.LocalStateError, "normalization changed"):
                self.worker.step()
        self.assertEqual(self.out.read_bytes(), self.samples[0] * 2)
        self.assertFalse(self.worker.state["complete"])

    def test_discovery_identity_survives_playlist_failure(self):
        self.lookup.resolve = lambda: None
        self.assertFalse(self.worker.step())
        state = json.loads(archive.checkpoint_path(self.out).read_text())
        self.assertEqual(state["replay_id"], "replay")
        self.assertEqual(state["started_at"], self.start)
        self.assertFalse(state["complete"])

    def test_timestamp_rollover_and_startup_failure_state(self):
        wrap = 2**33 / 90000
        self.assertAlmostEqual(archive._dts_delta(0.01, wrap - 0.03), 0.04, places=5)
        self.now += 100
        self.worker.step()
        committed = self.worker.state["bytes_written"]
        self.out.write_bytes(b"")
        with patch("sys.stderr", new_callable=io.StringIO):
            self.assertEqual(
                archive.main(
                    ["--source", "kick:sample", "--video-id", self.id, "--output", str(self.out)]
                ),
                1,
            )
        state = json.loads(archive.checkpoint_path(self.out).read_text())
        self.assertEqual(state["phase"], "needs_attention")
        self.assertEqual(state["bytes_written"], committed)

    def test_missing_origin_is_rejected(self):
        with self.assertRaisesRegex(archive.ArchiveRetry, "archive_origin_missing"):
            archive.parse_playlist(
                "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:18\n#EXTINF:1,\n18.ts",
                "https://example.invalid/vod",
            )
