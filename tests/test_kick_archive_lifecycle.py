import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch

from onlysavemevods import kick_archive
from onlysavemevods.config import BotConfig
from onlysavemevods.daemon import OnlySaveMeVodsDaemon
from onlysavemevods.downloader import DownloadManager, kick_archive_part, named_finalized_output_file
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore


class RecorderProcess:
    stdout = None

    def __init__(self):
        self.returncode = None
        self.exited = asyncio.Event()

    async def wait(self):
        await self.exited.wait()
        return self.returncode

    def terminate(self):
        self.returncode = -15
        self.exited.set()


class ArchiveLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.config = BotConfig(download_dir=root / "downloads", state_dir=root / "state")
        self.stream = LiveStream(
            video_id="kick:12345", url="https://kick.com/example", source="kick:example",
            title="Concert", channel="Example", platform="kick",
            raw={"release_timestamp": 1795719000},
        )
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)
        self.probe = AsyncMock(side_effect=AssertionError("should use the recorded archive"))
        self.manager = DownloadManager(self.config, self.state, probe=None, probe_video_func=self.probe)
        self.part = kick_archive_part(self.config, self.stream, 1)

    async def test_worker_launch_and_offline_resume_keep_checkpoint(self):
        process = RecorderProcess()
        spawn = AsyncMock(return_value=process)
        with patch("onlysavemevods.downloader.asyncio.create_subprocess_exec", spawn):
            self.assertTrue(await self.manager.start_stream(self.stream))
            args = spawn.await_args.args
            self.assertEqual(args[:3], (sys.executable, "-m", "onlysavemevods.kick_archive"))
            self.assertEqual(args[args.index("--output") + 1], str(self.part))
            self.assertEqual(args[args.index("--started-at") + 1], "1795719000")
            self.assertEqual(args[args.index("--yt-dlp-path") + 1], self.config.yt_dlp_path)
            self.assertIsNone(self.manager.active[self.stream.video_id].mixed_segment_task)
            self.assertIsNone(self.manager.active[self.stream.video_id].reconnect_task)
            env = spawn.await_args.kwargs["env"]
            self.assertTrue(Path(env["PYTHONPATH"].split(os.pathsep)[0], "onlysavemevods/kick_archive.py").is_file())
            await self.manager.stop_all()

        self.state.reconcile_stale_downloads()
        self.manager = DownloadManager(self.config, self.state, probe=None, probe_video_func=self.probe)
        saved = kick_archive.checkpoint_path(self.part).read_bytes()
        resume = AsyncMock(return_value=RecorderProcess())
        with patch("onlysavemevods.downloader.asyncio.create_subprocess_exec", resume):
            await self.manager.recover_post_exit_check(self.stream, 1)
            self.assertEqual(kick_archive.checkpoint_path(self.part).read_bytes(), saved)
            self.assertEqual(resume.await_args.args[resume.await_args.args.index("--output") + 1], str(self.part))
            self.assertEqual(self.state.get_stream(self.stream.video_id).status, "downloading")
            await self.manager.stop_all()
        self.probe.assert_not_awaited()

    async def test_full_slots_keep_archive_identity_and_resume_offline_after_restart(self):
        self.config.max_concurrent_downloads = 1
        # A startup reservation occupies the slot without a real network process.
        self.manager._starting_video_ids.add("other")
        self.assertFalse(await self.manager.start_stream(self.stream))
        retry = self.manager._spawn_retry_tasks[self.stream.video_id]
        self.assertFalse(await self.manager.start_stream(self.stream))
        self.assertIs(retry, self.manager._spawn_retry_tasks[self.stream.video_id])
        self.assertEqual(self.manager.active, {})
        record = self.state.get_stream(self.stream.video_id)
        self.assertEqual(record.status, "waiting_retry")
        self.assertIsNone(record.last_started_at)
        checkpoint = kick_archive.checkpoint_path(self.part)
        self.assertEqual(json.loads(checkpoint.read_text())["started_at"], self.stream.raw["release_timestamp"])
        await self.manager.stop_all()

        self.state.reconcile_stale_downloads()
        self.manager = DownloadManager(self.config, self.state, probe=None, probe_video_func=self.probe)
        from onlysavemevods.downloader import stream_from_state_record
        offline = stream_from_state_record(self.state.get_stream(self.stream.video_id))
        spawn = AsyncMock(return_value=RecorderProcess())
        with patch("onlysavemevods.downloader.asyncio.create_subprocess_exec", spawn):
            await self.manager.recover_post_exit_check(offline, 1)
            self.assertIn("onlysavemevods.kick_archive", spawn.await_args.args)
            self.assertEqual(self.state.get_stream(self.stream.video_id).status, "downloading")
            await self.manager.stop_all()
        self.probe.assert_not_awaited()

    async def test_manual_vod_is_left_under_dashboard_ownership(self):
        self.state.upsert_vod_stream(self.stream, status="downloading")
        daemon = OnlySaveMeVodsDaemon(self.config)
        try:
            with patch("onlysavemevods.downloader.asyncio.create_subprocess_exec") as spawn:
                await daemon._start_stream(self.stream)
                spawn.assert_not_called()
            record = daemon.state.get_stream(self.stream.video_id)
            self.assertEqual((record.recording_kind, record.status), ("vod", "downloading"))
            self.assertFalse(kick_archive.checkpoint_path(self.part).exists())
        finally:
            daemon.state.close()

    async def test_legacy_media_keeps_its_existing_downloader(self):
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, -15)
        self.part.parent.mkdir(parents=True)
        legacy = self.part.with_name("segment-001.mp4.part")
        legacy.write_bytes(b"old recording")
        spawn = AsyncMock(return_value=RecorderProcess())
        with patch("onlysavemevods.downloader.asyncio.create_subprocess_exec", spawn):
            self.assertTrue(await self.manager.start_stream(self.stream, segment_index=1))
            self.assertEqual(spawn.await_args.args[0], self.config.yt_dlp_path)
            self.assertFalse(kick_archive.checkpoint_path(self.part).exists())
            self.assertEqual(legacy.read_bytes(), b"old recording")
            await self.manager.stop_all()

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "requires ffmpeg/ffprobe")
    async def test_sealed_archive_publishes_once_and_survives_rediscovery(self):
        kick_archive.initialize_checkpoint(self.part, self.stream.video_id, started_at="1795719000")
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, 0)
        subprocess.run([
            "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=blue:s=160x90:r=25",
            "-f", "lavfi", "-i", "sine=frequency=400", "-t", "2", "-c:v", "mpeg2video",
            "-c:a", "aac", "-f", "mpegts", str(self.part),
        ], check=True, capture_output=True)
        async def inline_to_thread(func, /, *args, **kwargs):
            return func(*args, **kwargs)
        inline = patch("onlysavemevods.downloader.asyncio.to_thread", inline_to_thread)
        inline.start()
        self.addCleanup(inline.stop)
        checkpoint = kick_archive.checkpoint_path(self.part)
        state = json.loads(checkpoint.read_text())
        self.assertFalse(await self.manager.finalize_ended_segment(self.stream.video_id, 1, self.stream.channel))
        tracks = kick_archive.measure_file(self.part, "ffprobe")
        state.update(
            complete=True, saved_segments=1, available_segments=1,
            bytes_written=self.part.stat().st_size,
            codecs=[tracks[k][0] for k in ("video", "audio")],
            packet_counts={k: tracks[k][1] for k in ("video", "audio")},
        )
        checkpoint.write_text(json.dumps(state))
        with patch.dict(state["packet_counts"], video=tracks["video"][1] + 1):
            checkpoint.write_text(json.dumps(state))
            self.assertFalse(await self.manager.finalize_ended_segment(self.stream.video_id, 1, self.stream.channel))
            self.assertTrue(self.part.exists())
        checkpoint.write_text(json.dumps(state))
        await self.manager.handle_post_exit(self.stream, 1, expected_status="checking_after_exit")
        target = named_finalized_output_file(self.config, self.stream, 1, ".mp4")
        self.assertTrue(target.is_file())
        self.assertFalse(self.part.exists())
        self.assertTrue(await self.manager.finalize_ended_segment(self.stream.video_id, 1, self.stream.channel))
        self.assertEqual(self.state.get_stream(self.stream.video_id).status, "ended")
        daemon = OnlySaveMeVodsDaemon(self.config)
        try:
            with patch("onlysavemevods.downloader.asyncio.create_subprocess_exec") as spawn:
                await daemon._start_stream(self.stream)
                spawn.assert_not_called()
            self.assertEqual(daemon.state.get_stream(self.stream.video_id).status, "ended")
        finally:
            daemon.state.close()
        self.probe.assert_not_awaited()

    async def test_conflicting_final_output_or_size_mismatch_preserves_raw(self):
        kick_archive.initialize_checkpoint(self.part, self.stream.video_id)
        self.state.mark_downloading(self.stream, 1)
        self.part.write_bytes(b"recording")
        checkpoint = kick_archive.checkpoint_path(self.part)
        state = json.loads(checkpoint.read_text())
        state.update(complete=True, saved_segments=2, available_segments=2, bytes_written=10000)
        checkpoint.write_text(json.dumps(state))
        self.assertFalse(await self.manager.finalize_ended_segment(self.stream.video_id, 1, self.stream.channel))
        state["bytes_written"] = self.part.stat().st_size
        checkpoint.write_text(json.dumps(state))
        collision = self.part.with_name("segment-001.mp4")
        collision.write_bytes(b"unrelated")
        self.assertFalse(await self.manager.finalize_ended_segment(self.stream.video_id, 1, self.stream.channel))
        self.assertEqual(collision.read_bytes(), b"unrelated")
        self.assertEqual(self.part.read_bytes(), b"recording")
