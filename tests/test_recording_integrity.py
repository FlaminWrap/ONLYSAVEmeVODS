from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
import json
import logging
import unittest

from onlysavemevods.chat_refresh import ChatRefreshResult
from onlysavemevods.chat_render import parse_live_chat_file
from onlysavemevods.chat_timing import read_chat_timing
from onlysavemevods.config import BotConfig
from onlysavemevods.downloader import (
    ActiveDownload,
    DownloadManager,
    FinalizedSegmentFiles,
    finalized_segment_chat_file,
    prepare_chat_output_for_resume,
    segment_directory,
    segment_timing_file,
)
from onlysavemevods.models import LiveStream
from onlysavemevods.sources import live_stream_from_generic_info
from onlysavemevods.state import StateStore


LOGGER = logging.getLogger(__name__)


def youtube_chat_line(offset_ms: int, message: str) -> bytes:
    payload = {
        "replayChatItemAction": {
            "actions": [
                {
                    "addChatItemAction": {
                        "item": {
                            "liveChatTextMessageRenderer": {
                                "timestampUsec": str(
                                    1_800_000_000_000_000 + offset_ms * 1000
                                ),
                                "authorName": {"simpleText": "Recorder"},
                                "message": {"simpleText": message},
                            }
                        }
                    }
                }
            ]
        },
        "videoOffsetTimeMsec": str(offset_ms),
    }
    return json.dumps(payload).encode("utf-8")


def platform_session_stream(
    platform: str,
    title: str,
    release_timestamp: int,
) -> LiveStream:
    return live_stream_from_generic_info(
        {
            "id": "creator",
            "title": title,
            "channel": "creator",
            "webpage_url": f"https://example.test/{platform}/{release_timestamp}",
            "is_live": True,
            "release_timestamp": release_timestamp,
        },
        platform=platform,
        fallback_url=f"https://example.test/{platform}/creator",
        source=f"{platform}:creator",
    )


class ControlledProcess:
    def __init__(self, exit_code: int = 0) -> None:
        self.exit_code = exit_code
        self.returncode: int | None = None
        self.stdout = None
        self._finished = asyncio.Event()

    async def wait(self) -> int:
        await self._finished.wait()
        if self.returncode is None:
            self.returncode = self.exit_code
        return self.returncode

    def finish(self, exit_code: int | None = None) -> None:
        if exit_code is not None:
            self.exit_code = exit_code
        self._finished.set()

    def terminate(self) -> None:
        self.finish(-15)

    def kill(self) -> None:
        self.finish(-9)


class ImmediateProcess:
    def __init__(self, exit_code: int = 0) -> None:
        self.exit_code = exit_code
        self.returncode: int | None = None

    async def wait(self) -> int:
        self.returncode = self.exit_code
        return self.exit_code


def active_download(
    stream: LiveStream,
    process: object,
    *,
    chat_process: object | None = None,
) -> ActiveDownload:
    task = asyncio.current_task()
    assert task is not None
    return ActiveDownload(
        stream=stream,
        process=process,  # type: ignore[arg-type]
        segment_index=1,
        output_template=Path("/tmp/recording-integrity.%(ext)s"),
        task=task,
        chat_process=chat_process,  # type: ignore[arg-type]
    )


class ChatFileIntegrityTests(unittest.TestCase):
    def test_resume_demotes_final_chat_without_changing_bytes(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            directory = segment_directory(config, "youtube:LIVEVIDEO01", "Creator")
            directory.mkdir(parents=True)
            final_file = directory / "segment-001.live_chat.json"
            original = b'{"sentinel":"preserve exactly"}\n'
            final_file.write_bytes(original)

            prepare_chat_output_for_resume(
                config,
                "youtube:LIVEVIDEO01",
                1,
                "Creator",
                LOGGER,
            )

            part_file = directory / "segment-001.live_chat.json.part"
            self.assertFalse(final_file.exists())
            self.assertEqual(part_file.read_bytes(), original)

    def test_final_and_partial_chat_are_merged_in_file_order_and_parseable(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(download_dir=Path(tmp))
            directory = segment_directory(config, "youtube:LIVEVIDEO01", "Creator")
            directory.mkdir(parents=True)
            final_file = directory / "segment-001.live_chat.json"
            part_file = directory / "segment-001.live_chat.json.part"
            final_file.write_bytes(youtube_chat_line(1_000, "from-final"))
            part_file.write_bytes(youtube_chat_line(2_000, "from-part"))

            result = finalized_segment_chat_file(
                config,
                "youtube:LIVEVIDEO01",
                1,
                "Creator",
                LOGGER,
            )

            self.assertEqual(result, final_file)
            self.assertFalse(part_file.exists())
            merged = final_file.read_text(encoding="utf-8")
            self.assertLess(merged.index("from-final"), merged.index("from-part"))
            self.assertEqual(
                [entry.message for entry in parse_live_chat_file(final_file)],
                ["from-final", "from-part"],
            )


class RecordingIntegrityAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_media_spawn_retry_is_background_deduped_and_cancellable(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                retry_backoff_seconds=[300],
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            sleep_started = asyncio.Event()

            async def blocked_retry(_delay: float) -> None:
                sleep_started.set()
                await asyncio.Future()

            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                sleep_func=blocked_retry,
            )
            spawn = AsyncMock(side_effect=FileNotFoundError("yt-dlp"))
            try:
                with patch(
                    "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                    spawn,
                ):
                    first = await manager.start_stream(stream)
                    await sleep_started.wait()
                    second = await manager.start_stream(stream)
                    self.assertEqual(spawn.await_count, 1)
                    self.assertEqual(len(manager._spawn_retry_tasks), 1)
                    await manager.stop_all()
            finally:
                state.close()

        self.assertFalse(first)
        self.assertFalse(second)
        self.assertFalse(manager._spawn_retry_tasks)

    async def test_recording_start_defers_to_claimed_file_deletion(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            self.assertTrue(
                state.compare_and_set_stream_status(
                    stream.video_id,
                    expected_status="detected",
                    new_status="deleting",
                )
            )
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            try:
                with patch(
                    "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                    new=AsyncMock(),
                ) as spawn:
                    started = await manager.start_stream(stream)
            finally:
                state.close()

        self.assertFalse(started)
        spawn.assert_not_awaited()

    async def test_recording_start_loses_atomic_race_to_file_deletion(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            original_mark_downloading = state.mark_downloading

            def deletion_claims_immediately_before_start(
                candidate: LiveStream,
                segment_index: int,
            ) -> bool:
                self.assertTrue(
                    state.compare_and_set_stream_status(
                        candidate.video_id,
                        expected_status="detected",
                        new_status="deleting",
                    )
                )
                return original_mark_downloading(candidate, segment_index)

            try:
                with (
                    patch.object(
                        state,
                        "mark_downloading",
                        side_effect=deletion_claims_immediately_before_start,
                    ),
                    patch(
                        "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                        new=AsyncMock(),
                    ) as spawn,
                ):
                    started = await manager.start_stream(stream)
                record = state.get_stream(stream.video_id)
            finally:
                state.close()

        self.assertFalse(started)
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "deleting")
        spawn.assert_not_awaited()

    async def test_concurrent_starts_spawn_only_one_media_process(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=False,
                render_live_chat_video=False,
                reconnect_interval_seconds=0,
                youtube_stale_live_timeout_seconds=0,
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            media_process = ControlledProcess()
            spawn_entered = asyncio.Event()
            allow_spawn = asyncio.Event()

            async def delayed_spawn(*_args: object, **_kwargs: object) -> ControlledProcess:
                spawn_entered.set()
                await allow_spawn.wait()
                return media_process

            try:
                with patch(
                    "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                    new=AsyncMock(side_effect=delayed_spawn),
                ) as spawn:
                    first_task = asyncio.create_task(manager.start_stream(stream))
                    await spawn_entered.wait()
                    second = await manager.start_stream(stream)
                    allow_spawn.set()
                    first = await first_task
                    await manager.stop_all()
            finally:
                state.close()

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(spawn.await_count, 1)

    async def test_starting_process_counts_toward_global_concurrency_limit(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                max_concurrent_downloads=1,
                record_live_chat=False,
                render_live_chat_video=False,
                reconnect_interval_seconds=0,
                youtube_stale_live_timeout_seconds=0,
            )
            first_stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            second_stream = LiveStream(
                video_id="youtube:LIVEVIDEO02",
                url="https://www.youtube.com/watch?v=LIVEVIDEO02",
                channel="Other Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            media_process = ControlledProcess()
            spawn_entered = asyncio.Event()
            allow_spawn = asyncio.Event()

            async def delayed_spawn(*_args: object, **_kwargs: object) -> ControlledProcess:
                spawn_entered.set()
                await allow_spawn.wait()
                return media_process

            try:
                with patch(
                    "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                    new=AsyncMock(side_effect=delayed_spawn),
                ) as spawn:
                    first_task = asyncio.create_task(manager.start_stream(first_stream))
                    await spawn_entered.wait()
                    second = await manager.start_stream(second_stream)
                    allow_spawn.set()
                    first = await first_task
                    await manager.stop_all()
                deferred_record = state.get_stream(second_stream.video_id)
            finally:
                state.close()

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(spawn.await_count, 1)
        self.assertIsNotNone(deferred_record)
        assert deferred_record is not None
        self.assertEqual(deferred_record.status, "detected")

    async def test_shutdown_terminates_process_that_finishes_spawning_late(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=False,
                render_live_chat_video=False,
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            media_process = ControlledProcess()
            spawn_entered = asyncio.Event()
            allow_spawn = asyncio.Event()

            async def delayed_spawn(*_args: object, **_kwargs: object) -> ControlledProcess:
                spawn_entered.set()
                await allow_spawn.wait()
                return media_process

            try:
                with patch(
                    "onlysavemevods.downloader.asyncio.create_subprocess_exec",
                    new=AsyncMock(side_effect=delayed_spawn),
                ):
                    start_task = asyncio.create_task(manager.start_stream(stream))
                    await spawn_entered.wait()
                    await manager.stop_all()
                    allow_spawn.set()
                    started = await start_task
            finally:
                state.close()

        self.assertFalse(started)
        self.assertEqual(media_process.returncode, -15)
        self.assertFalse(manager.active)
        self.assertFalse(manager._starting_video_ids)

    async def test_unexpected_chat_exit_restarts_and_attaches_replacement(self) -> None:
        config = BotConfig()
        state = MagicMock()
        manager = DownloadManager(
            config,
            state,
            probe=None,  # type: ignore[arg-type]
            sleep_func=AsyncMock(),
        )
        stream = LiveStream(
            video_id="youtube:LIVEVIDEO01",
            url="https://www.youtube.com/watch?v=LIVEVIDEO01",
            platform="youtube",
        )
        media_process = MagicMock()
        media_process.returncode = None
        failed_chat = ControlledProcess(7)
        replacement = ControlledProcess(0)
        active = active_download(stream, media_process, chat_process=failed_chat)
        manager.active[stream.video_id] = active
        replacement_started = asyncio.Event()

        async def restart(*_args: object, **_kwargs: object):
            replacement_started.set()
            return replacement, None, None

        manager._start_chat_recorder = AsyncMock(side_effect=restart)  # type: ignore[method-assign]
        watcher = asyncio.create_task(
            manager._watch_chat_process(stream, 1, failed_chat)  # type: ignore[arg-type]
        )
        active.chat_task = watcher
        try:
            failed_chat.finish()
            await replacement_started.wait()
            for _ in range(10):
                if active.chat_process is replacement:
                    break
                await asyncio.sleep(0)

            self.assertIs(active.chat_process, replacement)
            manager._start_chat_recorder.assert_awaited_once_with(
                stream,
                1,
                record_timing=False,
            )
            event_message = state.add_stream_event.call_args.args[1]
            self.assertIn("exited unexpectedly", event_message)
            self.assertIn("code 7", event_message)

            media_process.returncode = 0
            replacement.finish()
            await watcher
        finally:
            if not watcher.done():
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)

    async def test_chat_exit_does_not_retry_after_media_ended_or_active_replaced(self) -> None:
        stream = LiveStream(
            video_id="youtube:LIVEVIDEO01",
            url="https://www.youtube.com/watch?v=LIVEVIDEO01",
            platform="youtube",
        )
        for replacement_reason in ("media-ended", "active-replaced"):
            with self.subTest(replacement_reason=replacement_reason):
                state = MagicMock()
                manager = DownloadManager(
                    BotConfig(),
                    state,
                    probe=None,  # type: ignore[arg-type]
                    sleep_func=AsyncMock(),
                )
                media_process = MagicMock()
                media_process.returncode = None
                failed_chat = ControlledProcess(9)
                active = active_download(stream, media_process, chat_process=failed_chat)
                manager.active[stream.video_id] = active
                manager._start_chat_recorder = AsyncMock()  # type: ignore[method-assign]
                watcher = asyncio.create_task(
                    manager._watch_chat_process(stream, 1, failed_chat)  # type: ignore[arg-type]
                )
                active.chat_task = watcher

                if replacement_reason == "media-ended":
                    media_process.returncode = 0
                else:
                    other_media = MagicMock()
                    other_media.returncode = None
                    manager.active[stream.video_id] = active_download(stream, other_media)
                failed_chat.finish()
                await watcher

                manager._start_chat_recorder.assert_not_awaited()
                state.add_stream_event.assert_not_called()

    async def test_media_exit_timing_is_persisted_before_slow_chat_stop(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=True,
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
                raw={"release_timestamp": 1_800_000_000},
            )
            manager = DownloadManager(config, MagicMock(), probe=None)  # type: ignore[arg-type]
            manager.write_segment_timing_started(
                stream,
                1,
                media_started_at="2027-01-15T07:59:55+00:00",
            )
            media_process = ImmediateProcess(0)
            active = active_download(stream, media_process)
            manager.active[stream.video_id] = active
            manager._stopping = True
            stop_entered = asyncio.Event()
            allow_stop = asyncio.Event()

            async def slow_chat_stop(*_args: object, **_kwargs: object) -> None:
                stop_entered.set()
                await allow_stop.wait()

            manager._stop_powerchat_listener = AsyncMock()  # type: ignore[method-assign]
            manager._stop_chat_recorder = AsyncMock(  # type: ignore[method-assign]
                side_effect=slow_chat_stop
            )
            expected_exit = "2027-01-15T08:30:00+00:00"
            with patch("onlysavemevods.downloader.utc_now_iso", return_value=expected_exit):
                watcher = asyncio.create_task(
                    manager._watch_process(stream, media_process, 1)  # type: ignore[arg-type]
                )
                await stop_entered.wait()
                timing = read_chat_timing(
                    segment_timing_file(config, stream.video_id, 1, stream.channel)
                )
                self.assertIsNotNone(timing)
                assert timing is not None
                self.assertEqual(timing.last_exit_at, expected_exit)
                self.assertFalse(watcher.done())
                allow_stop.set()
                await watcher

    async def test_old_watcher_does_not_clobber_a_replacement_download(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            old_process = ImmediateProcess(0)
            old_active = active_download(stream, old_process)
            manager.active[stream.video_id] = old_active
            replacement_process = MagicMock()
            replacement_process.returncode = None
            replacement: ActiveDownload | None = None

            async def replacement_arrives(*_args: object, **_kwargs: object) -> None:
                nonlocal replacement
                replacement = active_download(stream, replacement_process)
                manager.active[stream.video_id] = replacement

            manager._stop_powerchat_listener = AsyncMock()  # type: ignore[method-assign]
            manager._stop_chat_recorder = AsyncMock(  # type: ignore[method-assign]
                side_effect=replacement_arrives
            )
            try:
                await manager._watch_process(stream, old_process, 1)  # type: ignore[arg-type]
                record = state.get_stream(stream.video_id)
            finally:
                state.close()

        self.assertIs(manager.active.get(stream.video_id), replacement)
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "downloading")

    async def test_exit_state_survives_sidecar_cleanup_failure(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            media_process = ImmediateProcess(0)
            manager.active[stream.video_id] = active_download(stream, media_process)
            manager._stop_powerchat_listener = AsyncMock(  # type: ignore[method-assign]
                side_effect=OSError("sidecar disk error")
            )
            manager._stop_chat_recorder = AsyncMock(  # type: ignore[method-assign]
                side_effect=OSError("chat disk error")
            )
            manager.handle_post_exit = AsyncMock()  # type: ignore[method-assign]
            try:
                await manager._watch_process(stream, media_process, 1)  # type: ignore[arg-type]
                await asyncio.sleep(0)
                record = state.get_stream(stream.video_id)
            finally:
                state.close()

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "checking_after_exit")
        self.assertNotIn(stream.video_id, manager.active)
        manager.handle_post_exit.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_exit_state_write_retries_before_releasing_lifecycle(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                sleep_func=AsyncMock(),
            )
            media_process = ImmediateProcess(0)
            manager.active[stream.video_id] = active_download(stream, media_process)
            manager._stop_powerchat_listener = AsyncMock()  # type: ignore[method-assign]
            manager._stop_chat_recorder = AsyncMock()  # type: ignore[method-assign]
            manager.handle_post_exit = AsyncMock()  # type: ignore[method-assign]
            original_mark_exited = state.mark_exited
            attempts = 0

            def transient_mark_exited(video_id: str, exit_code: int) -> None:
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise OSError("temporarily locked")
                original_mark_exited(video_id, exit_code)

            try:
                with patch.object(
                    state,
                    "mark_exited",
                    side_effect=transient_mark_exited,
                ):
                    await manager._watch_process(stream, media_process, 1)  # type: ignore[arg-type]
                await asyncio.sleep(0)
                record = state.get_stream(stream.video_id)
            finally:
                state.close()

        self.assertEqual(attempts, 3)
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "checking_after_exit")
        self.assertNotIn(stream.video_id, manager.active)

    async def test_post_exit_status_change_during_sleep_prevents_finalization(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                post_exit_check_seconds=[1],
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_exited(stream.video_id, 0)
            manager: DownloadManager

            async def resume_during_sleep(_delay: float) -> None:
                state.mark_downloading(stream, 1)
                media_process = MagicMock()
                media_process.returncode = None
                manager.active[stream.video_id] = active_download(stream, media_process)

            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                sleep_func=resume_during_sleep,
                probe_video_func=AsyncMock(),
            )
            manager.finish_ended_stream = AsyncMock()  # type: ignore[method-assign]
            try:
                await manager.handle_post_exit(
                    stream,
                    1,
                    expected_status="checking_after_exit",
                )
                record = state.get_stream(stream.video_id)
            finally:
                state.close()

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.status, "downloading")
            manager.probe_video.assert_not_awaited()  # type: ignore[attr-defined]
            manager.finish_ended_stream.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_capacity_deferred_restart_keeps_post_exit_supervision(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                max_concurrent_downloads=1,
                post_exit_check_seconds=[0],
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
                is_live=True,
            )
            state = StateStore(config.db_path)
            state.mark_downloading(stream, 1)
            state.mark_exited(stream.video_id, 0)
            retry_sleep_started = asyncio.Event()

            async def blocked_sleep(_delay: float) -> None:
                retry_sleep_started.set()
                await asyncio.Future()

            manager = DownloadManager(
                config,
                state,
                probe=None,  # type: ignore[arg-type]
                sleep_func=blocked_sleep,
                probe_video_func=AsyncMock(return_value=stream),
            )
            manager._starting_video_ids.add("youtube:OTHERLIVE01")
            manager._check_youtube_before_restart = AsyncMock(  # type: ignore[method-assign]
                return_value="restart"
            )
            manager.choose_live_restart_segment = AsyncMock(  # type: ignore[method-assign]
                return_value=1
            )
            try:
                await manager.handle_post_exit(
                    stream,
                    1,
                    expected_status="checking_after_exit",
                )
                await retry_sleep_started.wait()
                record = state.get_stream(stream.video_id)
                self.assertIn(stream.video_id, manager._deferred_post_exit_retries)
                self.assertTrue(manager._post_exit_tasks)
            finally:
                for task in list(manager._post_exit_tasks):
                    task.cancel()
                await asyncio.gather(*manager._post_exit_tasks, return_exceptions=True)
                state.close()

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.status, "checking_after_exit")

    async def test_kick_and_twitch_new_sessions_start_before_old_session_finalizes(self) -> None:
        for platform in ("kick", "twitch"):
            with self.subTest(platform=platform), TemporaryDirectory() as tmp:
                source = f"{platform}:creator"
                old_stream = live_stream_from_generic_info(
                    {
                        "id": "creator",
                        "title": "Morning session",
                        "channel": "creator",
                        "webpage_url": f"https://example.test/{platform}/morning",
                        "is_live": True,
                        "release_timestamp": 1_800_000_000,
                    },
                    platform=platform,
                    fallback_url=f"https://example.test/{platform}/creator",
                    source=source,
                )
                new_stream = live_stream_from_generic_info(
                    {
                        "id": "creator",
                        "title": "Evening session",
                        "channel": "creator",
                        "webpage_url": f"https://example.test/{platform}/evening",
                        "is_live": True,
                        "release_timestamp": 1_800_043_200,
                    },
                    platform=platform,
                    fallback_url=f"https://example.test/{platform}/creator",
                    source=source,
                )
                self.assertNotEqual(old_stream.video_id, new_stream.video_id)

                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                    post_exit_check_seconds=[0],
                )
                state = StateStore(config.db_path)
                state.mark_downloading(old_stream, 1)
                state.mark_exited(old_stream.video_id, 0)
                manager = DownloadManager(
                    config,
                    state,
                    probe=None,  # type: ignore[arg-type]
                    probe_video_func=AsyncMock(return_value=new_stream),
                )
                calls: list[tuple[str, str]] = []

                async def start(stream: LiveStream, **_kwargs: object) -> bool:
                    calls.append(("start", stream.video_id))
                    return True

                async def finish(stream: LiveStream, *_args: object, **_kwargs: object) -> None:
                    calls.append(("finish", stream.video_id))

                manager.start_stream = AsyncMock(side_effect=start)  # type: ignore[method-assign]
                manager.finish_ended_stream = AsyncMock(  # type: ignore[method-assign]
                    side_effect=finish
                )
                try:
                    await manager.handle_post_exit(
                        old_stream,
                        1,
                        expected_status="checking_after_exit",
                    )
                finally:
                    state.close()

                self.assertEqual(
                    calls,
                    [
                        ("start", new_stream.video_id),
                        ("finish", old_stream.video_id),
                    ],
                )

    async def test_planned_reconnect_treats_new_platform_session_independently(self) -> None:
        for platform in ("kick", "twitch"):
            with self.subTest(platform=platform), TemporaryDirectory() as tmp:
                old_stream = platform_session_stream(
                    platform,
                    "Morning session",
                    1_800_000_000,
                )
                new_stream = platform_session_stream(
                    platform,
                    "Evening session",
                    1_800_043_200,
                )
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                )
                state = StateStore(config.db_path)
                state.mark_downloading(old_stream, 1)
                state.mark_exited(old_stream.video_id, 0)
                manager = DownloadManager(
                    config,
                    state,
                    probe=None,  # type: ignore[arg-type]
                    probe_video_func=AsyncMock(return_value=new_stream),
                )
                calls: list[tuple[str, str, dict[str, object]]] = []

                async def start(
                    stream: LiveStream,
                    **kwargs: object,
                ) -> bool:
                    calls.append(("start", stream.video_id, kwargs))
                    return True

                async def finish(
                    stream: LiveStream,
                    *_args: object,
                    **kwargs: object,
                ) -> None:
                    calls.append(("finish", stream.video_id, kwargs))

                manager.start_stream = AsyncMock(side_effect=start)  # type: ignore[method-assign]
                manager.finish_ended_stream = AsyncMock(side_effect=finish)  # type: ignore[method-assign]
                try:
                    await manager.handle_planned_reconnect(old_stream, 1)
                finally:
                    state.close()

                self.assertEqual(
                    calls,
                    [
                        ("start", new_stream.video_id, {}),
                        (
                            "finish",
                            old_stream.video_id,
                            {"expected_status": "checking_after_exit"},
                        ),
                    ],
                )

    async def test_spawn_retry_treats_new_platform_session_independently(self) -> None:
        for platform in ("kick", "twitch"):
            with self.subTest(platform=platform), TemporaryDirectory() as tmp:
                old_stream = platform_session_stream(
                    platform,
                    "Morning session",
                    1_800_000_000,
                )
                new_stream = platform_session_stream(
                    platform,
                    "Evening session",
                    1_800_043_200,
                )
                config = BotConfig(
                    download_dir=Path(tmp) / "downloads",
                    state_dir=Path(tmp) / "state",
                    retry_backoff_seconds=[0],
                )
                state = StateStore(config.db_path)
                state.mark_downloading(old_stream, 1)
                state.mark_waiting_retry(old_stream.video_id)
                manager = DownloadManager(
                    config,
                    state,
                    probe=None,  # type: ignore[arg-type]
                    sleep_func=AsyncMock(),
                    probe_video_func=AsyncMock(return_value=new_stream),
                )
                calls: list[tuple[str, str, dict[str, object]]] = []

                async def start(
                    stream: LiveStream,
                    **kwargs: object,
                ) -> bool:
                    calls.append(("start", stream.video_id, kwargs))
                    return True

                async def finish(
                    stream: LiveStream,
                    *_args: object,
                    **kwargs: object,
                ) -> None:
                    calls.append(("finish", stream.video_id, kwargs))

                manager.start_stream = AsyncMock(side_effect=start)  # type: ignore[method-assign]
                manager.finish_ended_stream = AsyncMock(side_effect=finish)  # type: ignore[method-assign]
                try:
                    await manager._schedule_spawn_retry(old_stream)
                finally:
                    state.close()

                self.assertEqual(
                    calls,
                    [
                        ("start", new_stream.video_id, {}),
                        (
                            "finish",
                            old_stream.video_id,
                            {"expected_status": "waiting_retry"},
                        ),
                    ],
                )

    async def test_failed_chat_refresh_invalidates_timeline_and_skips_render_job(self) -> None:
        with TemporaryDirectory() as tmp:
            config = BotConfig(
                download_dir=Path(tmp) / "downloads",
                state_dir=Path(tmp) / "state",
                record_live_chat=True,
                render_live_chat_video=True,
            )
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            root = Path(tmp)
            files = FinalizedSegmentFiles(
                segment_index=1,
                channel=stream.channel,
                media_file=root / "video.mp4",
                chat_file=root / "video.live_chat.json",
                timing_file=root / "video.timing.json",
            )
            failed = ChatRefreshResult(
                ok=False,
                changed=False,
                source="unchanged",
                message="private replay and no trustworthy timeline origin",
            )
            try:
                with patch(
                    "onlysavemevods.downloader.asyncio.to_thread",
                    new=AsyncMock(return_value=failed),
                ):
                    await manager.refresh_finalized_chat_files(stream, [files])
                jobs = manager.enqueue_finalized_post_processing(stream, [files])
            finally:
                state.close()

            self.assertFalse(files.chat_timeline_valid)
            self.assertNotIn("chat_render", [job.kind for job in jobs])

    async def test_raw_live_offsets_are_refused_by_manual_renderer(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BotConfig(download_dir=root / "downloads", state_dir=root / "state")
            stream = LiveStream(
                video_id="youtube:LIVEVIDEO01",
                url="https://www.youtube.com/watch?v=LIVEVIDEO01",
                channel="Creator",
                platform="youtube",
            )
            media_file = root / "video.mp4"
            chat_file = root / "video.live_chat.json"
            media_file.write_text("media", encoding="utf-8")
            payload = json.loads(youtube_chat_line(0, "raw").decode("utf-8"))
            payload["isLive"] = True
            chat_file.write_text(json.dumps(payload), encoding="utf-8")
            state = StateStore(config.db_path)
            state.upsert_detected(stream)
            manager = DownloadManager(config, state, probe=None)  # type: ignore[arg-type]
            manager.render_live_chat_video_process = AsyncMock()  # type: ignore[method-assign]
            try:
                rendered = await manager.render_live_chat_video(
                    stream,
                    media_file,
                    chat_file,
                    1,
                )
            finally:
                state.close()

        self.assertFalse(rendered)
        manager.render_live_chat_video_process.assert_not_awaited()  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
