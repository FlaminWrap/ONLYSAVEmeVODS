from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import hashlib
import re
import sqlite3
from datetime import datetime, timezone

from .models import LiveStream


LEGACY_KICK_STREAM_ID_RE = re.compile(
    r"^kick:.+\s\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$"
)
POST_PROCESSING_KINDS = frozenset(
    {"twitch_ad_repair", "transcription", "content_events", "chat_render"}
)
POST_PROCESSING_STATUSES = frozenset({"pending", "running", "done", "failed"})


@dataclass(frozen=True, slots=True)
class StreamRecord:
    video_id: str
    title: str
    channel: str
    url: str
    platform: str
    source: str
    youtube_video_format_id: str
    youtube_video_codec: str
    youtube_video_format_selector: str
    status: str
    segment_index: int
    first_seen_at: str
    updated_at: str
    last_started_at: str | None
    last_exit_at: str | None
    exit_code: int | None
    recording_kind: str = "live"
    youtube_stale_media_sequence: int | None = None
    youtube_stale_edge_at: str = ""
    youtube_stale_detected_at: str = ""


@dataclass(frozen=True, slots=True)
class StreamEventRecord:
    event_id: int
    video_id: str
    level: str
    message: str
    segment_index: int | None
    created_at: str


@dataclass(frozen=True, slots=True)
class WatermarkCopyRecord:
    copy_id: str
    video_id: str
    source_name: str
    output_name: str
    recipient_label: str
    status: str
    message: str
    error: str
    phase: str
    progress: float | None
    created_at: str
    updated_at: str
    started_at: str | None
    finished_at: str | None


@dataclass(frozen=True, slots=True)
class PostProcessingJobRecord:
    job_id: str
    video_id: str
    kind: str
    segment_index: int
    channel: str
    media_path: str
    chat_path: str
    timing_path: str
    status: str
    attempts: int
    error: str
    created_at: str
    updated_at: str


class StateStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        try:
            self._migrate()
        except Exception:
            self.conn.close()
            raise

    def close(self) -> None:
        self.conn.close()

    def _migrate(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS streams (
                video_id TEXT PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                channel TEXT NOT NULL DEFAULT '',
                url TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'youtube',
                source TEXT NOT NULL DEFAULT '',
                youtube_video_format_id TEXT NOT NULL DEFAULT '',
                youtube_video_codec TEXT NOT NULL DEFAULT '',
                youtube_video_format_selector TEXT NOT NULL DEFAULT '',
                youtube_stale_media_sequence INTEGER,
                youtube_stale_edge_at TEXT NOT NULL DEFAULT '',
                youtube_stale_detected_at TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                segment_index INTEGER NOT NULL DEFAULT 1,
                first_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_started_at TEXT,
                last_exit_at TEXT,
                exit_code INTEGER,
                recording_kind TEXT NOT NULL DEFAULT 'live'
            )
            """
        )
        self._ensure_stream_source_columns()
        recording_kind_added = self._ensure_stream_recording_kind_column()
        self._ensure_stream_video_format_columns()
        self._ensure_stream_stale_live_columns()
        self._mark_legacy_kick_detections_ended()
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stream_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT NOT NULL,
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL,
                segment_index INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        if recording_kind_added:
            self._backfill_legacy_vod_recording_kinds()
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watermark_copies (
                copy_id TEXT PRIMARY KEY,
                video_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                output_name TEXT NOT NULL,
                recipient_label TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                phase TEXT NOT NULL DEFAULT '',
                progress REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS post_processing_jobs (
                job_id TEXT PRIMARY KEY,
                video_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                segment_index INTEGER NOT NULL DEFAULT 1,
                channel TEXT NOT NULL DEFAULT '',
                media_path TEXT NOT NULL DEFAULT '',
                chat_path TEXT NOT NULL DEFAULT '',
                timing_path TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (video_id, kind, segment_index, media_path)
            )
            """
        )
        self._ensure_watermark_progress_columns()
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stream_events_video_created
            ON stream_events (video_id, created_at DESC, event_id DESC)
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watermark_copies_video_source
            ON watermark_copies (video_id, source_name, created_at)
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_post_processing_status_created
            ON post_processing_jobs (status, created_at, job_id)
            """
        )
        self.conn.commit()

    def _ensure_stream_source_columns(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(streams)").fetchall()
        columns = {str(row[1]) for row in rows}
        if "platform" not in columns:
            self.conn.execute(
                "ALTER TABLE streams "
                "ADD COLUMN platform TEXT NOT NULL DEFAULT 'youtube'"
            )
        if "source" not in columns:
            self.conn.execute(
                "ALTER TABLE streams "
                "ADD COLUMN source TEXT NOT NULL DEFAULT ''"
            )

    def _ensure_stream_recording_kind_column(self) -> bool:
        rows = self.conn.execute("PRAGMA table_info(streams)").fetchall()
        columns = {str(row[1]) for row in rows}
        if "recording_kind" not in columns:
            self.conn.execute(
                "ALTER TABLE streams "
                "ADD COLUMN recording_kind TEXT NOT NULL DEFAULT 'live'"
            )
            return True
        return False

    def _backfill_legacy_vod_recording_kinds(self) -> None:
        self.conn.execute(
            """
            UPDATE streams
            SET recording_kind = 'vod'
            WHERE EXISTS (
                SELECT 1
                FROM stream_events
                WHERE stream_events.video_id = streams.video_id
                  AND (
                      stream_events.message LIKE '%VOD download%'
                      OR stream_events.message IN ('Added VOD stream', 'Added manual VOD')
                  )
            )
            """
        )

    def _ensure_stream_video_format_columns(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(streams)").fetchall()
        columns = {str(row[1]) for row in rows}
        for name in (
            "youtube_video_format_id",
            "youtube_video_codec",
            "youtube_video_format_selector",
        ):
            if name not in columns:
                self.conn.execute(
                    f"ALTER TABLE streams ADD COLUMN {name} TEXT NOT NULL DEFAULT ''"
                )

    def _ensure_stream_stale_live_columns(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(streams)").fetchall()
        columns = {str(row[1]) for row in rows}
        if "youtube_stale_media_sequence" not in columns:
            self.conn.execute(
                "ALTER TABLE streams ADD COLUMN youtube_stale_media_sequence INTEGER"
            )
        for name in ("youtube_stale_edge_at", "youtube_stale_detected_at"):
            if name not in columns:
                self.conn.execute(
                    f"ALTER TABLE streams ADD COLUMN {name} TEXT NOT NULL DEFAULT ''"
                )

    def _mark_legacy_kick_detections_ended(self) -> None:
        rows = self.conn.execute(
            """
            SELECT video_id, title
            FROM streams
            WHERE platform = 'kick' AND status = 'detected'
            """
        ).fetchall()
        legacy_video_ids = [
            str(row["video_id"])
            for row in rows
            if (
                LEGACY_KICK_STREAM_ID_RE.fullmatch(str(row["video_id"]))
                and str(row["video_id"]) == f"kick:{str(row['title']).strip()}"
            )
        ]
        if not legacy_video_ids:
            return
        now = utc_now()
        self.conn.executemany(
            """
            UPDATE streams
            SET status = 'ended', updated_at = ?
            WHERE video_id = ? AND status = 'detected'
            """,
            [(now, video_id) for video_id in legacy_video_ids],
        )

    def _ensure_watermark_progress_columns(self) -> None:
        rows = self.conn.execute("PRAGMA table_info(watermark_copies)").fetchall()
        columns = {str(row[1]) for row in rows}
        if "phase" not in columns:
            self.conn.execute(
                "ALTER TABLE watermark_copies "
                "ADD COLUMN phase TEXT NOT NULL DEFAULT ''"
            )
        if "progress" not in columns:
            self.conn.execute(
                "ALTER TABLE watermark_copies "
                "ADD COLUMN progress REAL"
            )

    def reconcile_stale_downloads(self) -> None:
        now = utc_now()
        file_operation_rows = self.conn.execute(
            """
            SELECT video_id, segment_index, status
            FROM streams
            WHERE status IN ('deleting', 'cleaning_fragments')
            """
        ).fetchall()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'ended', updated_at = ?
            WHERE status IN ('deleting', 'cleaning_fragments')
            """,
            (now,),
        )
        for row in file_operation_rows:
            self._insert_stream_event(
                row["video_id"],
                f"Recovered interrupted file operation ({row['status']}) after service restart",
                level="warning",
                segment_index=int(row["segment_index"]),
                created_at=now,
            )
        live_rows = self.conn.execute(
            """
            SELECT video_id, segment_index, status
            FROM streams
            WHERE recording_kind = 'live'
              AND status IN ('downloading', 'waiting_retry')
            """
        ).fetchall()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'checking_after_exit',
                last_exit_at = COALESCE(last_exit_at, ?),
                updated_at = ?
            WHERE recording_kind = 'live'
              AND status IN ('downloading', 'waiting_retry')
            """,
            (now, now),
        )
        for row in live_rows:
            self._insert_stream_event(
                row["video_id"],
                (
                    "Recovering active download after service restart"
                    if str(row["status"]) == "downloading"
                    else "Recovering pending download retry after service restart"
                ),
                level="warning",
                segment_index=int(row["segment_index"]),
                created_at=now,
            )
        vod_rows = self.conn.execute(
            """
            SELECT video_id, segment_index
            FROM streams
            WHERE recording_kind = 'vod' AND status = 'downloading'
            """
        ).fetchall()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'interrupted', updated_at = ?
            WHERE recording_kind = 'vod' AND status = 'downloading'
            """,
            (now,),
        )
        for row in vod_rows:
            self._insert_stream_event(
                row["video_id"],
                "Interrupted VOD download after service restart",
                level="warning",
                segment_index=int(row["segment_index"]),
                created_at=now,
            )
        self.conn.commit()

    def mark_stale_downloads_interrupted(self) -> None:
        """Compatibility wrapper for callers using the old startup API."""
        self.reconcile_stale_downloads()

    def mark_stale_watermarks_interrupted(self) -> None:
        now = utc_now()
        self.conn.execute(
            """
            UPDATE watermark_copies
            SET status = 'interrupted',
                message = 'Interrupted before completion',
                phase = 'Interrupted',
                progress = NULL,
                updated_at = ?,
                finished_at = ?
            WHERE status IN ('queued', 'running')
            """,
            (now, now),
        )
        self.conn.commit()

    def upsert_detected(self, stream: LiveStream) -> StreamRecord:
        now = utc_now()
        existing = self.get_stream(stream.video_id)
        if existing is None:
            self.conn.execute(
                """
                INSERT INTO streams (
                    video_id, title, channel, url, platform, source, status, segment_index,
                    first_seen_at, updated_at, recording_kind
                ) VALUES (?, ?, ?, ?, ?, ?, 'detected', 1, ?, ?, 'live')
                """,
                (
                    stream.video_id,
                    stream.title,
                    stream.channel,
                    stream.url,
                    stream.platform,
                    stream.source,
                    now,
                    now,
                ),
            )
            self._insert_stream_event(
                stream.video_id,
                "Detected live stream",
                segment_index=1,
                created_at=now,
            )
        else:
            should_reopen = (
                existing.status == "ended"
                and stream.is_live
                and not existing.youtube_stale_detected_at
            )
            status = "detected" if should_reopen else existing.status
            self.conn.execute(
                """
                UPDATE streams
                SET title = ?, channel = ?, url = ?, platform = ?, source = ?,
                    status = ?, updated_at = ?, recording_kind = 'live'
                WHERE video_id = ?
                """,
                (
                    stream.title,
                    stream.channel,
                    stream.url,
                    stream.platform,
                    stream.source,
                    status,
                    now,
                    stream.video_id,
                ),
            )
            if should_reopen:
                self._insert_stream_event(
                    stream.video_id,
                    "Detected stream live again",
                    segment_index=existing.segment_index,
                    created_at=now,
                )
        self.conn.commit()
        record = self.get_stream(stream.video_id)
        assert record is not None
        return record

    def mark_youtube_stale_live(
        self,
        video_id: str,
        *,
        media_sequence: int | None,
        edge_at: str,
    ) -> None:
        now = utc_now()
        record = self.get_stream(video_id)
        self.conn.execute(
            """
            UPDATE streams
            SET youtube_stale_media_sequence = ?,
                youtube_stale_edge_at = ?,
                youtube_stale_detected_at = ?,
                status = 'stalled',
                updated_at = ?
            WHERE video_id = ?
            """,
            (media_sequence, edge_at, now, now, video_id),
        )
        detail = f" sequence={media_sequence}" if media_sequence is not None else ""
        if edge_at:
            detail += f" edge={edge_at}"
        self._insert_stream_event(
            video_id,
            f"YouTube live edge stalled; recording paused{detail}",
            level="warning",
            segment_index=record.segment_index if record is not None else None,
            created_at=now,
        )
        self.conn.commit()

    def clear_youtube_stale_live(self, video_id: str) -> None:
        now = utc_now()
        record = self.get_stream(video_id)
        self.conn.execute(
            """
            UPDATE streams
            SET youtube_stale_media_sequence = NULL,
                youtube_stale_edge_at = '',
                youtube_stale_detected_at = '',
                status = 'detected',
                updated_at = ?
            WHERE video_id = ?
            """,
            (now, video_id),
        )
        self._insert_stream_event(
            video_id,
            "YouTube live edge advanced again; resuming recording",
            segment_index=record.segment_index if record is not None else None,
            created_at=now,
        )
        self.conn.commit()

    def mark_downloading(self, stream: LiveStream, segment_index: int) -> bool:
        now = utc_now()
        self.upsert_detected(stream)
        cursor = self.conn.execute(
            """
            UPDATE streams
            SET status = 'downloading',
                segment_index = ?,
                last_started_at = ?,
                updated_at = ?,
                exit_code = NULL
            WHERE video_id = ?
              AND status NOT IN ('downloading', 'deleting', 'cleaning_fragments')
            """,
            (segment_index, now, now, stream.video_id),
        )
        if not cursor.rowcount:
            self.conn.commit()
            return False
        self._insert_stream_event(
            stream.video_id,
            f"Started download segment={segment_index:03d}",
            segment_index=segment_index,
            created_at=now,
        )
        self.conn.commit()
        return True

    def compare_and_set_stream_status(
        self,
        video_id: str,
        *,
        expected_status: str,
        new_status: str,
    ) -> bool:
        """Atomically claim a stream lifecycle transition across DB connections."""

        now = utc_now()
        cursor = self.conn.execute(
            """
            UPDATE streams
            SET status = ?, updated_at = ?
            WHERE video_id = ? AND status = ?
            """,
            (new_status, now, video_id, expected_status),
        )
        self.conn.commit()
        return bool(cursor.rowcount)

    def lock_youtube_video_format(
        self,
        video_id: str,
        *,
        format_id: str,
        codec: str,
        selector: str,
    ) -> StreamRecord:
        current = self.get_stream(video_id)
        if current is None:
            raise ValueError(f"stream is not recorded: {video_id}")
        if current.youtube_video_format_selector:
            return current

        now = utc_now()
        cursor = self.conn.execute(
            """
            UPDATE streams
            SET youtube_video_format_id = ?,
                youtube_video_codec = ?,
                youtube_video_format_selector = ?,
                updated_at = ?
            WHERE video_id = ? AND youtube_video_format_selector = ''
            """,
            (format_id, codec, selector, now, video_id),
        )
        if cursor.rowcount:
            self._insert_stream_event(
                video_id,
                "Locked YouTube video format "
                f"id={format_id} codec={codec} selector={selector}",
                segment_index=current.segment_index,
                created_at=now,
            )
        self.conn.commit()
        record = self.get_stream(video_id)
        assert record is not None
        return record

    def update_youtube_video_format(
        self,
        video_id: str,
        *,
        format_id: str,
        codec: str,
        selector: str,
    ) -> StreamRecord:
        current = self.get_stream(video_id)
        if current is None:
            raise ValueError(f"stream is not recorded: {video_id}")
        if (
            current.youtube_video_format_id == format_id
            and current.youtube_video_codec == codec
            and current.youtube_video_format_selector == selector
        ):
            return current

        now = utc_now()
        self.conn.execute(
            """
            UPDATE streams
            SET youtube_video_format_id = ?,
                youtube_video_codec = ?,
                youtube_video_format_selector = ?,
                updated_at = ?
            WHERE video_id = ?
            """,
            (format_id, codec, selector, now, video_id),
        )
        self._insert_stream_event(
            video_id,
            "Changed preferred YouTube video format "
            f"id={current.youtube_video_format_id}->{format_id} "
            f"codec={current.youtube_video_codec}->{codec} "
            f"selector={current.youtube_video_format_selector}->{selector}",
            segment_index=current.segment_index,
            created_at=now,
        )
        self.conn.commit()
        record = self.get_stream(video_id)
        assert record is not None
        return record

    def upsert_vod_stream(
        self,
        stream: LiveStream,
        *,
        status: str = "ended",
        event_message: str = "Added VOD stream",
    ) -> StreamRecord:
        now = utc_now()
        existing = self.get_stream(stream.video_id)
        if existing is None:
            self.conn.execute(
                """
                INSERT INTO streams (
                    video_id, title, channel, url, platform, source, status, segment_index,
                    first_seen_at, updated_at, recording_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 'vod')
                """,
                (
                    stream.video_id,
                    stream.title,
                    stream.channel,
                    stream.url,
                    stream.platform,
                    stream.source,
                    status,
                    now,
                    now,
                ),
            )
        else:
            self.conn.execute(
                """
                UPDATE streams
                SET title = ?, channel = ?, url = ?, platform = ?, source = ?,
                    status = ?, updated_at = ?, recording_kind = 'vod'
                WHERE video_id = ?
                """,
                (
                    stream.title,
                    stream.channel,
                    stream.url,
                    stream.platform,
                    stream.source,
                    status,
                    now,
                    stream.video_id,
                ),
            )
        self._insert_stream_event(
            stream.video_id,
            event_message,
            segment_index=1,
            created_at=now,
        )
        self.conn.commit()
        record = self.get_stream(stream.video_id)
        assert record is not None
        return record

    def mark_vod_downloading(self, stream: LiveStream, *, message: str = "Started VOD download") -> None:
        now = utc_now()
        existing = self.get_stream(stream.video_id)
        if existing is None:
            self.upsert_vod_stream(
                stream,
                status="downloading",
                event_message=message,
            )
            return
        self.conn.execute(
            """
            UPDATE streams
            SET title = ?, channel = ?, url = ?, platform = ?, source = ?,
                status = 'downloading', segment_index = 1, last_started_at = ?,
                updated_at = ?, exit_code = NULL, recording_kind = 'vod'
            WHERE video_id = ?
            """,
            (
                stream.title,
                stream.channel,
                stream.url,
                stream.platform,
                stream.source,
                now,
                now,
                stream.video_id,
            ),
        )
        self._insert_stream_event(
            stream.video_id,
            message,
            segment_index=1,
            created_at=now,
        )
        self.conn.commit()

    def mark_vod_download_finished(self, video_id: str, *, message: str = "VOD download completed") -> None:
        now = utc_now()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'ended', last_exit_at = ?, updated_at = ?, exit_code = 0
            WHERE video_id = ?
            """,
            (now, now, video_id),
        )
        self._insert_stream_event(
            video_id,
            message,
            segment_index=1,
            created_at=now,
        )
        self.conn.commit()

    def mark_vod_download_failed(
        self,
        video_id: str,
        message: str,
        *,
        restore_status: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        now = utc_now()
        status = restore_status or "interrupted"
        self.conn.execute(
            """
            UPDATE streams
            SET status = ?, last_exit_at = ?, updated_at = ?, exit_code = ?
            WHERE video_id = ?
            """,
            (status, now, now, exit_code, video_id),
        )
        self._insert_stream_event(
            video_id,
            message,
            level="error",
            segment_index=1,
            created_at=now,
        )
        self.conn.commit()

    def mark_waiting_retry(self, video_id: str, exit_code: int | None = None) -> None:
        now = utc_now()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'waiting_retry', updated_at = ?, exit_code = ?
            WHERE video_id = ?
            """,
            (now, exit_code, video_id),
        )
        self._insert_stream_event(
            video_id,
            "Waiting to retry"
            + (f" after exit code {exit_code}" if exit_code is not None else ""),
            level="warning",
            created_at=now,
        )
        self.conn.commit()

    def mark_exited(self, video_id: str, exit_code: int) -> None:
        now = utc_now()
        record = self.get_stream(video_id)
        status = (
            "stalled"
            if record is not None and record.youtube_stale_detected_at
            else "checking_after_exit"
        )
        self.conn.execute(
            """
            UPDATE streams
            SET status = ?,
                last_exit_at = ?,
                updated_at = ?,
                exit_code = ?
            WHERE video_id = ?
            """,
            (status, now, now, exit_code, video_id),
        )
        self._insert_stream_event(
            video_id,
            (
                f"yt-dlp exited with code {exit_code}; monitoring stalled stream"
                if status == "stalled"
                else f"yt-dlp exited with code {exit_code}; running post-exit checks"
            ),
            level="warning" if exit_code else "info",
            segment_index=record.segment_index if record is not None else None,
            created_at=now,
        )
        self.conn.commit()

    def mark_ended(self, video_id: str) -> None:
        now = utc_now()
        record = self.get_stream(video_id)
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'ended', updated_at = ?
            WHERE video_id = ?
            """,
            (now, video_id),
        )
        self._insert_stream_event(
            video_id,
            "Marked stream ended",
            segment_index=record.segment_index if record is not None else None,
            created_at=now,
        )
        self.conn.commit()

    def set_segment_index(self, video_id: str, segment_index: int) -> None:
        now = utc_now()
        self.conn.execute(
            """
            UPDATE streams
            SET segment_index = ?, updated_at = ?
            WHERE video_id = ?
            """,
            (segment_index, now, video_id),
        )
        self._insert_stream_event(
            video_id,
            f"Switched to segment={segment_index:03d}",
            segment_index=segment_index,
            created_at=now,
        )
        self.conn.commit()

    def bump_segment_index(self, video_id: str) -> int:
        record = self.get_stream(video_id)
        next_segment = (record.segment_index if record else 1) + 1
        self.set_segment_index(video_id, next_segment)
        return next_segment

    def add_stream_event(
        self,
        video_id: str,
        message: str,
        *,
        level: str = "info",
        segment_index: int | None = None,
    ) -> None:
        if not video_id or not message.strip():
            return
        self._insert_stream_event(
            video_id,
            message.strip(),
            level=level,
            segment_index=segment_index,
            created_at=utc_now(),
        )
        self.conn.commit()

    def list_stream_events(
        self,
        video_ids: list[str],
        *,
        limit_per_stream: int = 8,
    ) -> dict[str, list[StreamEventRecord]]:
        events: dict[str, list[StreamEventRecord]] = {}
        if limit_per_stream <= 0:
            return {video_id: [] for video_id in video_ids}
        for video_id in video_ids:
            rows = self.conn.execute(
                """
                SELECT event_id, video_id, level, message, segment_index, created_at
                FROM stream_events
                WHERE video_id = ?
                ORDER BY event_id DESC
                LIMIT ?
                """,
                (video_id, limit_per_stream),
            ).fetchall()
            records = [_event_record_from_row(row) for row in rows]
            events[video_id] = list(reversed(records))
        return events

    def _insert_stream_event(
        self,
        video_id: str,
        message: str,
        *,
        level: str = "info",
        segment_index: int | None = None,
        created_at: str,
    ) -> None:
        normalized_level = level if level in {"debug", "info", "warning", "error"} else "info"
        self.conn.execute(
            """
            INSERT INTO stream_events (
                video_id, level, message, segment_index, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (video_id, normalized_level, message, segment_index, created_at),
        )
        self.conn.execute(
            """
            DELETE FROM stream_events
            WHERE video_id = ?
              AND event_id NOT IN (
                  SELECT event_id
                  FROM stream_events
                  WHERE video_id = ?
                  ORDER BY event_id DESC
                  LIMIT 200
              )
            """,
            (video_id, video_id),
        )

    def get_stream(self, video_id: str) -> StreamRecord | None:
        row = self.conn.execute(
            """
            SELECT video_id, title, channel, url, status, segment_index,
                   platform, source, recording_kind,
                   youtube_video_format_id, youtube_video_codec,
                   youtube_video_format_selector,
                   youtube_stale_media_sequence, youtube_stale_edge_at,
                   youtube_stale_detected_at,
                   first_seen_at, updated_at, last_started_at, last_exit_at, exit_code
            FROM streams
            WHERE video_id = ?
            """,
            (video_id,),
        ).fetchone()
        if row is None:
            return None
        return _record_from_row(row)

    def list_streams(self, limit: int | None = 100) -> list[StreamRecord]:
        query = """
            SELECT video_id, title, channel, url, status, segment_index,
                   platform, source, recording_kind,
                   youtube_video_format_id, youtube_video_codec,
                   youtube_video_format_selector,
                   youtube_stale_media_sequence, youtube_stale_edge_at,
                   youtube_stale_detected_at,
                   first_seen_at, updated_at, last_started_at, last_exit_at, exit_code
            FROM streams
            ORDER BY updated_at DESC, first_seen_at DESC
        """
        values: tuple[int, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            values = (limit,)
        rows = self.conn.execute(query, values).fetchall()
        return [_record_from_row(row) for row in rows]

    def list_streams_by_status(
        self,
        statuses: list[str],
        *,
        limit: int | None = 1000,
    ) -> list[StreamRecord]:
        normalized = [status.strip() for status in statuses if status.strip()]
        if not normalized:
            return []
        placeholders = ", ".join("?" for _status in normalized)
        query = f"""
            SELECT video_id, title, channel, url, status, segment_index,
                   platform, source, recording_kind,
                   youtube_video_format_id, youtube_video_codec,
                   youtube_video_format_selector,
                   youtube_stale_media_sequence, youtube_stale_edge_at,
                   youtube_stale_detected_at,
                   first_seen_at, updated_at, last_started_at, last_exit_at, exit_code
            FROM streams
            WHERE status IN ({placeholders})
            ORDER BY updated_at DESC, first_seen_at DESC
        """
        values: list[str | int] = list(normalized)
        if limit is not None:
            query += " LIMIT ?"
            values.append(limit)
        rows = self.conn.execute(query, values).fetchall()
        return [_record_from_row(row) for row in rows]

    def enqueue_post_processing_job(
        self,
        *,
        video_id: str,
        kind: str,
        segment_index: int,
        media_path: str | Path,
        channel: str = "",
        chat_path: str | Path = "",
        timing_path: str | Path = "",
        reset_terminal: bool = False,
    ) -> PostProcessingJobRecord:
        normalized_kind = kind.strip().casefold()
        if normalized_kind not in POST_PROCESSING_KINDS:
            raise ValueError(f"unsupported post-processing job kind: {kind}")
        normalized_video_id = video_id.strip()
        normalized_media_path = _path_text(media_path)
        if not normalized_video_id or not normalized_media_path:
            raise ValueError("video_id and media_path are required")
        normalized_segment = max(1, int(segment_index))
        identity = (
            f"{normalized_video_id}\0{normalized_kind}\0"
            f"{normalized_segment}\0{normalized_media_path}"
        )
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
        job_id = f"post-{normalized_kind}-{digest}"
        now = utc_now()
        self.conn.execute(
            """
            INSERT INTO post_processing_jobs (
                job_id, video_id, kind, segment_index, channel,
                media_path, chat_path, timing_path, status,
                attempts, error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?)
            ON CONFLICT (video_id, kind, segment_index, media_path) DO NOTHING
            """,
            (
                job_id,
                normalized_video_id,
                normalized_kind,
                normalized_segment,
                channel.strip(),
                normalized_media_path,
                _path_text(chat_path),
                _path_text(timing_path),
                now,
                now,
            ),
        )
        if reset_terminal:
            # A VOD redownload commonly reuses the same destination path and
            # therefore the same durable identity. Treat terminal rows as a
            # fresh processing request, while leaving work that is already
            # pending or running alone.
            self.conn.execute(
                """
                UPDATE post_processing_jobs
                SET channel = ?, chat_path = ?, timing_path = ?,
                    status = 'pending', attempts = 0, error = '', updated_at = ?
                WHERE video_id = ? AND kind = ? AND segment_index = ?
                  AND media_path = ? AND status IN ('done', 'failed')
                """,
                (
                    channel.strip(),
                    _path_text(chat_path),
                    _path_text(timing_path),
                    now,
                    normalized_video_id,
                    normalized_kind,
                    normalized_segment,
                    normalized_media_path,
                ),
            )
        self.conn.commit()
        row = self.conn.execute(
            """
            SELECT job_id, video_id, kind, segment_index, channel,
                   media_path, chat_path, timing_path, status, attempts,
                   error, created_at, updated_at
            FROM post_processing_jobs
            WHERE video_id = ? AND kind = ? AND segment_index = ? AND media_path = ?
            """,
            (
                normalized_video_id,
                normalized_kind,
                normalized_segment,
                normalized_media_path,
            ),
        ).fetchone()
        assert row is not None
        return _post_processing_record_from_row(row)

    def get_post_processing_job(
        self,
        job_id: str,
    ) -> PostProcessingJobRecord | None:
        row = self.conn.execute(
            """
            SELECT job_id, video_id, kind, segment_index, channel,
                   media_path, chat_path, timing_path, status, attempts,
                   error, created_at, updated_at
            FROM post_processing_jobs
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        return _post_processing_record_from_row(row) if row is not None else None

    def list_post_processing_jobs(
        self,
        *,
        video_id: str | None = None,
        statuses: list[str] | None = None,
        limit: int | None = 1000,
    ) -> list[PostProcessingJobRecord]:
        clauses: list[str] = []
        values: list[str | int] = []
        if video_id is not None:
            clauses.append("video_id = ?")
            values.append(video_id)
        if statuses is not None:
            normalized_statuses = [
                status.strip().casefold()
                for status in statuses
                if status.strip().casefold() in POST_PROCESSING_STATUSES
            ]
            if not normalized_statuses:
                return []
            placeholders = ", ".join("?" for _status in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            values.extend(normalized_statuses)
        query = """
            SELECT job_id, video_id, kind, segment_index, channel,
                   media_path, chat_path, timing_path, status, attempts,
                   error, created_at, updated_at
            FROM post_processing_jobs
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, job_id"
        if limit is not None:
            query += " LIMIT ?"
            values.append(max(0, int(limit)))
        rows = self.conn.execute(query, values).fetchall()
        return [_post_processing_record_from_row(row) for row in rows]

    def requeue_incomplete_post_processing_jobs(self) -> int:
        """Return crash-interrupted running jobs to the durable pending queue."""
        now = utc_now()
        cursor = self.conn.execute(
            """
            UPDATE post_processing_jobs
            SET status = 'pending',
                error = '',
                updated_at = ?
            WHERE status = 'running'
            """,
            (now,),
        )
        self.conn.commit()
        return max(0, cursor.rowcount)

    def mark_post_processing_job_running(self, job_id: str) -> bool:
        cursor = self.conn.execute(
            """
            UPDATE post_processing_jobs
            SET status = 'running', attempts = attempts + 1,
                error = '', updated_at = ?
            WHERE job_id = ? AND status = 'pending'
            """,
            (utc_now(), job_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def mark_post_processing_job_done(self, job_id: str) -> bool:
        cursor = self.conn.execute(
            """
            UPDATE post_processing_jobs
            SET status = 'done', error = '', updated_at = ?
            WHERE job_id = ? AND status = 'running'
            """,
            (utc_now(), job_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def mark_post_processing_job_failed(self, job_id: str, error: str) -> bool:
        cursor = self.conn.execute(
            """
            UPDATE post_processing_jobs
            SET status = 'failed', error = ?, updated_at = ?
            WHERE job_id = ? AND status = 'running'
            """,
            (error.strip()[:2000], utc_now(), job_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def replace_pending_post_processing_media_path(
        self,
        *,
        video_id: str,
        segment_index: int,
        old_path: str | Path,
        new_path: str | Path,
    ) -> int:
        normalized_segment = max(1, int(segment_index))
        normalized_old_path = _path_text(old_path)
        normalized_new_path = _path_text(new_path)
        if normalized_old_path == normalized_new_path:
            return 0

        changed = 0
        now = utc_now()
        with self.conn:
            rows = self.conn.execute(
                """
                SELECT job_id, kind, channel, chat_path, timing_path
                FROM post_processing_jobs
                WHERE video_id = ? AND segment_index = ?
                  AND media_path = ? AND status = 'pending'
                """,
                (video_id, normalized_segment, normalized_old_path),
            ).fetchall()
            for row in rows:
                existing = self.conn.execute(
                    """
                    SELECT job_id, status
                    FROM post_processing_jobs
                    WHERE video_id = ? AND kind = ? AND segment_index = ?
                      AND media_path = ?
                    """,
                    (
                        video_id,
                        str(row[1]),
                        normalized_segment,
                        normalized_new_path,
                    ),
                ).fetchone()
                if existing is None:
                    self.conn.execute(
                        """
                        UPDATE post_processing_jobs
                        SET media_path = ?, updated_at = ?
                        WHERE job_id = ? AND status = 'pending'
                        """,
                        (normalized_new_path, now, str(row[0])),
                    )
                else:
                    # The repaired-path identity may already exist after a
                    # restart or re-download. Merge the current pending work
                    # into it instead of violating the unique constraint.
                    existing_status = str(existing[1])
                    if existing_status in {"done", "failed"}:
                        self.conn.execute(
                            """
                            UPDATE post_processing_jobs
                            SET channel = ?, chat_path = ?, timing_path = ?,
                                status = 'pending', attempts = 0, error = '',
                                updated_at = ?
                            WHERE job_id = ?
                            """,
                            (
                                str(row[2]),
                                str(row[3]),
                                str(row[4]),
                                now,
                                str(existing[0]),
                            ),
                        )
                    elif existing_status == "pending":
                        self.conn.execute(
                            """
                            UPDATE post_processing_jobs
                            SET channel = ?, chat_path = ?, timing_path = ?,
                                updated_at = ?
                            WHERE job_id = ?
                            """,
                            (
                                str(row[2]),
                                str(row[3]),
                                str(row[4]),
                                now,
                                str(existing[0]),
                            ),
                        )
                    self.conn.execute(
                        "DELETE FROM post_processing_jobs WHERE job_id = ?",
                        (str(row[0]),),
                    )
                changed += 1
        return changed

    def delete_stream(self, video_id: str) -> bool:
        cursor = self.conn.execute(
            "DELETE FROM streams WHERE video_id = ?",
            (video_id,),
        )
        if cursor.rowcount:
            self.conn.execute(
                "DELETE FROM stream_events WHERE video_id = ?",
                (video_id,),
            )
            self.conn.execute(
                "DELETE FROM watermark_copies WHERE video_id = ?",
                (video_id,),
            )
            self.conn.execute(
                "DELETE FROM post_processing_jobs WHERE video_id = ?",
                (video_id,),
            )
        self.conn.commit()
        return cursor.rowcount > 0

    def create_watermark_copy(
        self,
        *,
        copy_id: str,
        video_id: str,
        source_name: str,
        output_name: str,
        recipient_label: str,
        message: str = "Queued",
    ) -> WatermarkCopyRecord:
        now = utc_now()
        self.conn.execute(
            """
            INSERT INTO watermark_copies (
                copy_id, video_id, source_name, output_name, recipient_label,
                status, message, error, phase, progress, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'queued', ?, '', 'Queued', 0.0, ?, ?)
            """,
            (
                copy_id,
                video_id,
                source_name,
                output_name,
                recipient_label,
                message,
                now,
                now,
            ),
        )
        self.conn.commit()
        record = self.get_watermark_copy(copy_id)
        assert record is not None
        return record

    def update_watermark_copy(
        self,
        copy_id: str,
        *,
        status: str | None = None,
        message: str | None = None,
        error: str | None = None,
        phase: str | None = None,
        progress: float | None = None,
        started: bool = False,
        finished: bool = False,
    ) -> None:
        current = self.get_watermark_copy(copy_id)
        if current is None:
            return
        now = utc_now()
        self.conn.execute(
            """
            UPDATE watermark_copies
            SET status = ?,
                message = ?,
                error = ?,
                phase = ?,
                progress = ?,
                updated_at = ?,
                started_at = ?,
                finished_at = ?
            WHERE copy_id = ?
            """,
            (
                status if status is not None else current.status,
                message if message is not None else current.message,
                error if error is not None else current.error,
                phase if phase is not None else current.phase,
                progress if progress is not None else current.progress,
                now,
                now if started else current.started_at,
                now if finished else current.finished_at,
                copy_id,
            ),
        )
        self.conn.commit()

    def get_watermark_copy(self, copy_id: str) -> WatermarkCopyRecord | None:
        row = self.conn.execute(
            """
            SELECT copy_id, video_id, source_name, output_name, recipient_label,
                   status, message, error, phase, progress, created_at, updated_at,
                   started_at, finished_at
            FROM watermark_copies
            WHERE copy_id = ?
            """,
            (copy_id,),
        ).fetchone()
        if row is None:
            return None
        return _watermark_record_from_row(row)

    def delete_watermark_copy(self, copy_id: str) -> bool:
        cursor = self.conn.execute(
            "DELETE FROM watermark_copies WHERE copy_id = ?",
            (copy_id,),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def list_watermark_copies(
        self,
        *,
        video_id: str | None = None,
        source_name: str | None = None,
        statuses: list[str] | None = None,
        limit: int = 500,
    ) -> list[WatermarkCopyRecord]:
        clauses: list[str] = []
        values: list[Any] = []
        if video_id is not None:
            clauses.append("video_id = ?")
            values.append(video_id)
        if source_name is not None:
            clauses.append("source_name = ?")
            values.append(source_name)
        if statuses:
            placeholders = ", ".join("?" for _status in statuses)
            clauses.append(f"status IN ({placeholders})")
            values.extend(statuses)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"""
            SELECT copy_id, video_id, source_name, output_name, recipient_label,
                   status, message, error, phase, progress, created_at, updated_at,
                   started_at, finished_at
            FROM watermark_copies
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*values, limit),
        ).fetchall()
        return [_watermark_record_from_row(row) for row in rows]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _record_from_row(row: sqlite3.Row) -> StreamRecord:
    values: dict[str, Any] = dict(row)
    return StreamRecord(**values)


def _event_record_from_row(row: sqlite3.Row) -> StreamEventRecord:
    values: dict[str, Any] = dict(row)
    return StreamEventRecord(**values)


def _watermark_record_from_row(row: sqlite3.Row) -> WatermarkCopyRecord:
    values: dict[str, Any] = dict(row)
    return WatermarkCopyRecord(**values)


def _post_processing_record_from_row(
    row: sqlite3.Row,
) -> PostProcessingJobRecord:
    values: dict[str, Any] = dict(row)
    return PostProcessingJobRecord(**values)


def _path_text(value: str | Path) -> str:
    text = str(value).strip()
    return text if text and text != "." else ""
