from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import sqlite3
from datetime import datetime, timezone

from .models import LiveStream


@dataclass(frozen=True, slots=True)
class StreamRecord:
    video_id: str
    title: str
    channel: str
    url: str
    status: str
    segment_index: int
    first_seen_at: str
    updated_at: str
    last_started_at: str | None
    last_exit_at: str | None
    exit_code: int | None


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


class StateStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

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
                status TEXT NOT NULL,
                segment_index INTEGER NOT NULL DEFAULT 1,
                first_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_started_at TEXT,
                last_exit_at TEXT,
                exit_code INTEGER
            )
            """
        )
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
        self._ensure_watermark_progress_columns()
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watermark_copies_video_source
            ON watermark_copies (video_id, source_name, created_at)
            """
        )
        self.conn.commit()

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

    def mark_stale_downloads_interrupted(self) -> None:
        now = utc_now()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'interrupted', updated_at = ?
            WHERE status = 'downloading'
            """,
            (now,),
        )
        self.conn.commit()

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
                    video_id, title, channel, url, status, segment_index,
                    first_seen_at, updated_at
                ) VALUES (?, ?, ?, ?, 'detected', 1, ?, ?)
                """,
                (
                    stream.video_id,
                    stream.title,
                    stream.channel,
                    stream.url,
                    now,
                    now,
                ),
            )
        else:
            status = "detected" if existing.status == "ended" and stream.is_live else existing.status
            self.conn.execute(
                """
                UPDATE streams
                SET title = ?, channel = ?, url = ?, status = ?, updated_at = ?
                WHERE video_id = ?
                """,
                (
                    stream.title,
                    stream.channel,
                    stream.url,
                    status,
                    now,
                    stream.video_id,
                ),
            )
        self.conn.commit()
        record = self.get_stream(stream.video_id)
        assert record is not None
        return record

    def mark_downloading(self, stream: LiveStream, segment_index: int) -> None:
        now = utc_now()
        self.upsert_detected(stream)
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'downloading',
                segment_index = ?,
                last_started_at = ?,
                updated_at = ?,
                exit_code = NULL
            WHERE video_id = ?
            """,
            (segment_index, now, now, stream.video_id),
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
        self.conn.commit()

    def mark_exited(self, video_id: str, exit_code: int) -> None:
        now = utc_now()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'checking_after_exit',
                last_exit_at = ?,
                updated_at = ?,
                exit_code = ?
            WHERE video_id = ?
            """,
            (now, now, exit_code, video_id),
        )
        self.conn.commit()

    def mark_ended(self, video_id: str) -> None:
        now = utc_now()
        self.conn.execute(
            """
            UPDATE streams
            SET status = 'ended', updated_at = ?
            WHERE video_id = ?
            """,
            (now, video_id),
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
        self.conn.commit()

    def bump_segment_index(self, video_id: str) -> int:
        record = self.get_stream(video_id)
        next_segment = (record.segment_index if record else 1) + 1
        self.set_segment_index(video_id, next_segment)
        return next_segment

    def get_stream(self, video_id: str) -> StreamRecord | None:
        row = self.conn.execute(
            """
            SELECT video_id, title, channel, url, status, segment_index,
                   first_seen_at, updated_at, last_started_at, last_exit_at, exit_code
            FROM streams
            WHERE video_id = ?
            """,
            (video_id,),
        ).fetchone()
        if row is None:
            return None
        return _record_from_row(row)

    def list_streams(self, limit: int = 100) -> list[StreamRecord]:
        rows = self.conn.execute(
            """
            SELECT video_id, title, channel, url, status, segment_index,
                   first_seen_at, updated_at, last_started_at, last_exit_at, exit_code
            FROM streams
            ORDER BY updated_at DESC, first_seen_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_record_from_row(row) for row in rows]

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


def _watermark_record_from_row(row: sqlite3.Row) -> WatermarkCopyRecord:
    values: dict[str, Any] = dict(row)
    return WatermarkCopyRecord(**values)
