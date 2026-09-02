from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import logging
import math
import os
from pathlib import Path
from threading import Lock
import tempfile
import time
from typing import Mapping, Sequence


LIVE_EDGE_FRAGMENT_MARGIN = 2
DOWNLOAD_PROGRESS_STALE_SECONDS = 30
DOWNLOAD_PROGRESS_WRITE_INTERVAL_SECONDS = 1.0
DOWNLOAD_PROGRESS_FILENAME = "live-download-progress.json"
DOWNLOAD_PROGRESS_FILE_VERSION = 1
MAX_DOWNLOAD_PROGRESS_FILE_BYTES = 1024 * 1024
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DownloadTrackProgress:
    track: str
    fragment_index: int
    fragment_count: int
    progress: float
    lag_fragments: int
    caught_up: bool
    updated_at: float
    segment_index: int = 0
    stale: bool = False


DownloadProgressSnapshot = dict[str, tuple[DownloadTrackProgress, ...]]


_DOWNLOAD_PROGRESS: DownloadProgressSnapshot = {}
_DOWNLOAD_PROGRESS_LOCK = Lock()
_DOWNLOAD_PROGRESS_LAST_WRITE: dict[Path, float] = {}


def download_progress_path(state_dir: Path) -> Path:
    return Path(state_dir) / DOWNLOAD_PROGRESS_FILENAME


def record_download_progress(
    video_id: str,
    fragments: Mapping[str, tuple[int, int]],
    *,
    segment_index: int = 0,
    updated_at: float | None = None,
    progress_file: Path | None = None,
) -> tuple[DownloadTrackProgress, ...]:
    """Publish the latest yt-dlp fragment positions for a live recording.

    Normal downloads publish semantic ``video``/``audio`` keys from the
    internal progress template. Numeric ``1``/``2`` contexts remain as a
    compatibility fallback for yt-dlp's usual requested-format order. An
    unprefixed progress line represents one combined media download.
    """

    observed: list[tuple[str, int, int]] = []
    known_tracks = {"0", "video", "audio", "media"}
    prefixed = any(context not in known_tracks for context in fragments)
    for context, values in fragments.items():
        try:
            fragment_index, fragment_count = (int(values[0]), int(values[1]))
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
        if fragment_index < 0 or fragment_count <= 0:
            continue
        if context in {"video", "audio", "media"}:
            track = context
        elif prefixed:
            track = {"1": "video", "2": "audio"}.get(
                context,
                f"media-{context}",
            )
        else:
            track = "media"
        observed.append(
            (track, min(fragment_index, fragment_count), fragment_count)
        )

    order = {"video": 0, "audio": 1, "media": 2}
    observed.sort(key=lambda item: (order.get(item[0], 3), item[0]))
    signature = tuple(observed)
    now = time.time() if updated_at is None else float(updated_at)

    with _DOWNLOAD_PROGRESS_LOCK:
        current = _DOWNLOAD_PROGRESS.get(video_id, ())
        current_signature = tuple(
            (item.track, item.fragment_index, item.fragment_count)
            for item in current
        )
        current_segment = current[0].segment_index if current else None
        if current_signature == signature and current_segment == segment_index:
            return current
        current_by_track = {item.track: item for item in current}

        progress = tuple(
            DownloadTrackProgress(
                track=track,
                fragment_index=fragment_index,
                fragment_count=fragment_count,
                progress=min(1.0, max(0.0, fragment_index / fragment_count)),
                lag_fragments=max(0, fragment_count - fragment_index),
                caught_up=(
                    fragment_count - fragment_index
                ) <= LIVE_EDGE_FRAGMENT_MARGIN,
                updated_at=(
                    current_by_track[track].updated_at
                    if current_segment == segment_index
                    and track in current_by_track
                    and current_by_track[track].fragment_index == fragment_index
                    and current_by_track[track].fragment_count == fragment_count
                    else now
                ),
                segment_index=segment_index,
            )
            for track, fragment_index, fragment_count in observed
        )
        if progress:
            _DOWNLOAD_PROGRESS[video_id] = progress
        else:
            _DOWNLOAD_PROGRESS.pop(video_id, None)
        if progress_file is not None:
            progress_file = Path(progress_file)
            last_write = _DOWNLOAD_PROGRESS_LAST_WRITE.get(progress_file, 0.0)
            write_due = (
                not current
                or {item.track for item in current}
                != {item.track for item in progress}
                or time.monotonic() - last_write
                >= DOWNLOAD_PROGRESS_WRITE_INTERVAL_SECONDS
            )
            if write_due:
                _write_download_progress_file(progress_file, _DOWNLOAD_PROGRESS)
                _DOWNLOAD_PROGRESS_LAST_WRITE[progress_file] = time.monotonic()
        return progress


def download_progress_for(
    video_id: str,
    *,
    segment_index: int | None = None,
    snapshot: Mapping[str, Sequence[DownloadTrackProgress]] | None = None,
) -> list[DownloadTrackProgress]:
    if snapshot is not None:
        progress = list(snapshot.get(video_id, ()))
    else:
        with _DOWNLOAD_PROGRESS_LOCK:
            progress = list(_DOWNLOAD_PROGRESS.get(video_id, ()))
    if segment_index is None:
        return progress
    return [item for item in progress if item.segment_index == segment_index]


def clear_download_progress(
    video_id: str,
    *,
    progress_file: Path | None = None,
) -> None:
    with _DOWNLOAD_PROGRESS_LOCK:
        changed = _DOWNLOAD_PROGRESS.pop(video_id, None) is not None
        if progress_file is not None:
            progress_file = Path(progress_file)
        if progress_file is not None and (changed or progress_file.exists()):
            _write_download_progress_file(progress_file, _DOWNLOAD_PROGRESS)
            _DOWNLOAD_PROGRESS_LAST_WRITE[progress_file] = time.monotonic()


def clear_all_download_progress(*, progress_file: Path | None = None) -> None:
    """Clear runtime telemetry. Primarily useful when resetting process state."""

    with _DOWNLOAD_PROGRESS_LOCK:
        _DOWNLOAD_PROGRESS.clear()
        if progress_file is not None:
            _write_download_progress_file(progress_file, _DOWNLOAD_PROGRESS)
            _DOWNLOAD_PROGRESS_LAST_WRITE[Path(progress_file)] = time.monotonic()
        else:
            _DOWNLOAD_PROGRESS_LAST_WRITE.clear()


def flush_download_progress(progress_file: Path) -> None:
    with _DOWNLOAD_PROGRESS_LOCK:
        _write_download_progress_file(progress_file, _DOWNLOAD_PROGRESS)
        _DOWNLOAD_PROGRESS_LAST_WRITE[Path(progress_file)] = time.monotonic()


def download_progress_revision(
    video_ids: list[str],
    *,
    segment_indexes: Mapping[str, int] | None = None,
    snapshot: Mapping[str, Sequence[DownloadTrackProgress]] | None = None,
) -> str:
    if snapshot is None:
        with _DOWNLOAD_PROGRESS_LOCK:
            selected = {
                video_id: tuple(_DOWNLOAD_PROGRESS.get(video_id, ()))
                for video_id in video_ids
            }
    else:
        selected = {
            video_id: tuple(snapshot.get(video_id, ()))
            for video_id in video_ids
        }
    rows = [
        (
            video_id,
            item.segment_index,
            item.track,
            item.fragment_index,
            item.fragment_count,
            item.updated_at,
            item.stale,
        )
        for video_id in video_ids
        for item in selected.get(video_id, ())
        if segment_indexes is None
        or item.segment_index == segment_indexes.get(video_id)
    ]
    if not rows:
        return ""
    payload = ";".join(
        f"{video_id}:{segment}:{track}:{index}:{count}:{updated_at:.6f}:{int(stale)}"
        for video_id, segment, track, index, count, updated_at, stale in rows
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def load_download_progress(
    progress_file: Path,
    *,
    current_time: float | None = None,
) -> DownloadProgressSnapshot:
    """Read an immutable telemetry snapshot shared by daemon and web processes."""

    try:
        if progress_file.stat().st_size > MAX_DOWNLOAD_PROGRESS_FILE_BYTES:
            return {}
        payload = json.loads(progress_file.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("version") != DOWNLOAD_PROGRESS_FILE_VERSION:
        return {}
    streams = payload.get("streams")
    if not isinstance(streams, dict):
        return {}

    result: DownloadProgressSnapshot = {}
    now = time.time() if current_time is None else float(current_time)
    for video_id, stream_payload in streams.items():
        if not isinstance(video_id, str) or not isinstance(stream_payload, dict):
            continue
        try:
            segment_index = int(stream_payload.get("segment_index", 0))
        except (TypeError, ValueError, OverflowError):
            continue
        raw_tracks = stream_payload.get("tracks")
        if not isinstance(raw_tracks, list):
            continue
        tracks: list[DownloadTrackProgress] = []
        for raw_track in raw_tracks:
            if not isinstance(raw_track, dict):
                continue
            track = raw_track.get("track")
            try:
                fragment_index = int(raw_track.get("fragment_index"))
                fragment_count = int(raw_track.get("fragment_count"))
                updated_at = float(raw_track.get("updated_at"))
            except (TypeError, ValueError, OverflowError):
                continue
            if (
                not isinstance(track, str)
                or not track
                or fragment_index < 0
                or fragment_count <= 0
                or not math.isfinite(updated_at)
            ):
                continue
            fragment_index = min(fragment_index, fragment_count)
            tracks.append(
                DownloadTrackProgress(
                    track=track,
                    fragment_index=fragment_index,
                    fragment_count=fragment_count,
                    progress=fragment_index / fragment_count,
                    lag_fragments=max(0, fragment_count - fragment_index),
                    caught_up=(
                        fragment_count - fragment_index
                    ) <= LIVE_EDGE_FRAGMENT_MARGIN,
                    updated_at=updated_at,
                    segment_index=segment_index,
                    stale=(
                        now - updated_at
                    ) >= DOWNLOAD_PROGRESS_STALE_SECONDS,
                )
            )
        if tracks:
            order = {"video": 0, "audio": 1, "media": 2}
            tracks.sort(key=lambda item: (order.get(item.track, 3), item.track))
            result[video_id] = tuple(tracks)
    return result


def _write_download_progress_file(
    progress_file: Path,
    snapshot: Mapping[str, Sequence[DownloadTrackProgress]],
) -> None:
    temporary_path: Path | None = None
    try:
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        if not snapshot:
            progress_file.unlink(missing_ok=True)
            return
        streams = {
            video_id: {
                "segment_index": items[0].segment_index,
                "tracks": [
                    {
                        "track": item.track,
                        "fragment_index": item.fragment_index,
                        "fragment_count": item.fragment_count,
                        "updated_at": item.updated_at,
                    }
                    for item in items
                ],
            }
            for video_id, items in snapshot.items()
            if items
        }
        payload = {
            "version": DOWNLOAD_PROGRESS_FILE_VERSION,
            "streams": streams,
        }
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=progress_file.parent,
            prefix=f".{progress_file.name}.{os.getpid()}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(payload, temporary, separators=(",", ":"), sort_keys=True)
        os.replace(temporary_path, progress_file)
    except OSError as exc:
        LOGGER.warning(
            "Unable to persist live download progress to %s: %s",
            progress_file,
            exc,
        )
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
