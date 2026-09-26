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
FRAGMENT_HIGH_WATER_DIRNAME = "live-fragment-high-water"
FRAGMENT_HIGH_WATER_FILE_VERSION = 1
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
    highest_fragment_index: int = 0
    highest_fragment_count: int = 0
    segment_index: int = 0
    stale: bool = False


DownloadProgressSnapshot = dict[str, tuple[DownloadTrackProgress, ...]]


_DOWNLOAD_PROGRESS: DownloadProgressSnapshot = {}
_DOWNLOAD_PROGRESS_LOCK = Lock()
_DOWNLOAD_PROGRESS_LAST_WRITE: dict[Path, float] = {}
_FRAGMENT_HIGH_WATER_CACHE: dict[Path, dict[str, tuple[int, int]]] = {}
_FRAGMENT_HIGH_WATER_DIRTY: set[Path] = set()


def download_progress_path(state_dir: Path) -> Path:
    return Path(state_dir) / DOWNLOAD_PROGRESS_FILENAME


def fragment_high_water_path(
    state_dir: Path,
    video_id: str,
    segment_index: int,
) -> Path:
    """Use one durable file per stream segment, without putting IDs in paths."""

    stream_hash = hashlib.sha256(video_id.encode("utf-8")).hexdigest()
    return (
        Path(state_dir)
        / FRAGMENT_HIGH_WATER_DIRNAME
        / f"{stream_hash}-segment-{segment_index:03d}.json"
    )


def fragment_high_water_for(
    video_id: str,
    segment_index: int,
    *,
    state_dir: Path,
) -> dict[str, tuple[int, int]]:
    """Return yt-dlp's highest reported index and count for each track.

    These are observations, not proof that the fragments were saved and not
    absolute YouTube media sequence numbers.
    """

    path = fragment_high_water_path(state_dir, video_id, segment_index)
    with _DOWNLOAD_PROGRESS_LOCK:
        if path not in _FRAGMENT_HIGH_WATER_CACHE:
            _FRAGMENT_HIGH_WATER_CACHE[path] = _load_fragment_high_water_file(
                path, video_id, segment_index
            )
        return dict(_FRAGMENT_HIGH_WATER_CACHE[path])


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
        current_by_track = {item.track: item for item in current}

        high_water_path = (
            fragment_high_water_path(Path(progress_file).parent, video_id, segment_index)
            if progress_file is not None
            else None
        )
        stored_highs: dict[str, tuple[int, int]] = {}
        if high_water_path is not None:
            if high_water_path not in _FRAGMENT_HIGH_WATER_CACHE:
                _FRAGMENT_HIGH_WATER_CACHE[high_water_path] = (
                    _load_fragment_high_water_file(
                        high_water_path, video_id, segment_index
                    )
                )
            stored_highs = _FRAGMENT_HIGH_WATER_CACHE[high_water_path]

        highs: dict[str, tuple[int, int]] = {}
        for track, fragment_index, fragment_count in observed:
            prior_index, prior_count = stored_highs.get(track, (0, 0))
            prior_current = current_by_track.get(track)
            if prior_current is not None and current_segment == segment_index:
                prior_index = max(prior_index, prior_current.highest_fragment_index)
                prior_count = max(prior_count, prior_current.highest_fragment_count)
            highs[track] = (
                max(fragment_index, prior_index),
                max(fragment_count, prior_count),
            )
        if high_water_path is not None:
            highs_changed = any(
                stored_highs.get(track) != value for track, value in highs.items()
            )
            if highs_changed:
                stored_highs.update(highs)
            if highs_changed or high_water_path in _FRAGMENT_HIGH_WATER_DIRTY:
                if _write_fragment_high_water_file(
                    high_water_path, video_id, segment_index, stored_highs
                ):
                    _FRAGMENT_HIGH_WATER_DIRTY.discard(high_water_path)
                else:
                    _FRAGMENT_HIGH_WATER_DIRTY.add(high_water_path)

        if (
            current_signature == signature
            and current_segment == segment_index
            and all(
                (item.highest_fragment_index, item.highest_fragment_count)
                == highs.get(item.track)
                for item in current
            )
        ):
            return current

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
                highest_fragment_index=highs[track][0],
                highest_fragment_count=highs[track][1],
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
        _FRAGMENT_HIGH_WATER_CACHE.clear()
        _FRAGMENT_HIGH_WATER_DIRTY.clear()
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
            item.highest_fragment_index,
            item.highest_fragment_count,
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
        f"{video_id}:{segment}:{track}:{index}:{count}:{highest_index}:{highest_count}:{updated_at:.6f}:{int(stale)}"
        for video_id, segment, track, index, count, highest_index, highest_count, updated_at, stale in rows
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
            try:
                highest_index = int(
                    raw_track.get("highest_fragment_index", fragment_index)
                )
                highest_count = int(
                    raw_track.get("highest_fragment_count", fragment_count)
                )
            except (TypeError, ValueError, OverflowError):
                highest_index, highest_count = fragment_index, fragment_count
            if (
                highest_index < fragment_index
                or highest_count < fragment_count
                or highest_index > highest_count
            ):
                highest_index, highest_count = fragment_index, fragment_count
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
                    highest_fragment_index=highest_index,
                    highest_fragment_count=highest_count,
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


def _load_fragment_high_water_file(
    path: Path,
    video_id: str,
    segment_index: int,
) -> dict[str, tuple[int, int]]:
    try:
        if path.stat().st_size > MAX_DOWNLOAD_PROGRESS_FILE_BYTES:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, ValueError):
        return {}
    if (
        not isinstance(payload, dict)
        or payload.get("version") != FRAGMENT_HIGH_WATER_FILE_VERSION
        or payload.get("video_id") != video_id
        or payload.get("segment_index") != segment_index
    ):
        return {}
    raw_tracks = payload.get("tracks")
    if not isinstance(raw_tracks, dict):
        return {}
    result: dict[str, tuple[int, int]] = {}
    for track, values in raw_tracks.items():
        if not isinstance(track, str) or not track or not isinstance(values, dict):
            continue
        index = values.get("highest_fragment_index")
        count = values.get("highest_fragment_count")
        if (
            type(index) is int
            and type(count) is int
            and 0 <= index <= count
            and count > 0
        ):
            result[track] = (index, count)
    return result


def _write_fragment_high_water_file(
    path: Path,
    video_id: str,
    segment_index: int,
    tracks: Mapping[str, tuple[int, int]],
) -> bool:
    temporary_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": FRAGMENT_HIGH_WATER_FILE_VERSION,
            "video_id": video_id,
            "segment_index": segment_index,
            "tracks": {
                track: {
                    "highest_fragment_index": index,
                    "highest_fragment_count": count,
                }
                for track, (index, count) in tracks.items()
            },
        }
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.{os.getpid()}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(payload, temporary, separators=(",", ":"), sort_keys=True)
        os.replace(temporary_path, path)
        return True
    except OSError as exc:
        LOGGER.warning("Unable to persist fragment high-water to %s: %s", path, exc)
        return False
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


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
                        "highest_fragment_index": item.highest_fragment_index,
                        "highest_fragment_count": item.highest_fragment_count,
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
