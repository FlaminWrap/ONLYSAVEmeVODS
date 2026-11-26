"""Record a growing Kick replay in order. The checkpoint is the commit record."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

from .youtube import YtDlpError


class ArchiveRetry(Exception):
    """An upstream condition that must never cause the cursor to skip ahead."""


class LocalStateError(Exception):
    """Local media cannot be reconciled with the committed checkpoint."""


def _epoch(value) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def checkpoint_path(output: Path) -> Path:
    return output.with_name(f".kick-archive-{output.name}.json")


def _sync_dir(directory):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_state(path, state, *, initial=False):
    with tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, prefix=".kick-archive-state-", delete=False
    ) as f:
        temp = Path(f.name)
        try:
            json.dump(state, f, separators=(",", ":"), allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
            if initial:
                try:
                    os.link(temp, path)
                except FileExistsError:
                    return
            else:
                os.replace(temp, path)
            _sync_dir(path.parent)
        finally:
            temp.unlink(missing_ok=True)


def initialize_checkpoint(output: Path, video_id: str, *, started_at: str = "") -> None:
    path = checkpoint_path(output)
    if path.exists():
        return
    match = re.fullmatch(r"segment-(\d+)\.ts\.part", output.name)
    if not match:
        raise LocalStateError("invalid segment output name")
    if output.exists() and output.stat().st_size:
        raise LocalStateError("existing media has no archive checkpoint")
    output.parent.mkdir(parents=True, exist_ok=True)
    _write_state(
        path,
        {
            "version": 1,
            "video_id": video_id,
            "segment_index": int(match[1]),
            "phase": "waiting_archive",
            "reason": "",
            "complete": False,
            "saved_segments": 0,
            "available_segments": 0,
            "bytes_written": 0,
            "saved_seconds": 0.0,
            "available_seconds": 0.0,
            "target_delay_seconds": 60.0,
            "delay_seconds": None,
            "started_at": _epoch(started_at),
            "updated_at": time.time(),
            "prefix_hash": "",
            "epoch_log_bytes": 0,
        },
        initial=True,
    )


@dataclass(frozen=True)
class Segment:
    url: str
    duration: float
    discontinuity: bool
    gap: bool
    byte_range: tuple[int, int] | None

    def digest(self, previous: str) -> str:
        # CDN host and authentication query can rotate; the replay-relative
        # media path and timeline identify the archived segment.
        item = (
            urlsplit(self.url).path,
            round(self.duration, 4),
            self.discontinuity,
            self.byte_range,
        )
        return hashlib.sha256(
            (previous + json.dumps(item, separators=(",", ":"))).encode()
        ).hexdigest()


@dataclass(frozen=True)
class Playlist:
    segments: list[Segment]
    endlist: bool
    target: float
    started_at: float | None


def parse_playlist(raw: str, url: str) -> Playlist:
    if not raw.lstrip().startswith("#EXTM3U"):
        raise ArchiveRetry("invalid_playlist")
    segments = []
    target, first_date, duration, span = 0.0, None, None, None
    discontinuity = gap = ended = False
    try:
        for line in map(str.strip, raw.splitlines()):
            if line.startswith("#EXT-X-MEDIA-SEQUENCE:") and int(line.partition(":")[2]) != 0:
                raise ArchiveRetry("archive_origin_missing")
            if line.startswith("#EXT-X-TARGETDURATION:"):
                target = float(line.partition(":")[2])
            elif line.startswith("#EXTINF:"):
                duration = float(line.partition(":")[2].partition(",")[0])
            elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:") and not segments:
                first_date = _epoch(line.partition(":")[2])
            elif line.startswith("#EXT-X-BYTERANGE:"):
                size, sep, offset = line.partition(":")[2].partition("@")
                span = (int(offset) if sep else None, int(size))
            elif line == "#EXT-X-DISCONTINUITY":
                discontinuity = True
            elif line == "#EXT-X-GAP":
                gap = True
            elif line == "#EXT-X-ENDLIST":
                ended = True
            elif (
                line.startswith("#EXT-X-KEY:") and line.split(":", 1)[1] != "METHOD=NONE"
            ) or line.startswith(("#EXT-X-MAP:", "#EXT-X-STREAM-INF:", "#EXT-X-SKIP:")):
                raise ArchiveRetry("unsupported_playlist")
            elif line and not line.startswith("#"):
                if duration is None or not math.isfinite(duration) or duration <= 0:
                    raise ArchiveRetry("invalid_segment_duration")
                resource = urljoin(url, line)
                if urlsplit(resource).scheme not in {"https", "http"}:
                    raise ArchiveRetry("invalid_media_url")
                if span:
                    start, size = span
                    if start is None:
                        if (
                            not segments
                            or segments[-1].url != resource
                            or not segments[-1].byte_range
                        ):
                            raise ArchiveRetry("invalid_range")
                        start = sum(segments[-1].byte_range)
                    if start < 0 or size <= 0:
                        raise ArchiveRetry("invalid_range")
                    span = (start, size)
                segments.append(Segment(resource, duration, discontinuity, gap, span))
                duration, span, discontinuity, gap = None, None, False, False
        if duration is not None or not segments or not math.isfinite(target) or target <= 0:
            raise ArchiveRetry("incomplete_playlist")
    except (ValueError, OverflowError):
        raise ArchiveRetry("invalid_playlist") from None
    return Playlist(segments, ended, target, first_date)


def _response(url, headers):
    return urlopen(Request(url, headers=headers), timeout=25)


def fetch_playlist(url, headers):
    with _response(url, headers) as response:
        data = response.read(16 * 1024 * 1024 + 1)
        if len(data) > 16 * 1024 * 1024:
            raise ArchiveRetry("oversized_playlist")
        try:
            return parse_playlist(data.decode("utf-8-sig"), response.geturl())
        except UnicodeError:
            raise ArchiveRetry("invalid_playlist") from None


def fetch_segment(segment, headers, temp):
    if segment.gap:
        raise ArchiveRetry("missing_segment")
    headers = dict(headers)
    if segment.byte_range:
        start, length = segment.byte_range
        headers["Range"] = f"bytes={start}-{start + length - 1}"
    with _response(segment.url, headers) as response, temp.open("wb") as f:
        expected = response.headers.get("Content-Length")
        if segment.byte_range and (
            response.status != 206
            or not re.fullmatch(
                rf"bytes {start}-{start + length - 1}/(?:[0-9]+|\*)",
                response.headers.get("Content-Range", ""),
            )
        ):
            raise ArchiveRetry("invalid_range_response")
        if expected is not None and not str(expected).isdigit():
            raise ArchiveRetry("invalid_response_length")
        size = 0
        # Validate TS sync across the entire response without buffering a broadcast.
        while block := response.read(188 * 4096):
            sync_offset = (-size) % 188
            sync = block[sync_offset::188]
            if sync != b"G" * len(sync):
                raise ArchiveRetry("invalid_segment")
            f.write(block)
            size += len(block)
        if (
            not size
            or size % 188
            or (expected is not None and size != int(expected))
            or (segment.byte_range and size != length)
        ):
            raise ArchiveRetry("truncated_segment")


def probe_segment(temp, segment, ffprobe):
    # Container byte counts alone cannot distinguish a truncated transport
    # stream with whole TS packets from a complete one.
    proc = subprocess.run(
        [
            ffprobe,
            "-v",
            "error",
            "-show_packets",
            "-show_streams",
            "-show_entries",
            "stream=index,codec_name,codec_type:packet=stream_index,dts_time,duration_time,flags",
            "-of",
            "json",
            "-f",
            "mpegts",
            str(temp),
        ],
        capture_output=True,
    )
    if proc.returncode or proc.stderr.strip():
        raise ArchiveRetry("damaged_media")
    try:
        data = json.loads(proc.stdout)
        streams = {stream["codec_type"]: stream for stream in data["streams"]}
        tracks = {}
        for kind in ("video", "audio"):
            stream = streams[kind]
            packets = [p for p in data["packets"] if p["stream_index"] == stream["index"]]
            if not packets or any("C" in p["flags"] for p in packets):
                raise ArchiveRetry("damaged_media")
            times = [float(p["dts_time"]) for p in packets]
            gaps = [_dts_delta(later, earlier) for earlier, later in zip(times, times[1:])]
            last_duration = float(packets[-1].get("duration_time", 0))
            cadence = statistics.median(gaps) if gaps else last_duration
            if (
                not all(map(math.isfinite, times))
                or not math.isfinite(cadence)
                or cadence <= 0
                or any(gap <= 0 or gap > max(0.12, cadence * 3) for gap in gaps)
            ):
                raise ArchiveRetry("packet_timing_gap")
            end = times[-1] + (last_duration if last_duration > 0 else cadence)
            duration = sum(gaps) + (last_duration if last_duration > 0 else cadence)
            # Container boundaries can differ by a few AAC/video packet periods.
            tolerance = max(0.12, min(0.25, cadence * 4))
            if abs(duration - segment.duration) > tolerance:
                raise ArchiveRetry("incomplete_media")
            tracks[kind] = {
                "codec": stream["codec_name"],
                "start": times[0],
                "end": end,
                "cadence": cadence,
                "packets": len(packets),
            }
        return tracks
    except (KeyError, TypeError, ValueError):
        raise ArchiveRetry("incomplete_media") from None


def _dts_delta(current, previous, transport_stream=True):
    delta = current - previous
    if transport_stream:
        wrap = 2**33 / 90000
        delta -= round(delta / wrap) * wrap
    return delta


def measure_file(path, ffprobe, *, input_format="mpegts"):
    # Stream packet metadata so long recordings stay bounded in memory. A
    # header/trailer duration alone is ambiguous past the 26.5-hour MPEG-TS timestamp wrap.
    tracks, streams = {}, {}
    with (
        tempfile.TemporaryFile() as errors,
        subprocess.Popen(
            [
                ffprobe,
                "-v",
                "error",
                "-show_packets",
                "-show_streams",
                "-show_entries",
                "packet=stream_index,dts_time,duration_time,flags:stream=index,codec_name,codec_type",
                "-of",
                "compact=p=1:nk=0",
                "-f",
                input_format,
                str(path),
            ],
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
        ) as proc,
    ):
        timer = threading.Timer(3600, proc.kill)
        timer.daemon = True
        timer.start()
        try:
            for line in proc.stdout:
                row_type, *columns = line.rstrip().split("|")
                if row_type not in {"packet", "stream"}:
                    continue
                row = dict(field.split("=", 1) for field in columns if "=" in field)
                if row_type == "stream":
                    streams[row["codec_type"]] = (row["index"], row["codec_name"])
                    continue
                idx = row["stream_index"]
                timestamp = float(row["dts_time"])
                duration = float(row.get("duration_time", 0))
                if (
                    "C" in row["flags"]
                    or not math.isfinite(timestamp)
                    or not math.isfinite(duration)
                ):
                    raise ValueError
                if idx not in tracks:
                    tracks[idx] = [0, timestamp, 0.0, duration]
                count, last, elapsed, previous_duration = tracks[idx]
                if count:
                    delta = _dts_delta(timestamp, last, input_format == "mpegts")
                    if delta <= 0 or delta > max(0.25, previous_duration * 3):
                        raise ValueError
                    elapsed += delta
                tracks[idx] = [count + 1, timestamp, elapsed, duration]
            if proc.wait() or errors.tell():
                raise ValueError
            result = {}
            for kind in ("video", "audio"):
                idx, codec = streams[kind]
                count, _, elapsed, duration = tracks[idx]
                result[kind] = (codec, count, elapsed + duration)
            return result
        except (KeyError, TypeError, ValueError):
            proc.kill()
            raise LocalStateError(
                "unable to verify audio and video in the completed archive"
            ) from None
        finally:
            timer.cancel()


def _append(source, dest):
    with source.open("rb") as src, dest.open("ab") as out:
        shutil.copyfileobj(src, out, 1024 * 1024)
        out.flush()
        os.fsync(out.fileno())
        return out.tell()


def _truncate(path, size):
    actual = path.stat().st_size if path.exists() else 0
    if actual < size:
        raise LocalStateError("committed media or continuity log is missing")
    if actual > size:
        with path.open("r+b") as f:
            f.truncate(size)
            os.fsync(f.fileno())


class ArchiveRecorder:
    """Maintain a durable prefix of a single replay rendition."""

    def __init__(
        self,
        output,
        video_id,
        resolver,
        *,
        delay=60,
        ffmpeg="ffmpeg",
        ffprobe="ffprobe",
        clock=time.time,
    ):
        self.output, self.resolver, self.clock = output, resolver, clock
        self.path = checkpoint_path(output)
        self.state = json.loads(self.path.read_text())
        state = self.state
        if state.get("version") != 1 or state.get("video_id") != video_id:
            raise LocalStateError("checkpoint identity mismatch")
        self.base = self.path.with_suffix("")
        self.temp = Path(str(self.base) + ".fetch")
        self.epochs = Path(str(self.base) + ".epochs")
        self.prepared = Path(str(self.base) + ".normalized")
        self.ffmpeg, self.ffprobe = ffmpeg, ffprobe
        state["target_delay_seconds"] = delay
        if state.get("prepared_inode"):
            self._install_prepared()
        _truncate(output, state["bytes_written"])
        _truncate(self.epochs, state["epoch_log_bytes"])

    def report(self, phase: str, reason: str = "") -> None:
        state = self.state
        state.update(phase=phase, reason=reason, updated_at=self.clock())
        origin = state.get("started_at") or state.get("estimated_start")
        if origin is not None:
            edge = (
                state["available_seconds"] if state.get("archive_ended") else self.clock() - origin
            )
            state["delay_seconds"] = max(0.0, edge - state["saved_seconds"])
        _write_state(self.path, state)
        public = {
            key: state[key]
            for key in (
                "phase",
                "reason",
                "saved_segments",
                "available_segments",
                "bytes_written",
                "saved_seconds",
                "delay_seconds",
                "complete",
            )
        }
        print("KICK_ARCHIVE_STATE", json.dumps(public, separators=(",", ":")), flush=True)

    def _new_epoch(self):
        """Record the byte offset at an explicit HLS discontinuity."""
        state = self.state
        with self.epochs.open("ab") as f:
            f.write(f"{state['bytes_written']} {state['saved_seconds']:.9f}\n".encode())
            f.flush()
            os.fsync(f.fileno())
            state["epoch_log_bytes"] = f.tell()

    def save(self, segment: Segment, headers: dict, index: int, count: int) -> None:
        state = self.state
        fetch_segment(segment, headers, self.temp)
        tracks = probe_segment(self.temp, segment, self.ffprobe)
        codecs = [tracks[k]["codec"] for k in ("video", "audio")]
        if state.get("codecs", codecs) != codecs:
            raise ArchiveRetry("codec_changed")
        if index and segment.discontinuity:
            self._new_epoch()
        elif index:
            for kind, last_end in state.get("track_ends", {}).items():
                track = tracks[kind]
                delta = _dts_delta(track["start"], last_end)
                if abs(delta) > max(0.12, track["cadence"] * 3):
                    raise ArchiveRetry("source_timestamp_gap")
        counts = state.get("packet_counts", {})
        size = _append(self.temp, self.output)
        state.update(
            track_ends={kind: track["end"] for kind, track in tracks.items()},
            codecs=codecs,
            packet_counts={
                kind: counts.get(kind, 0) + track["packets"] for kind, track in tracks.items()
            },
            bytes_written=size,
            saved_segments=index + 1,
            saved_seconds=state["saved_seconds"] + segment.duration,
            prefix_hash=segment.digest(state["prefix_hash"]),
        )
        self.report("recording")
        print(
            f"__ONLYSAVEmeVODS_LIVE_PROGRESS__\tkick_archive\t{codecs[0]}\t{codecs[1]}\t{index + 1}\t{count}\t0\t1",
            flush=True,
        )
        self.temp.unlink(missing_ok=True)

    def _install_prepared(self):
        # Recovery is possible on either side of the atomic media rename.
        state = self.state
        if self.prepared.exists() and self.prepared.stat().st_ino == state["prepared_inode"]:
            os.replace(self.prepared, self.output)
            _sync_dir(self.output.parent)
        if not self.output.exists() or (self.output.stat().st_ino, self.output.stat().st_size) != (
            state["prepared_inode"],
            state["prepared_bytes"],
        ):
            raise LocalStateError("normalized archive cannot be reconciled")
        state["bytes_written"] = state.pop("prepared_bytes")
        state.pop("prepared_inode")
        state["complete"] = True
        self.report("complete")

    def finish(self):
        """Normalize declared timestamp resets, then publish the completed prefix."""
        state = self.state
        self.report("finalizing")
        if state["epoch_log_bytes"]:
            # Rebase each continuous epoch to the archived timeline. Reading
            # byte ranges from our own TS avoids copying every epoch to disk.
            spans = [(0, 0.0)]
            for line in self.epochs.read_text().splitlines():
                offset, seconds = line.split()
                spans.append((int(offset), float(seconds)))
            spans.append((state["bytes_written"], state["saved_seconds"]))
            concat = Path(str(self.base) + ".ffconcat")
            with concat.open("w") as f:
                f.write("ffconcat version 1.0\n")
                for (begin, t0), (end, t1) in zip(spans, spans[1:]):
                    source = f"subfile,,start,{begin},end,{end},,:{self.output.absolute()}"
                    f.write("file '" + source.replace("'", "'\\''") + "'\n")
                    f.write(f"duration {t1 - t0:.9f}\n")
            result = subprocess.run(
                [
                    self.ffmpeg,
                    "-nostdin",
                    "-v",
                    "error",
                    "-xerror",
                    "-y",
                    "-protocol_whitelist",
                    "file,subfile",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    str(concat),
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0",
                    "-c",
                    "copy",
                    "-f",
                    "mpegts",
                    str(self.prepared),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if result.returncode or not self.prepared.stat().st_size:
                raise LocalStateError("unable to normalize archive discontinuities")
            normalized = measure_file(self.prepared, self.ffprobe)
            for i, kind in enumerate(("video", "audio")):
                codec, count, duration = normalized[kind]
                if (
                    codec != state["codecs"][i]
                    or count != state["packet_counts"][kind]
                    or abs(duration - state["saved_seconds"]) > 0.5
                ):
                    raise LocalStateError(
                        "normalization changed packet counts or recording duration"
                    )
            with self.prepared.open("rb") as f:
                os.fsync(f.fileno())
                stat = os.fstat(f.fileno())
            state.update(prepared_inode=stat.st_ino, prepared_bytes=stat.st_size)
            self.report("finalizing")
            self._install_prepared()
            concat.unlink(missing_ok=True)
        else:
            state["complete"] = True
            self.report("complete")

    def step(self) -> bool:
        """Process all eligible segments in one manifest, without a per-segment delay."""
        state = self.state
        if state["complete"]:
            return True
        try:
            resolved = self.resolver.resolve()
        finally:
            # Discovery may identify the replay before playback becomes
            # accessible. Persist its identity even on a failed first fetch.
            changed = False
            for key in ("replay_id", "format_id", "started_at"):
                value = getattr(self.resolver, key, None)
                if value is not None and state.get(key) != value:
                    if state.get(key) is not None:
                        raise ArchiveRetry("archive_changed")
                    state[key] = value
                    changed = True
            if changed:
                self.report("waiting_archive")
        if resolved is None:
            self.report("waiting_archive", "unavailable")
            return False
        url, ended = resolved
        state["archive_ended"] = ended
        headers = getattr(self.resolver, "http_headers", {}) or {}
        playlist = fetch_playlist(url, headers)
        segments = playlist.segments
        saved = state["saved_segments"]
        if len(segments) < state["available_segments"]:
            raise ArchiveRetry("snapshot_regressed")
        signature = ""
        for seg in segments[:saved]:
            signature = seg.digest(signature)
        if signature != state["prefix_hash"]:
            raise ArchiveRetry("archive_changed")
        available = sum(seg.duration for seg in segments)
        state.update(available_seconds=available, available_segments=len(segments))
        if state.get("started_at") is None and "estimated_start" not in state:
            state["estimated_start"] = playlist.started_at or self.clock() - available
        origin = state.get("started_at") or state.get("estimated_start")
        final_duration = getattr(self.resolver, "expected_duration", None)
        final_candidate = (
            ended and playlist.endlist and (not final_duration or available + 1 >= final_duration)
        )
        now = self.clock()
        if final_candidate:
            final_hash = signature
            for seg in segments[saved:]:
                final_hash = seg.digest(final_hash)
            if (
                state.get("stable_final_hash") != final_hash
                or state.get("stable_final_since") is None
            ):
                state.update(stable_final_hash=final_hash, stable_final_since=now)
        else:
            state.pop("stable_final_since", None)
        # Kick also sends ENDLIST for ongoing snapshots. The last listed
        # segment may still be extended/replaced; don't commit it until it
        # has a successor or has survived final metadata and the quiet period.
        settled = final_candidate and now - state["stable_final_since"] >= max(
            60.0, 2 * playlist.target
        )
        commit_count = len(segments) if settled else len(segments) - 1
        self.report("recording" if saved < commit_count else "waiting_snapshot")
        for index in range(saved, commit_count):
            seg = segments[index]
            eligible = self.clock() - origin - state["target_delay_seconds"]
            if not ended and state["saved_seconds"] + seg.duration > eligible:
                self.report("waiting_delay")
                return False
            self.save(seg, headers, index, len(segments))
        if not settled:
            self.report("waiting_snapshot")
            return False
        self.finish()
        return True

    def run(self, poll: float) -> None:
        failures = 0
        last_invalidated = -float("inf")
        while True:
            try:
                if self.step():
                    return
                failures = 0
            except (
                HTTPError,
                URLError,
                TimeoutError,
                ConnectionError,
                http.client.HTTPException,
                ArchiveRetry,
                YtDlpError,
                ValueError,
            ) as exc:
                failures = min(failures + 1, 5)
                reason = str(exc) if isinstance(exc, ArchiveRetry) else "unavailable"
                if isinstance(exc, HTTPError) and exc.code in {401, 403, 404, 410}:
                    reason = "missing_or_expired"
                    invalidate = getattr(self.resolver, "invalidate", None)
                    now = time.monotonic()
                    if invalidate and now - last_invalidated >= 20:
                        invalidate()
                        last_invalidated = now
                self.report("retrying", reason)
            retry_delay = min(30.0, poll * 2 ** (failures - 1)) if failures else 0.0
            time.sleep(max(poll, retry_delay))


def main(argv=None):
    # Only the Linux worker needs the file-locking module. The dashboard and
    # finalizer also import these helpers on platforms without fcntl.
    import fcntl

    from .kick_archive_resolver import KickArchiveResolver

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--video-id", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--delay-seconds", default=60.0, type=float)
    parser.add_argument("--poll-seconds", default=5.0, type=float)
    parser.add_argument("--started-at", default="")
    parser.add_argument("--yt-dlp-path", default="yt-dlp")
    parser.add_argument("--ffmpeg-path", default="ffmpeg")
    parser.add_argument("--ffprobe-path", default="ffprobe")
    args = parser.parse_args(argv)
    if args.delay_seconds < 0 or args.poll_seconds <= 0:
        parser.error("invalid delay or polling interval")
    path = checkpoint_path(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Kick archive already has an active writer", file=sys.stderr)
            return 1
        try:
            initialize_checkpoint(args.output, args.video_id, started_at=args.started_at)
            state = json.loads(path.read_text())
            resolver = KickArchiveResolver(
                args.source,
                args.video_id,
                args.yt_dlp_path,
                started_at=state.get("started_at"),
                replay_id=state.get("replay_id"),
                format_id=state.get("format_id"),
            )
            recorder = ArchiveRecorder(
                args.output,
                args.video_id,
                resolver,
                delay=args.delay_seconds,
                ffmpeg=args.ffmpeg_path,
                ffprobe=args.ffprobe_path,
            )
            recorder.run(args.poll_seconds)
        except (LocalStateError, OSError, ValueError, KeyError) as exc:
            # Under ENOSPC even status writes can fail. Re-read the committed
            # state; never publish tentative counters or replace foreign state.
            try:
                state = json.loads(path.read_text())
                if (
                    state.get("version") == 1
                    and state.get("video_id") == args.video_id
                    and isinstance(state.get("saved_segments"), int)
                    and isinstance(state.get("bytes_written"), int)
                ):
                    state.update(
                        phase="needs_attention", reason="local_media_error", updated_at=time.time()
                    )
                    _write_state(path, state)
            except (OSError, ValueError, AttributeError):
                pass
            print(f"Kick archive local failure ({type(exc).__name__})", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
