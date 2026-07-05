from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit
import json
import logging
import math
import re
import shutil
import subprocess
import tempfile

from PIL import Image
import numpy as np

from .chat_render import ffprobe_path_for
from .config import BotConfig
from .models import LiveStream
from .sources import source_display_name


LOGGER = logging.getLogger(__name__)
ProgressCallback = Callable[[str, float | None], None]
COMMERCIAL_TEXT_RE = re.compile(r"\bcommercial\s+break\b|\bbreak\s+in\s+progress\b")
PROGRESS_TEXT_RE = re.compile(r"\bprogress\b")
DEFAULT_VOD_PRE_ROLL_SECONDS = 30.0
DEFAULT_VOD_POST_ROLL_SECONDS = 60.0
ALIGNMENT_STEP_SECONDS = 1.0
ALIGNMENT_MAX_MEAN_DIFF = 45.0
FRAME_SCALE = "320:-1"


class TwitchAdRepairError(RuntimeError):
    """Raised when a Twitch ad repair operation cannot continue."""


@dataclass(frozen=True, slots=True)
class TwitchAdSegment:
    start: float
    end: float
    confidence: float
    text: str = ""

    @property
    def duration(self) -> float:
        return max(0.0, self.end - self.start)


@dataclass(frozen=True, slots=True)
class TwitchVodMetadata:
    url: str
    timestamp: float
    duration: float
    title: str = ""


@dataclass(frozen=True, slots=True)
class TwitchAdRepairSegmentResult:
    ad: TwitchAdSegment
    repaired: bool
    message: str
    vod_slice: str = ""
    vod_slice_start: float = 0.0
    vod_replacement_start: float = 0.0
    vod_replacement_end: float = 0.0
    alignment_difference: float | None = None


@dataclass(frozen=True, slots=True)
class TwitchAdRepairResult:
    repaired: bool
    output_file: str
    message: str
    ad_segments: list[TwitchAdSegment]
    segment_results: list[TwitchAdRepairSegmentResult]
    vod_url: str = ""


def repair_twitch_ads_for_media(
    config: BotConfig,
    stream: LiveStream,
    media_file: Path,
    *,
    started_at: str | None = None,
    progress_callback: ProgressCallback | None = None,
    logger: logging.Logger | None = None,
) -> TwitchAdRepairResult:
    """Detect Twitch commercial slates and write a repaired copy when possible."""
    log = logger or LOGGER
    progress = progress_callback or (lambda _phase, _value: None)
    media_file = Path(media_file)
    if stream.platform.casefold() != "twitch":
        return TwitchAdRepairResult(False, "", "Twitch ad repair only applies to Twitch streams", [], [])
    if not config.twitch_ad_repair_enabled:
        return TwitchAdRepairResult(False, "", "Twitch ad repair is disabled", [], [])
    if ".repaired" in media_file.stem:
        return TwitchAdRepairResult(False, "", "Skipping already repaired media", [], [])
    if not media_file.is_file():
        return TwitchAdRepairResult(False, "", f"Media file does not exist: {media_file}", [], [])
    if not executable_available(config.twitch_ad_repair_tesseract_path):
        result = TwitchAdRepairResult(
            False,
            "",
            "Twitch ad repair unavailable; tesseract is not available",
            [],
            [],
        )
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    progress("Scanning for Twitch commercial breaks", 0.05)
    scan = detect_twitch_commercial_breaks(
        media_file,
        ffmpeg_path=config.ffmpeg_path,
        tesseract_path=config.twitch_ad_repair_tesseract_path,
        scan_seconds=config.twitch_ad_repair_scan_seconds,
        sample_seconds=config.twitch_ad_repair_sample_seconds,
        max_ad_seconds=config.twitch_ad_repair_max_seconds,
        logger=log,
    )
    if not scan:
        result = TwitchAdRepairResult(False, "", "No Twitch commercial break slate detected", [], [])
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    channel = twitch_channel_from_stream(stream)
    if not channel:
        result = TwitchAdRepairResult(False, "", "Unable to determine Twitch channel", scan, [])
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    progress("Finding recent Twitch VOD", 0.18)
    vod_url = find_recent_twitch_vod_url(
        channel,
        yt_dlp_path=config.yt_dlp_path,
        search_limit=config.twitch_ad_repair_vod_search_limit,
    )
    if not vod_url:
        result = TwitchAdRepairResult(False, "", "No recent Twitch VOD was found", scan, [])
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    progress("Probing Twitch VOD", 0.22)
    vod = probe_twitch_vod(vod_url, yt_dlp_path=config.yt_dlp_path)
    media_duration = probe_media_duration(media_file, ffprobe_path_for(config.ffmpeg_path))
    recording_started_ts = recording_started_timestamp(started_at, media_file, media_duration)
    if recording_started_ts is None:
        result = TwitchAdRepairResult(
            False,
            "",
            "Unable to estimate recording start time for VOD alignment",
            scan,
            [],
            vod_url=vod_url,
        )
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    vod_recording_offset = recording_started_ts - vod.timestamp
    if vod_recording_offset < -300:
        result = TwitchAdRepairResult(
            False,
            "",
            "Recording appears to start before the available Twitch VOD",
            scan,
            [],
            vod_url=vod_url,
        )
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    repairable_ads = [
        ad for ad in scan if ad.duration > 0 and ad.duration <= config.twitch_ad_repair_max_seconds
    ]
    if not repairable_ads:
        result = TwitchAdRepairResult(
            False,
            "",
            "Detected commercial breaks exceeded the configured repair duration",
            scan,
            [],
            vod_url=vod_url,
        )
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    output_file = repaired_media_path(media_file)
    temp_files: list[Path] = []
    segment_results: list[TwitchAdRepairSegmentResult] = []
    try:
        with tempfile.TemporaryDirectory(prefix="onlysavemevods-twitch-ad-") as tmp:
            temp_dir = Path(tmp)
            repaired_parts: list[tuple[TwitchAdSegment, Path, float, float]] = []
            for index, ad in enumerate(repairable_ads, start=1):
                progress(
                    f"Downloading Twitch VOD slice for ad {index}/{len(repairable_ads)}",
                    0.25 + (index - 1) * 0.15 / max(1, len(repairable_ads)),
                )
                estimated_start = max(0.0, vod_recording_offset + ad.start - DEFAULT_VOD_PRE_ROLL_SECONDS)
                estimated_end = max(
                    estimated_start + ad.duration + DEFAULT_VOD_POST_ROLL_SECONDS,
                    vod_recording_offset + ad.end + DEFAULT_VOD_POST_ROLL_SECONDS,
                )
                if vod.duration > 0:
                    estimated_end = min(estimated_end, vod.duration)
                if estimated_end <= estimated_start:
                    segment_results.append(
                        TwitchAdRepairSegmentResult(ad, False, "VOD slice range was empty")
                    )
                    continue
                slice_file = download_twitch_vod_slice(
                    vod.url,
                    estimated_start,
                    estimated_end,
                    temp_dir / f"vod-ad-{index}.%(ext)s",
                    yt_dlp_path=config.yt_dlp_path,
                )
                temp_files.append(slice_file)
                progress(
                    f"Aligning Twitch VOD slice for ad {index}/{len(repairable_ads)}",
                    0.45 + (index - 1) * 0.2 / max(1, len(repairable_ads)),
                )
                match_time, diff = find_best_alignment_time(
                    local_media=media_file,
                    vod_slice=slice_file,
                    local_time=min(media_duration - 0.1, ad.end + 0.5),
                    ffmpeg_path=config.ffmpeg_path,
                    logger=log,
                )
                if match_time is None or diff is None or diff > ALIGNMENT_MAX_MEAN_DIFF:
                    message = "Unable to align VOD slice with the captured stream"
                    if diff is not None:
                        message += f" (mean frame difference {diff:.1f})"
                    segment_results.append(
                        TwitchAdRepairSegmentResult(
                            ad,
                            False,
                            message,
                            vod_slice=str(slice_file),
                            vod_slice_start=estimated_start,
                            alignment_difference=diff,
                        )
                    )
                    continue
                replacement_start = max(0.0, match_time - ad.duration)
                replacement_end = replacement_start + ad.duration
                slice_duration = probe_media_duration(slice_file, ffprobe_path_for(config.ffmpeg_path))
                if replacement_end > slice_duration:
                    segment_results.append(
                        TwitchAdRepairSegmentResult(
                            ad,
                            False,
                            "Aligned replacement extends beyond downloaded VOD slice",
                            vod_slice=str(slice_file),
                            vod_slice_start=estimated_start,
                            vod_replacement_start=replacement_start,
                            vod_replacement_end=replacement_end,
                            alignment_difference=diff,
                        )
                    )
                    continue
                repaired_parts.append((ad, slice_file, replacement_start, replacement_end))
                segment_results.append(
                    TwitchAdRepairSegmentResult(
                        ad,
                        True,
                        "Replacement slice aligned",
                        vod_slice=str(slice_file),
                        vod_slice_start=estimated_start,
                        vod_replacement_start=replacement_start,
                        vod_replacement_end=replacement_end,
                        alignment_difference=diff,
                    )
                )

            if not repaired_parts:
                result = TwitchAdRepairResult(
                    False,
                    "",
                    "Commercial slate detected, but no VOD replacements could be aligned",
                    scan,
                    segment_results,
                    vod_url=vod_url,
                )
                write_twitch_ad_repair_sidecar(media_file, result)
                return result

            progress("Rendering repaired Twitch copy", 0.78)
            render_repaired_media(
                media_file,
                repaired_parts,
                output_file,
                ffmpeg_path=config.ffmpeg_path,
                logger=log,
            )
    except (OSError, subprocess.SubprocessError, TwitchAdRepairError) as exc:
        log.warning("Twitch ad repair failed for %s: %s", media_file, exc)
        result = TwitchAdRepairResult(
            False,
            "",
            str(exc) or exc.__class__.__name__,
            scan,
            segment_results,
            vod_url=vod_url,
        )
        write_twitch_ad_repair_sidecar(media_file, result)
        return result

    progress("Twitch ad repair completed", 1.0)
    repaired_count = sum(1 for item in segment_results if item.repaired)
    result = TwitchAdRepairResult(
        True,
        str(output_file),
        f"Repaired {repaired_count} Twitch commercial break(s)",
        scan,
        segment_results,
        vod_url=vod_url,
    )
    write_twitch_ad_repair_sidecar(media_file, result)
    return result


def detect_twitch_commercial_breaks(
    media_file: Path,
    *,
    ffmpeg_path: str = "ffmpeg",
    tesseract_path: str = "tesseract",
    scan_seconds: int = 300,
    sample_seconds: int = 2,
    max_ad_seconds: int = 180,
    logger: logging.Logger | None = None,
) -> list[TwitchAdSegment]:
    log = logger or LOGGER
    if not executable_available(tesseract_path):
        log.info("Twitch ad repair skipped; tesseract is not available")
        return []
    media_duration = probe_media_duration(media_file, ffprobe_path_for(ffmpeg_path))
    scan_duration = media_duration if scan_seconds <= 0 else min(media_duration, float(scan_seconds))
    if scan_duration <= 0:
        return []
    sample_step = max(1, int(sample_seconds))
    positive_samples: list[tuple[float, str]] = []
    with tempfile.TemporaryDirectory(prefix="onlysavemevods-twitch-ocr-") as tmp:
        tmp_dir = Path(tmp)
        sample_count = int(math.ceil(scan_duration / sample_step)) + 1
        for index in range(sample_count):
            timestamp = min(scan_duration, float(index * sample_step))
            frame_file = tmp_dir / f"frame-{index:05d}.png"
            if not extract_frame(media_file, timestamp, frame_file, ffmpeg_path=ffmpeg_path):
                continue
            text = ocr_frame(frame_file, tesseract_path=tesseract_path)
            if is_twitch_commercial_break_text(text):
                positive_samples.append((timestamp, text.strip()))
    return merge_commercial_samples(
        positive_samples,
        sample_seconds=float(sample_step),
        media_duration=media_duration,
        max_ad_seconds=float(max_ad_seconds),
    )


def is_twitch_commercial_break_text(text: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()
    if not normalized:
        return False
    return bool(COMMERCIAL_TEXT_RE.search(normalized) and PROGRESS_TEXT_RE.search(normalized))


def merge_commercial_samples(
    samples: list[tuple[float, str]],
    *,
    sample_seconds: float,
    media_duration: float,
    max_ad_seconds: float,
) -> list[TwitchAdSegment]:
    if not samples:
        return []
    merged: list[TwitchAdSegment] = []
    gap = max(sample_seconds * 2.5, 5.0)
    start = samples[0][0]
    last = samples[0][0]
    texts = [samples[0][1]]
    for timestamp, text in samples[1:]:
        if timestamp - last <= gap:
            last = timestamp
            texts.append(text)
            continue
        merged.extend(_sample_group_to_segment(start, last, texts, sample_seconds, media_duration, max_ad_seconds))
        start = timestamp
        last = timestamp
        texts = [text]
    merged.extend(_sample_group_to_segment(start, last, texts, sample_seconds, media_duration, max_ad_seconds))
    return merged


def _sample_group_to_segment(
    start: float,
    last: float,
    texts: list[str],
    sample_seconds: float,
    media_duration: float,
    max_ad_seconds: float,
) -> list[TwitchAdSegment]:
    end = min(media_duration, last + sample_seconds * 2.0)
    if end <= start:
        return []
    if max_ad_seconds > 0 and end - start > max_ad_seconds:
        end = start + max_ad_seconds
    confidence = min(1.0, 0.55 + len(texts) * 0.08)
    display_text = " | ".join(dict.fromkeys(texts))[:500]
    return [TwitchAdSegment(start=start, end=end, confidence=confidence, text=display_text)]


def find_recent_twitch_vod_url(
    channel: str,
    *,
    yt_dlp_path: str = "yt-dlp",
    search_limit: int = 5,
) -> str:
    channel = channel.strip().strip("/")
    if not channel:
        return ""
    url = f"https://www.twitch.tv/{channel}/videos?filter=archives&sort=time"
    command = [
        yt_dlp_path,
        "--flat-playlist",
        "--dump-single-json",
        "--playlist-end",
        str(max(1, search_limit)),
        "--no-warnings",
        url,
    ]
    result = run_command(command)
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise TwitchAdRepairError("yt-dlp returned invalid Twitch VOD playlist JSON") from exc
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return ""
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        candidate = entry.get("webpage_url") or entry.get("url") or entry.get("id")
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        candidate = candidate.strip()
        if candidate.startswith(("http://", "https://")):
            return candidate
        if candidate.isdigit():
            return f"https://www.twitch.tv/videos/{candidate}"
    return ""


def probe_twitch_vod(vod_url: str, *, yt_dlp_path: str = "yt-dlp") -> TwitchVodMetadata:
    command = [yt_dlp_path, "-J", "--skip-download", "--no-warnings", vod_url]
    result = run_command(command)
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise TwitchAdRepairError("yt-dlp returned invalid Twitch VOD metadata JSON") from exc
    timestamp = payload.get("timestamp") or payload.get("release_timestamp")
    if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
        raise TwitchAdRepairError("Twitch VOD metadata did not include a timestamp")
    duration = payload.get("duration")
    if not isinstance(duration, (int, float)) or isinstance(duration, bool):
        duration = 0.0
    return TwitchVodMetadata(
        url=str(payload.get("webpage_url") or vod_url),
        timestamp=float(timestamp),
        duration=float(duration),
        title=str(payload.get("title") or ""),
    )


def download_twitch_vod_slice(
    vod_url: str,
    start_seconds: float,
    end_seconds: float,
    output_template: Path,
    *,
    yt_dlp_path: str = "yt-dlp",
) -> Path:
    output_template.parent.mkdir(parents=True, exist_ok=True)
    section = f"*{format_section_time(start_seconds)}-{format_section_time(end_seconds)}"
    command = [
        yt_dlp_path,
        "--download-sections",
        section,
        "--force-keyframes-at-cuts",
        "--no-playlist",
        "-f",
        "bestvideo*+bestaudio/best",
        "-o",
        str(output_template),
        vod_url,
    ]
    run_command(command)
    candidates = [
        path
        for path in output_template.parent.glob(output_template.name.replace("%(ext)s", "*"))
        if path.is_file() and not path.name.endswith((".part", ".ytdl"))
    ]
    if not candidates:
        raise TwitchAdRepairError("Twitch VOD slice download did not create a media file")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def find_best_alignment_time(
    *,
    local_media: Path,
    vod_slice: Path,
    local_time: float,
    ffmpeg_path: str = "ffmpeg",
    logger: logging.Logger | None = None,
) -> tuple[float | None, float | None]:
    log = logger or LOGGER
    duration = probe_media_duration(vod_slice, ffprobe_path_for(ffmpeg_path))
    if duration <= 0:
        return None, None
    with tempfile.TemporaryDirectory(prefix="onlysavemevods-twitch-align-") as tmp:
        tmp_dir = Path(tmp)
        local_frame = tmp_dir / "local.png"
        if not extract_frame(local_media, max(0.0, local_time), local_frame, ffmpeg_path=ffmpeg_path):
            return None, None
        local_array = frame_array(local_frame)
        best_time: float | None = None
        best_diff: float | None = None
        sample_count = int(math.floor(duration / ALIGNMENT_STEP_SECONDS)) + 1
        for index in range(sample_count):
            timestamp = min(duration, index * ALIGNMENT_STEP_SECONDS)
            frame_file = tmp_dir / f"vod-{index:05d}.png"
            if not extract_frame(vod_slice, timestamp, frame_file, ffmpeg_path=ffmpeg_path):
                continue
            try:
                diff = mean_frame_difference(local_array, frame_array(frame_file))
            except ValueError as exc:
                log.debug("Unable to compare alignment frame %s: %s", frame_file, exc)
                continue
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_time = timestamp
        return best_time, best_diff


def render_repaired_media(
    media_file: Path,
    replacements: list[tuple[TwitchAdSegment, Path, float, float]],
    output_file: Path,
    *,
    ffmpeg_path: str = "ffmpeg",
    logger: logging.Logger | None = None,
) -> None:
    log = logger or LOGGER
    replacements = sorted(replacements, key=lambda item: item[0].start)
    if not replacements:
        raise TwitchAdRepairError("No repair replacements were available")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_output = output_file.with_name(f"{output_file.stem}.rendering{output_file.suffix}")
    inputs = [media_file, *[replacement[1] for replacement in replacements]]
    command = [ffmpeg_path, "-y", "-v", "warning", "-nostats"]
    for input_file in inputs:
        command.extend(["-i", str(input_file)])

    filter_parts: list[str] = []
    concat_labels: list[str] = []
    cursor = 0.0
    part_index = 0

    def add_original(start: float, end: float | None) -> None:
        nonlocal part_index
        if end is not None and end - start <= 0.05:
            return
        v_label = f"v{part_index}"
        a_label = f"a{part_index}"
        trim = f"start={start:.3f}" + (f":end={end:.3f}" if end is not None else "")
        filter_parts.append(
            f"[0:v]trim={trim},setpts=PTS-STARTPTS,fps=30,format=yuv420p[{v_label}]"
        )
        filter_parts.append(f"[0:a]atrim={trim},asetpts=PTS-STARTPTS[{a_label}]")
        concat_labels.extend([f"[{v_label}]", f"[{a_label}]"])
        part_index += 1

    def add_replacement(input_index: int, start: float, end: float) -> None:
        nonlocal part_index
        if end - start <= 0.05:
            return
        v_label = f"v{part_index}"
        a_label = f"a{part_index}"
        trim = f"start={start:.3f}:end={end:.3f}"
        filter_parts.append(
            f"[{input_index}:v]trim={trim},setpts=PTS-STARTPTS,fps=30,format=yuv420p[{v_label}]"
        )
        filter_parts.append(f"[{input_index}:a]atrim={trim},asetpts=PTS-STARTPTS[{a_label}]")
        concat_labels.extend([f"[{v_label}]", f"[{a_label}]"])
        part_index += 1

    for input_index, (ad, _slice_file, replacement_start, replacement_end) in enumerate(
        replacements,
        start=1,
    ):
        if ad.start < cursor:
            log.info("Skipping overlapping Twitch ad repair segment start=%s", ad.start)
            continue
        add_original(cursor, ad.start)
        add_replacement(input_index, replacement_start, replacement_end)
        cursor = ad.end
    add_original(cursor, None)

    if len(concat_labels) < 4:
        raise TwitchAdRepairError("Repair filter did not contain enough media parts")
    filter_parts.append(
        "".join(concat_labels) + f"concat=n={len(concat_labels) // 2}:v=1:a=1[v][a]"
    )
    command.extend(
        [
            "-filter_complex",
            ";".join(filter_parts),
            "-map",
            "[v]",
            "-map",
            "[a]",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "21",
            "-profile:v",
            "main",
            "-level",
            "4.0",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            str(temp_output),
        ]
    )
    try:
        run_command(command)
        temp_output.replace(output_file)
    except Exception:
        if temp_output.exists():
            try:
                temp_output.unlink()
            except OSError:
                pass
        raise


def write_twitch_ad_repair_sidecar(media_file: Path, result: TwitchAdRepairResult) -> Path:
    sidecar = twitch_ad_repair_sidecar_path(media_file)
    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "media_file": media_file.name,
        "repaired": result.repaired,
        "output_file": result.output_file,
        "message": result.message,
        "vod_url": result.vod_url,
        "ad_segments": [asdict(segment) for segment in result.ad_segments],
        "segment_results": [asdict(segment_result) for segment_result in result.segment_results],
    }
    sidecar.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return sidecar


def twitch_ad_repair_sidecar_path(media_file: Path) -> Path:
    return media_file.with_suffix(media_file.suffix + ".twitch-ad-repair.json")


def repaired_media_path(media_file: Path) -> Path:
    return media_file.with_name(f"{media_file.stem}.repaired{media_file.suffix}")


def twitch_channel_from_stream(stream: LiveStream) -> str:
    for value in (stream.source, stream.url, stream.channel):
        channel = twitch_channel_from_value(value)
        if channel:
            return channel
    return ""


def twitch_channel_from_value(value: str) -> str:
    raw = (value or "").strip().strip("/")
    if not raw:
        return ""
    if raw.casefold().startswith("twitch:"):
        return raw.split(":", 1)[1].strip().strip("/").split("/", 1)[0]
    if raw.startswith(("http://", "https://")):
        parts = urlsplit(raw)
        host = parts.netloc.casefold()
        if host.startswith("www."):
            host = host[4:]
        if host == "twitch.tv" or host.endswith(".twitch.tv"):
            path = parts.path.strip("/")
            if path and not path.startswith("videos/"):
                return path.split("/", 1)[0]
    display = source_display_name(raw)
    if display and not display.isdigit():
        return display.lstrip("@")
    return ""


def recording_started_timestamp(
    started_at: str | None,
    media_file: Path,
    media_duration: float,
) -> float | None:
    parsed = parse_iso_timestamp(started_at)
    if parsed is not None:
        return parsed
    try:
        stat = media_file.stat()
    except OSError:
        return None
    if media_duration > 0:
        return stat.st_mtime - media_duration
    return stat.st_mtime


def parse_iso_timestamp(value: str | None) -> float | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def probe_media_duration(media_file: Path, ffprobe_path: str = "ffprobe") -> float:
    command = [
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=nk=1:nw=1",
        str(media_file),
    ]
    result = run_command(command)
    try:
        duration = float((result.stdout or "").strip())
    except ValueError as exc:
        raise TwitchAdRepairError(f"Unable to parse media duration for {media_file}") from exc
    if duration <= 0:
        raise TwitchAdRepairError(f"Invalid media duration for {media_file}: {duration}")
    return duration


def extract_frame(media_file: Path, timestamp: float, output_file: Path, *, ffmpeg_path: str) -> bool:
    command = [
        ffmpeg_path,
        "-y",
        "-v",
        "error",
        "-ss",
        f"{max(0.0, timestamp):.3f}",
        "-i",
        str(media_file),
        "-frames:v",
        "1",
        "-vf",
        f"scale={FRAME_SCALE}",
        str(output_file),
    ]
    try:
        run_command(command, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    return output_file.is_file()


def ocr_frame(frame_file: Path, *, tesseract_path: str) -> str:
    command = [tesseract_path, str(frame_file), "stdout", "--psm", "6"]
    try:
        result = run_command(command, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout or ""


def frame_array(frame_file: Path) -> np.ndarray:
    with Image.open(frame_file) as image:
        return np.asarray(image.convert("L"), dtype=np.int16)


def mean_frame_difference(left: np.ndarray, right: np.ndarray) -> float:
    if left.shape != right.shape:
        raise ValueError(f"frame shapes differ: {left.shape} vs {right.shape}")
    return float(np.mean(np.abs(left - right)))


def executable_available(command: str) -> bool:
    if not command:
        return False
    if any(separator in command for separator in ("/", "\\")):
        return Path(command).is_file()
    return shutil.which(command) is not None


def format_section_time(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    whole = int(seconds)
    millis = int(round((seconds - whole) * 1000))
    if millis == 1000:
        whole += 1
        millis = 0
    hours, remainder = divmod(whole, 3600)
    minutes, secs = divmod(remainder, 60)
    if millis:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def run_command(
    command: list[str],
    *,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        if detail:
            raise TwitchAdRepairError(detail) from exc
        raise TwitchAdRepairError(f"Command failed: {command[0]}") from exc
