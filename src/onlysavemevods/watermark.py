from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
import hashlib
import hmac
import os
import secrets
import subprocess
import time

from .config import (
    DEFAULT_WATERMARK_SECRET_ENV,
    LEGACY_WATERMARK_SECRET_ENV,
    BotConfig,
)
from .downloader import command_for_log
from .state import WatermarkCopyRecord


PATTERN_WIDTH = 64
PATTERN_HEIGHT = 36
MAX_DETECT_FRAMES = 180
DEFAULT_SAMPLE_INTERVAL_SECONDS = 1.0
WATERMARK_STATUS_DONE = "done"
WATERMARK_STATUS_FAILED = "failed"
WATERMARK_STATUS_RUNNING = "running"
WATERMARK_STATUS_QUEUED = "queued"
WATERMARK_STATUS_INTERRUPTED = "interrupted"

STRENGTH_DELTAS = {
    "invisible": 1.15,
    "balanced": 1.8,
    "robust": 2.7,
}
DETECT_MIN_SCORE = 0.012
DETECT_MIN_MARGIN = 0.004


class WatermarkError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DetectionCandidate:
    copy_id: str
    video_id: str
    source_name: str
    output_name: str
    recipient_label: str
    score: float


@dataclass(frozen=True, slots=True)
class DetectionResult:
    matched: bool
    confidence: str
    score: float
    margin: float
    frames_analyzed: int
    best: DetectionCandidate | None
    runner_up: DetectionCandidate | None
    candidates: list[DetectionCandidate]
    message: str


def watermark_secret(config: BotConfig) -> str:
    secret = os.environ.get(config.watermark_secret_env, "")
    secret = secret.strip()
    if secret:
        return secret
    if config.watermark_secret_env == DEFAULT_WATERMARK_SECRET_ENV:
        return os.environ.get(LEGACY_WATERMARK_SECRET_ENV, "").strip()
    return ""


def require_watermark_secret(config: BotConfig) -> str:
    secret = watermark_secret(config)
    if not secret:
        raise WatermarkError(
            f"Watermark secret is not configured; set {config.watermark_secret_env}"
        )
    return secret


def new_copy_id() -> str:
    return "wm_" + secrets.token_urlsafe(16)


def validate_recipient_label(label: str) -> str:
    normalized = " ".join(label.strip().split())
    if not normalized:
        raise WatermarkError("Recipient label is required")
    if len(normalized) > 160:
        normalized = normalized[:160].rstrip()
    return normalized


def watermarked_output_name(source_name: str, copy_id: str) -> str:
    source = Path(source_name).name
    stem = source.removesuffix(Path(source).suffix)
    short_id = copy_id_short(copy_id)
    return str(Path(".watermarks") / f"{stem} - wm-{short_id}.mp4")


def copy_id_short(copy_id: str) -> str:
    return copy_id.removeprefix("wm_")[:10]


def resolve_watermark_output_file(stream_directory: Path, output_name: str) -> Path | None:
    if not output_name:
        return None
    relative = Path(output_name)
    if relative.is_absolute() or ".." in relative.parts:
        return None
    return stream_directory / relative


def derive_pattern(
    secret: str,
    copy_id: str,
    video_id: str,
    source_name: str,
    *,
    width: int = PATTERN_WIDTH,
    height: int = PATTERN_HEIGHT,
) -> Any:
    np = optional_numpy_dependency()
    key = secret.encode("utf-8")
    message = f"{copy_id}\0{video_id}\0{source_name}".encode("utf-8")
    byte_count = width * height
    output = bytearray()
    counter = 0
    while len(output) < byte_count:
        output.extend(
            hmac.new(
                key,
                message + counter.to_bytes(4, "big"),
                hashlib.sha256,
            ).digest()
        )
        counter += 1

    raw = np.frombuffer(bytes(output[:byte_count]), dtype=np.uint8).astype(np.float32)
    pattern = np.where(raw >= 128, 1.0, -1.0).reshape((height, width))
    pattern -= float(pattern.mean())
    std = float(pattern.std())
    if std > 0:
        pattern /= std
    return pattern


def build_audio_mux_command(
    ffmpeg_path: str,
    watermarked_video: Path,
    source_media: Path,
    output_file: Path,
) -> list[str]:
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-i",
        str(watermarked_video),
        "-i",
        str(source_media),
        "-map",
        "0:v:0",
        "-map",
        "1:a?",
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-b:a",
        "160k",
        "-movflags",
        "+faststart",
        "-shortest",
        str(output_file),
    ]


def create_watermarked_copy(
    *,
    source_file: Path,
    output_file: Path,
    secret: str,
    copy_id: str,
    video_id: str,
    source_name: str,
    strength: str,
    ffmpeg_path: str,
    overwrite: bool = False,
) -> None:
    if output_file.exists() and not overwrite:
        return
    np, cv2 = optional_cv_dependencies()
    if strength not in STRENGTH_DELTAS:
        raise WatermarkError(f"Unsupported watermark strength: {strength}")

    cap = cv2.VideoCapture(str(source_file))
    if not cap.isOpened():
        raise WatermarkError(f"Unable to open media file: {source_file}")

    fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
    if fps <= 0 or fps != fps:
        fps = 30.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    if width <= 0 or height <= 0:
        cap.release()
        raise WatermarkError(f"Unable to read video dimensions: {source_file}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_video = output_file.with_name(f"{output_file.stem}.video{output_file.suffix}")
    temp_output = output_file.with_name(f"{output_file.stem}.muxing{output_file.suffix}")
    temp_video.unlink(missing_ok=True)
    temp_output.unlink(missing_ok=True)

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(temp_video), fourcc, fps, (width, height))
    if not writer.isOpened():
        cap.release()
        raise WatermarkError(f"Unable to create temporary video: {temp_video}")

    pattern = derive_pattern(secret, copy_id, video_id, source_name)
    frame_pattern = cv2.resize(pattern, (width, height), interpolation=cv2.INTER_CUBIC)
    delta = float(STRENGTH_DELTAS[strength])
    frame_count = 0
    try:
        while True:
            ok, frame = cap.read()
            if not ok:
                break
            writer.write(apply_watermark_to_frame(frame, frame_pattern, delta, np, cv2))
            frame_count += 1
    finally:
        cap.release()
        writer.release()

    if frame_count <= 0:
        temp_video.unlink(missing_ok=True)
        raise WatermarkError(f"No frames found in media file: {source_file}")

    command = build_audio_mux_command(ffmpeg_path, temp_video, source_file, temp_output)
    try:
        result = subprocess.run(command, capture_output=True, check=False)
    except FileNotFoundError as exc:
        temp_video.unlink(missing_ok=True)
        raise WatermarkError(f"ffmpeg not found: {ffmpeg_path}") from exc
    except OSError as exc:
        temp_video.unlink(missing_ok=True)
        raise WatermarkError(f"Unable to start ffmpeg: {exc}") from exc

    if result.returncode != 0:
        temp_video.unlink(missing_ok=True)
        temp_output.unlink(missing_ok=True)
        message = process_failure_message(result.stdout or b"", result.stderr or b"")
        command_text = command_for_log(command)
        raise WatermarkError(message or f"ffmpeg failed while muxing: {command_text}")

    if output_file.exists():
        output_file.unlink()
    temp_output.rename(output_file)
    temp_video.unlink(missing_ok=True)


def apply_watermark_to_frame(
    frame: Any,
    frame_pattern: Any,
    delta: float,
    np: Any,
    cv2: Any,
) -> Any:
    ycrcb = cv2.cvtColor(frame, cv2.COLOR_BGR2YCrCb)
    y = ycrcb[:, :, 0].astype(np.float32)
    mask = visibility_mask(y, np, cv2)
    ycrcb[:, :, 0] = np.clip(y + (frame_pattern * delta * mask), 0, 255).astype(
        np.uint8
    )
    return cv2.cvtColor(ycrcb, cv2.COLOR_YCrCb2BGR)


def visibility_mask(y: Any, np: Any, cv2: Any) -> Any:
    midtone = np.clip((y - 24.0) / 56.0, 0.0, 1.0) * np.clip(
        (244.0 - y) / 56.0,
        0.0,
        1.0,
    )
    local_mean = cv2.blur(y, (9, 9))
    texture = np.clip(np.abs(y - local_mean) / 10.0, 0.35, 1.0)
    return midtone * texture


def detect_watermark(
    *,
    media_file: Path,
    records: Sequence[WatermarkCopyRecord],
    secret: str,
    max_frames: int = MAX_DETECT_FRAMES,
    sample_interval_seconds: float = DEFAULT_SAMPLE_INTERVAL_SECONDS,
) -> DetectionResult:
    frames = sample_detection_frames(
        media_file,
        max_frames=max_frames,
        sample_interval_seconds=sample_interval_seconds,
    )
    candidates = score_watermark_records(frames, records, secret)
    if not frames:
        return DetectionResult(
            matched=False,
            confidence="none",
            score=0.0,
            margin=0.0,
            frames_analyzed=0,
            best=None,
            runner_up=None,
            candidates=[],
            message="No usable video frames found",
        )
    if not candidates:
        return DetectionResult(
            matched=False,
            confidence="none",
            score=0.0,
            margin=0.0,
            frames_analyzed=len(frames),
            best=None,
            runner_up=None,
            candidates=[],
            message="No completed watermark copies are recorded",
        )

    best = candidates[0]
    runner_up = candidates[1] if len(candidates) > 1 else None
    runner_score = runner_up.score if runner_up is not None else 0.0
    margin = best.score - runner_score
    matched = best.score >= DETECT_MIN_SCORE and margin >= DETECT_MIN_MARGIN
    if matched and best.score >= DETECT_MIN_SCORE * 2:
        confidence = "high"
    elif matched:
        confidence = "medium"
    else:
        confidence = "none"
    message = (
        f"Matched {best.copy_id} for {best.recipient_label}"
        if matched
        else "No confident watermark match"
    )
    return DetectionResult(
        matched=matched,
        confidence=confidence,
        score=best.score,
        margin=margin,
        frames_analyzed=len(frames),
        best=best,
        runner_up=runner_up,
        candidates=candidates[:10],
        message=message,
    )


def score_watermark_records(
    frames: Sequence[Any],
    records: Sequence[WatermarkCopyRecord],
    secret: str,
) -> list[DetectionCandidate]:
    candidates: list[DetectionCandidate] = []
    if not frames:
        return candidates
    for record in records:
        pattern = derive_pattern(
            secret,
            record.copy_id,
            record.video_id,
            record.source_name,
        )
        pattern = normalize_array(pattern)
        scores = [float((frame * pattern).mean()) for frame in frames]
        score = robust_average(scores)
        candidates.append(
            DetectionCandidate(
                copy_id=record.copy_id,
                video_id=record.video_id,
                source_name=record.source_name,
                output_name=record.output_name,
                recipient_label=record.recipient_label,
                score=score,
            )
        )
    return sorted(candidates, key=lambda item: item.score, reverse=True)


def sample_detection_frames(
    media_file: Path,
    *,
    max_frames: int = MAX_DETECT_FRAMES,
    sample_interval_seconds: float = DEFAULT_SAMPLE_INTERVAL_SECONDS,
) -> list[Any]:
    np, cv2 = optional_cv_dependencies()
    cap = cv2.VideoCapture(str(media_file))
    if not cap.isOpened():
        raise WatermarkError(f"Unable to open media file: {media_file}")
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
    if fps <= 0 or fps != fps:
        fps = 30.0
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    duration = frame_count / fps if frame_count > 0 else 0.0
    if duration > 0:
        step = max(sample_interval_seconds, duration / max_frames)
        timestamps = [index * step for index in range(max_frames) if index * step < duration]
    else:
        timestamps = [index * sample_interval_seconds for index in range(max_frames)]

    frames: list[Any] = []
    try:
        for timestamp in timestamps:
            cap.set(cv2.CAP_PROP_POS_MSEC, timestamp * 1000.0)
            ok, frame = cap.read()
            if not ok:
                continue
            normalized = detection_frame_variants(frame, np, cv2)
            if normalized:
                frames.extend(normalized)
    finally:
        cap.release()
    return frames[:max_frames]


def detection_frame_variants(frame: Any, np: Any, cv2: Any) -> list[Any]:
    y = cv2.cvtColor(frame, cv2.COLOR_BGR2YCrCb)[:, :, 0].astype(np.float32)
    crops = [y]
    content_crop = crop_letterbox(y, np)
    if content_crop is not y:
        crops.append(content_crop)
    height, width = y.shape[:2]
    for crop_ratio in (0.03, 0.06):
        dx = int(width * crop_ratio)
        dy = int(height * crop_ratio)
        if dx > 0 and dy > 0 and width - (2 * dx) >= 32 and height - (2 * dy) >= 18:
            crops.append(y[dy : height - dy, dx : width - dx])

    variants: list[Any] = []
    seen_shapes: set[tuple[int, int]] = set()
    for crop in crops:
        if crop.size == 0:
            continue
        key = crop.shape[:2]
        if key in seen_shapes:
            continue
        seen_shapes.add(key)
        small = cv2.resize(
            crop,
            (PATTERN_WIDTH, PATTERN_HEIGHT),
            interpolation=cv2.INTER_AREA,
        )
        if float(small.std()) < 3.0:
            continue
        variants.append(normalize_array(small))
    return variants


def crop_letterbox(y: Any, np: Any) -> Any:
    threshold = max(8.0, float(y.mean()) * 0.12)
    rows = np.where(y.mean(axis=1) > threshold)[0]
    cols = np.where(y.mean(axis=0) > threshold)[0]
    if len(rows) < 18 or len(cols) < 32:
        return y
    top, bottom = int(rows[0]), int(rows[-1]) + 1
    left, right = int(cols[0]), int(cols[-1]) + 1
    if top <= 1 and left <= 1 and bottom >= y.shape[0] - 1 and right >= y.shape[1] - 1:
        return y
    return y[top:bottom, left:right]


def normalize_array(array: Any) -> Any:
    np = optional_numpy_dependency()
    normalized = array.astype(np.float32)
    normalized -= float(normalized.mean())
    std = float(normalized.std())
    if std <= 0:
        return normalized
    return normalized / std


def robust_average(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) >= 8:
        trim = max(1, len(sorted_values) // 8)
        sorted_values = sorted_values[trim:-trim]
    return sum(sorted_values) / len(sorted_values)


def detection_result_to_dict(result: DetectionResult) -> dict[str, Any]:
    return {
        "matched": result.matched,
        "confidence": result.confidence,
        "score": result.score,
        "margin": result.margin,
        "frames_analyzed": result.frames_analyzed,
        "message": result.message,
        "best": candidate_to_dict(result.best),
        "runner_up": candidate_to_dict(result.runner_up),
        "candidates": [candidate_to_dict(candidate) for candidate in result.candidates],
    }


def candidate_to_dict(candidate: DetectionCandidate | None) -> dict[str, Any] | None:
    if candidate is None:
        return None
    return {
        "copy_id": candidate.copy_id,
        "video_id": candidate.video_id,
        "source_name": candidate.source_name,
        "output_name": candidate.output_name,
        "recipient_label": candidate.recipient_label,
        "score": candidate.score,
    }


def optional_cv_dependencies() -> tuple[Any, Any]:
    try:
        import cv2
        import numpy as np
    except ImportError as exc:
        raise WatermarkError(
            "Watermarking requires numpy and opencv-python-headless. "
            "Install the project dependencies before using watermark features."
        ) from exc
    return np, cv2


def optional_numpy_dependency() -> Any:
    try:
        import numpy as np
    except ImportError as exc:
        raise WatermarkError(
            "Watermarking requires numpy. Install the project dependencies before "
            "using watermark features."
        ) from exc
    return np


def process_failure_message(stdout: bytes, stderr: bytes) -> str:
    output = (stderr or stdout).decode("utf-8", "replace").strip()
    if not output:
        return ""
    return output.splitlines()[-1][-500:]


def format_detection_text(result: DetectionResult) -> str:
    if not result.best:
        return result.message
    parts = [
        result.message,
        f"score={result.score:.5f}",
        f"margin={result.margin:.5f}",
        f"frames={result.frames_analyzed}",
    ]
    if result.matched:
        parts.extend(
            [
                f"copy_id={result.best.copy_id}",
                f"recipient={result.best.recipient_label}",
                f"video_id={result.best.video_id}",
                f"source={result.best.source_name}",
            ]
        )
    return "\n".join(parts)


def elapsed_message(started_at: float) -> str:
    return f"Completed in {time.monotonic() - started_at:.1f}s"
