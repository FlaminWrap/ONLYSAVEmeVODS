from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol
import importlib.util
import json
import logging
import math
import subprocess

import numpy as np

from .config import (
    BotConfig,
    StreamEventRuleConfig,
    streamer_for_channel,
)
from .transcription import load_whisperx_subtitle_segments, transcription_output_file


LOGGER = logging.getLogger(__name__)
CONTENT_EVENT_SUFFIX = ".stream-events.json"
CONTENT_EVENT_VERSION = 1
CONTENT_EVENT_SAMPLE_RATE = 16_000
MIN_AUDIO_WINDOW_SECONDS = 0.25


class ContentEventDetectorUnavailable(RuntimeError):
    """Raised when optional content event detection dependencies are unavailable."""


class ContentEventDetectionError(RuntimeError):
    """Raised when media cannot be analyzed for content events."""


class AudioClassifierBackend(Protocol):
    def classify(self, audio: np.ndarray, sample_rate: int) -> list[dict[str, float]]:
        """Return label/score rows for one audio window."""


ProgressCallback = Callable[[str, float | None], None]


@dataclass(frozen=True, slots=True)
class ContentEventSettings:
    enabled: bool
    model: str
    device: str
    window_seconds: float
    hop_seconds: float
    min_confidence: float
    max_events_per_media: int
    rules: list[StreamEventRuleConfig]


@dataclass(slots=True)
class EventCandidate:
    start: float
    end: float
    rule: str
    severity: str
    score: float
    loudness_dbfs: float | None = None
    labels: dict[str, float] = field(default_factory=dict)
    keywords: set[str] = field(default_factory=set)
    text: str = ""

    @property
    def duration(self) -> float:
        return max(0.0, self.end - self.start)


class HuggingFaceAudioSetBackend:
    def __init__(self, settings: ContentEventSettings) -> None:
        try:
            import torch
            from transformers import AutoFeatureExtractor, AutoModelForAudioClassification
        except Exception as exc:  # noqa: BLE001 - optional dependency boundary.
            raise ContentEventDetectorUnavailable(
                "Install onlysavemevods[stream-events] to enable ML content event detection."
            ) from exc

        self._torch = torch
        if settings.device == "auto":
            self._device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            self._device = settings.device
        try:
            self._feature_extractor = AutoFeatureExtractor.from_pretrained(settings.model)
            self._model = AutoModelForAudioClassification.from_pretrained(settings.model)
            self._model.to(self._device)
            self._model.eval()
        except Exception as exc:  # noqa: BLE001 - model access/cache errors need a clear message.
            raise ContentEventDetectorUnavailable(str(exc)) from exc

    def classify(self, audio: np.ndarray, sample_rate: int) -> list[dict[str, float]]:
        inputs = self._feature_extractor(
            audio,
            sampling_rate=sample_rate,
            return_tensors="pt",
        )
        inputs = {
            key: value.to(self._device) if hasattr(value, "to") else value
            for key, value in inputs.items()
        }
        with self._torch.no_grad():
            logits = self._model(**inputs).logits[0]
            scores = self._torch.sigmoid(logits)
            top_count = min(25, int(scores.numel()))
            values, indices = self._torch.topk(scores, top_count)
        id2label = getattr(self._model.config, "id2label", {})
        rows: list[dict[str, float]] = []
        for value, index in zip(values.tolist(), indices.tolist(), strict=False):
            label = str(id2label.get(index, index))
            rows.append({"label": label, "score": float(value)})
        return rows


def optional_module_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


def content_event_detector_status(config: BotConfig | None = None) -> dict[str, Any]:
    available = optional_module_available("transformers") and optional_module_available("torch")
    enabled = bool(config.stream_event_detection_enabled) if config is not None else False
    if not available:
        message = "Install onlysavemevods[stream-events] to enable ML content event detection."
    elif not enabled:
        message = "Content event detection is disabled."
    else:
        message = "Content event detection backend is available."
    return {"available": available, "enabled": enabled, "message": message}


def content_event_file(media_file: Path) -> Path:
    return media_file.with_suffix(CONTENT_EVENT_SUFFIX)


def content_events_exist(media_file: Path) -> bool:
    return content_event_file(media_file).is_file()


def load_content_event_payload(media_file: Path) -> dict[str, Any]:
    path = content_event_file(media_file)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_content_events(media_file: Path) -> list[dict[str, Any]]:
    payload = load_content_event_payload(media_file)
    events = payload.get("events", [])
    return [event for event in events if isinstance(event, dict)]


def effective_content_event_settings(config: BotConfig, channel: str = "") -> ContentEventSettings:
    enabled = config.stream_event_detection_enabled
    model = config.stream_event_model
    device = config.stream_event_device
    window_seconds = config.stream_event_window_seconds
    hop_seconds = config.stream_event_hop_seconds
    min_confidence = config.stream_event_min_confidence
    max_events = config.stream_event_max_events_per_media
    rules = list(config.stream_event_rules)

    match = streamer_for_channel(config, channel) if channel else None
    if match is not None:
        _name, streamer = match
        override = streamer.stream_event_detection
        if override is not None:
            if override.enabled is not None:
                enabled = override.enabled
            model = override.model or model
            device = override.device or device
            window_seconds = override.window_seconds or window_seconds
            hop_seconds = override.hop_seconds or hop_seconds
            min_confidence = (
                override.min_confidence
                if override.min_confidence >= 0
                else min_confidence
            )
            max_events = override.max_events_per_media or max_events
        if streamer.stream_event_rules:
            rules = list(streamer.stream_event_rules)

    return ContentEventSettings(
        enabled=enabled,
        model=model,
        device=device,
        window_seconds=window_seconds,
        hop_seconds=hop_seconds,
        min_confidence=min_confidence,
        max_events_per_media=max_events,
        rules=rules,
    )


def detect_content_events_for_media(
    config: BotConfig,
    media_file: Path,
    *,
    overwrite: bool = False,
    channel: str = "",
    logger: logging.Logger = LOGGER,
    progress_callback: ProgressCallback | None = None,
    backend: AudioClassifierBackend | None = None,
) -> bool:
    settings = effective_content_event_settings(config, channel)
    if not settings.enabled:
        logger.info("Content event detection is disabled for %s", media_file)
        return False
    output_file = content_event_file(media_file)
    if output_file.is_file() and not overwrite:
        logger.info("Content event output already exists for %s", media_file)
        return True

    def emit(phase: str, progress: float | None = None) -> None:
        if progress_callback is not None:
            progress_callback(phase, progress)

    emit("Preparing event detection", 0.02)
    transcript_segments = load_whisperx_subtitle_segments(
        transcription_output_file(media_file, ".json"),
        logger=logger,
    )
    rules = [rule for rule in settings.rules if rule.enabled]
    warnings: list[str] = []
    if not rules:
        warnings.append("No content event rules are configured.")

    candidates: list[EventCandidate] = []
    candidates.extend(keyword_only_candidates(rules, transcript_segments))

    audio_rules = [
        rule
        for rule in rules
        if rule.labels or rule.min_loudness_dbfs is not None
    ]
    if any(rule.keywords for rule in rules) and not transcript_segments:
        warnings.append("Transcript keywords were configured, but no transcript JSON was found.")

    if audio_rules:
        if any(rule.labels for rule in audio_rules) and backend is None:
            backend = HuggingFaceAudioSetBackend(settings)
        candidates.extend(
            audio_rule_candidates(
                config,
                media_file,
                settings,
                audio_rules,
                transcript_segments,
                backend,
                emit,
            )
        )

    emit("Merging content events", 0.85)
    events = finalize_candidates(candidates, rules, settings)
    payload = {
        "version": CONTENT_EVENT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "media": media_file.name,
        "model": settings.model,
        "settings": {
            "window_seconds": settings.window_seconds,
            "hop_seconds": settings.hop_seconds,
            "min_confidence": settings.min_confidence,
            "max_events_per_media": settings.max_events_per_media,
        },
        "rules": [rule_summary(rule) for rule in settings.rules],
        "warnings": warnings,
        "events": [candidate_to_dict(event) for event in events],
    }
    emit("Writing content event sidecar", 0.95)
    output_file.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    emit("Content event detection complete", 1.0)
    logger.info("Detected %s content events for %s", len(events), media_file)
    return True


def keyword_only_candidates(
    rules: list[StreamEventRuleConfig],
    transcript_segments: list[dict[str, Any]],
) -> list[EventCandidate]:
    candidates: list[EventCandidate] = []
    keyword_rules = [
        rule
        for rule in rules
        if rule.keywords and not rule.labels and rule.min_loudness_dbfs is None
    ]
    if not keyword_rules or not transcript_segments:
        return candidates
    for segment in transcript_segments:
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        start = float(segment.get("start") or 0.0)
        end = float(segment.get("end") or start)
        for rule in keyword_rules:
            matched = matched_keywords(text, rule.keywords)
            if not matched:
                continue
            candidates.append(
                EventCandidate(
                    start=start,
                    end=max(end, start + MIN_AUDIO_WINDOW_SECONDS),
                    rule=rule.name,
                    severity=rule.severity,
                    score=1.0,
                    keywords=set(matched),
                    text=text,
                )
            )
    return candidates


def audio_rule_candidates(
    config: BotConfig,
    media_file: Path,
    settings: ContentEventSettings,
    rules: list[StreamEventRuleConfig],
    transcript_segments: list[dict[str, Any]],
    backend: AudioClassifierBackend | None,
    emit: ProgressCallback,
) -> list[EventCandidate]:
    candidates: list[EventCandidate] = []
    duration = probe_media_duration(config.ffmpeg_path, media_file)
    for start, audio in iter_audio_windows(
        config.ffmpeg_path,
        media_file,
        settings.window_seconds,
        settings.hop_seconds,
    ):
        end = start + max(len(audio) / CONTENT_EVENT_SAMPLE_RATE, MIN_AUDIO_WINDOW_SECONDS)
        progress = None
        if duration:
            progress = 0.1 + min(0.7, 0.7 * (start / duration))
        emit("Analyzing audio events", progress)
        loudness = rms_dbfs(audio)
        labels: list[dict[str, float]] = []
        if backend is not None:
            labels = [
                row
                for row in backend.classify(audio, CONTENT_EVENT_SAMPLE_RATE)
                if float(row.get("score", 0.0)) >= settings.min_confidence
            ]
        text = transcript_text_for_window(transcript_segments, start, end)
        for rule in rules:
            candidate = candidate_for_rule(
                rule,
                start,
                end,
                loudness,
                labels,
                text,
                settings.min_confidence,
            )
            if candidate is not None:
                candidates.append(candidate)
    return candidates


def iter_audio_windows(
    ffmpeg_path: str,
    media_file: Path,
    window_seconds: float,
    hop_seconds: float,
) -> Any:
    window_samples = max(1, int(window_seconds * CONTENT_EVENT_SAMPLE_RATE))
    hop_samples = max(1, int(hop_seconds * CONTENT_EVENT_SAMPLE_RATE))
    command = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(media_file),
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(CONTENT_EVENT_SAMPLE_RATE),
        "-f",
        "f32le",
        "-",
    ]
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise ContentEventDetectionError(f"ffmpeg not found: {ffmpeg_path}") from exc
    if process.stdout is None:
        raise ContentEventDetectionError("Unable to read ffmpeg audio output")

    buffer = np.empty(0, dtype=np.float32)
    start_sample = 0
    while True:
        chunk = process.stdout.read(262_144)
        if not chunk:
            break
        usable = len(chunk) - (len(chunk) % 4)
        if usable <= 0:
            continue
        samples = np.frombuffer(chunk[:usable], dtype=np.float32)
        buffer = np.concatenate((buffer, samples))
        while len(buffer) >= window_samples:
            yield start_sample / CONTENT_EVENT_SAMPLE_RATE, buffer[:window_samples].copy()
            buffer = buffer[hop_samples:]
            start_sample += hop_samples

    stderr = process.stderr.read().decode("utf-8", "replace") if process.stderr else ""
    exit_code = process.wait()
    if process.stdout is not None:
        process.stdout.close()
    if process.stderr is not None:
        process.stderr.close()
    if exit_code != 0:
        raise ContentEventDetectionError(stderr.strip() or f"ffmpeg exited with {exit_code}")
    if len(buffer) >= int(MIN_AUDIO_WINDOW_SECONDS * CONTENT_EVENT_SAMPLE_RATE):
        yield start_sample / CONTENT_EVENT_SAMPLE_RATE, buffer.copy()


def probe_media_duration(ffmpeg_path: str, media_file: Path) -> float | None:
    ffprobe = str(Path(ffmpeg_path).with_name("ffprobe")) if "/" in ffmpeg_path else "ffprobe"
    try:
        result = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(media_file),
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        duration = float(result.stdout.strip())
    except ValueError:
        return None
    return duration if duration > 0 else None


def candidate_for_rule(
    rule: StreamEventRuleConfig,
    start: float,
    end: float,
    loudness: float,
    labels: list[dict[str, float]],
    text: str,
    min_confidence: float,
) -> EventCandidate | None:
    matched_label_scores: dict[str, float] = {}
    if rule.labels:
        for row in labels:
            label = str(row.get("label") or "")
            score = float(row.get("score") or 0.0)
            if score >= min_confidence and label_matches(label, rule.labels):
                matched_label_scores[label] = max(score, matched_label_scores.get(label, 0.0))
        if not matched_label_scores:
            return None
    matched = matched_keywords(text, rule.keywords) if rule.keywords else []
    if rule.keywords and not matched:
        return None
    if rule.min_loudness_dbfs is not None and loudness < rule.min_loudness_dbfs:
        return None
    score = max(matched_label_scores.values(), default=1.0 if matched else 0.75)
    return EventCandidate(
        start=start,
        end=end,
        rule=rule.name,
        severity=rule.severity,
        score=score,
        loudness_dbfs=loudness,
        labels=matched_label_scores,
        keywords=set(matched),
        text=text,
    )


def finalize_candidates(
    candidates: list[EventCandidate],
    rules: list[StreamEventRuleConfig],
    settings: ContentEventSettings,
) -> list[EventCandidate]:
    rules_by_name = {rule.name: rule for rule in rules}
    merged = merge_candidates(candidates, max_gap=max(settings.hop_seconds * 1.5, 0.5))
    filtered: list[EventCandidate] = []
    for event in merged:
        rule = rules_by_name.get(event.rule)
        if rule is None:
            continue
        if rule.min_duration_seconds and event.duration < rule.min_duration_seconds:
            continue
        if rule.max_duration_seconds and event.duration > rule.max_duration_seconds:
            continue
        filtered.append(event)
    if len(filtered) > settings.max_events_per_media:
        filtered = sorted(filtered, key=lambda item: item.score, reverse=True)[
            : settings.max_events_per_media
        ]
    return sorted(filtered, key=lambda item: item.start)


def merge_candidates(candidates: list[EventCandidate], *, max_gap: float) -> list[EventCandidate]:
    merged: list[EventCandidate] = []
    for candidate in sorted(candidates, key=lambda item: (item.rule, item.start, item.end)):
        if (
            merged
            and merged[-1].rule == candidate.rule
            and candidate.start - merged[-1].end <= max_gap
        ):
            target = merged[-1]
            target.end = max(target.end, candidate.end)
            target.score = max(target.score, candidate.score)
            if candidate.loudness_dbfs is not None:
                target.loudness_dbfs = max(
                    target.loudness_dbfs
                    if target.loudness_dbfs is not None
                    else candidate.loudness_dbfs,
                    candidate.loudness_dbfs,
                )
            for label, score in candidate.labels.items():
                target.labels[label] = max(score, target.labels.get(label, 0.0))
            target.keywords.update(candidate.keywords)
            if candidate.text and candidate.text not in target.text:
                target.text = (target.text + " " + candidate.text).strip()
        else:
            merged.append(candidate)
    return merged


def candidate_to_dict(candidate: EventCandidate) -> dict[str, Any]:
    labels = [
        {"label": label, "score": round(score, 4)}
        for label, score in sorted(candidate.labels.items(), key=lambda item: item[1], reverse=True)
    ]
    return {
        "start": round(candidate.start, 3),
        "end": round(candidate.end, 3),
        "duration": round(candidate.duration, 3),
        "rule": candidate.rule,
        "severity": candidate.severity,
        "score": round(candidate.score, 4),
        "loudness_dbfs": (
            round(candidate.loudness_dbfs, 2)
            if candidate.loudness_dbfs is not None
            else None
        ),
        "labels": labels,
        "keywords": sorted(candidate.keywords),
        "text": candidate.text[:240],
    }


def rule_summary(rule: StreamEventRuleConfig) -> dict[str, Any]:
    return {
        "name": rule.name,
        "enabled": rule.enabled,
        "labels": list(rule.labels),
        "keywords": list(rule.keywords),
        "min_loudness_dbfs": rule.min_loudness_dbfs,
        "min_duration_seconds": rule.min_duration_seconds,
        "max_duration_seconds": rule.max_duration_seconds,
        "severity": rule.severity,
    }


def label_matches(label: str, targets: list[str]) -> bool:
    normalized_label = normalize_match_text(label)
    for target in targets:
        normalized_target = normalize_match_text(target)
        if not normalized_target:
            continue
        if normalized_label == normalized_target:
            return True
        if normalized_target in normalized_label or normalized_label in normalized_target:
            return True
    return False


def matched_keywords(text: str, keywords: list[str]) -> list[str]:
    normalized = text.casefold()
    return [keyword for keyword in keywords if keyword.casefold() in normalized]


def transcript_text_for_window(
    transcript_segments: list[dict[str, Any]],
    start: float,
    end: float,
) -> str:
    parts: list[str] = []
    for segment in transcript_segments:
        seg_start = float(segment.get("start") or 0.0)
        seg_end = float(segment.get("end") or seg_start)
        if seg_end < start or seg_start > end:
            continue
        text = str(segment.get("text") or "").strip()
        if text:
            parts.append(text)
    return " ".join(parts)


def rms_dbfs(audio: np.ndarray) -> float:
    if len(audio) == 0:
        return -120.0
    samples = audio.astype(np.float64, copy=False)
    rms = math.sqrt(float(np.mean(np.square(samples))))
    if rms <= 0:
        return -120.0
    return max(-120.0, 20.0 * math.log10(rms))


def normalize_match_text(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").replace("-", " ").split())
