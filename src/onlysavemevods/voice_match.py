from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
import importlib.util
import json
import logging
import math
import os
import time

from .config import (
    BotConfig,
    VoiceProfileConfig,
    sanitize_voice_component,
    streamer_for_channel,
    voice_sample_dir,
    voice_sample_path,
)


LOGGER = logging.getLogger(__name__)
VOICE_ATTRIBUTION_SUFFIX = ".voice-attribution.json"
VOICE_SAMPLE_METADATA_VERSION = 1
VOICE_ATTRIBUTION_VERSION = 1
MAX_SAMPLE_SEGMENTS = 40
MAX_SAMPLE_SECONDS = 180.0
MAX_MATCH_SECONDS = 180.0
MIN_SEGMENT_SECONDS = 0.25


class VoiceMatcherUnavailable(RuntimeError):
    """Raised when optional voice matching dependencies are unavailable."""


class EmbeddingBackend(Protocol):
    def embed(
        self,
        media_file: Path,
        ranges: list[tuple[float, float]] | None = None,
    ) -> list[float]:
        """Return one embedding vector for the whole file or selected ranges."""


@dataclass(frozen=True, slots=True)
class VoiceMatchCandidate:
    voice: str
    distance: float
    threshold: float


@dataclass(frozen=True, slots=True)
class VoiceMatchDecision:
    speaker: str
    voice: str
    status: str
    distance: float
    margin: float | None
    threshold: float
    candidates: list[VoiceMatchCandidate]


def optional_module_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


def voice_matcher_status(config: BotConfig | None = None) -> dict[str, Any]:
    available = optional_module_available("pyannote.audio") and optional_module_available("pyannote.core")
    token_env = config.whisperx_hf_token_env if config is not None else "HF_TOKEN"
    token_configured = bool(token_env and os.environ.get(token_env, "").strip())
    if not available:
        message = "Install onlysavemevods[voice-match] to enable automatic voice matching."
    elif token_env and not token_configured:
        message = f"{token_env} is not set; pyannote may be unable to load the embedding model."
    else:
        message = "Voice matching backend is available."
    return {
        "available": available,
        "token_env": token_env,
        "token_configured": token_configured,
        "message": message,
    }


class PyannoteEmbeddingBackend:
    def __init__(self, config: BotConfig) -> None:
        try:
            from pyannote.audio import Inference
            from pyannote.core import Segment
        except Exception as exc:  # noqa: BLE001 - optional dependency boundary.
            raise VoiceMatcherUnavailable(
                "pyannote.audio and pyannote.core are required for voice matching"
            ) from exc

        token = os.environ.get(config.whisperx_hf_token_env, "").strip()
        kwargs: dict[str, Any] = {"window": "whole"}
        if token:
            kwargs["use_auth_token"] = token
        try:
            self._inference = Inference(config.voice_match_model, **kwargs)
        except TypeError:
            kwargs.pop("use_auth_token", None)
            try:
                self._inference = Inference(config.voice_match_model, **kwargs)
            except Exception as fallback_exc:  # noqa: BLE001 - model auth/cache errors need graceful handling.
                raise VoiceMatcherUnavailable(str(fallback_exc)) from fallback_exc
        except Exception as exc:  # noqa: BLE001 - model auth/cache errors need graceful handling.
            raise VoiceMatcherUnavailable(str(exc)) from exc
        self._segment_cls = Segment

    def embed(
        self,
        media_file: Path,
        ranges: list[tuple[float, float]] | None = None,
    ) -> list[float]:
        if ranges:
            vectors: list[list[float]] = []
            for start, end in ranges:
                segment = self._segment_cls(float(start), float(end))
                try:
                    raw = self._inference.crop(str(media_file), segment)
                except TypeError:
                    raw = self._inference.crop({"audio": str(media_file)}, segment)
                vectors.append(vector_from_embedding(raw))
            return average_vectors(vectors)
        try:
            raw = self._inference(str(media_file))
        except TypeError:
            raw = self._inference({"audio": str(media_file)})
        return vector_from_embedding(raw)


def vector_from_embedding(value: Any) -> list[float]:
    if hasattr(value, "detach"):
        value = value.detach().cpu().numpy()
    elif hasattr(value, "numpy"):
        value = value.numpy()
    try:
        import numpy as np
    except Exception:  # noqa: BLE001 - numpy is a hard project dependency, but keep error clear.
        np = None
    if np is not None:
        array = np.asarray(value, dtype=float).reshape(-1)
        vector = [float(item) for item in array.tolist()]
    else:
        if isinstance(value, (list, tuple)):
            flat: list[float] = []
            for item in value:
                if isinstance(item, (list, tuple)):
                    flat.extend(float(inner) for inner in item)
                else:
                    flat.append(float(item))
            vector = flat
        else:
            raise VoiceMatcherUnavailable("embedding output cannot be converted to a vector")
    if not vector:
        raise VoiceMatcherUnavailable("embedding output was empty")
    return normalize_vector(vector)


def normalize_vector(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if not norm:
        return vector
    return [value / norm for value in vector]


def average_vectors(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        raise VoiceMatcherUnavailable("no embeddings were produced")
    length = len(vectors[0])
    usable = [vector for vector in vectors if len(vector) == length]
    if not usable:
        raise VoiceMatcherUnavailable("embedding dimensions did not match")
    averaged = [sum(vector[index] for vector in usable) / len(usable) for index in range(length)]
    return normalize_vector(averaged)


def cosine_distance(left: list[float], right: list[float]) -> float:
    length = min(len(left), len(right))
    if not length:
        return 1.0
    dot = sum(left[index] * right[index] for index in range(length))
    return max(0.0, min(2.0, 1.0 - dot))


def transcription_json_file(media_file: Path) -> Path:
    return media_file.with_suffix(".json")


def voice_attribution_file(media_file: Path) -> Path:
    return media_file.with_suffix(VOICE_ATTRIBUTION_SUFFIX)


def load_transcript_segments(json_file: Path, logger: logging.Logger = LOGGER) -> list[dict[str, Any]]:
    if not json_file.is_file():
        return []
    try:
        payload = json.loads(json_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Unable to read transcript JSON %s: %s", json_file, exc)
        return []
    raw_segments = payload.get("segments") if isinstance(payload, dict) else None
    if not isinstance(raw_segments, list):
        return []

    segments: list[dict[str, Any]] = []
    for raw_segment in raw_segments:
        if not isinstance(raw_segment, dict):
            continue
        text = str(raw_segment.get("text") or "").strip()
        try:
            start = max(0.0, float(raw_segment.get("start", 0.0)))
            end = max(start + 0.001, float(raw_segment.get("end", start + 0.001)))
        except (TypeError, ValueError):
            continue
        speaker = str(raw_segment.get("speaker") or speaker_from_words(raw_segment.get("words")) or "").strip()
        segments.append({"start": start, "end": end, "text": text, "speaker": speaker})
    return segments


def speaker_from_words(words: Any) -> str:
    if not isinstance(words, list):
        return ""
    counts: dict[str, int] = {}
    for word in words:
        if not isinstance(word, dict):
            continue
        speaker = str(word.get("speaker") or "").strip()
        if speaker:
            counts[speaker] = counts.get(speaker, 0) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda item: item[1])[0]


def speaker_labels_in_segments(segments: list[dict[str, Any]]) -> list[str]:
    return sorted({str(segment.get("speaker") or "").strip() for segment in segments if str(segment.get("speaker") or "").strip()})


def ranges_for_speaker(
    segments: list[dict[str, Any]],
    speaker: str,
    *,
    max_seconds: float = MAX_MATCH_SECONDS,
) -> list[tuple[float, float]]:
    ranges: list[tuple[float, float]] = []
    total = 0.0
    for segment in segments:
        if str(segment.get("speaker") or "").strip() != speaker:
            continue
        start = float(segment.get("start", 0.0))
        end = float(segment.get("end", start))
        duration = max(0.0, end - start)
        if duration < MIN_SEGMENT_SECONDS:
            continue
        if total + duration > max_seconds:
            end = start + max(0.0, max_seconds - total)
            duration = max(0.0, end - start)
        if duration >= MIN_SEGMENT_SECONDS:
            ranges.append((start, end))
            total += duration
        if total >= max_seconds:
            break
    return ranges


def load_voice_attribution_payload(media_file: Path) -> dict[str, Any]:
    path = voice_attribution_file(media_file)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_voice_attribution_payload(media_file: Path, payload: dict[str, Any]) -> None:
    path = voice_attribution_file(media_file)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def voice_attribution_labels_for_media(media_file: Path) -> dict[str, str]:
    payload = load_voice_attribution_payload(media_file)
    matches = payload.get("matches") if isinstance(payload, dict) else None
    if not isinstance(matches, dict):
        return {}
    labels: dict[str, str] = {}
    for speaker, raw_match in matches.items():
        if not isinstance(raw_match, dict):
            continue
        status = str(raw_match.get("status") or "")
        voice = str(raw_match.get("voice") or "").strip()
        if voice and status in {"auto", "approved"}:
            labels[str(speaker)] = voice
    return labels


def voice_match_rows_for_media(media_file: Path) -> list[dict[str, Any]]:
    payload = load_voice_attribution_payload(media_file)
    matches = payload.get("matches") if isinstance(payload, dict) else None
    if not isinstance(matches, dict):
        return []
    rows: list[dict[str, Any]] = []
    for speaker, raw_match in sorted(matches.items()):
        if not isinstance(raw_match, dict):
            continue
        rows.append({"speaker": speaker, **raw_match})
    return rows


def update_voice_attribution_decision(
    media_file: Path,
    speaker_label: str,
    action: str,
    *,
    voice_name: str = "",
) -> bool:
    speaker = speaker_label.strip()
    if not speaker:
        return False
    payload = load_voice_attribution_payload(media_file)
    if not payload:
        payload = {
            "version": VOICE_ATTRIBUTION_VERSION,
            "media_name": media_file.name,
            "generated_at": time.time(),
            "matches": {},
        }
    matches = payload.setdefault("matches", {})
    if not isinstance(matches, dict):
        matches = {}
        payload["matches"] = matches
    existing = matches.get(speaker)
    if not isinstance(existing, dict):
        existing = {"speaker": speaker, "voice": voice_name.strip(), "status": "suggested"}
    if action == "approve":
        if voice_name.strip():
            existing["voice"] = voice_name.strip()
        existing["status"] = "approved"
    elif action == "reject":
        existing["status"] = "rejected"
    else:
        return False
    matches[speaker] = existing
    payload["updated_at"] = time.time()
    write_voice_attribution_payload(media_file, payload)
    return True


def match_known_voices_for_media(
    config: BotConfig,
    media_file: Path,
    *,
    channel: str = "",
    logger: logging.Logger = LOGGER,
    backend: EmbeddingBackend | None = None,
) -> bool:
    if not config.voice_match_enabled:
        return False
    match = streamer_for_channel(config, channel)
    if match is None:
        return False
    streamer_name, streamer = match
    profiles = {
        name: profile
        for name, profile in streamer.voices.items()
        if profile.enabled and profile.samples
    }
    if not profiles:
        return False

    segments = load_transcript_segments(transcription_json_file(media_file), logger=logger)
    speakers = speaker_labels_in_segments(segments)
    if not speakers:
        return False

    if backend is None:
        try:
            backend = PyannoteEmbeddingBackend(config)
        except VoiceMatcherUnavailable as exc:
            logger.warning("Voice matching unavailable for %s: %s", media_file, exc)
            return False

    try:
        profile_embeddings = {
            name: voice_profile_embedding(config, streamer_name, name, profile, backend)
            for name, profile in profiles.items()
        }
    except VoiceMatcherUnavailable as exc:
        logger.warning("Unable to build voice profile embeddings for %s: %s", streamer_name, exc)
        return False

    matches: dict[str, dict[str, Any]] = {}
    for speaker in speakers:
        ranges = ranges_for_speaker(segments, speaker)
        if not ranges:
            continue
        try:
            speaker_embedding = backend.embed(media_file, ranges)
        except VoiceMatcherUnavailable as exc:
            logger.warning("Unable to embed speaker %s from %s: %s", speaker, media_file, exc)
            continue
        candidates = sorted(
            (
                VoiceMatchCandidate(
                    voice=name,
                    distance=cosine_distance(speaker_embedding, embedding),
                    threshold=profiles[name].threshold or config.voice_match_threshold,
                )
                for name, embedding in profile_embeddings.items()
            ),
            key=lambda candidate: candidate.distance,
        )
        if not candidates:
            continue
        best = candidates[0]
        second = candidates[1] if len(candidates) > 1 else None
        margin = None if second is None else second.distance - best.distance
        confident = best.distance <= best.threshold and (
            margin is None or margin >= config.voice_match_min_margin
        )
        matches[speaker] = {
            "speaker": speaker,
            "voice": best.voice,
            "status": "auto" if confident else "suggested",
            "distance": round(best.distance, 6),
            "margin": None if margin is None else round(margin, 6),
            "threshold": best.threshold,
            "candidates": [
                {
                    "voice": candidate.voice,
                    "distance": round(candidate.distance, 6),
                    "threshold": candidate.threshold,
                }
                for candidate in candidates[:5]
            ],
        }

    if not matches:
        return False
    payload = {
        "version": VOICE_ATTRIBUTION_VERSION,
        "generated_at": time.time(),
        "media_name": media_file.name,
        "channel": channel,
        "streamer": streamer_name,
        "model": config.voice_match_model,
        "matches": matches,
    }
    write_voice_attribution_payload(media_file, payload)
    logger.info(
        "Matched known voices media=%s streamer=%s speakers=%s auto=%s",
        media_file,
        streamer_name,
        sorted(matches),
        sorted(label for label, item in matches.items() if item.get("status") == "auto"),
    )
    return True


def voice_profile_embedding(
    config: BotConfig,
    streamer_name: str,
    voice_name: str,
    profile: VoiceProfileConfig,
    backend: EmbeddingBackend,
) -> list[float]:
    vectors = [
        embedding_for_sample(config, streamer_name, voice_name, sample, backend)
        for sample in profile.samples
    ]
    return average_vectors(vectors)


def embedding_for_sample(
    config: BotConfig,
    streamer_name: str,
    voice_name: str,
    sample: str,
    backend: EmbeddingBackend,
) -> list[float]:
    sample_path = voice_sample_path(config, streamer_name, voice_name, sample)
    if not sample_path.is_file():
        raise VoiceMatcherUnavailable(f"voice sample does not exist: {sample_path}")
    cached = load_cached_embedding(sample_path, config.voice_match_model)
    if cached is not None:
        return cached
    media_file, ranges = voice_sample_embedding_source(sample_path)
    vector = backend.embed(media_file, ranges)
    write_cached_embedding(sample_path, config.voice_match_model, vector)
    return vector


def voice_sample_embedding_source(sample_path: Path) -> tuple[Path, list[tuple[float, float]] | None]:
    if sample_path.suffix == ".json":
        try:
            payload = json.loads(sample_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise VoiceMatcherUnavailable(f"unable to read voice sample metadata: {sample_path}") from exc
        if isinstance(payload, dict) and payload.get("kind") == "transcript-segments":
            media_file = Path(str(payload.get("media_file") or ""))
            ranges = payload.get("ranges")
            if not media_file.is_file():
                raise VoiceMatcherUnavailable(f"sample source media does not exist: {media_file}")
            if not isinstance(ranges, list):
                raise VoiceMatcherUnavailable(f"sample metadata has no ranges: {sample_path}")
            parsed_ranges: list[tuple[float, float]] = []
            for item in ranges:
                if not isinstance(item, (list, tuple)) or len(item) != 2:
                    continue
                try:
                    start = float(item[0])
                    end = float(item[1])
                except (TypeError, ValueError):
                    continue
                if end - start >= MIN_SEGMENT_SECONDS:
                    parsed_ranges.append((start, end))
            if not parsed_ranges:
                raise VoiceMatcherUnavailable(f"sample metadata has no usable ranges: {sample_path}")
            return media_file, parsed_ranges
    return sample_path, None


def embedding_cache_path(sample_path: Path) -> Path:
    return sample_path.with_name(sample_path.name + ".embedding.json")


def load_cached_embedding(sample_path: Path, model: str) -> list[float] | None:
    cache_path = embedding_cache_path(sample_path)
    if not cache_path.is_file():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        sample_mtime = sample_path.stat().st_mtime
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("model") != model:
        return None
    if float(payload.get("sample_mtime", -1.0)) != sample_mtime:
        return None
    raw_embedding = payload.get("embedding")
    if not isinstance(raw_embedding, list):
        return None
    try:
        return normalize_vector([float(item) for item in raw_embedding])
    except (TypeError, ValueError):
        return None


def write_cached_embedding(sample_path: Path, model: str, vector: list[float]) -> None:
    cache_path = embedding_cache_path(sample_path)
    try:
        sample_mtime = sample_path.stat().st_mtime
        cache_path.write_text(
            json.dumps(
                {
                    "model": model,
                    "sample_mtime": sample_mtime,
                    "embedding": vector,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    except OSError:
        LOGGER.debug("Unable to write voice embedding cache %s", cache_path, exc_info=True)


def create_transcript_voice_sample(
    config: BotConfig,
    streamer_name: str,
    voice_name: str,
    media_file: Path,
    speaker_label: str,
) -> str:
    speaker = speaker_label.strip()
    if not speaker:
        raise ValueError("speaker label is required")
    segments = load_transcript_segments(transcription_json_file(media_file))
    ranges = ranges_for_speaker(segments, speaker, max_seconds=MAX_SAMPLE_SECONDS)
    if not ranges:
        raise ValueError(f"no transcript segments found for {speaker}")
    ranges = ranges[:MAX_SAMPLE_SEGMENTS]
    directory = voice_sample_dir(config, streamer_name, voice_name)
    directory.mkdir(parents=True, exist_ok=True)
    stem = sanitize_voice_component(f"{Path(media_file).stem}-{speaker}", fallback="sample")[:96]
    sample_name = unique_sample_name(directory, f"{stem}.voice-sample.json")
    payload = {
        "version": VOICE_SAMPLE_METADATA_VERSION,
        "kind": "transcript-segments",
        "created_at": time.time(),
        "streamer": streamer_name,
        "voice": voice_name,
        "media_file": str(media_file.resolve()),
        "speaker_label": speaker,
        "ranges": [[round(start, 3), round(end, 3)] for start, end in ranges],
    }
    (directory / sample_name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return sample_name


def unique_sample_name(directory: Path, filename: str) -> str:
    path = directory / filename
    if not path.exists():
        return filename
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    for index in range(2, 1000):
        candidate = f"{stem}-{index}{suffix}"
        if not (directory / candidate).exists():
            return candidate
    raise ValueError("unable to allocate a unique voice sample filename")
