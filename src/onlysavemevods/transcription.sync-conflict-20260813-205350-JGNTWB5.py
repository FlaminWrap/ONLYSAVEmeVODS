from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Callable
import asyncio
import json
import logging
import os
import re
import shlex
import time

from .config import BotConfig, VoiceDetectionConfig, streamer_for_channel


LOGGER = logging.getLogger(__name__)
PRIMARY_SUBTITLE_SUFFIXES = (".srt", ".vtt")
TRANSCRIPTION_OUTPUT_SUFFIXES = (".srt", ".vtt", ".txt", ".tsv", ".json")
SENSITIVE_COMMAND_OPTIONS = {"--hf_token"}
HUGGINGFACE_TOKEN_RE = re.compile(r"\bhf_[A-Za-z0-9]{20,}\b")
ProgressCallback = Callable[[str, float | None], None]


def transcription_config_for_channel(config: BotConfig, channel: str) -> BotConfig:
    override = voice_detection_override_for_channel(config, channel)
    if override is None:
        return config
    return apply_voice_detection_override(config, override)


def voice_detection_override_for_channel(
    config: BotConfig,
    channel: str,
) -> VoiceDetectionConfig | None:
    if not channel.strip():
        return None
    if channel in config.channel_voice_detection:
        return config.channel_voice_detection[channel]
    target = channel.strip().casefold()
    for configured_channel, override in config.channel_voice_detection.items():
        if configured_channel.strip().casefold() == target:
            return override
    match = streamer_for_channel(config, channel)
    if match is not None:
        _, streamer = match
        if streamer.voice_detection is not None:
            return streamer.voice_detection
    return None


def apply_voice_detection_override(
    config: BotConfig,
    override: VoiceDetectionConfig,
) -> BotConfig:
    diarize = override.mode != "off"
    min_speakers = override.min_speakers if diarize else 0
    max_speakers = override.max_speakers if diarize else 0
    hf_token_env = override.hf_token_env or config.whisperx_hf_token_env
    return replace(
        config,
        whisperx_diarize=diarize,
        whisperx_min_speakers=min_speakers,
        whisperx_max_speakers=max_speakers,
        whisperx_hf_token_env=hf_token_env,
    )


def speaker_labels_for_channel(config: BotConfig, channel: str) -> dict[str, str]:
    if not channel.strip():
        return {}
    labels: dict[str, str] = {}
    match = streamer_for_channel(config, channel)
    if match is not None:
        _, streamer = match
        labels.update(streamer.speaker_labels)
    if channel in config.channel_speaker_labels:
        labels.update(config.channel_speaker_labels[channel])
        return labels
    target = speaker_label_channel_key(channel)
    for configured_channel, channel_labels in config.channel_speaker_labels.items():
        if speaker_label_channel_key(configured_channel) == target:
            labels.update(channel_labels)
            break
    return labels


def speaker_label_channel_key(channel: str) -> str:
    target = channel.strip().rstrip("/")
    if "/" in target:
        target = target.rsplit("/", 1)[-1]
    if target.startswith("@"):
        target = target[1:]
    folded = target.casefold()
    compact = re.sub(r"[^a-z0-9]+", "", folded)
    return compact or folded


def voice_detection_mode(config: BotConfig) -> str:
    if not config.whisperx_diarize:
        return "off"
    if config.whisperx_min_speakers and config.whisperx_max_speakers:
        if config.whisperx_min_speakers == config.whisperx_max_speakers:
            return "fixed"
        return "range"
    if config.whisperx_min_speakers or config.whisperx_max_speakers:
        return "range"
    return "auto"


def voice_detection_speaker_summary(config: BotConfig) -> str:
    if not config.whisperx_diarize:
        return "disabled"
    min_speakers = config.whisperx_min_speakers
    max_speakers = config.whisperx_max_speakers
    if min_speakers and max_speakers:
        if min_speakers == max_speakers:
            return f"exactly {min_speakers}"
        return f"{min_speakers}-{max_speakers}"
    if min_speakers:
        return f"at least {min_speakers}"
    if max_speakers:
        return f"up to {max_speakers}"
    return "auto"


def build_whisperx_command(config: BotConfig, media_file: Path) -> list[str]:
    command = [
        config.whisperx_path,
        str(media_file),
        "--model",
        config.whisperx_model,
        "--device",
        config.whisperx_device,
        "--compute_type",
        config.whisperx_compute_type,
        "--batch_size",
        str(config.whisperx_batch_size),
        "--output_dir",
        str(media_file.parent),
        "--output_format",
        "all",
    ]
    if config.whisperx_language:
        command.extend(["--language", config.whisperx_language])
    if config.whisperx_diarize:
        command.append("--diarize")
        if config.whisperx_min_speakers:
            command.extend(["--min_speakers", str(config.whisperx_min_speakers)])
        if config.whisperx_max_speakers:
            command.extend(["--max_speakers", str(config.whisperx_max_speakers)])
    return command


async def transcribe_media_file(
    config: BotConfig,
    media_file: Path,
    *,
    overwrite: bool = False,
    logger: logging.Logger = LOGGER,
    progress_callback: ProgressCallback | None = None,
    channel: str = "",
) -> bool:
    def emit(phase: str, progress: float | None = None) -> None:
        if progress_callback is None:
            return
        progress_callback(phase, clamp_progress(progress))

    emit("Preparing transcription", 0.02)
    if overwrite:
        cleanup_transcription_outputs(media_file, logger)
    if transcription_outputs_exist(media_file):
        rewrite_speaker_labels_for_media(config, media_file, channel=channel, logger=logger)
        logger.info("Subtitle output already exists for %s", media_file)
        return True

    emit("Checking WhisperX inputs", 0.05)
    token = hf_token_for_config(config)
    if config.whisperx_diarize and config.whisperx_hf_token_env and not token:
        logger.warning(
            "WhisperX diarization is enabled but %s is not set; "
            "using cached Hugging Face credentials if available",
            shlex.quote(config.whisperx_hf_token_env),
        )

    command = build_whisperx_command(config, media_file)
    logger.info(
        "Transcribing subtitles with WhisperX media=%s model=%s voice_detection=%s",
        media_file,
        config.whisperx_model,
        voice_detection_mode(config),
    )
    logger.debug("WhisperX command: %s", command_for_log(command))
    started_at = time.monotonic()

    emit("Starting WhisperX", 0.1)
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=whisperx_process_env(token),
        )
    except FileNotFoundError:
        logger.warning(
            "Unable to start WhisperX; binary not found: %s",
            config.whisperx_path,
        )
        return False
    except OSError:
        logger.exception("Unable to start WhisperX for %s", media_file)
        return False

    output_task = None
    output_buffer: list[str] = []
    if process.stdout is not None:
        output_task = asyncio.create_task(
            monitor_process_output(
                process.stdout,
                output_buffer,
                logger,
                label="WhisperX",
                line_callback=lambda line: emit(*whisperx_progress_from_line(line)),
            )
        )

    emit("WhisperX running", 0.2)
    exit_code = await process.wait()
    if output_task is not None:
        try:
            await output_task
        except Exception as exc:  # noqa: BLE001 - output logging must not mask exit.
            logger.debug("WhisperX output monitor failed: %s", exc)

    emit("Finalizing transcription outputs", 0.9)
    if process.returncode != 0:
        log_process_output(logger, "WhisperX", output_buffer, failed=True)
        logger.warning(
            "WhisperX failed for %s exit_code=%s",
            media_file,
            exit_code,
        )
        return False

    log_process_output(logger, "WhisperX", output_buffer)
    rewrite_speaker_labels_for_media(config, media_file, channel=channel, logger=logger)
    outputs = existing_transcription_outputs(media_file)
    logger.info(
        "Created subtitle outputs for %s outputs=%s elapsed=%.1fs",
        media_file,
        [path.name for path in outputs],
        time.monotonic() - started_at,
    )
    ok = transcription_outputs_exist(media_file)
    emit("Transcription complete" if ok else "Missing subtitle outputs", 1.0 if ok else None)
    return ok


def rewrite_speaker_labels_for_media(
    config: BotConfig,
    media_file: Path,
    *,
    channel: str = "",
    logger: logging.Logger = LOGGER,
) -> bool:
    json_file = transcription_output_file(media_file, ".json")
    segments = load_whisperx_subtitle_segments(json_file, logger=logger)
    if not segments or not any(segment.get("speaker") for segment in segments):
        return False

    speaker_labels = speaker_labels_for_channel(config, channel)
    srt_file = transcription_output_file(media_file, ".srt")
    vtt_file = transcription_output_file(media_file, ".vtt")
    try:
        srt_file.write_text(
            render_speaker_srt(segments, speaker_labels),
            encoding="utf-8",
        )
        vtt_file.write_text(
            render_speaker_vtt(segments, speaker_labels),
            encoding="utf-8",
        )
    except OSError as exc:
        logger.warning(
            "Unable to write speaker-attributed subtitles for %s: %s",
            media_file,
            exc,
        )
        return False

    logger.info(
        "Applied speaker labels to subtitles media=%s channel=%s labels=%s",
        media_file,
        channel or "-",
        sorted(speaker_labels),
    )
    return True


def load_whisperx_subtitle_segments(
    json_file: Path,
    *,
    logger: logging.Logger = LOGGER,
) -> list[dict[str, Any]]:
    if not json_file.is_file():
        return []
    try:
        payload = json.loads(json_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Unable to read WhisperX JSON output %s: %s", json_file, exc)
        return []
    raw_segments = payload.get("segments") if isinstance(payload, dict) else None
    if not isinstance(raw_segments, list):
        return []

    segments: list[dict[str, Any]] = []
    for raw_segment in raw_segments:
        if not isinstance(raw_segment, dict):
            continue
        text = str(raw_segment.get("text") or "").strip()
        if not text:
            continue
        try:
            start = max(0.0, float(raw_segment.get("start", 0.0)))
            end = max(start + 0.001, float(raw_segment.get("end", start + 0.001)))
        except (TypeError, ValueError):
            continue
        speaker = str(
            raw_segment.get("speaker")
            or speaker_from_words(raw_segment.get("words"))
            or ""
        ).strip()
        segments.append(
            {"start": start, "end": end, "text": text, "speaker": speaker}
        )
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


def render_speaker_srt(
    segments: list[dict[str, Any]],
    speaker_labels: dict[str, str],
) -> str:
    blocks: list[str] = []
    for index, segment in enumerate(segments, start=1):
        blocks.append(
            "\n".join(
                [
                    str(index),
                    (
                        f"{format_srt_timestamp(float(segment['start']))} --> "
                        f"{format_srt_timestamp(float(segment['end']))}"
                    ),
                    subtitle_text_with_speaker(
                        str(segment['text']),
                        str(segment.get('speaker') or ''),
                        speaker_labels,
                    ),
                ]
            )
        )
    return "\n\n".join(blocks) + ("\n" if blocks else "")


def render_speaker_vtt(
    segments: list[dict[str, Any]],
    speaker_labels: dict[str, str],
) -> str:
    blocks = ["WEBVTT", ""]
    for segment in segments:
        blocks.append(
            "\n".join(
                [
                    (
                        f"{format_vtt_timestamp(float(segment['start']))} --> "
                        f"{format_vtt_timestamp(float(segment['end']))}"
                    ),
                    subtitle_text_with_speaker(
                        str(segment['text']),
                        str(segment.get('speaker') or ''),
                        speaker_labels,
                    ),
                ]
            )
        )
        blocks.append("")
    return "\n".join(blocks)


def subtitle_text_with_speaker(
    text: str,
    speaker: str,
    speaker_labels: dict[str, str],
) -> str:
    text = text.strip()
    speaker = speaker.strip()
    if not speaker:
        return text
    name = speaker_labels.get(speaker, speaker)
    if text.startswith(f"{name}:"):
        return text
    return f"{name}: {text}"


def format_srt_timestamp(seconds: float) -> str:
    return format_subtitle_timestamp(seconds, separator=",")


def format_vtt_timestamp(seconds: float) -> str:
    return format_subtitle_timestamp(seconds, separator=".")


def format_subtitle_timestamp(seconds: float, *, separator: str) -> str:
    total_ms = max(0, int(round(seconds * 1000)))
    hours, remainder = divmod(total_ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    secs, millis = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}{separator}{millis:03d}"


def clamp_progress(value: float | None) -> float | None:
    if value is None:
        return None
    return min(1.0, max(0.0, float(value)))


def whisperx_progress_from_line(line: str) -> tuple[str, float | None]:
    lower = line.casefold()
    if "vad" in lower or "voice activity" in lower:
        return "Detecting speech", 0.25
    if "transcrib" in lower:
        return "Transcribing audio", 0.4
    if "align" in lower:
        return "Aligning transcript", 0.6
    if "diar" in lower or "speaker" in lower or "pyannote" in lower:
        return "Detecting speakers", 0.75
    if "writing" in lower or "saving" in lower or "output" in lower:
        return "Writing subtitle files", 0.88
    return "WhisperX running", None


def hf_token_for_config(config: BotConfig) -> str:
    if not config.whisperx_hf_token_env:
        return ""
    return os.environ.get(config.whisperx_hf_token_env, "").strip()


def whisperx_process_env(hf_token: str) -> dict[str, str]:
    env = dict(os.environ)
    if hf_token:
        env["HF_TOKEN"] = hf_token
        env["HUGGINGFACE_HUB_TOKEN"] = hf_token
    if env.get("XDG_CACHE_HOME"):
        cache_dir = Path(env["XDG_CACHE_HOME"])
        env.setdefault("MPLCONFIGDIR", str(cache_dir / "matplotlib"))
        env.setdefault("NLTK_DATA", str(cache_dir / "nltk_data"))
    return env


def transcription_output_file(media_file: Path, suffix: str) -> Path:
    return media_file.with_suffix(suffix)


def transcription_outputs_exist(media_file: Path) -> bool:
    return all(
        transcription_output_file(media_file, suffix).is_file()
        for suffix in PRIMARY_SUBTITLE_SUFFIXES
    )


def existing_transcription_outputs(media_file: Path) -> list[Path]:
    return [
        transcription_output_file(media_file, suffix)
        for suffix in TRANSCRIPTION_OUTPUT_SUFFIXES
        if transcription_output_file(media_file, suffix).is_file()
    ]


def cleanup_transcription_outputs(
    media_file: Path,
    logger: logging.Logger = LOGGER,
) -> None:
    for suffix in TRANSCRIPTION_OUTPUT_SUFFIXES:
        path = transcription_output_file(media_file, suffix)
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.warning(
                "Unable to remove subtitle output before retranscribe: %s",
                path,
            )


def command_for_log(command: list[str]) -> str:
    redacted: list[str] = []
    redact_next = False
    for arg in command:
        if redact_next:
            redacted.append("<redacted>")
            redact_next = False
            continue

        option, separator, _value = arg.partition("=")
        if separator and option in SENSITIVE_COMMAND_OPTIONS:
            redacted.append(f"{option}=<redacted>")
            continue

        redacted.append(arg)
        if arg in SENSITIVE_COMMAND_OPTIONS:
            redact_next = True

    return shlex.join(redacted)


async def monitor_process_output(
    stream: asyncio.StreamReader,
    output_buffer: list[str],
    logger: logging.Logger,
    *,
    label: str,
    line_callback: Callable[[str], None] | None = None,
) -> None:
    buffer = ""
    while not stream.at_eof():
        chunk = await stream.read(4096)
        if not chunk:
            break

        buffer += chunk.decode("utf-8", "replace").replace("\r", "\n")
        lines = buffer.split("\n")
        buffer = lines.pop()
        for line in lines:
            handle_process_output_line(line, output_buffer, logger, label=label, line_callback=line_callback)

    if buffer:
        handle_process_output_line(buffer, output_buffer, logger, label=label, line_callback=line_callback)


def handle_process_output_line(
    line: str,
    output_buffer: list[str],
    logger: logging.Logger,
    *,
    label: str,
    line_callback: Callable[[str], None] | None = None,
) -> None:
    line = line.strip()
    if not line:
        return
    line = redact_sensitive_text(line)
    if line_callback is not None:
        try:
            line_callback(line)
        except Exception as exc:  # noqa: BLE001 - progress reporting must not stop logging.
            logger.debug("%s progress callback failed: %s", label, exc)
    output_buffer.append(line)
    del output_buffer[:-200]
    logger.info("%s: %s", label, line)


def redact_sensitive_text(value: str) -> str:
    return HUGGINGFACE_TOKEN_RE.sub("hf_<redacted>", value)


def log_process_output(
    logger: logging.Logger,
    label: str,
    output_lines: list[str],
    *,
    failed: bool = False,
) -> None:
    output = "\n".join(output_lines).strip()
    if not output:
        return
    if len(output) > 4000:
        output = output[-4000:]
    if failed:
        logger.warning("%s output:\n%s", label, output)
    else:
        logger.debug("%s output:\n%s", label, output)
