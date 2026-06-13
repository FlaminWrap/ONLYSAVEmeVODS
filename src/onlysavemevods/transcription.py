from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Callable
import asyncio
import logging
import os
import re
import shlex
import time

from .config import BotConfig, VoiceDetectionConfig


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
) -> bool:
    def emit(phase: str, progress: float | None = None) -> None:
        if progress_callback is None:
            return
        progress_callback(phase, clamp_progress(progress))

    emit("Preparing transcription", 0.02)
    if overwrite:
        cleanup_transcription_outputs(media_file, logger)
    if transcription_outputs_exist(media_file):
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
