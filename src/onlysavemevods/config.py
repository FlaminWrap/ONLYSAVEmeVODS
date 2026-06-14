from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import json
import re
import tomllib


DEFAULT_POST_EXIT_CHECK_SECONDS = [
    30,
    60,
    90,
    120,
    150,
    180,
    210,
    240,
    270,
    300,
    330,
    360,
    390,
    420,
    450,
    480,
    510,
    540,
    570,
    600,
]

DEFAULT_RETRY_BACKOFF_SECONDS = [30, 60, 120, 300]
LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
WATERMARK_STRENGTHS = {"invisible", "balanced", "robust"}
HUGGINGFACE_TOKEN_PREFIX = "hf_"
DISALLOWED_EXTRA_YT_DLP_ARGS = {
    "--dump-json",
    "--dump-single-json",
    "--simulate",
    "--skip-download",
    "-J",
    "-j",
    "-s",
}
DEFAULT_DB_FILENAME = "onlysavemevods.sqlite3"
DEFAULT_WATERMARK_SECRET_ENV = "ONLYSAVEMEVODS_WATERMARK_SECRET"
DEFAULT_VOICE_MATCH_MODEL = "pyannote/embedding"
DEFAULT_VOICE_MATCH_THRESHOLD = 0.35
DEFAULT_VOICE_MATCH_MIN_MARGIN = 0.05
DEFAULT_VOICE_SAMPLE_MAX_BYTES = 104_857_600
CONFIG_UPDATE_COMMENT = (
    "# Added by ONLYSAVEmeVODS config update. "
    "Existing settings above were left unchanged."
)
BARE_TOML_ASSIGNMENT_RE = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=", re.MULTILINE)


class ConfigError(ValueError):
    """Raised when a config file cannot be parsed into a usable bot config."""


VOICE_DETECTION_MODES = {"off", "auto", "range", "fixed"}


@dataclass(frozen=True, slots=True)
class VoiceDetectionConfig:
    mode: str = "auto"
    min_speakers: int = 0
    max_speakers: int = 0
    hf_token_env: str = ""


@dataclass(frozen=True, slots=True)
class VoiceProfileConfig:
    enabled: bool = True
    samples: list[str] = field(default_factory=list)
    threshold: float = 0.0
    notes: str = ""


@dataclass(frozen=True, slots=True)
class StreamerConfig:
    sources: list[str] = field(default_factory=list)
    download_dir_name: str = ""
    voice_detection: VoiceDetectionConfig | None = None
    speaker_labels: dict[str, str] = field(default_factory=dict)
    voices: dict[str, VoiceProfileConfig] = field(default_factory=dict)


@dataclass(slots=True)
class BotConfig:
    channels: list[str] = field(default_factory=list)
    streamers: dict[str, StreamerConfig] = field(default_factory=dict)
    download_dir: Path = Path("downloads")
    state_dir: Path = Path("state")
    poll_interval_seconds: int = 60
    max_concurrent_downloads: int = 4
    live_from_start: bool = True
    record_live_chat: bool = False
    render_live_chat_video: bool = False
    chat_render_panel_workers: int = 0
    chat_render_timeout_seconds: int = 60 * 60
    chat_render_use_nvenc: bool = False
    chat_render_nvenc_devices: list[str] = field(default_factory=list)
    transcribe_subtitles: bool = False
    transcription_max_concurrent: int = 1
    whisperx_path: str = "whisperx"
    whisperx_model: str = "large-v3"
    whisperx_device: str = "cuda"
    whisperx_compute_type: str = "float16"
    whisperx_batch_size: int = 16
    whisperx_language: str = ""
    whisperx_diarize: bool = True
    whisperx_hf_token_env: str = "HF_TOKEN"
    whisperx_min_speakers: int = 0
    whisperx_max_speakers: int = 0
    channel_voice_detection: dict[str, VoiceDetectionConfig] = field(default_factory=dict)
    channel_speaker_labels: dict[str, dict[str, str]] = field(default_factory=dict)
    voice_match_enabled: bool = True
    voice_match_model: str = DEFAULT_VOICE_MATCH_MODEL
    voice_match_threshold: float = DEFAULT_VOICE_MATCH_THRESHOLD
    voice_match_min_margin: float = DEFAULT_VOICE_MATCH_MIN_MARGIN
    voice_sample_max_bytes: int = DEFAULT_VOICE_SAMPLE_MAX_BYTES
    keep_fragments_for_resume: bool = True
    reconnect_interval_seconds: int = 0
    post_exit_check_seconds: list[int] = field(
        default_factory=lambda: list(DEFAULT_POST_EXIT_CHECK_SECONDS)
    )
    retry_backoff_seconds: list[int] = field(
        default_factory=lambda: list(DEFAULT_RETRY_BACKOFF_SECONDS)
    )
    extra_yt_dlp_args: list[str] = field(default_factory=list)
    channel_scan_limit: int = 10
    discovery_probe_concurrency: int = 4
    web_enabled: bool = True
    web_host: str = "127.0.0.1"
    web_port: int = 8080
    log_level: str = "INFO"
    yt_dlp_path: str = "yt-dlp"
    ffmpeg_path: str = "ffmpeg"
    watermark_enabled: bool = False
    watermark_secret_env: str = DEFAULT_WATERMARK_SECRET_ENV
    watermark_strength: str = "invisible"
    watermark_detect_upload_max_bytes: int = 2_147_483_648
    config_path: Path | None = None

    @property
    def db_path(self) -> Path:
        return self.state_dir / DEFAULT_DB_FILENAME


def load_config(path: str | Path) -> BotConfig:
    config_path = Path(path).expanduser()
    base_dir = config_path.parent.resolve()

    if config_path.exists():
        try:
            raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"Invalid TOML in {config_path}: {exc}") from exc
    else:
        raw = {}

    if not isinstance(raw, dict):
        raise ConfigError("The config file root must be a TOML table")

    chat_render_nvenc_devices = _as_optional_devices(
        raw.get("chat_render_nvenc_devices", []),
        "chat_render_nvenc_devices",
    )
    whisperx_min_speakers = _as_non_negative_int(
        raw.get("whisperx_min_speakers", 0),
        "whisperx_min_speakers",
    )
    whisperx_max_speakers = _as_non_negative_int(
        raw.get("whisperx_max_speakers", 0),
        "whisperx_max_speakers",
    )
    if (
        whisperx_min_speakers
        and whisperx_max_speakers
        and whisperx_min_speakers > whisperx_max_speakers
    ):
        raise ConfigError(
            "whisperx_min_speakers must be less than or equal to whisperx_max_speakers"
        )
    channel_voice_detection = _as_channel_voice_detection(
        raw.get("channel_voice_detection", {}),
        "channel_voice_detection",
    )
    channel_speaker_labels = _as_channel_speaker_labels(
        raw.get("channel_speaker_labels", {}),
        "channel_speaker_labels",
    )
    streamers = _as_streamers(raw.get("streamers", {}), "streamers")

    return BotConfig(
        channels=_as_str_list(raw.get("channels", []), "channels"),
        streamers=streamers,
        download_dir=_resolve_path(raw.get("download_dir", "downloads"), base_dir),
        state_dir=_resolve_path(raw.get("state_dir", "state"), base_dir),
        poll_interval_seconds=_as_positive_int(
            raw.get("poll_interval_seconds", 60), "poll_interval_seconds"
        ),
        max_concurrent_downloads=_as_positive_int(
            raw.get("max_concurrent_downloads", 4), "max_concurrent_downloads"
        ),
        live_from_start=_as_bool(raw.get("live_from_start", True), "live_from_start"),
        record_live_chat=_as_bool(
            raw.get("record_live_chat", False),
            "record_live_chat",
        ),
        render_live_chat_video=_as_bool(
            raw.get("render_live_chat_video", False),
            "render_live_chat_video",
        ),
        chat_render_panel_workers=_as_non_negative_int(
            raw.get("chat_render_panel_workers", 0),
            "chat_render_panel_workers",
        ),
        chat_render_timeout_seconds=_as_non_negative_int(
            raw.get("chat_render_timeout_seconds", 60 * 60),
            "chat_render_timeout_seconds",
        ),
        chat_render_use_nvenc=_as_bool(
            raw.get("chat_render_use_nvenc", False),
            "chat_render_use_nvenc",
        ),
        chat_render_nvenc_devices=chat_render_nvenc_devices,
        transcribe_subtitles=_as_bool(
            raw.get("transcribe_subtitles", False),
            "transcribe_subtitles",
        ),
        transcription_max_concurrent=_as_positive_int(
            raw.get("transcription_max_concurrent", 1),
            "transcription_max_concurrent",
        ),
        whisperx_path=_as_str(raw.get("whisperx_path", "whisperx"), "whisperx_path"),
        whisperx_model=_as_str(
            raw.get("whisperx_model", "large-v3"),
            "whisperx_model",
        ),
        whisperx_device=_as_str(raw.get("whisperx_device", "cuda"), "whisperx_device"),
        whisperx_compute_type=_as_str(
            raw.get("whisperx_compute_type", "float16"),
            "whisperx_compute_type",
        ),
        whisperx_batch_size=_as_positive_int(
            raw.get("whisperx_batch_size", 16),
            "whisperx_batch_size",
        ),
        whisperx_language=_as_optional_str(
            raw.get("whisperx_language", ""),
            "whisperx_language",
        ),
        whisperx_diarize=_as_bool(
            raw.get("whisperx_diarize", True),
            "whisperx_diarize",
        ),
        whisperx_hf_token_env=_as_env_var_name(
            raw.get("whisperx_hf_token_env", "HF_TOKEN"),
            "whisperx_hf_token_env",
        ),
        whisperx_min_speakers=whisperx_min_speakers,
        whisperx_max_speakers=whisperx_max_speakers,
        channel_voice_detection=channel_voice_detection,
        channel_speaker_labels=channel_speaker_labels,
        voice_match_enabled=_as_bool(
            raw.get("voice_match_enabled", True),
            "voice_match_enabled",
        ),
        voice_match_model=_as_str(
            raw.get("voice_match_model", DEFAULT_VOICE_MATCH_MODEL),
            "voice_match_model",
        ),
        voice_match_threshold=_as_non_negative_float(
            raw.get("voice_match_threshold", DEFAULT_VOICE_MATCH_THRESHOLD),
            "voice_match_threshold",
        ),
        voice_match_min_margin=_as_non_negative_float(
            raw.get("voice_match_min_margin", DEFAULT_VOICE_MATCH_MIN_MARGIN),
            "voice_match_min_margin",
        ),
        voice_sample_max_bytes=_as_positive_int(
            raw.get("voice_sample_max_bytes", DEFAULT_VOICE_SAMPLE_MAX_BYTES),
            "voice_sample_max_bytes",
        ),
        keep_fragments_for_resume=_as_bool(
            raw.get("keep_fragments_for_resume", True),
            "keep_fragments_for_resume",
        ),
        reconnect_interval_seconds=_as_non_negative_int(
            raw.get("reconnect_interval_seconds", 0), "reconnect_interval_seconds"
        ),
        post_exit_check_seconds=_as_offset_list(
            raw.get("post_exit_check_seconds", DEFAULT_POST_EXIT_CHECK_SECONDS),
            "post_exit_check_seconds",
        ),
        retry_backoff_seconds=_as_offset_list(
            raw.get("retry_backoff_seconds", DEFAULT_RETRY_BACKOFF_SECONDS),
            "retry_backoff_seconds",
        ),
        extra_yt_dlp_args=_as_extra_yt_dlp_args(
            raw.get("extra_yt_dlp_args", []), "extra_yt_dlp_args"
        ),
        channel_scan_limit=_as_positive_int(
            raw.get("channel_scan_limit", 10), "channel_scan_limit"
        ),
        discovery_probe_concurrency=_as_positive_int(
            raw.get("discovery_probe_concurrency", 4),
            "discovery_probe_concurrency",
        ),
        web_enabled=_as_bool(raw.get("web_enabled", True), "web_enabled"),
        web_host=_as_str(raw.get("web_host", "127.0.0.1"), "web_host"),
        web_port=_as_port(raw.get("web_port", 8080), "web_port"),
        log_level=_as_log_level(raw.get("log_level", "INFO"), "log_level"),
        yt_dlp_path=_as_str(raw.get("yt_dlp_path", "yt-dlp"), "yt_dlp_path"),
        ffmpeg_path=_as_str(raw.get("ffmpeg_path", "ffmpeg"), "ffmpeg_path"),
        watermark_enabled=_as_bool(
            raw.get("watermark_enabled", False),
            "watermark_enabled",
        ),
        watermark_secret_env=_as_str(
            raw.get("watermark_secret_env", DEFAULT_WATERMARK_SECRET_ENV),
            "watermark_secret_env",
        ),
        watermark_strength=_as_watermark_strength(
            raw.get("watermark_strength", "invisible"),
            "watermark_strength",
        ),
        watermark_detect_upload_max_bytes=_as_positive_int(
            raw.get("watermark_detect_upload_max_bytes", 2_147_483_648),
            "watermark_detect_upload_max_bytes",
        ),
        config_path=config_path.resolve(),
    )


def ensure_config_dirs(config: BotConfig) -> None:
    config.download_dir.mkdir(parents=True, exist_ok=True)
    config.state_dir.mkdir(parents=True, exist_ok=True)


def monitored_sources(config: BotConfig) -> list[str]:
    sources: list[str] = []
    seen: set[str] = set()
    for source in [*config.channels, *streamer_sources(config)]:
        key = source.strip().rstrip("/").casefold()
        if key and key not in seen:
            sources.append(source)
            seen.add(key)
    return sources


def streamer_sources(config: BotConfig) -> list[str]:
    sources: list[str] = []
    for streamer in config.streamers.values():
        sources.extend(streamer.sources)
    return sources


def streamer_for_channel(
    config: BotConfig,
    channel: str,
) -> tuple[str, StreamerConfig] | None:
    target = source_lookup_key(channel)
    if not target:
        return None
    for name, streamer in config.streamers.items():
        if source_lookup_key(name) == target:
            return name, streamer
        for source in streamer.sources:
            if source_lookup_key(source) == target:
                return name, streamer
    return None


def streamer_display_name_for_channel(config: BotConfig, channel: str) -> str:
    match = streamer_for_channel(config, channel)
    return match[0] if match is not None else ""


def download_group_name_for_channel(config: BotConfig, channel: str) -> str:
    match = streamer_for_channel(config, channel)
    if match is None:
        return channel.strip()
    name, streamer = match
    return streamer.download_dir_name or name


def source_display_name(source: str) -> str:
    target = source.strip().rstrip("/")
    if not target:
        return ""
    if "/" in target:
        target = target.rsplit("/", 1)[-1]
    return target or source.strip()


def source_lookup_key(source: str) -> str:
    target = source_display_name(source)
    if target.startswith("@"):
        target = target[1:]
    folded = target.casefold()
    compact = re.sub(r"[^a-z0-9]+", "", folded)
    return compact or folded


def _insert_root_config_block(current_text: str, lines: list[str]) -> str:
    addition = "\n".join(lines) + "\n"
    table_match = re.search(r"(?m)^\[", current_text)
    if table_match is None:
        prefix = current_text
        if prefix and not prefix.endswith("\n"):
            prefix += "\n"
        return prefix + addition

    prefix = current_text[: table_match.start()].rstrip()
    suffix = current_text[table_match.start() :].lstrip("\n")
    if prefix:
        return prefix + addition + "\n" + suffix
    return addition + "\n" + suffix


def _repair_misplaced_root_config_values(
    current_text: str,
    defaults: dict[str, Any],
) -> tuple[str, list[str]]:
    root_default_keys = {
        key for key, value in defaults.items() if not isinstance(value, dict)
    }
    table_match = re.search(r"(?m)^\[", current_text)
    if not root_default_keys or table_match is None:
        return current_text, []

    root_text = current_text[: table_match.start()]
    root_keys = {
        match.group(1)
        for match in BARE_TOML_ASSIGNMENT_RE.finditer(root_text)
    }
    repaired_keys: list[str] = []
    moved_lines: list[str] = []
    pending_update_comments: list[str] = []
    output_lines: list[str] = []
    in_root = True

    def remember_repaired(key: str) -> None:
        if key not in repaired_keys:
            repaired_keys.append(key)

    for line in current_text.splitlines(keepends=True):
        if re.match(r"^\s*\[", line):
            in_root = False
        if not in_root and line.strip() == CONFIG_UPDATE_COMMENT:
            pending_update_comments.append(line)
            continue

        assignment_match = BARE_TOML_ASSIGNMENT_RE.match(line)
        if (
            not in_root
            and assignment_match is not None
            and assignment_match.group(1) in root_default_keys
        ):
            key = assignment_match.group(1)
            pending_update_comments.clear()
            remember_repaired(key)
            if key not in root_keys:
                moved_lines.append(line.rstrip("\r\n"))
                root_keys.add(key)
            continue

        if pending_update_comments:
            output_lines.extend(pending_update_comments)
            pending_update_comments.clear()
        output_lines.append(line)

    if pending_update_comments:
        output_lines.extend(pending_update_comments)

    if not repaired_keys:
        return current_text, []

    repaired_text = "".join(output_lines)
    if moved_lines:
        repaired_text = _insert_root_config_block(
            repaired_text,
            ["", CONFIG_UPDATE_COMMENT, *moved_lines],
        )
    return repaired_text, repaired_keys


def append_missing_config_values(
    config_path: str | Path,
    defaults_path: str | Path,
) -> list[str]:
    target = Path(config_path).expanduser()
    defaults_file = Path(defaults_path).expanduser()

    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    try:
        defaults_text = defaults_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(
            f"Unable to read default config file {defaults_file}: {exc}"
        ) from exc

    try:
        defaults = tomllib.loads(defaults_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {defaults_file}: {exc}") from exc
    if not isinstance(defaults, dict):
        raise ConfigError("Config files must have TOML tables at the root")

    repaired: list[str] = []
    try:
        current = tomllib.loads(current_text)
    except tomllib.TOMLDecodeError as exc:
        repaired_text, repaired = _repair_misplaced_root_config_values(
            current_text,
            defaults,
        )
        if not repaired or repaired_text == current_text:
            raise ConfigError(f"Invalid TOML in {target}: {exc}") from exc
        try:
            current = tomllib.loads(repaired_text)
        except tomllib.TOMLDecodeError:
            raise ConfigError(f"Invalid TOML in {target}: {exc}") from exc
        current_text = repaired_text

    if not isinstance(current, dict):
        raise ConfigError("Config files must have TOML tables at the root")

    missing = [key for key in defaults if key not in current]
    if not missing and not repaired:
        return []

    updated_text = current_text
    if missing:
        block = ["", CONFIG_UPDATE_COMMENT]
        for key in missing:
            block.append(f"{key} = {_toml_value(defaults[key], key)}")
        updated_text = _insert_root_config_block(current_text, block)

    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return [*repaired, *missing]


def update_config_values(
    config_path: str | Path,
    updates: dict[str, Any],
) -> list[str]:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    try:
        current = tomllib.loads(current_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {target}: {exc}") from exc
    if not isinstance(current, dict):
        raise ConfigError("Config files must have TOML tables at the root")

    changed: list[str] = []
    updated_text = current_text
    missing: list[str] = []
    for key, value in updates.items():
        line = f"{key} = {_toml_value(value, key)}"
        if key in current:
            if current[key] == value:
                continue
            pattern = re.compile(rf"(?m)^{re.escape(key)}\s*=.*$")
            updated_text, count = pattern.subn(line, updated_text, count=1)
            if count == 0:
                missing.append(key)
            else:
                changed.append(key)
        else:
            missing.append(key)

    if missing:
        lines = ["", CONFIG_UPDATE_COMMENT]
        for key in missing:
            lines.append(f"{key} = {_toml_value(updates[key], key)}")
        updated_text = _insert_root_config_block(updated_text, lines)
        changed.extend(missing)

    if not changed:
        return []

    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return changed


def update_streamer_config(
    config_path: str | Path,
    streamer_name: str,
    sources: list[str],
    download_dir_name: str = "",
) -> bool:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    streamer_name = streamer_name.strip()
    if not streamer_name:
        raise ConfigError("streamer name is required")
    normalized_sources = _as_str_list(sources, f"streamers.{streamer_name}.sources")
    if not normalized_sources:
        raise ConfigError(f"streamers.{streamer_name}.sources must not be empty")
    normalized_download_dir_name = _as_optional_str(
        download_dir_name,
        f"streamers.{streamer_name}.download_dir_name",
    )

    table_name = f"streamers.{_toml_key(streamer_name)}"
    pattern = re.compile(rf"(?ms)^\[{re.escape(table_name)}\]\n.*?(?=^\[|\Z)")
    block = _streamer_block(
        streamer_name,
        normalized_sources,
        normalized_download_dir_name,
    )
    if pattern.search(current_text):
        updated_text = pattern.sub(block + "\n", current_text, count=1)
    else:
        prefix = current_text.rstrip()
        updated_text = (prefix + "\n\n" if prefix else "") + block + "\n"

    _validate_generated_config(target, updated_text)
    _validate_generated_streamers(target, updated_text)
    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return updated_text != current_text


def remove_streamer_config(
    config_path: str | Path,
    streamer_name: str,
) -> bool:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    streamer_name = streamer_name.strip()
    if not streamer_name:
        raise ConfigError("streamer name is required")
    _require_configured_streamer(current_text, streamer_name, target)

    table_key = re.escape(_toml_key(streamer_name))
    pattern = re.compile(rf"(?ms)^\[streamers\.{table_key}(?:\.[^\]]+)?\]\n.*?(?=^\[|\Z)")
    updated_text, count = pattern.subn("", current_text)
    updated_text = re.sub(r"\n{3,}", "\n\n", updated_text).rstrip() + "\n"
    if count == 0:
        return False

    _validate_generated_config(target, updated_text)
    _validate_generated_streamers(target, updated_text)
    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return updated_text != current_text


def update_streamer_voice_detection_config(
    config_path: str | Path,
    streamer_name: str,
    voice_config: VoiceDetectionConfig | None,
) -> bool:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    streamer_name = streamer_name.strip()
    if not streamer_name:
        raise ConfigError("streamer voice detection override requires a streamer")
    _require_configured_streamer(current_text, streamer_name, target)

    table_name = f"streamers.{_toml_key(streamer_name)}.voice_detection"
    pattern = re.compile(rf"(?ms)^\[{re.escape(table_name)}\]\n.*?(?=^\[|\Z)")
    if voice_config is None:
        updated_text, count = pattern.subn("", current_text, count=1)
        updated_text = re.sub(r"\n{3,}", "\n\n", updated_text).rstrip() + "\n"
        if count == 0:
            return False
    else:
        block = _streamer_voice_detection_block(streamer_name, voice_config)
        if pattern.search(current_text):
            updated_text = pattern.sub(block + "\n", current_text, count=1)
        else:
            prefix = current_text.rstrip()
            updated_text = (prefix + "\n\n" if prefix else "") + block + "\n"

    _validate_generated_config(target, updated_text)
    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return updated_text != current_text


def update_streamer_speaker_labels_config(
    config_path: str | Path,
    streamer_name: str,
    labels: dict[str, str],
) -> bool:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    streamer_name = streamer_name.strip()
    if not streamer_name:
        raise ConfigError("speaker labels require a streamer")
    _require_configured_streamer(current_text, streamer_name, target)

    normalized = _normalize_speaker_labels(
        labels,
        f"streamers.{streamer_name}.speaker_labels",
    )
    table_name = f"streamers.{_toml_key(streamer_name)}.speaker_labels"
    pattern = re.compile(rf"(?ms)^\[{re.escape(table_name)}\]\n.*?(?=^\[|\Z)")
    if not normalized:
        updated_text, count = pattern.subn("", current_text, count=1)
        updated_text = re.sub(r"\n{3,}", "\n\n", updated_text).rstrip() + "\n"
        if count == 0:
            return False
    else:
        block = _streamer_speaker_labels_block(streamer_name, normalized)
        if pattern.search(current_text):
            updated_text = pattern.sub(block + "\n", current_text, count=1)
        else:
            prefix = current_text.rstrip()
            updated_text = (prefix + "\n\n" if prefix else "") + block + "\n"

    _validate_generated_config(target, updated_text)
    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return updated_text != current_text


def update_streamer_voice_profile_config(
    config_path: str | Path,
    streamer_name: str,
    voice_name: str,
    voice_profile: VoiceProfileConfig | None,
) -> bool:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    streamer_name = streamer_name.strip()
    voice_name = validate_voice_name(voice_name)
    if not streamer_name:
        raise ConfigError("voice profile requires a streamer")
    _require_configured_streamer(current_text, streamer_name, target)

    table_name = f"streamers.{_toml_key(streamer_name)}.voices.{_toml_key(voice_name)}"
    pattern = re.compile(rf"(?ms)^\[{re.escape(table_name)}\]\n.*?(?=^\[|\Z)")
    if voice_profile is None:
        updated_text, count = pattern.subn("", current_text, count=1)
        updated_text = re.sub(r"\n{3,}", "\n\n", updated_text).rstrip() + "\n"
        if count == 0:
            return False
    else:
        normalized = _normalize_voice_profile(
            voice_profile,
            f"streamers.{streamer_name}.voices.{voice_name}",
        )
        block = _streamer_voice_profile_block(streamer_name, voice_name, normalized)
        if pattern.search(current_text):
            updated_text = pattern.sub(block + "\n", current_text, count=1)
        else:
            prefix = current_text.rstrip()
            updated_text = (prefix + "\n\n" if prefix else "") + block + "\n"

    _validate_generated_config(target, updated_text)
    _validate_generated_streamers(target, updated_text)
    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return updated_text != current_text


def validate_voice_name(value: str) -> str:
    name = _as_optional_str(value, "voice name")
    if not name:
        raise ConfigError("voice name is required")
    if any(char in name for char in "\r\n"):
        raise ConfigError("voice name must be a single line")
    return name


def sanitize_voice_component(value: str, *, fallback: str = "voice") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip()).strip("._-")
    return cleaned[:80] or fallback


def sanitize_voice_sample_filename(filename: str) -> str:
    name = Path(filename or "sample").name
    if name in {"", ".", ".."}:
        name = "sample"
    stem = sanitize_voice_component(Path(name).stem, fallback="sample")
    suffix = re.sub(r"[^A-Za-z0-9.]", "", Path(name).suffix.lower())[:16]
    return f"{stem}{suffix or '.wav'}"


def voice_sample_dir(config: BotConfig, streamer_name: str, voice_name: str) -> Path:
    streamer_part = sanitize_voice_component(streamer_name, fallback="streamer")
    voice_part = sanitize_voice_component(voice_name, fallback="voice")
    return config.state_dir / "voice_samples" / streamer_part / voice_part


def voice_sample_path(config: BotConfig, streamer_name: str, voice_name: str, sample: str) -> Path:
    sample_name = _as_voice_sample_name(sample, "sample")
    return voice_sample_dir(config, streamer_name, voice_name) / sample_name


def add_voice_sample_to_profile(profile: VoiceProfileConfig | None, sample_name: str) -> VoiceProfileConfig:
    sample_name = _as_voice_sample_name(sample_name, "sample")
    existing = profile or VoiceProfileConfig()
    samples = list(existing.samples)
    if sample_name not in samples:
        samples.append(sample_name)
    return VoiceProfileConfig(
        enabled=existing.enabled,
        samples=samples,
        threshold=existing.threshold,
        notes=existing.notes,
    )


def update_channel_voice_detection_config(
    config_path: str | Path,
    channel: str,
    voice_config: VoiceDetectionConfig | None,
) -> bool:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    channel = channel.strip()
    if not channel:
        raise ConfigError("channel voice detection override requires a channel")

    header = f"[channel_voice_detection.{_toml_key(channel)}]"
    pattern = re.compile(
        rf"(?ms)^\[channel_voice_detection\.{re.escape(_toml_key(channel))}\]\n.*?(?=^\[|\Z)"
    )
    if voice_config is None:
        updated_text, count = pattern.subn("", current_text, count=1)
        updated_text = re.sub(r"\n{3,}", "\n\n", updated_text).rstrip() + "\n"
        if count == 0:
            return False
    else:
        block = _channel_voice_detection_block(channel, voice_config)
        if pattern.search(current_text):
            updated_text = pattern.sub(block + "\n", current_text, count=1)
        else:
            prefix = current_text.rstrip()
            updated_text = (prefix + "\n\n" if prefix else "") + block + "\n"

    _validate_generated_config(target, updated_text)

    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return updated_text != current_text


def update_channel_speaker_labels_config(
    config_path: str | Path,
    channel: str,
    labels: dict[str, str],
) -> bool:
    target = Path(config_path).expanduser()
    try:
        current_text = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"Config file does not exist: {target}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read config file {target}: {exc}") from exc

    channel = channel.strip()
    if not channel:
        raise ConfigError("speaker labels require a channel")

    normalized = _normalize_speaker_labels(
        labels,
        f"channel_speaker_labels.{channel}",
    )
    pattern = re.compile(
        rf"(?ms)^\[channel_speaker_labels\.{re.escape(_toml_key(channel))}\]\n.*?(?=^\[|\Z)"
    )
    if not normalized:
        updated_text, count = pattern.subn("", current_text, count=1)
        updated_text = re.sub(r"\n{3,}", "\n\n", updated_text).rstrip() + "\n"
        if count == 0:
            return False
    else:
        block = _channel_speaker_labels_block(channel, normalized)
        if pattern.search(current_text):
            updated_text = pattern.sub(block + "\n", current_text, count=1)
        else:
            prefix = current_text.rstrip()
            updated_text = (prefix + "\n\n" if prefix else "") + block + "\n"

    _validate_generated_config(target, updated_text)

    try:
        target.write_text(updated_text, encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return updated_text != current_text


def _validate_generated_config(target: Path, updated_text: str) -> None:
    try:
        parsed = tomllib.loads(updated_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Generated invalid TOML for {target}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError("Generated config must have a TOML table at the root")


def _validate_generated_streamers(target: Path, updated_text: str) -> None:
    try:
        parsed = tomllib.loads(updated_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Generated invalid TOML for {target}: {exc}") from exc
    if isinstance(parsed, dict):
        _as_streamers(parsed.get("streamers", {}), "streamers")


def _require_configured_streamer(
    current_text: str,
    streamer_name: str,
    target: Path,
) -> None:
    try:
        parsed = tomllib.loads(current_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {target}: {exc}") from exc
    streamers = parsed.get("streamers", {}) if isinstance(parsed, dict) else {}
    if not isinstance(streamers, dict) or streamer_name not in streamers:
        raise ConfigError(f"streamer is not configured: {streamer_name}")


def _streamer_block(
    streamer_name: str,
    sources: list[str],
    download_dir_name: str,
) -> str:
    lines = [f"[streamers.{_toml_key(streamer_name)}]"]
    lines.append(f"sources = {_toml_value(sources, 'sources')}")
    if download_dir_name:
        lines.append(
            f"download_dir_name = {_toml_value(download_dir_name, 'download_dir_name')}"
        )
    return "\n".join(lines)


def _streamer_voice_detection_block(
    streamer_name: str,
    voice_config: VoiceDetectionConfig,
) -> str:
    lines = [f"[streamers.{_toml_key(streamer_name)}.voice_detection]"]
    lines.append(f"mode = {_toml_value(voice_config.mode, 'mode')}")
    if voice_config.mode == "fixed":
        lines.append(f"speakers = {voice_config.min_speakers}")
    elif voice_config.mode == "range":
        if voice_config.min_speakers:
            lines.append(f"min_speakers = {voice_config.min_speakers}")
        if voice_config.max_speakers:
            lines.append(f"max_speakers = {voice_config.max_speakers}")
    if voice_config.hf_token_env:
        lines.append(f"hf_token_env = {_toml_value(voice_config.hf_token_env, 'hf_token_env')}")
    return "\n".join(lines)


def _streamer_speaker_labels_block(streamer_name: str, labels: dict[str, str]) -> str:
    lines = [f"[streamers.{_toml_key(streamer_name)}.speaker_labels]"]
    for label, name in sorted(labels.items()):
        lines.append(f"{_toml_key(label)} = {_toml_value(name, label)}")
    return "\n".join(lines)


def _streamer_voice_profile_block(
    streamer_name: str,
    voice_name: str,
    profile: VoiceProfileConfig,
) -> str:
    lines = [f"[streamers.{_toml_key(streamer_name)}.voices.{_toml_key(voice_name)}]"]
    lines.append(f"enabled = {_toml_value(profile.enabled, 'enabled')}")
    lines.append(f"samples = {_toml_value(profile.samples, 'samples')}")
    if profile.threshold:
        lines.append(f"threshold = {_toml_value(profile.threshold, 'threshold')}")
    if profile.notes:
        lines.append(f"notes = {_toml_value(profile.notes, 'notes')}")
    return "\n".join(lines)


def _channel_voice_detection_block(
    channel: str,
    voice_config: VoiceDetectionConfig,
) -> str:
    lines = [f"[channel_voice_detection.{_toml_key(channel)}]"]
    lines.append(f"mode = {_toml_value(voice_config.mode, 'mode')}")
    if voice_config.mode == "fixed":
        lines.append(f"speakers = {voice_config.min_speakers}")
    elif voice_config.mode == "range":
        if voice_config.min_speakers:
            lines.append(f"min_speakers = {voice_config.min_speakers}")
        if voice_config.max_speakers:
            lines.append(f"max_speakers = {voice_config.max_speakers}")
    if voice_config.hf_token_env:
        lines.append(f"hf_token_env = {_toml_value(voice_config.hf_token_env, 'hf_token_env')}")
    return "\n".join(lines)


def _channel_speaker_labels_block(channel: str, labels: dict[str, str]) -> str:
    lines = [f"[channel_speaker_labels.{_toml_key(channel)}]"]
    for label, name in sorted(labels.items()):
        lines.append(f"{_toml_key(label)} = {_toml_value(name, label)}")
    return "\n".join(lines)


def _toml_key(value: str) -> str:
    return json.dumps(value)


def _toml_value(value: Any, name: str) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item, name) for item in value) + "]"
    raise ConfigError(f"Cannot append unsupported default config value for {name}")


def _resolve_path(value: Any, base_dir: Path) -> Path:
    path = Path(_as_str(value, "path")).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _resolve_optional_path(value: Any, base_dir: Path, name: str) -> Path | None:
    raw = _as_optional_str(value, name)
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _as_streamers(value: Any, name: str) -> dict[str, StreamerConfig]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a TOML table")
    streamers: dict[str, StreamerConfig] = {}
    seen_sources: dict[str, str] = {}
    for raw_streamer_name, raw_config in value.items():
        if not isinstance(raw_streamer_name, str) or not raw_streamer_name.strip():
            raise ConfigError(f"{name} keys must be non-empty streamer names")
        streamer_name = raw_streamer_name.strip()
        if not isinstance(raw_config, dict):
            raise ConfigError(f"{name}.{streamer_name} must be a TOML table")
        if "sources" in raw_config and "channels" in raw_config:
            raise ConfigError(
                f"{name}.{streamer_name} must use either sources or channels, not both"
            )
        source_field = "sources" if "sources" in raw_config else "channels"
        sources = _as_str_list(
            raw_config.get(source_field, []),
            f"{name}.{streamer_name}.{source_field}",
        )
        if not sources:
            raise ConfigError(f"{name}.{streamer_name}.{source_field} must not be empty")
        for source in sources:
            dedupe_key = source.strip().rstrip("/").casefold()
            if dedupe_key in seen_sources:
                raise ConfigError(
                    f"{name}.{streamer_name}.{source_field} duplicates source "
                    f"from {seen_sources[dedupe_key]}"
                )
            seen_sources[dedupe_key] = streamer_name

        raw_voice_detection = raw_config.get("voice_detection")
        voice_detection = None
        if raw_voice_detection is not None:
            if not isinstance(raw_voice_detection, dict):
                raise ConfigError(
                    f"{name}.{streamer_name}.voice_detection must be a TOML table"
                )
            voice_detection = _as_voice_detection_config(
                raw_voice_detection,
                f"{name}.{streamer_name}.voice_detection",
            )

        raw_speaker_labels = raw_config.get("speaker_labels", {})
        if not isinstance(raw_speaker_labels, dict):
            raise ConfigError(
                f"{name}.{streamer_name}.speaker_labels must be a TOML table"
            )
        speaker_labels = _normalize_speaker_labels(
            raw_speaker_labels,
            f"{name}.{streamer_name}.speaker_labels",
        )

        voices = _as_voice_profiles(
            raw_config.get("voices", {}),
            f"{name}.{streamer_name}.voices",
        )

        streamers[streamer_name] = StreamerConfig(
            sources=sources,
            download_dir_name=_as_optional_str(
                raw_config.get("download_dir_name", ""),
                f"{name}.{streamer_name}.download_dir_name",
            ),
            voice_detection=voice_detection,
            speaker_labels=speaker_labels,
            voices=voices,
        )
    return streamers


def _as_voice_profiles(value: Any, name: str) -> dict[str, VoiceProfileConfig]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a TOML table")
    profiles: dict[str, VoiceProfileConfig] = {}
    for raw_voice_name, raw_profile in value.items():
        if not isinstance(raw_voice_name, str) or not raw_voice_name.strip():
            raise ConfigError(f"{name} keys must be non-empty voice names")
        if not isinstance(raw_profile, dict):
            raise ConfigError(f"{name}.{raw_voice_name} must be a TOML table")
        voice_name = validate_voice_name(raw_voice_name)
        profiles[voice_name] = _as_voice_profile_config(
            raw_profile,
            f"{name}.{voice_name}",
        )
    return profiles


def _as_voice_profile_config(raw: dict[str, Any], name: str) -> VoiceProfileConfig:
    return _normalize_voice_profile(
        VoiceProfileConfig(
            enabled=_as_bool(raw.get("enabled", True), f"{name}.enabled"),
            samples=_as_voice_sample_list(raw.get("samples", []), f"{name}.samples"),
            threshold=_as_non_negative_float(raw.get("threshold", 0.0), f"{name}.threshold"),
            notes=_as_optional_str(raw.get("notes", ""), f"{name}.notes"),
        ),
        name,
    )


def _normalize_voice_profile(profile: VoiceProfileConfig, name: str) -> VoiceProfileConfig:
    return VoiceProfileConfig(
        enabled=bool(profile.enabled),
        samples=_as_voice_sample_list(profile.samples, f"{name}.samples"),
        threshold=_as_non_negative_float(profile.threshold, f"{name}.threshold"),
        notes=_as_optional_str(profile.notes, f"{name}.notes"),
    )


def _as_voice_sample_list(value: Any, name: str) -> list[str]:
    return [
        _as_voice_sample_name(item, f"{name} item")
        for item in _as_str_list(value, name)
    ]


def _as_voice_sample_name(value: Any, name: str) -> str:
    sample = _as_str(value, name).strip()
    if not sample:
        raise ConfigError(f"{name} must be a non-empty sample filename")
    if sample in {".", ".."} or "/" in sample or "\\" in sample or Path(sample).is_absolute():
        raise ConfigError(f"{name} must be a managed sample filename")
    return sample


def _as_channel_voice_detection(
    value: Any,
    name: str,
) -> dict[str, VoiceDetectionConfig]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a TOML table")
    overrides: dict[str, VoiceDetectionConfig] = {}
    for channel, raw_config in value.items():
        if not isinstance(channel, str) or not channel.strip():
            raise ConfigError(f"{name} keys must be non-empty channel names")
        if not isinstance(raw_config, dict):
            raise ConfigError(f"{name}.{channel} must be a TOML table")
        overrides[channel.strip()] = _as_voice_detection_config(
            raw_config,
            f"{name}.{channel}",
        )
    return overrides


def _as_channel_speaker_labels(
    value: Any,
    name: str,
) -> dict[str, dict[str, str]]:
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a TOML table")
    channels: dict[str, dict[str, str]] = {}
    for channel, raw_labels in value.items():
        if not isinstance(channel, str) or not channel.strip():
            raise ConfigError(f"{name} keys must be non-empty channel names")
        if not isinstance(raw_labels, dict):
            raise ConfigError(f"{name}.{channel} must be a TOML table")
        labels = _normalize_speaker_labels(raw_labels, f"{name}.{channel}")
        if labels:
            channels[channel.strip()] = labels
    return channels


def _normalize_speaker_labels(value: dict[str, Any], name: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for raw_label, raw_name in value.items():
        label = _as_speaker_label(raw_label, f"{name} speaker label")
        speaker_name = _as_optional_str(raw_name, f"{name}.{label}")
        if speaker_name:
            labels[label] = speaker_name
    return labels


def _as_speaker_label(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty speaker label")
    label = value.strip()
    if any(char.isspace() for char in label):
        raise ConfigError(f"{name} must not contain whitespace")
    return label


def _as_non_negative_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ConfigError(f"{name} must be a number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if parsed < 0:
        raise ConfigError(f"{name} must not be negative")
    return parsed


def _as_voice_detection_config(raw: dict[str, Any], name: str) -> VoiceDetectionConfig:
    mode = _as_voice_detection_mode(raw.get("mode", "auto"), f"{name}.mode")
    speakers = _as_non_negative_int(raw.get("speakers", 0), f"{name}.speakers")
    min_speakers = _as_non_negative_int(
        raw.get("min_speakers", 0),
        f"{name}.min_speakers",
    )
    max_speakers = _as_non_negative_int(
        raw.get("max_speakers", 0),
        f"{name}.max_speakers",
    )
    hf_token_env = _as_optional_str(raw.get("hf_token_env", ""), f"{name}.hf_token_env")
    if hf_token_env:
        hf_token_env = _as_env_var_name(hf_token_env, f"{name}.hf_token_env")

    if mode == "fixed":
        if speakers:
            min_speakers = speakers
            max_speakers = speakers
        elif not (min_speakers and max_speakers and min_speakers == max_speakers):
            raise ConfigError(f"{name} fixed voice detection requires speakers")
    elif mode == "range":
        if speakers:
            raise ConfigError(f"{name} range voice detection uses min_speakers/max_speakers")
        if not min_speakers and not max_speakers:
            raise ConfigError(f"{name} range voice detection requires a speaker bound")
        if min_speakers and max_speakers and min_speakers > max_speakers:
            raise ConfigError(f"{name}.min_speakers must be less than or equal to max_speakers")
    elif mode in {"off", "auto"}:
        if speakers or min_speakers or max_speakers:
            raise ConfigError(f"{name} {mode} voice detection does not accept speaker counts")
        min_speakers = 0
        max_speakers = 0

    return VoiceDetectionConfig(
        mode=mode,
        min_speakers=min_speakers,
        max_speakers=max_speakers,
        hf_token_env=hf_token_env,
    )


def _as_voice_detection_mode(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string")
    mode = value.strip().lower()
    if mode not in VOICE_DETECTION_MODES:
        allowed = ", ".join(sorted(VOICE_DETECTION_MODES))
        raise ConfigError(f"{name} must be one of: {allowed}")
    return mode


def _as_str(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty string")
    return value


def _as_optional_str(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string")
    return value.strip()


def _as_env_var_name(value: Any, name: str) -> str:
    env_name = _as_optional_str(value, name)
    if not env_name:
        return ""
    if env_name.startswith(HUGGINGFACE_TOKEN_PREFIX):
        raise ConfigError(
            f"{name} must be an environment variable name, not the token value"
        )
    if not env_name.replace("_", "A").isalnum() or env_name[0].isdigit():
        raise ConfigError(f"{name} must be a valid environment variable name")
    return env_name


def _as_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{name} must be true or false")
    return value


def _as_positive_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigError(f"{name} must be a positive integer")
    return value


def _as_positive_float(value: Any, name: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or float(value) <= 0
    ):
        raise ConfigError(f"{name} must be a positive number")
    return float(value)


def _as_non_negative_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigError(f"{name} must be a non-negative integer")
    return value


def _as_optional_device(value: Any, name: str) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return str(value)
    raise ConfigError(
        f"{name} must be an empty string, device name, or non-negative integer"
    )


def _as_optional_devices(value: Any, name: str) -> list[str]:
    if isinstance(value, str):
        values = [item.strip() for item in value.split(",")]
    elif isinstance(value, list):
        values = [
            _as_optional_device(item, f"{name}[{index}]")
            for index, item in enumerate(value)
        ]
    elif isinstance(value, int) and not isinstance(value, bool):
        values = [_as_optional_device(value, name)]
    else:
        raise ConfigError(
            f"{name} must be a list, comma-separated string, or non-negative integer"
        )

    devices = [device for device in values if device]
    if len(set(devices)) != len(devices):
        raise ConfigError(f"{name} must not contain duplicate devices")
    return devices


def _as_port(value: Any, name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
        or value > 65535
    ):
        raise ConfigError(f"{name} must be a TCP port from 1 to 65535")
    return value


def _as_log_level(value: Any, name: str) -> str:
    level = _as_str(value, name).upper()
    if level not in LOG_LEVELS:
        raise ConfigError(f"{name} must be one of: {', '.join(sorted(LOG_LEVELS))}")
    return level


def _as_choice(value: Any, name: str, allowed: set[str]) -> str:
    choice = _as_str(value, name).casefold()
    if choice not in allowed:
        raise ConfigError(f"{name} must be one of: {', '.join(sorted(allowed))}")
    return choice


def _as_watermark_strength(value: Any, name: str) -> str:
    strength = _as_str(value, name).casefold()
    if strength not in WATERMARK_STRENGTHS:
        allowed = ", ".join(sorted(WATERMARK_STRENGTHS))
        raise ConfigError(f"{name} must be one of: {allowed}")
    return strength


def _as_str_list(value: Any, name: str) -> list[str]:
    if not isinstance(value, list):
        raise ConfigError(f"{name} must be a list of strings")
    result = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(f"{name}[{index}] must be a non-empty string")
        result.append(item.strip())
    return result


def _as_extra_yt_dlp_args(value: Any, name: str) -> list[str]:
    args = _as_str_list(value, name)
    for arg in args:
        option = arg.partition("=")[0]
        if option in DISALLOWED_EXTRA_YT_DLP_ARGS:
            raise ConfigError(
                f"{name} cannot include {option}; it would stop media downloads"
            )
    return args


def _as_offset_list(value: Any, name: str) -> list[int]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{name} must be a non-empty list of seconds")

    result: list[int] = []
    previous = -1
    for index, item in enumerate(value):
        if not isinstance(item, int) or isinstance(item, bool) or item < 0:
            raise ConfigError(f"{name}[{index}] must be a non-negative integer")
        if item <= previous:
            raise ConfigError(f"{name} must be strictly increasing")
        result.append(item)
        previous = item
    return result
