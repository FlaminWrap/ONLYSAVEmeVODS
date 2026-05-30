from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import json
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


class ConfigError(ValueError):
    """Raised when a config file cannot be parsed into a usable bot config."""


@dataclass(slots=True)
class BotConfig:
    channels: list[str] = field(default_factory=list)
    download_dir: Path = Path("downloads")
    state_dir: Path = Path("state")
    poll_interval_seconds: int = 60
    max_concurrent_downloads: int = 4
    live_from_start: bool = True
    record_live_chat: bool = False
    render_live_chat_video: bool = False
    chat_render_panel_workers: int = 0
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
    watermark_secret_env: str = "YTDLBOT_WATERMARK_SECRET"
    watermark_strength: str = "invisible"
    watermark_detect_upload_max_bytes: int = 2_147_483_648
    config_path: Path | None = None

    @property
    def db_path(self) -> Path:
        return self.state_dir / "ytdlbot.sqlite3"


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

    return BotConfig(
        channels=_as_str_list(raw.get("channels", []), "channels"),
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
            raw.get("watermark_secret_env", "YTDLBOT_WATERMARK_SECRET"),
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
        current = tomllib.loads(current_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {target}: {exc}") from exc
    try:
        defaults = tomllib.loads(defaults_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {defaults_file}: {exc}") from exc

    if not isinstance(current, dict) or not isinstance(defaults, dict):
        raise ConfigError("Config files must have TOML tables at the root")

    missing = [key for key in defaults if key not in current]
    if not missing:
        return []

    block = [
        "",
        "# Added by YTDLBot config update. Existing settings above were left unchanged.",
    ]
    for key in missing:
        block.append(f"{key} = {_toml_value(defaults[key], key)}")

    prefix = current_text
    if prefix and not prefix.endswith("\n"):
        prefix += "\n"
    try:
        target.write_text(prefix + "\n".join(block) + "\n", encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Unable to update config file {target}: {exc}") from exc
    return missing


def _toml_value(value: Any, name: str) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
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
