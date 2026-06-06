from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence
import json
import logging
import shlex
import shutil
import subprocess
import tempfile

from .chat_render import (
    ffprobe_path_for,
    iter_live_chat_json_objects,
    parse_live_chat_file,
    probe_video_duration,
)
from .config import BotConfig


LOGGER = logging.getLogger(__name__)
CHAT_REFRESH_TIMEOUT_SECONDS = 60 * 60
CHAT_LIVE_BACKUP_SUFFIX = ".raw-live.json.bak"
YOUTUBE_START_TIMESTAMP_KEYS = (
    "actual_start_timestamp",
    "live_start_timestamp",
    "start_timestamp",
    "release_timestamp",
    "timestamp",
)


@dataclass(frozen=True, slots=True)
class ChatRefreshResult:
    ok: bool
    changed: bool
    source: str
    message: str
    backup_file: Path | None = None


def refresh_chat_sidecar(
    config: BotConfig,
    *,
    video_url: str,
    media_file: Path,
    chat_file: Path,
    last_exit_at: str | None = None,
    stream_metadata: Mapping[str, Any] | None = None,
    logger: logging.Logger = LOGGER,
) -> ChatRefreshResult:
    replay = refresh_chat_from_replay(
        config,
        video_url=video_url,
        chat_file=chat_file,
        logger=logger,
    )
    if replay.ok:
        return replay

    logger.warning(
        "Unable to refresh chat replay for %s; trying recorded live chat sync: %s",
        chat_file,
        replay.message,
    )
    synced = sync_recorded_live_chat(
        config,
        media_file=media_file,
        chat_file=chat_file,
        last_exit_at=last_exit_at,
        stream_metadata=stream_metadata,
        logger=logger,
    )
    if synced.ok:
        return synced

    return ChatRefreshResult(
        ok=False,
        changed=False,
        source="unchanged",
        message=f"{replay.message}; {synced.message}",
    )


def refresh_chat_from_replay(
    config: BotConfig,
    *,
    video_url: str,
    chat_file: Path,
    logger: logging.Logger = LOGGER,
) -> ChatRefreshResult:
    with tempfile.TemporaryDirectory(
        prefix=f"{chat_file.stem}.refresh.",
        dir=str(chat_file.parent),
    ) as tmp:
        output_template = Path(tmp) / "chat.%(ext)s"
        command = build_chat_replay_download_command(config, video_url, output_template)
        logger.info("Refreshing live chat replay for %s", chat_file)
        logger.debug("yt-dlp chat replay command: %s", shlex.join(command))
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                check=False,
                timeout=CHAT_REFRESH_TIMEOUT_SECONDS,
            )
        except FileNotFoundError:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message=f"yt-dlp not found: {config.yt_dlp_path}",
            )
        except subprocess.TimeoutExpired:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message="yt-dlp timed out while refreshing chat replay",
            )
        except OSError as exc:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message=str(exc) or exc.__class__.__name__,
            )

        if result.returncode != 0:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message=process_output_message(result.stdout, result.stderr)
                or f"yt-dlp exited with code {result.returncode}",
            )

        candidates = sorted(Path(tmp).glob("*.live_chat.json"))
        if not candidates:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message="yt-dlp did not write a live chat replay file",
            )

        candidate = candidates[0]
        try:
            entries = parse_live_chat_file(candidate)
        except OSError as exc:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message=f"unable to read refreshed chat: {exc}",
            )
        if not entries:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message="refreshed chat replay had no usable messages",
            )
        if live_chat_file_has_live_markers(candidate):
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message="refreshed chat still looks like a live capture",
            )

        replace_file(candidate, chat_file)
        return ChatRefreshResult(
            ok=True,
            changed=True,
            source="replay",
            message=f"Refreshed chat replay with {len(entries)} messages",
        )


def build_chat_replay_download_command(
    config: BotConfig,
    video_url: str,
    output_template: Path,
) -> list[str]:
    command = [
        config.yt_dlp_path,
        *yt_dlp_args_without_live_from_start(config.extra_yt_dlp_args),
        "--skip-download",
        "--write-subs",
        "--sub-langs",
        "live_chat",
        "--no-playlist",
        "-o",
        str(output_template),
        video_url,
    ]
    return command


def yt_dlp_args_without_live_from_start(args: Sequence[str]) -> list[str]:
    filtered: list[str] = []
    for arg in args:
        if arg == "--live-from-start":
            continue
        if arg.startswith("--live-from-start="):
            continue
        filtered.append(arg)
    return filtered


def sync_recorded_live_chat(
    config: BotConfig,
    *,
    media_file: Path,
    chat_file: Path,
    last_exit_at: str | None = None,
    stream_metadata: Mapping[str, Any] | None = None,
    logger: logging.Logger = LOGGER,
) -> ChatRefreshResult:
    if not live_chat_file_has_live_markers(chat_file):
        return ChatRefreshResult(
            ok=False,
            changed=False,
            source="sync",
            message="existing chat does not look like a live capture",
        )

    origin_us = stream_start_timestamp_us(stream_metadata or {})
    origin_source = "metadata" if origin_us is not None else ""
    duration = 0.0
    if origin_us is None:
        try:
            duration = probe_video_duration(media_file, ffprobe_path_for(config.ffmpeg_path))
        except Exception as exc:  # noqa: BLE001 - failure is reported as sync unavailable.
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="sync",
                message=f"unable to probe media duration for chat sync: {exc}",
            )
        origin_us = media_origin_from_exit(last_exit_at, duration)
        origin_source = "media exit time" if origin_us is not None else ""

    if origin_us is None:
        return ChatRefreshResult(
            ok=False,
            changed=False,
            source="sync",
            message="unable to determine media timeline origin for recorded chat",
        )

    try:
        normalized, changed_count = normalized_live_chat_lines(chat_file, origin_us)
    except OSError as exc:
        return ChatRefreshResult(
            ok=False,
            changed=False,
            source="sync",
            message=f"unable to read recorded chat: {exc}",
        )

    if changed_count == 0:
        return ChatRefreshResult(
            ok=False,
            changed=False,
            source="sync",
            message="recorded chat had no timestamped live messages to sync",
        )

    backup_file = unique_live_chat_backup_file(chat_file)
    replacement = chat_file.with_name(f"{chat_file.name}.syncing")
    try:
        shutil.copy2(chat_file, backup_file)
        replacement.write_text("".join(normalized), encoding="utf-8")
        replacement.replace(chat_file)
    except OSError as exc:
        replacement.unlink(missing_ok=True)
        backup_file.unlink(missing_ok=True)
        return ChatRefreshResult(
            ok=False,
            changed=False,
            source="sync",
            message=f"unable to write synced chat: {exc}",
        )

    logger.info(
        "Synced recorded live chat %s using %s; messages=%d backup=%s",
        chat_file,
        origin_source,
        changed_count,
        backup_file,
    )
    return ChatRefreshResult(
        ok=True,
        changed=True,
        source="sync",
        message=f"Synced recorded live chat using {origin_source}",
        backup_file=backup_file,
    )


def normalized_live_chat_lines(path: Path, media_origin_us: int) -> tuple[list[str], int]:
    lines: list[str] = []
    changed_count = 0
    for item in iter_live_chat_json_objects(path):
        timestamp_us = first_timestamp_us(item)
        if timestamp_us is not None and object_has_live_marker(item):
            offset_ms = max(0, round((timestamp_us - media_origin_us) / 1000))
            if apply_video_offset_ms(item, offset_ms):
                changed_count += 1
        lines.append(json.dumps(item, ensure_ascii=False) + "\n")
    return lines, changed_count


def live_chat_file_has_live_markers(path: Path) -> bool:
    try:
        return any(object_has_live_marker(item) for item in iter_live_chat_json_objects(path))
    except OSError:
        return False


def object_has_live_marker(node: Any) -> bool:
    if isinstance(node, list):
        return any(object_has_live_marker(item) for item in node)
    if not isinstance(node, dict):
        return False
    if node.get("isLive") is True:
        return True
    return any(object_has_live_marker(value) for value in node.values())


def first_timestamp_us(node: Any) -> int | None:
    if isinstance(node, list):
        for item in node:
            timestamp = first_timestamp_us(item)
            if timestamp is not None:
                return timestamp
        return None
    if not isinstance(node, dict):
        return None
    timestamp = coerce_int(node.get("timestampUsec"))
    if timestamp is not None:
        return timestamp
    for value in node.values():
        timestamp = first_timestamp_us(value)
        if timestamp is not None:
            return timestamp
    return None


def apply_video_offset_ms(node: Any, offset_ms: int) -> bool:
    changed = False
    if isinstance(node, list):
        for item in node:
            changed = apply_video_offset_ms(item, offset_ms) or changed
        return changed
    if not isinstance(node, dict):
        return False

    if "videoOffsetTimeMsec" in node or "replayChatItemAction" in node:
        node["videoOffsetTimeMsec"] = str(offset_ms)
        changed = True
    for value in node.values():
        changed = apply_video_offset_ms(value, offset_ms) or changed
    return changed


def stream_start_timestamp_us(metadata: Mapping[str, Any]) -> int | None:
    for key in YOUTUBE_START_TIMESTAMP_KEYS:
        timestamp = timestamp_value_to_us(metadata.get(key))
        if timestamp is not None:
            return timestamp
    return None


def timestamp_value_to_us(value: Any) -> int | None:
    number = coerce_float(value)
    if number is not None and number > 0:
        if number > 10_000_000_000_000:
            return round(number)
        if number > 10_000_000_000:
            return round(number * 1000)
        return round(number * 1_000_000)
    if isinstance(value, str) and value:
        try:
            return round(datetime.fromisoformat(value).timestamp() * 1_000_000)
        except ValueError:
            return None
    return None


def media_origin_from_exit(last_exit_at: str | None, media_duration_seconds: float) -> int | None:
    if not last_exit_at or media_duration_seconds <= 0:
        return None
    try:
        exit_timestamp = datetime.fromisoformat(last_exit_at).timestamp()
    except ValueError:
        return None
    return round((exit_timestamp - media_duration_seconds) * 1_000_000)


def unique_live_chat_backup_file(chat_file: Path) -> Path:
    base = chat_file.with_name(f"{chat_file.name}{CHAT_LIVE_BACKUP_SUFFIX}")
    if not base.exists():
        return base
    for index in range(2, 1000):
        candidate = chat_file.with_name(f"{chat_file.name}.{index}{CHAT_LIVE_BACKUP_SUFFIX}")
        if not candidate.exists():
            return candidate
    return chat_file.with_name(f"{chat_file.name}.{datetime.now().timestamp():.0f}{CHAT_LIVE_BACKUP_SUFFIX}")


def replace_file(source: Path, target: Path) -> None:
    replacement = target.with_name(f"{target.name}.refreshing")
    shutil.copy2(source, replacement)
    replacement.replace(target)


def process_output_message(stdout: bytes, stderr: bytes) -> str:
    output = (stderr or stdout).decode("utf-8", "replace").strip()
    if not output:
        return ""
    return output.splitlines()[-1][-500:]


def coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
