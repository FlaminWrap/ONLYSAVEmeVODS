from __future__ import annotations

from collections import Counter
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
from .chat_timing import (
    ChatTiming,
    chat_timing_file_for_chat_file,
    iso_timestamp_to_us,
    read_chat_timing,
    stream_start_timestamp_us,
)
from .config import BotConfig


LOGGER = logging.getLogger(__name__)
CHAT_REFRESH_TIMEOUT_SECONDS = 60 * 60
CHAT_LIVE_BACKUP_SUFFIX = ".raw-live.json.bak"


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
    timing_file: Path | None = None,
    allow_replay: bool = True,
    logger: logging.Logger = LOGGER,
) -> ChatRefreshResult:
    timing = read_chat_timing(timing_file or chat_timing_file_for_chat_file(chat_file))
    recorded_live_from_start = (
        timing.media_live_from_start if timing is not None else config.live_from_start
    )
    if recorded_live_from_start and allow_replay:
        replay = refresh_chat_from_replay(
            config,
            video_url=video_url,
            chat_file=chat_file,
            logger=logger,
        )
    elif not allow_replay:
        replay = ChatRefreshResult(
            ok=False,
            changed=False,
            source="replay",
            message="video is terminally unavailable; using recorded chat sync",
        )
    else:
        replay = ChatRefreshResult(
            ok=False,
            changed=False,
            source="replay",
            message=(
                "whole-stream chat replay is not segment-relative; "
                "using recorded chat sync"
            ),
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
        timing_file=timing_file,
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

        existing_file = chat_file.is_file()
        existing_entries: list[Any] = []
        if existing_file:
            try:
                existing_entries = parse_live_chat_file(chat_file)
            except OSError as exc:
                return ChatRefreshResult(
                    ok=False,
                    changed=False,
                    source="replay",
                    message=f"unable to compare recorded live chat: {exc}",
                )
            if existing_entries:
                regression = replay_coverage_regression(existing_entries, entries)
                if regression:
                    return ChatRefreshResult(
                        ok=False,
                        changed=False,
                        source="replay",
                        message=regression,
                    )

        backup_file = unique_live_chat_backup_file(chat_file) if existing_file else None
        try:
            if backup_file is not None:
                shutil.copy2(chat_file, backup_file)
            replace_file(candidate, chat_file)
        except OSError as exc:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="replay",
                message=f"unable to replace recorded chat with replay: {exc}",
                backup_file=backup_file if backup_file and backup_file.is_file() else None,
            )
        return ChatRefreshResult(
            ok=True,
            changed=True,
            source="replay",
            message=f"Refreshed chat replay with {len(entries)} messages",
            backup_file=backup_file,
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
    timing_file: Path | None = None,
    logger: logging.Logger = LOGGER,
) -> ChatRefreshResult:
    if not live_chat_file_has_live_markers(chat_file):
        try:
            existing_entries = parse_live_chat_file(chat_file)
        except OSError as exc:
            return ChatRefreshResult(
                ok=False,
                changed=False,
                source="existing",
                message=f"unable to read existing recorded chat: {exc}",
            )
        if existing_entries:
            return ChatRefreshResult(
                ok=True,
                changed=False,
                source="existing",
                message=(
                    "Existing recorded chat already has a valid timeline with "
                    f"{len(existing_entries)} messages"
                ),
            )
        return ChatRefreshResult(
            ok=False,
            changed=False,
            source="sync",
            message="existing chat has no usable messages or live capture markers",
        )

    timing = read_chat_timing(timing_file or chat_timing_file_for_chat_file(chat_file))
    origin_us, origin_source = media_origin_from_timing(
        timing,
        stream_metadata=stream_metadata or {},
        config_live_from_start=config.live_from_start,
    )
    duration_error = ""
    if origin_us is None:
        try:
            duration = probe_video_duration(media_file, ffprobe_path_for(config.ffmpeg_path))
        except Exception as exc:  # noqa: BLE001 - failure is reported as sync unavailable.
            duration = 0.0
            duration_error = str(exc) or exc.__class__.__name__

        exit_candidates = (
            (timing.last_exit_at, "timing media exit") if timing is not None else (None, ""),
            (last_exit_at, "media exit"),
        )
        for exit_at, source in exit_candidates:
            origin_us = media_origin_from_exit(exit_at, duration)
            if origin_us is not None:
                origin_source = source
                break

    if origin_us is None and timing is not None:
        origin_us = iso_timestamp_to_us(timing.media_started_at)
        if origin_us is not None:
            origin_source = "timing media start"

    if origin_us is None:
        detail = (
            f"; unable to probe media duration: {duration_error}"
            if duration_error
            else ""
        )
        return ChatRefreshResult(
            ok=False,
            changed=False,
            source="sync",
            message=(
                "unable to determine media timeline origin for recorded chat"
                f"{detail}"
            ),
        )

    chat_delay_message = chat_capture_delay_message(timing, origin_us)
    if chat_delay_message:
        logger.info("%s for %s", chat_delay_message, chat_file)

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
        "Synced recorded live chat %s using %s; messages=%d backup=%s%s",
        chat_file,
        origin_source,
        changed_count,
        backup_file,
        f"; {chat_delay_message}" if chat_delay_message else "",
    )
    message = f"Synced recorded live chat using {origin_source}"
    if chat_delay_message:
        message = f"{message}; {chat_delay_message}"
    return ChatRefreshResult(
        ok=True,
        changed=True,
        source="sync",
        message=message,
        backup_file=backup_file,
    )


def media_origin_from_timing(
    timing: ChatTiming | None,
    *,
    stream_metadata: Mapping[str, Any],
    config_live_from_start: bool,
) -> tuple[int | None, str]:
    recorded_live_from_start = (
        timing.media_live_from_start
        if timing is not None
        else config_live_from_start
    )
    if not recorded_live_from_start:
        return None, ""
    if timing is not None:
        stream_origin = iso_timestamp_to_us(timing.stream_started_at)
        if stream_origin is not None:
            return stream_origin, "timing stream start"
    metadata_origin = stream_start_timestamp_us(stream_metadata)
    if metadata_origin is not None:
        return metadata_origin, "metadata stream start"

    return None, ""


def chat_capture_delay_message(timing: ChatTiming | None, origin_us: int) -> str:
    if timing is None:
        return ""
    chat_started_at = iso_timestamp_to_us(timing.chat_started_at)
    if chat_started_at is None:
        return ""
    delay_seconds = (chat_started_at - origin_us) / 1_000_000
    if delay_seconds < 1:
        return ""
    return f"chat capture began {delay_seconds:.1f}s after media origin"


def normalized_live_chat_lines(path: Path, media_origin_us: int) -> tuple[list[str], int]:
    lines: list[str] = []
    changed_count = 0
    for item in iter_live_chat_json_objects(path):
        timestamp_us = first_timestamp_us(item)
        live_capture_item = object_has_live_marker(item)
        if live_capture_item and timestamp_us is None:
            # A live capture's offset is relative to the chat subprocess, not
            # the media.  Without YouTube's absolute timestamp this item cannot
            # be aligned safely; retain it in the raw backup only.
            continue
        if timestamp_us is not None and live_capture_item:
            if timestamp_us < media_origin_us:
                continue
            offset_ms = max(0, round((timestamp_us - media_origin_us) / 1000))
            if not apply_video_offset_ms(item, offset_ms):
                # An absolute timestamp is not enough if the object has no
                # replay offset field that the renderer can consume.
                continue
            changed_count += 1
        if live_capture_item:
            clear_live_markers(item)
        lines.append(json.dumps(item, ensure_ascii=False) + "\n")
    return lines, changed_count


def replay_coverage_regression(
    existing_entries: Sequence[Any],
    replay_entries: Sequence[Any],
) -> str:
    if len(replay_entries) < len(existing_entries):
        return (
            "refreshed chat replay would drop recorded messages "
            f"({len(replay_entries)} replay < {len(existing_entries)} recorded)"
        )

    existing_coverage = absolute_timestamp_coverage(existing_entries)
    replay_coverage = absolute_timestamp_coverage(replay_entries)
    if existing_coverage is not None and replay_coverage is None:
        return "refreshed chat replay has no absolute timestamp coverage"
    if existing_coverage is not None and replay_coverage is not None and (
        replay_coverage[0] > existing_coverage[0]
        or replay_coverage[1] < existing_coverage[1]
    ):
        return (
            "refreshed chat replay would reduce absolute timestamp coverage "
            f"from {existing_coverage[0]}-{existing_coverage[1]} to "
            f"{replay_coverage[0]}-{replay_coverage[1]}"
        )

    existing_messages = Counter(
        (
            getattr(entry, "timestamp_us", None),
            getattr(entry, "author", ""),
            getattr(entry, "message", ""),
        )
        for entry in existing_entries
    )
    replay_messages = Counter(
        (
            getattr(entry, "timestamp_us", None),
            getattr(entry, "author", ""),
            getattr(entry, "message", ""),
        )
        for entry in replay_entries
    )
    missing_messages = existing_messages - replay_messages
    if missing_messages:
        return (
            "refreshed chat replay would omit "
            f"{sum(missing_messages.values())} locally recorded message(s)"
        )
    return ""


def absolute_timestamp_coverage(entries: Sequence[Any]) -> tuple[int, int] | None:
    timestamps = [
        int(entry.timestamp_us)
        for entry in entries
        if getattr(entry, "timestamp_us", None) is not None
    ]
    if not timestamps:
        return None
    return min(timestamps), max(timestamps)


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


def clear_live_markers(node: Any) -> None:
    if isinstance(node, list):
        for item in node:
            clear_live_markers(item)
        return
    if not isinstance(node, dict):
        return
    if node.get("isLive") is True:
        node["isLive"] = False
    for value in node.values():
        clear_live_markers(value)


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


def media_origin_from_exit(last_exit_at: str | None, media_duration_seconds: float) -> int | None:
    if not last_exit_at or media_duration_seconds <= 0:
        return None
    exit_timestamp_us = iso_timestamp_to_us(last_exit_at)
    if exit_timestamp_us is None:
        return None
    return exit_timestamp_us - round(media_duration_seconds * 1_000_000)


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
