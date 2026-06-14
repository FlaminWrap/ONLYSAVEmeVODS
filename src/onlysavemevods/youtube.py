from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit
import concurrent.futures
import json
import logging
import re
import shlex
import subprocess

from .models import LiveStream, qualified_stream_id, video_url


LOGGER = logging.getLogger(__name__)
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
YOUTUBE_ID_IN_URL_RE = re.compile(
    r"(?:v=|/shorts/|/live/|/embed/|youtu\.be/|/)([A-Za-z0-9_-]{11})(?:[/?&#]|$)"
)
CACHEABLE_NON_LIVE_STATUSES = {"not_live", "was_live"}
CHANNEL_PAGE_SUFFIXES = ("/streams", "/videos", "/live", "/featured")


class YtDlpError(RuntimeError):
    """Raised when yt-dlp exits unsuccessfully or returns invalid JSON."""


class TerminalVideoUnavailableError(YtDlpError):
    """Raised when YouTube reports a video is permanently unavailable."""


TERMINAL_VIDEO_UNAVAILABLE_PATTERNS = (
    re.compile(r"\bprivate video\b", re.IGNORECASE),
    re.compile(r"\bthis video is private\b", re.IGNORECASE),
    re.compile(r"\bvideo unavailable\b.*\bprivate\b", re.IGNORECASE),
    re.compile(r"\bthis video has been (?:removed|deleted)\b", re.IGNORECASE),
    re.compile(r"\bvideo unavailable\b.*\b(?:removed|deleted)\b", re.IGNORECASE),
    re.compile(r"\bno longer available\b.*\bterminated\b", re.IGNORECASE),
)


@dataclass(slots=True)
class YtDlpRunner:
    binary: str = "yt-dlp"

    def run_json(self, args: list[str], timeout: int = 120) -> dict[str, Any]:
        command = [self.binary, *args]
        LOGGER.debug("Running yt-dlp metadata command: %s", shlex.join(command))
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                check=False,
                text=True,
                timeout=timeout,
            )
        except FileNotFoundError as exc:
            raise YtDlpError(f"yt-dlp binary not found: {self.binary}") from exc
        except subprocess.TimeoutExpired as exc:
            raise YtDlpError(f"yt-dlp timed out after {timeout}s: {' '.join(command)}") from exc

        if completed.returncode != 0:
            message = completed.stderr.strip() or completed.stdout.strip()
            error = f"yt-dlp failed with code {completed.returncode}: {message}"
            LOGGER.debug(
                "yt-dlp metadata command failed rc=%s message=%s",
                completed.returncode,
                truncate_for_log(message),
            )
            if is_terminal_video_unavailable_message(message):
                LOGGER.info(
                    "yt-dlp reported terminal video unavailable: %s",
                    first_log_line(message),
                )
                raise TerminalVideoUnavailableError(error)
            raise YtDlpError(error)

        output = completed.stdout.strip()
        LOGGER.debug("yt-dlp metadata command returned %s bytes", len(output))
        try:
            parsed = json.loads(output)
        except json.JSONDecodeError as exc:
            raise YtDlpError(f"yt-dlp returned invalid JSON: {output[:500]}") from exc

        if not isinstance(parsed, dict):
            raise YtDlpError("yt-dlp returned JSON that was not an object")
        return parsed


def is_terminal_video_unavailable_message(message: str) -> bool:
    return any(
        pattern.search(message)
        for pattern in TERMINAL_VIDEO_UNAVAILABLE_PATTERNS
    )


def first_log_line(message: str) -> str:
    return truncate_for_log(message.splitlines()[0] if message else "")


def truncate_for_log(message: str, limit: int = 1000) -> str:
    if len(message) <= limit:
        return message
    return f"{message[:limit]}... <truncated>"


class YoutubeProbe:
    def __init__(
        self,
        runner: YtDlpRunner | None = None,
        *,
        channel_scan_limit: int = 10,
        discovery_probe_concurrency: int = 4,
    ) -> None:
        self.runner = runner or YtDlpRunner()
        self.channel_scan_limit = channel_scan_limit
        self.discovery_probe_concurrency = max(1, discovery_probe_concurrency)
        self._known_non_live_video_ids: set[str] = set()

    def discover_channel_live_streams(
        self,
        channel: str,
        *,
        skip_video_ids: set[str] | None = None,
        include_channel_live: bool = True,
    ) -> list[LiveStream]:
        LOGGER.debug(
            "Discovering channel streams channel=%s include_live=%s scan_limit=%s "
            "concurrency=%s skip=%s",
            channel,
            include_channel_live,
            self.channel_scan_limit,
            self.discovery_probe_concurrency,
            sorted(skip_video_ids or ()),
        )
        live_streams: list[LiveStream] = []
        seen: set[str] = set(skip_video_ids or ())

        if include_channel_live:
            live_stream = self.probe_channel_live_stream(channel)
            if live_stream:
                live_streams.append(live_stream)
                seen.add(live_stream.video_id)

        streams_url = channel_streams_url(channel)
        playlist = self.runner.run_json(
            [
                "--dump-single-json",
                "--flat-playlist",
                "--playlist-end",
                str(self.channel_scan_limit),
                "--skip-download",
                "--no-warnings",
                streams_url,
            ]
        )

        video_ids = _candidate_video_ids(playlist)
        candidates: list[str] = []
        for candidate in video_ids:
            qualified_candidate = qualified_stream_id("youtube", candidate)
            if qualified_candidate in seen or qualified_candidate in self._known_non_live_video_ids:
                continue
            seen.add(qualified_candidate)
            candidates.append(candidate)

        LOGGER.debug(
            "Channel %s streams page returned %s candidates; probing %s after skips",
            channel,
            len(video_ids),
            len(candidates),
        )
        live_streams.extend(self._probe_candidate_videos(candidates))
        LOGGER.debug(
            "Channel %s discovery found %s live stream(s)",
            channel,
            len(live_streams),
        )
        return live_streams

    def probe_channel_live_stream(self, channel: str) -> LiveStream | None:
        live_url = channel_live_url(channel)
        LOGGER.debug("Probing channel live URL channel=%s url=%s", channel, live_url)
        try:
            stream = self.probe_video(live_url)
        except YtDlpError as exc:
            LOGGER.debug("Channel live URL probe failed for %s: %s", channel, exc)
            return None

        self._remember_non_live(stream)
        return stream if stream.is_live else None

    def probe_video(self, url_or_id: str) -> LiveStream:
        target = (
            url_or_id
            if url_or_id.startswith(("http://", "https://"))
            else video_url(url_or_id)
        )
        info = self.runner.run_json(
            [
                "--dump-json",
                "--skip-download",
                "--no-playlist",
                "--no-warnings",
                target,
            ]
        )
        stream = live_stream_from_info(info, fallback_url=target)
        LOGGER.debug(
            "Probed video id=%s is_live=%s live_status=%r title=%r channel=%r",
            stream.video_id,
            stream.is_live,
            stream.live_status,
            stream.title,
            stream.channel,
        )
        return stream

    def _probe_candidate_videos(self, video_ids: list[str]) -> list[LiveStream]:
        if not video_ids:
            LOGGER.debug("No candidate videos to probe")
            return []

        if self.discovery_probe_concurrency == 1 or len(video_ids) == 1:
            return [
                stream
                for video_id in video_ids
                if (stream := self._probe_candidate_video(video_id))
            ]

        live_streams: list[LiveStream] = []
        max_workers = min(self.discovery_probe_concurrency, len(video_ids))
        LOGGER.debug(
            "Probing %s candidate videos with %s worker(s)",
            len(video_ids),
            max_workers,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._probe_candidate_video, video_id)
                for video_id in video_ids
            ]
            for future in futures:
                stream = future.result()
                if stream:
                    live_streams.append(stream)
        return live_streams

    def _probe_candidate_video(self, video_id: str) -> LiveStream | None:
        try:
            stream = self.probe_video(video_url(video_id))
        except YtDlpError as exc:
            LOGGER.debug("Candidate video probe failed for %s: %s", video_id, exc)
            return None

        self._remember_non_live(stream)
        LOGGER.debug(
            "Candidate video %s live=%s live_status=%r",
            video_id,
            stream.is_live,
            stream.live_status,
        )
        return stream if stream.is_live else None

    def _remember_non_live(self, stream: LiveStream) -> None:
        if (
            not stream.is_live
            and stream.live_status in CACHEABLE_NON_LIVE_STATUSES
        ):
            self._known_non_live_video_ids.add(stream.video_id)


def live_stream_from_info(info: dict[str, Any], *, fallback_url: str = "") -> LiveStream:
    raw_video_id = str(info.get("id") or extract_video_id(fallback_url) or "")
    if not raw_video_id:
        raise YtDlpError("yt-dlp video metadata did not include a video id")

    live_status = str(info.get("live_status") or "")
    is_live = bool(info.get("is_live")) or live_status == "is_live"
    return LiveStream(
        video_id=qualified_stream_id("youtube", raw_video_id),
        url=str(info.get("webpage_url") or video_url(raw_video_id)),
        title=str(info.get("title") or ""),
        channel=str(info.get("channel") or info.get("uploader") or ""),
        live_status=live_status,
        is_live=is_live,
        platform="youtube",
        source=fallback_url,
        raw=info,
    )


def channel_streams_url(channel: str) -> str:
    base_url = channel_base_url(channel)
    parts = urlsplit(base_url)
    path = f"{parts.path.rstrip('/')}/streams" if parts.path.rstrip("/") else "/streams"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def channel_live_url(channel: str) -> str:
    base_url = channel_base_url(channel)
    parts = urlsplit(base_url)
    path = f"{parts.path.rstrip('/')}/live" if parts.path.rstrip("/") else "/live"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def channel_base_url(channel: str) -> str:
    target = channel.strip()
    if not target:
        raise ValueError("channel cannot be empty")

    if target.startswith("@"):
        target = f"https://www.youtube.com/{target}"
    elif not target.startswith(("http://", "https://")):
        if target.startswith(("youtube.com/", "www.youtube.com/")):
            target = f"https://{target}"
        else:
            target = f"https://www.youtube.com/@{target.lstrip('@')}"

    parts = urlsplit(target)
    path = parts.path.rstrip("/")
    for suffix in CHANNEL_PAGE_SUFFIXES:
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    return urlunsplit((parts.scheme or "https", parts.netloc or "www.youtube.com", path, "", ""))


def extract_video_id(value: str) -> str | None:
    if YOUTUBE_ID_RE.match(value):
        return value

    parts = urlsplit(value)
    query_id = parse_qs(parts.query).get("v", [None])[0]
    if query_id and YOUTUBE_ID_RE.match(query_id):
        return query_id

    match = YOUTUBE_ID_IN_URL_RE.search(value)
    if match:
        return match.group(1)
    return None


def _candidate_video_ids(playlist: dict[str, Any]) -> list[str]:
    entries = playlist.get("entries") or []
    if not isinstance(entries, list):
        return []

    candidates: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue

        for key in ("id", "url", "webpage_url"):
            value = entry.get(key)
            if isinstance(value, str):
                video_id = extract_video_id(value)
                if video_id:
                    candidates.append(video_id)
                    break
    return candidates


def is_probable_executable(path: str | Path) -> bool:
    return bool(str(path).strip())
