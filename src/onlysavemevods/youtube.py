from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit
from urllib.request import Request, urlopen
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
HLS_MANIFEST_READ_LIMIT = 2 * 1024 * 1024
WATCH_PAGE_READ_LIMIT = 2 * 1024 * 1024
WATCH_PAGE_TIMEOUT_SECONDS = 15


class YtDlpError(RuntimeError):
    """Raised when yt-dlp exits unsuccessfully or returns invalid JSON."""


class TerminalVideoUnavailableError(YtDlpError):
    """Raised when YouTube reports a video is permanently unavailable."""


class ConfirmedLiveTerminationError(TerminalVideoUnavailableError):
    """Raised when YouTube explicitly says a live stream was terminated."""


class ConfirmedVideoRemovalError(TerminalVideoUnavailableError):
    """Raised when YouTube explicitly says the requested video was removed."""


class ConfirmedLiveEndError(TerminalVideoUnavailableError):
    """Raised when the requested video explicitly reports its broadcast ended."""


CONFIRMED_LIVE_TERMINATION_PATTERN = re.compile(
    r"^ERROR:\s*\[youtube\]\s+[A-Za-z0-9_-]{11}:[^\n]*"
    r"\blive\s+stream\s+(?:has\s+been|was|is)\s+terminated\s+due\s+to\b",
    re.IGNORECASE | re.MULTILINE,
)

VIDEO_REMOVAL_REASON_PATTERN = re.compile(
    r"\bthis\s+video\s+(?:has\s+been|was|is)\s+(?:removed|deleted)\b",
    re.IGNORECASE,
)
YOUTUBE_ERROR_LINE_PATTERN = re.compile(
    r"^ERROR:[ \t]*\[youtube\][ \t]+(?P<video_id>[A-Za-z0-9_-]{11}):[ \t]*(?P<reason>[^\r\n]*)$",
    re.IGNORECASE | re.MULTILINE,
)
GENERIC_VIDEO_UNAVAILABLE_PATTERN = re.compile(
    r"^(?:yt-dlp failed with code \d+:[ \t]*)?"
    r"ERROR:[ \t]*\[youtube\][ \t]+(?P<video_id>[A-Za-z0-9_-]{11}):"
    r"[ \t]*Video unavailable\.?[ \t]*$",
    re.IGNORECASE | re.MULTILINE,
)
AGE_RESTRICTED_VIDEO_PATTERN = re.compile(
    r"^(?:yt-dlp failed with code \d+:[ \t]*)?"
    r"ERROR:[ \t]*\[youtube\][ \t]+(?P<video_id>[A-Za-z0-9_-]{11}):"
    r"[^\r\n]*\bSign in to confirm your age\b[^\r\n]*$",
    re.IGNORECASE | re.MULTILINE,
)
INITIAL_PLAYER_RESPONSE_PATTERN = re.compile(
    r"\bytInitialPlayerResponse[ \t]*=[ \t]*"
)


TERMINAL_VIDEO_UNAVAILABLE_PATTERNS = (
    re.compile(r"\bprivate video\b", re.IGNORECASE),
    re.compile(r"\bthis video is private\b", re.IGNORECASE),
    re.compile(r"\bvideo unavailable\b.*\bprivate\b", re.IGNORECASE),
    re.compile(r"\bthis video has been (?:removed|deleted)\b", re.IGNORECASE),
    re.compile(r"\bvideo unavailable\b.*\b(?:removed|deleted)\b", re.IGNORECASE),
    re.compile(r"\bno longer available\b.*\bterminated\b", re.IGNORECASE),
    re.compile(r"\bblocked due to (?:the )?claimed content\b", re.IGNORECASE),
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
            if is_confirmed_live_termination_message(message):
                LOGGER.info(
                    "yt-dlp reported confirmed live termination: %s",
                    first_log_line(message),
                )
                raise ConfirmedLiveTerminationError(error)
            if is_confirmed_video_removal_message(
                message, video_id=extract_video_id(args[-1]) if args else None
            ):
                LOGGER.info(
                    "yt-dlp reported confirmed video removal: %s",
                    first_log_line(message),
                )
                raise ConfirmedVideoRemovalError(error)
            if is_terminal_video_unavailable_message(message):
                LOGGER.info(
                    "yt-dlp reported terminal video unavailable: %s",
                    first_log_line(message),
                )
                raise TerminalVideoUnavailableError(error)
            raise YtDlpError(error)

        output = completed.stdout.strip()
        LOGGER.debug("yt-dlp metadata command returned %s bytes", len(output))
        if not output:
            raise YtDlpError("yt-dlp returned no JSON output")
        try:
            parsed = json.loads(output)
        except json.JSONDecodeError as exc:
            raise YtDlpError(f"yt-dlp returned invalid JSON: {output[:500]}") from exc

        if not isinstance(parsed, dict):
            raise YtDlpError("yt-dlp returned JSON that was not an object")
        return parsed


@dataclass(frozen=True, slots=True)
class YouTubeLiveEdge:
    media_sequence: int | None
    newest_segment_at: datetime | None
    has_endlist: bool = False


def is_confirmed_live_termination_message(message: str) -> bool:
    return bool(CONFIRMED_LIVE_TERMINATION_PATTERN.search(message))


def is_confirmed_video_removal_message(
    message: str, *, video_id: str | None = None
) -> bool:
    return any(
        VIDEO_REMOVAL_REASON_PATTERN.search(match.group("reason"))
        for match in YOUTUBE_ERROR_LINE_PATTERN.finditer(message)
        if video_id is None or video_id == match.group("video_id")
    )


def is_terminal_video_unavailable_message(message: str) -> bool:
    return (
        is_confirmed_live_termination_message(message)
        or is_confirmed_video_removal_message(message)
        or any(
            pattern.search(message)
            for pattern in TERMINAL_VIDEO_UNAVAILABLE_PATTERNS
        )
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
        live_from_start: bool = False,
    ) -> None:
        self.runner = runner or YtDlpRunner()
        self.channel_scan_limit = channel_scan_limit
        self.discovery_probe_concurrency = max(1, discovery_probe_concurrency)
        self.live_from_start = live_from_start
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
        try:
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
        except YtDlpError as exc:
            if not live_streams:
                raise
            LOGGER.warning(
                "Channel streams page check failed for %s; keeping %s confirmed "
                "live stream(s) from the channel live URL: %s",
                channel,
                len(live_streams),
                exc,
            )
            return live_streams

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
        args = [
            "--dump-json",
            "--skip-download",
            "--no-playlist",
            "--no-warnings",
        ]
        if self.live_from_start:
            args.append("--live-from-start")
        args.append(target)
        info = self._run_video_metadata(args, target)
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

    def probe_live_edge(self, url_or_id: str) -> YouTubeLiveEdge:
        """Inspect YouTube's anonymous HLS edge without changing download mode."""
        target = (
            url_or_id
            if url_or_id.startswith(("http://", "https://"))
            else video_url(url_or_id)
        )
        info = self._run_video_metadata(
            [
                "--dump-json",
                "--skip-download",
                "--no-playlist",
                "--no-warnings",
                target,
            ],
            target,
        )
        manifest_url, headers = youtube_hls_media_manifest(info)
        if not manifest_url:
            raise YtDlpError("yt-dlp metadata did not include a YouTube HLS media manifest")

        request = Request(manifest_url, headers=headers)
        try:
            with urlopen(request, timeout=30) as response:
                payload = response.read(HLS_MANIFEST_READ_LIMIT + 1)
        except OSError as exc:
            raise YtDlpError(f"unable to read YouTube HLS media manifest: {exc}") from exc
        if len(payload) > HLS_MANIFEST_READ_LIMIT:
            raise YtDlpError("YouTube HLS media manifest exceeded the safety limit")
        return parse_youtube_hls_live_edge(payload.decode("utf-8", "replace"))

    def _run_video_metadata(self, args: list[str], target: str) -> dict[str, Any]:
        try:
            return self.runner.run_json(args)
        except YtDlpError as exc:
            if isinstance(
                exc,
                (
                    ConfirmedLiveTerminationError,
                    ConfirmedVideoRemovalError,
                    ConfirmedLiveEndError,
                ),
            ):
                raise
            match = GENERIC_VIDEO_UNAVAILABLE_PATTERN.search(str(exc)) or (
                AGE_RESTRICTED_VIDEO_PATTERN.search(str(exc))
            )
            video_id = extract_video_id(target)
            if match is None or video_id != match.group("video_id"):
                raise
            page = self._read_watch_page(video_id)
            if page is not None:
                reason = watch_page_video_removal_reason(page, video_id)
                if reason:
                    raise ConfirmedVideoRemovalError(
                        f"YouTube watch page confirmed removal of {video_id}: {reason}"
                    ) from exc
                ended_at = watch_page_confirmed_live_end(page, video_id)
                if ended_at is not None:
                    raise ConfirmedLiveEndError(
                        f"YouTube watch page confirmed broadcast end of {video_id} "
                        f"at {ended_at.isoformat()}"
                    ) from exc
            raise

    def _read_watch_page(self, video_id: str) -> str | None:
        request = Request(
            f"https://www.youtube.com/watch?v={video_id}&hl=en",
            headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"},
        )
        try:
            with urlopen(request, timeout=WATCH_PAGE_TIMEOUT_SECONDS) as response:
                final_url = urlsplit(response.geturl())
                if (
                    final_url.hostname not in {"www.youtube.com", "youtube.com"}
                    or final_url.path != "/watch"
                    or parse_qs(final_url.query).get("v") != [video_id]
                ):
                    return None
                payload = response.read(WATCH_PAGE_READ_LIMIT + 1)
            if len(payload) > WATCH_PAGE_READ_LIMIT:
                return None
            return payload.decode("utf-8", "replace")
        except (OSError, ValueError, TypeError, RecursionError) as exc:
            LOGGER.debug(
                "Unable to read YouTube watch page for %s: %s",
                video_id,
                exc,
            )
            return None

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


def _watch_page_player(page: str) -> dict[str, Any] | None:
    match = INITIAL_PLAYER_RESPONSE_PATTERN.search(page)
    if match is None:
        return None
    try:
        player, _end = json.JSONDecoder().raw_decode(page[match.end():].lstrip())
    except (ValueError, RecursionError):
        return None
    return player if isinstance(player, dict) else None


def watch_page_video_removal_reason(page: str, video_id: str) -> str | None:
    """Read an explicit removal reason from the requested video's player error."""
    player = _watch_page_player(page)
    if player is None:
        return None
    details = player.get("videoDetails")
    if isinstance(details, dict) and details.get("videoId") not in (None, video_id):
        return None
    status = player.get("playabilityStatus")
    if (
        not isinstance(status, dict)
        or not isinstance(status.get("status"), str)
        or status["status"] not in {"ERROR", "UNPLAYABLE"}
    ):
        return None

    reasons: list[str] = []
    if isinstance(status.get("reason"), str):
        reasons.append(status["reason"])
    screen = status.get("errorScreen")
    if isinstance(screen, dict):
        legacy = screen.get("playerErrorMessageRenderer")
        if isinstance(legacy, dict):
            reasons.extend(
                _player_error_text(legacy.get(key))
                for key in ("reason", "subreason")
            )
        interstitial = screen.get("playerInterstitialRenderer")
        if isinstance(interstitial, dict):
            content = interstitial.get("content")
            model = (
                content.get("interstitialViewModel")
                if isinstance(content, dict)
                else None
            )
            if isinstance(model, dict):
                reasons.extend(
                    _player_error_text(model.get(key))
                    for key in ("title", "description")
                )
    return next(
        (reason for reason in reasons if VIDEO_REMOVAL_REASON_PATTERN.search(reason)),
        None,
    )


def watch_page_confirmed_live_end(
    page: str,
    video_id: str,
    *,
    now: datetime | None = None,
) -> datetime | None:
    """Require an explicit past end for the exact requested broadcast."""
    player = _watch_page_player(page)
    if player is None:
        return None
    details = player.get("videoDetails")
    if not isinstance(details, dict) or details.get("videoId") != video_id:
        return None
    microformat = player.get("microformat")
    renderer = (
        microformat.get("playerMicroformatRenderer")
        if isinstance(microformat, dict)
        else None
    )
    if not isinstance(renderer, dict):
        return None
    if renderer.get("externalVideoId") not in (None, video_id):
        return None
    broadcast = renderer.get("liveBroadcastDetails")
    if not isinstance(broadcast, dict) or broadcast.get("isLiveNow") is not False:
        return None
    for metadata in (player, details, renderer, broadcast):
        for key in ("isLive", "isLiveNow", "isUpcoming"):
            if key in metadata and metadata[key] is not False:
                return None

    def timestamp(value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return None
            return parsed.astimezone(timezone.utc)
        except (ValueError, OverflowError):
            return None

    ended_at = timestamp(broadcast.get("endTimestamp"))
    current_time = now if now is not None else datetime.now(timezone.utc)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        return None
    if ended_at is None or ended_at >= current_time:
        return None
    if "startTimestamp" in broadcast:
        started_at = timestamp(broadcast["startTimestamp"])
        if started_at is None or started_at > ended_at:
            return None
    return ended_at


def _player_error_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return ""
    for key in ("simpleText", "content"):
        if isinstance(value.get(key), str):
            return value[key]
    runs = value.get("runs")
    if isinstance(runs, list):
        return "".join(
            run["text"]
            for run in runs
            if isinstance(run, dict) and isinstance(run.get("text"), str)
        )
    return ""


def youtube_hls_media_manifest(
    info: dict[str, Any],
) -> tuple[str, dict[str, str]]:
    formats = info.get("formats")
    if not isinstance(formats, list):
        return "", {}

    candidates: list[tuple[tuple[float, float, float], str, dict[str, str]]] = []
    for position, raw_format in enumerate(formats):
        if not isinstance(raw_format, dict) or raw_format.get("has_drm"):
            continue
        protocol = str(raw_format.get("protocol") or "").casefold()
        manifest_url = str(raw_format.get("url") or "").strip()
        if not protocol.startswith("m3u8") or not manifest_url.startswith(
            ("http://", "https://")
        ):
            continue
        raw_headers = raw_format.get("http_headers")
        headers = (
            {
                str(key): str(value)
                for key, value in raw_headers.items()
                if value is not None
            }
            if isinstance(raw_headers, dict)
            else {}
        )
        rank = (
            _safe_number(raw_format.get("height")),
            _safe_number(raw_format.get("tbr")),
            float(position),
        )
        candidates.append((rank, manifest_url, headers))

    if not candidates:
        return "", {}
    _rank, manifest_url, headers = max(candidates, key=lambda candidate: candidate[0])
    return manifest_url, headers


def parse_youtube_hls_live_edge(manifest: str) -> YouTubeLiveEdge:
    media_sequence: int | None = None
    newest_segment_at: datetime | None = None
    next_segment_at: datetime | None = None
    pending_duration: float | None = None
    has_endlist = False

    for raw_line in manifest.splitlines():
        line = raw_line.strip()
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_sequence = int(line.partition(":")[2].strip())
            except ValueError:
                media_sequence = None
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            next_segment_at = parse_hls_datetime(line.partition(":")[2].strip())
            if next_segment_at is not None:
                newest_segment_at = next_segment_at
        elif line.startswith("#EXTINF:"):
            raw_duration = line.partition(":")[2].partition(",")[0].strip()
            try:
                pending_duration = max(0.0, float(raw_duration))
            except ValueError:
                pending_duration = None
        elif line == "#EXT-X-ENDLIST":
            has_endlist = True
        elif line and not line.startswith("#") and pending_duration is not None:
            if next_segment_at is not None:
                next_segment_at += timedelta(seconds=pending_duration)
                newest_segment_at = next_segment_at
            pending_duration = None

    return YouTubeLiveEdge(
        media_sequence=media_sequence,
        newest_segment_at=newest_segment_at,
        has_endlist=has_endlist,
    )


def parse_hls_datetime(value: str) -> datetime | None:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_number(value: object) -> float:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return number if number == number else 0.0


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
