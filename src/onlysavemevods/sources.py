from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlsplit
import re
import logging

from .models import LiveStream, qualified_stream_id
from .youtube import YoutubeProbe, YtDlpError, YtDlpRunner, live_stream_from_info


LOGGER = logging.getLogger(__name__)
SUPPORTED_PLATFORMS = {"youtube", "twitch", "kick", "rumble"}
PREFIX_RE = re.compile(r"^(?P<platform>[A-Za-z][A-Za-z0-9_-]*):(?P<value>.+)$")


class SourceError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SourceSpec:
    raw: str
    platform: str
    url: str
    display_name: str


class SourceMonitor:
    def __init__(
        self,
        runner: YtDlpRunner | None = None,
        *,
        channel_scan_limit: int = 10,
        discovery_probe_concurrency: int = 4,
    ) -> None:
        self.runner = runner or YtDlpRunner()
        self.youtube = YoutubeProbe(
            self.runner,
            channel_scan_limit=channel_scan_limit,
            discovery_probe_concurrency=discovery_probe_concurrency,
        )

    def discover_live_streams(
        self,
        source: str,
        *,
        skip_video_ids: set[str] | None = None,
    ) -> list[LiveStream]:
        spec = resolve_source(source)
        if spec.platform == "youtube":
            streams = self.youtube.discover_channel_live_streams(
                spec.url,
                skip_video_ids=skip_video_ids,
            )
            return [stream_with_source(stream, spec.raw) for stream in streams]

        try:
            stream = self.probe_video(spec.url, source=spec.raw, platform=spec.platform)
        except YtDlpError as exc:
            LOGGER.debug("Direct source probe failed source=%s: %s", spec.raw, exc)
            return self.discover_playlist_live_streams(
                spec,
                skip_video_ids=skip_video_ids,
            )
        return [stream] if stream.is_live else []

    def discover_playlist_live_streams(
        self,
        spec: SourceSpec,
        *,
        skip_video_ids: set[str] | None = None,
    ) -> list[LiveStream]:
        try:
            playlist = self.runner.run_json(
                [
                    "--dump-single-json",
                    "--flat-playlist",
                    "--playlist-end",
                    str(self.youtube.channel_scan_limit),
                    "--skip-download",
                    "--no-warnings",
                    spec.url,
                ]
            )
        except YtDlpError as exc:
            LOGGER.debug("Playlist source probe failed source=%s: %s", spec.raw, exc)
            return []

        live_streams: list[LiveStream] = []
        seen = set(skip_video_ids or ())
        for candidate in playlist_candidate_urls(playlist, spec.url):
            if candidate == spec.url:
                continue
            try:
                stream = self.probe_video(candidate, source=spec.raw, platform=spec.platform)
            except YtDlpError as exc:
                LOGGER.debug(
                    "Playlist candidate probe failed source=%s candidate=%s: %s",
                    spec.raw,
                    candidate,
                    exc,
                )
                continue
            if stream.video_id in seen:
                continue
            seen.add(stream.video_id)
            if stream.is_live:
                live_streams.append(stream)
        return live_streams

    def probe_video(
        self,
        url_or_id: str,
        *,
        source: str = "",
        platform: str = "",
    ) -> LiveStream:
        spec = resolve_source(url_or_id, default_platform=platform or None)
        if spec.platform == "youtube":
            stream = self.youtube.probe_video(spec.url)
            return stream_with_source(stream, source or spec.raw)

        info = self.runner.run_json(
            [
                "--dump-json",
                "--skip-download",
                "--no-playlist",
                "--no-warnings",
                spec.url,
            ]
        )
        return live_stream_from_generic_info(
            info,
            platform=spec.platform,
            fallback_url=spec.url,
            source=source or spec.raw,
        )


def resolve_source(source: str, *, default_platform: str | None = None) -> SourceSpec:
    raw = source.strip()
    if not raw:
        raise SourceError("source cannot be empty")

    if raw.startswith(("http://", "https://")):
        return resolve_url_source(raw)

    match = PREFIX_RE.match(raw)
    if match:
        platform = match.group("platform").casefold().replace("_", "-")
        value = match.group("value").strip()
        if platform not in SUPPORTED_PLATFORMS:
            raise SourceError(f"unsupported source platform: {platform}")
        if not value:
            raise SourceError(f"{platform} source must not be empty")
        return prefixed_source(raw, platform, value)

    if default_platform:
        platform = default_platform.casefold()
        if platform not in SUPPORTED_PLATFORMS:
            raise SourceError(f"unsupported source platform: {platform}")
        return prefixed_source(raw, platform, raw)

    return SourceSpec(
        raw=raw,
        platform="youtube",
        url=raw,
        display_name=source_display_name(raw),
    )


def resolve_url_source(url: str) -> SourceSpec:
    parts = urlsplit(url)
    host = parts.netloc.casefold()
    if host.startswith("www."):
        host = host[4:]
    if host in {"youtube.com", "youtu.be", "m.youtube.com"} or host.endswith(
        ".youtube.com"
    ):
        return SourceSpec(url, "youtube", url, source_display_name(url))
    if host in {"twitch.tv", "m.twitch.tv", "go.twitch.tv"} or host.endswith(
        ".twitch.tv"
    ):
        return SourceSpec(url, "twitch", url, source_display_name(url))
    if host == "kick.com" or host.endswith(".kick.com"):
        return SourceSpec(url, "kick", url, source_display_name(url))
    if host == "rumble.com" or host.endswith(".rumble.com"):
        return SourceSpec(url, "rumble", url, source_display_name(url))
    raise SourceError(f"unsupported source URL host: {parts.netloc or url}")


def prefixed_source(raw: str, platform: str, value: str) -> SourceSpec:
    if value.startswith(("http://", "https://")):
        spec = resolve_url_source(value)
        if spec.platform != platform:
            raise SourceError(
                f"{platform} source points at {spec.platform} URL: {value}"
            )
        return SourceSpec(raw, platform, spec.url, spec.display_name)
    if platform == "youtube":
        url = value if value.startswith("@") else f"@{value.lstrip('@')}"
    elif platform == "twitch":
        url = f"https://www.twitch.tv/{value.strip('/')}"
    elif platform == "kick":
        url = f"https://kick.com/{value.strip('/')}"
    elif platform == "rumble":
        path = value.strip("/")
        if not path.startswith(("user/", "c/", "v")):
            path = f"user/{path}"
        url = f"https://rumble.com/{path}"
    else:
        raise SourceError(f"unsupported source platform: {platform}")
    return SourceSpec(raw, platform, url, source_display_name(value))


def validate_source(source: str) -> None:
    resolve_source(source)


def canonical_source(source: str, *, default_platform: str | None = None) -> str:
    spec = resolve_source(source, default_platform=default_platform)
    raw = source.strip()
    if not raw.startswith(("http://", "https://")):
        return spec.raw

    parts = urlsplit(spec.url)
    path = parts.path.strip("/")
    if spec.platform == "youtube":
        if path.startswith("@"):
            return path.split("/", 1)[0]
        return raw
    if spec.platform in {"twitch", "kick"}:
        channel = path.split("/", 1)[0]
        return f"{spec.platform}:{channel}" if channel else raw
    if spec.platform == "rumble":
        return f"rumble:{path}" if path else raw
    return raw


def playlist_candidate_urls(playlist: dict[str, Any], base_url: str) -> list[str]:
    entries = playlist.get("entries")
    if not isinstance(entries, list):
        return []
    urls: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        candidate = entry.get("webpage_url") or entry.get("url") or entry.get("id")
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        url = playlist_candidate_url(candidate.strip(), base_url)
        if url not in seen:
            urls.append(url)
            seen.add(url)
    return urls


def playlist_candidate_url(candidate: str, base_url: str) -> str:
    if candidate.startswith(("http://", "https://", "/")):
        return urljoin(base_url, candidate)
    parts = urlsplit(base_url)
    host = parts.netloc.casefold()
    if (host == "rumble.com" or host.endswith(".rumble.com")) and candidate.startswith("v"):
        return f"{parts.scheme}://{parts.netloc}/{candidate}"
    return urljoin(base_url.rstrip("/") + "/", candidate)


def stream_with_source(stream: LiveStream, source: str) -> LiveStream:
    return LiveStream(
        video_id=stream.video_id,
        url=stream.url,
        title=stream.title,
        channel=stream.channel,
        live_status=stream.live_status,
        is_live=stream.is_live,
        platform=stream.platform,
        source=source,
        raw=stream.raw,
    )


def live_stream_from_generic_info(
    info: dict[str, Any],
    *,
    platform: str,
    fallback_url: str,
    source: str = "",
) -> LiveStream:
    if platform in {"twitch", "kick"}:
        raw_id = str(
            source_display_name(source or fallback_url)
            or info.get("uploader_id")
            or info.get("channel_id")
            or info.get("id")
        ).strip()
    else:
        raw_id = str(
            info.get("id")
            or info.get("display_id")
            or info.get("channel_id")
            or source_display_name(fallback_url)
        ).strip()
    if not raw_id:
        raise YtDlpError("yt-dlp metadata did not include a usable stream id")
    live_status = str(info.get("live_status") or "")
    is_live = bool(info.get("is_live")) or live_status == "is_live"
    return LiveStream(
        video_id=qualified_stream_id(platform, raw_id),
        url=str(info.get("webpage_url") or fallback_url),
        title=str(info.get("title") or ""),
        channel=str(
            info.get("channel")
            or info.get("uploader")
            or info.get("uploader_id")
            or info.get("channel_id")
            or source_display_name(source or fallback_url)
        ),
        live_status=live_status,
        is_live=is_live,
        platform=platform,
        source=source or fallback_url,
        raw=info,
    )


def source_display_name(source: str) -> str:
    value = source.strip().rstrip("/")
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        parts = urlsplit(value)
        path = parts.path.strip("/")
        if path:
            return path.rsplit("/", 1)[-1]
        return parts.netloc
    if ":" in value and not value.startswith("@"):
        value = value.split(":", 1)[1]
    return value.strip("/") or source.strip()
