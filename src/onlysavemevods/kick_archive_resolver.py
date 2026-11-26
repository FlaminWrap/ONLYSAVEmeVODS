"""Match a Kick live broadcast to its growing replay and pin its rendition."""

from __future__ import annotations

import re
import time
from urllib.parse import urlsplit
from uuid import UUID

from .models import unqualified_stream_id
from .sources import resolve_source
from .youtube import YtDlpError, YtDlpRunner


def _replay_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        canonical = str(UUID(value))
    except ValueError:
        return None
    return canonical if canonical == value.lower() else None


class KickArchiveResolver:
    def __init__(
        self,
        source: str,
        video_id: str,
        yt_dlp_path: str = "yt-dlp",
        *,
        started_at: float | None = None,
        replay_id: str | None = None,
        format_id: str | None = None,
    ):
        spec = resolve_source(source, default_platform="kick")
        url = urlsplit(spec.url)
        channel = url.path.strip("/").split("/")[0].lower()
        if (
            spec.platform != "kick"
            or url.hostname not in {"kick.com", "www.kick.com"}
            or not re.fullmatch(r"[\w-]+", channel, re.ASCII)
        ):
            raise ValueError("A Kick channel is required to resolve its replay")
        self.channel_url = f"https://kick.com/{channel}"
        self.live_id = unqualified_stream_id(video_id, "kick")
        if replay_id is not None and not _replay_uuid(replay_id):
            raise ValueError("Invalid saved Kick replay ID")
        self.replay_id = replay_id or _replay_uuid(self.live_id)
        self.started_at = started_at
        self.format_id = format_id
        self.expected_duration: float | None = None
        self.http_headers: dict[str, str] = {}
        self.runner = YtDlpRunner(yt_dlp_path)
        self._cached: tuple[str, bool] | None = None
        self._refresh_after = 0.0

    def invalidate(self) -> None:
        self._cached = None
        self._refresh_after = 0.0

    def _metadata(self, url: str, *, listing: bool = False) -> dict:
        flags = ["--flat-playlist"] if listing else ["--no-playlist"]
        return self.runner.run_json(
            [
                "--dump-single-json",
                "--skip-download",
                "--no-warnings",
                "--socket-timeout",
                "12",
                *flags,
                url,
            ],
            timeout=50,
        )

    def _resolve(self) -> tuple[str, bool] | None:
        if self.started_at is None and self.replay_id is None:
            live = self._metadata(self.channel_url)
            # Kick's numeric live ID and its replay UUID are different. Both
            # endpoints expose the same broadcast start_time. created_at may differ.
            if str(live.get("id")) != self.live_id or not live.get("release_timestamp"):
                return None
            self.started_at = live["release_timestamp"]

        listing = self._metadata(self.channel_url + "/videos", listing=True)
        entries = [
            item for item in listing.get("entries") or [] if isinstance(item, dict)
        ]
        if self.replay_id is None:
            matches = [
                item
                for item in entries
                if item.get("timestamp") == self.started_at
                and _replay_uuid(item.get("id"))
            ]
            if len(matches) != 1:
                return None
            self.replay_id = matches[0]["id"]
        entry = next((item for item in entries if item.get("id") == self.replay_id), {})
        replay = self._metadata(f"{self.channel_url}/videos/{self.replay_id}")
        if replay.get("id") != self.replay_id:
            raise ValueError("Kick returned a different replay")
        for metadata in (entry, replay):
            stamp = metadata.get("timestamp")
            if stamp and self.started_at and stamp != self.started_at:
                raise ValueError(
                    "Kick replay start time does not match the recorded broadcast"
                )
            if stamp and self.started_at is None:
                self.started_at = stamp

        # Preserve the selected source rendition across refreshes and restarts.
        # A quality disappearing temporarily must not splice two encodings.
        wanted = self.format_id or replay.get("format_id")
        fmt = next(
            (
                item
                for item in replay.get("formats") or []
                if wanted and item.get("format_id") == wanted
            ),
            None,
        )
        if (
            not fmt
            or fmt.get("protocol") not in {"m3u8_native", "m3u8"}
            or fmt.get("vcodec") == "none"
            or fmt.get("acodec") == "none"
            or not str(fmt.get("url", "")).startswith("https://")
        ):
            raise ValueError("Waiting for the selected combined Kick replay rendition")
        self.format_id = wanted
        self.http_headers = {
            key: value
            for key, value in (fmt.get("http_headers") or {}).items()
            if key.lower() in {"user-agent", "referer", "origin", "accept"}
        }
        # An ongoing Kick replay can carry EXT-X-ENDLIST in every snapshot.
        # Require explicit provider completion for this UUID before draining the tail.
        duration = entry.get("duration") or replay.get("duration")
        ended = entry.get("is_live") is False or replay.get("live_status") == "was_live"
        still_live = replay.get("is_live") is True or entry.get("is_live") is True
        self.expected_duration = (
            float(duration) if ended and not still_live and duration else None
        )
        return fmt["url"], bool(self.expected_duration and self.expected_duration > 0)

    def resolve(self) -> tuple[str, bool] | None:
        now = time.monotonic()
        if now >= self._refresh_after:
            self._cached = None
            self._refresh_after = now + 20.0
            try:
                self._cached = self._resolve()
            except YtDlpError:
                # yt-dlp errors can contain signed CDN URLs. Keep them out of
                # persisted progress and the public dashboard.
                raise YtDlpError(
                    "Kick replay metadata is unavailable; retrying"
                ) from None
        return self._cached
