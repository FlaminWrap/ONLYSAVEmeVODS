from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, unquote, urlsplit
import json
import unittest

from onlysavemevods.kick_chat import (
    KickChatReplayError,
    download_kick_vod_chat_replay,
    fetch_kick_chat_history,
    kick_vod_chat_metadata,
)
from onlysavemevods.models import LiveStream


class KickChatReplayTests(unittest.TestCase):
    def test_metadata_uses_kick_vod_raw_fields(self) -> None:
        stream = LiveStream(
            video_id="kick:Hungover 2026-07-05 06:18",
            url="https://kick.com/oumb/videos/1ffadbc3-f208-4ec3-8207-95fbe499851c",
            title="Hungover",
            channel="OUMB3rd",
            platform="kick",
            raw={
                "id": "1ffadbc3-f208-4ec3-8207-95fbe499851c",
                "channel_id": 1062937,
                "timestamp": 1783217906,
                "duration": 10881,
            },
        )

        metadata = kick_vod_chat_metadata(stream)

        self.assertEqual(metadata.vod_uuid, "1ffadbc3-f208-4ec3-8207-95fbe499851c")
        self.assertEqual(metadata.channel_id, "1062937")
        self.assertEqual(metadata.stream_start, datetime.fromtimestamp(1783217906, tz=timezone.utc))
        self.assertEqual(metadata.duration_seconds, 10881)

    def test_history_paginates_offsets_and_dedupes_messages(self) -> None:
        metadata = kick_vod_chat_metadata(
            LiveStream(
                video_id="kick:vod",
                url="https://kick.com/oumb/videos/voduuid",
                title="Kick VOD",
                platform="kick",
                raw={
                    "id": "voduuid",
                    "channel_id": "channel-1",
                    "timestamp": "2026-07-05T02:18:22Z",
                    "duration": 12,
                },
            )
        )
        calls: list[str] = []

        def requester(url: str) -> dict[str, object]:
            calls.append(url)
            if "02%3A18%3A22" in url:
                return {
                    "data": {
                        "messages": [
                            {
                                "id": "m1",
                                "created_at": "2026-07-05T02:18:24Z",
                                "content": "hello",
                                "sender": {"username": "Alice"},
                            }
                        ]
                    }
                }
            if "02%3A18%3A27" in url:
                return {
                    "data": {
                        "messages": [
                            {
                                "id": "m1",
                                "created_at": "2026-07-05T02:18:24Z",
                                "content": "hello",
                                "sender": {"username": "Alice"},
                            },
                            {
                                "message_id": "m2",
                                "created_at": "2026-07-05T02:18:30Z",
                                "content": "second",
                                "sender": {"username": "Bob"},
                            },
                        ]
                    }
                }
            return {"data": {"messages": []}}

        messages = fetch_kick_chat_history(
            metadata,
            requester=requester,
            sleep_seconds=0,
        )

        self.assertGreaterEqual(len(calls), 3)
        self.assertEqual([message["id"] for message in messages], ["m1", "m2"])
        self.assertEqual(messages[0]["offset_ms"], 2000)
        self.assertEqual(messages[1]["offset_ms"], 8000)

    def test_history_continues_after_long_quiet_gap(self) -> None:
        metadata = kick_vod_chat_metadata(
            LiveStream(
                video_id="kick:vod",
                url="https://kick.com/oumb/videos/voduuid",
                title="Kick VOD",
                platform="kick",
                raw={
                    "id": "voduuid",
                    "channel_id": "channel-1",
                    "timestamp": "2026-07-05T02:18:22Z",
                    "duration": 80,
                },
            )
        )

        def requester(url: str) -> dict[str, object]:
            start_time = unquote(parse_qs(urlsplit(url).query)["start_time"][0])
            if start_time == "2026-07-05T02:18:22Z":
                return {
                    "data": {
                        "messages": [
                            {
                                "id": "m1",
                                "created_at": "2026-07-05T02:18:24Z",
                                "content": "first",
                                "sender": {"username": "Alice"},
                            }
                        ]
                    }
                }
            if start_time == "2026-07-05T02:19:32Z":
                return {
                    "data": {
                        "messages": [
                            {
                                "id": "m2",
                                "created_at": "2026-07-05T02:19:35Z",
                                "content": "after quiet gap",
                                "sender": {"username": "Bob"},
                            }
                        ]
                    }
                }
            return {"data": {"messages": []}}

        messages = fetch_kick_chat_history(
            metadata,
            requester=requester,
            sleep_seconds=0,
        )

        self.assertEqual([message["id"] for message in messages], ["m1", "m2"])
        self.assertEqual(messages[1]["offset_ms"], 73_000)

    def test_download_writes_normalized_sidecar(self) -> None:
        with TemporaryDirectory() as tmp:
            output_template = Path(tmp) / "Hungover [kick].%(ext)s"
            stream = LiveStream(
                video_id="kick:vod",
                url="https://kick.com/oumb/videos/voduuid",
                title="Kick VOD",
                platform="kick",
                source="kick:oumb",
                raw={
                    "id": "voduuid",
                    "channel_id": "channel-1",
                    "timestamp": "2026-07-05T02:18:22Z",
                    "duration": 5,
                },
            )

            def requester(_url: str) -> dict[str, object]:
                return {
                    "data": {
                        "messages": [
                            {
                                "id": "m1",
                                "created_at": "2026-07-05T02:18:23Z",
                                "content": "replay [emote:4148074:HYPERCLAP] chat",
                                "sender": {"username": "Alice"},
                            }
                        ]
                    }
                }

            result = download_kick_vod_chat_replay(
                stream,
                output_template,
                requester=requester,
                sleep_seconds=0,
            )

            chat_file = Path(tmp) / "Hungover [kick].live_chat.json"
            payload = json.loads(chat_file.read_text(encoding="utf-8"))

        self.assertTrue(result.ok, result.message)
        self.assertEqual(result.messages, 1)
        self.assertEqual(payload["platform"], "kick")
        self.assertEqual(payload["source"], "kick:oumb")
        self.assertEqual(payload["messages"][0]["message"], "replay [emote:4148074:HYPERCLAP] chat")
        self.assertEqual(payload["messages"][0]["offset_ms"], 1000)
        self.assertEqual(
            payload["messages"][0]["emotes"],
            [
                {
                    "id": "4148074",
                    "name": "HYPERCLAP",
                    "image_url": "https://files.kick.com/emotes/4148074/fullsize",
                }
            ],
        )

    def test_missing_history_reports_unavailable(self) -> None:
        metadata = kick_vod_chat_metadata(
            LiveStream(
                video_id="kick:vod",
                url="https://kick.com/oumb/videos/voduuid",
                title="Kick VOD",
                platform="kick",
                raw={
                    "id": "voduuid",
                    "channel_id": "channel-1",
                    "timestamp": "2026-07-05T02:18:22Z",
                    "duration": 5,
                },
            )
        )

        with self.assertRaises(KickChatReplayError):
            fetch_kick_chat_history(
                metadata,
                requester=lambda _url: {"data": {"messages": []}},
                sleep_seconds=0,
            )


if __name__ == "__main__":
    unittest.main()
