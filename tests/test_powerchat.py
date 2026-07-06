from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from onlysavemevods.powerchat import (
    PowerchatRecorder,
    copy_powerchat_segment_sidecar,
    handle_powerchat_socket_message,
    load_powerchat_sidecar,
    normalize_powerchat_payload,
    powerchat_totals,
    powerchat_ws_url,
    write_powerchat_sidecar,
)


class PowerchatTests(unittest.TestCase):
    def test_plain_tts_gift_message_becomes_unit_total(self) -> None:
        payload = {
            "message": "KDrizzy69 just gifted 50 Kicks on Kick",
            "username": "toneirl",
            "isPlainMessage": True,
            "shouldPlayTTS": True,
            "customMessageFont": "tts-message-kick",
        }

        event = normalize_powerchat_payload(
            payload,
            source="tts",
            received_at="2026-07-05T10:00:30+00:00",
            stream_started_at="2026-07-05T10:00:00+00:00",
        )

        assert event is not None
        self.assertEqual(event["kind"], "unit")
        self.assertEqual(event["donor"], "KDrizzy69")
        self.assertEqual(event["platform"], "Kick")
        self.assertEqual(event["unit_amount"], 50.0)
        self.assertEqual(event["unit"], "Kicks")
        self.assertEqual(event["offset_seconds"], 30.0)
        self.assertEqual(powerchat_totals([event])["units"], [
            {"platform": "Kick", "unit": "Kicks", "amount": 50.0}
        ])
        self.assertEqual(powerchat_totals([event])["money"], [])

    def test_structured_fiat_payload_becomes_money_total(self) -> None:
        event = normalize_powerchat_payload(
            {
                "messageId": "donation-1",
                "message": "Great stream",
                "donator": "Alice",
                "amount": "5.50",
                "currency": "usd",
                "paymentPlatform": "Powerchat",
            },
            source="feed",
            received_at="2026-07-05T10:01:00+00:00",
        )

        assert event is not None
        self.assertEqual(event["kind"], "money")
        self.assertEqual(event["dedupe_key"], "id:donation-1")
        self.assertEqual(powerchat_totals([event]), {
            "money": [{"currency": "USD", "amount": 5.5}],
            "units": [],
        })

    def test_powerchat_paypal_payload_without_currency_defaults_to_usd(self) -> None:
        event = normalize_powerchat_payload(
            {
                "donator": "Anonymous",
                "message": "Can I get a Buffalo ayooo",
                "amount": 3,
                "paymentPlatform": "paypal",
                "id": 1499195,
                "createdAt": "2026-07-06T04:26:34.417Z",
                "username": "onlyusemeblade",
                "shouldPlayTTS": True,
            },
            source="feed",
            stream_started_at="2026-07-06T04:26:00Z",
        )

        assert event is not None
        self.assertEqual(event["kind"], "money")
        self.assertEqual(event["dedupe_key"], "id:1499195")
        self.assertEqual(event["donor"], "Anonymous")
        self.assertEqual(event["platform"], "PayPal")
        self.assertEqual(event["money_amount"], 3.0)
        self.assertEqual(event["money_currency"], "USD")
        self.assertEqual(event["offset_seconds"], 34.417)
        self.assertEqual(powerchat_totals([event]), {
            "money": [{"currency": "USD", "amount": 3.0}],
            "units": [],
        })

    def test_load_sidecar_repairs_legacy_unknown_structured_money_events(self) -> None:
        with TemporaryDirectory() as tmp:
            sidecar = Path(tmp) / "stream.powerchat-events.json"
            sidecar.write_text(
                json.dumps(
                    {
                        "event_count": 2,
                        "totals": {"money": [], "units": []},
                        "events": [
                            {
                                "id": "1498949",
                                "dedupe_key": "id:1498949",
                                "kind": "unknown",
                                "source": "feed",
                                "received_at": "2026-07-06T03:05:43+00:00",
                                "offset_seconds": 113.0,
                                "donor": "Anonymous",
                                "platform": "Square",
                                "message": "Great firework show last night",
                                "money_amount": None,
                                "money_currency": "",
                                "unit_amount": None,
                                "unit": "",
                                "raw": {
                                    "id": 1498949,
                                    "donator": "Anonymous",
                                    "message": "Great firework show last night",
                                    "amount": 3,
                                    "paymentPlatform": "square",
                                    "createdAt": "2026-07-06T03:05:43.902Z",
                                },
                            },
                            {
                                "id": "1498981",
                                "dedupe_key": "id:1498981",
                                "kind": "unknown",
                                "source": "feed",
                                "received_at": "2026-07-06T03:14:30+00:00",
                                "offset_seconds": 640.0,
                                "donor": "Anonymous",
                                "platform": "Paypal",
                                "message": "",
                                "money_amount": None,
                                "money_currency": "",
                                "unit_amount": None,
                                "unit": "",
                                "raw": {
                                    "id": 1498981,
                                    "donator": "Anonymous",
                                    "message": "",
                                    "amount": 21,
                                    "paymentPlatform": "paypal",
                                    "createdAt": "2026-07-06T03:14:30.000Z",
                                },
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            payload = load_powerchat_sidecar(sidecar)

        self.assertEqual(payload["event_count"], 2)
        self.assertEqual(payload["totals"], {
            "money": [{"currency": "USD", "amount": 24.0}],
            "units": [],
        })
        self.assertEqual([event["kind"] for event in payload["events"]], ["money", "money"])
        self.assertEqual(payload["events"][0]["platform"], "Square")
        self.assertEqual(payload["events"][1]["platform"], "PayPal")
        self.assertEqual(payload["events"][0]["offset_seconds"], 113.0)

    def test_recorder_dedupes_feed_and_tts_duplicates(self) -> None:
        with TemporaryDirectory() as tmp:
            sidecar = Path(tmp) / "segment-001.powerchat-events.json"
            recorder = PowerchatRecorder(
                sidecar_path=sidecar,
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
                stream_started_at="2026-07-05T10:00:00+00:00",
            )
            payload = {"message": "KDrizzy69 just gifted 50 Kicks on Kick"}

            first = recorder.record_payload(
                payload,
                source="tts",
                received_at="2026-07-05T10:00:30+00:00",
            )
            duplicate = recorder.record_payload(
                payload,
                source="feed",
                received_at="2026-07-05T10:00:35+00:00",
            )
            later = recorder.record_payload(
                payload,
                source="feed",
                received_at="2026-07-05T10:02:00+00:00",
            )
            payload_on_disk = load_powerchat_sidecar(sidecar)

        self.assertTrue(first)
        self.assertFalse(duplicate)
        self.assertTrue(later)
        self.assertEqual(payload_on_disk["event_count"], 2)
        self.assertEqual(payload_on_disk["totals"]["units"][0]["amount"], 100.0)

    def test_socket_message_preserves_unknown_raw_payload(self) -> None:
        with TemporaryDirectory() as tmp:
            recorder = PowerchatRecorder(
                sidecar_path=Path(tmp) / "segment-001.powerchat-events.json",
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )

            recorded = handle_powerchat_socket_message(
                json.dumps({"message": "plain support alert", "username": "Bob"}),
                source="tts",
                recorder=recorder,
            )

        self.assertTrue(recorded)
        self.assertEqual(recorder.events[0]["kind"], "unknown")
        self.assertEqual(recorder.events[0]["raw"]["username"], "Bob")

    def test_copy_segment_sidecar_to_media_sidecar(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "segment-001.powerchat-events.json"
            media = root / "Stream Title [kick_oumb].mp4"
            media.write_text("media", encoding="utf-8")
            event = normalize_powerchat_payload(
                "KDrizzy69 just gifted 50 Kicks on Kick",
                source="tts",
                received_at="2026-07-05T10:00:30+00:00",
            )
            assert event is not None
            write_powerchat_sidecar(
                source,
                events=[event],
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )

            target = copy_powerchat_segment_sidecar(
                source,
                media,
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )
            payload = load_powerchat_sidecar(target or Path())

        self.assertIsNotNone(target)
        assert target is not None
        self.assertEqual(target.name, "Stream Title [kick_oumb].powerchat-events.json")
        self.assertFalse(source.exists())
        self.assertEqual(payload["event_count"], 1)

    def test_powerchat_ws_url_normalizes_username(self) -> None:
        self.assertEqual(
            powerchat_ws_url(" OUMB ", suffix="_feed"),
            "wss://powerchat.live/oumb_feed",
        )


if __name__ == "__main__":
    unittest.main()
