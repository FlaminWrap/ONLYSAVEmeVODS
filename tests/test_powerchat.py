from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from onlysavemevods.powerchat import (
    PowerchatRecorder,
    copy_powerchat_segment_sidecar,
    handle_powerchat_socket_message,
    load_powerchat_sidecar,
    merge_powerchat_events,
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

    def test_test_payment_platforms_are_recorded_but_not_counted(self) -> None:
        for payment_platform in ("test", " TEST ", "powerchat-test", "POWERCHAT-TEST"):
            with self.subTest(payment_platform=payment_platform):
                event = normalize_powerchat_payload(
                    {
                        "messageId": f"test-{payment_platform.strip().lower()}",
                        "message": "Test donation alert",
                        "donator": "Test User",
                        "amount": 50,
                        "currency": "gbp",
                        "paymentPlatform": payment_platform,
                    },
                    source="feed",
                    received_at="2026-07-05T10:01:00+00:00",
                )

                assert event is not None
                self.assertEqual(event["kind"], "money")
                self.assertEqual(event["money_amount"], 50.0)
                self.assertEqual(event["money_currency"], "GBP")
                self.assertTrue(event["is_test"])
                self.assertEqual(
                    powerchat_totals([event]),
                    {"money": [], "units": []},
                )

    def test_test_like_payload_fields_do_not_exclude_real_donations(self) -> None:
        cases = [
            (
                "test text and amount",
                {
                    "message": "This is a test of the alerts",
                    "donator": "Test User",
                    "amount": 50,
                    "currency": "USD",
                    "paymentPlatform": "Powerchat",
                },
                "feed",
            ),
            (
                "replay flag",
                {
                    "message": "Replayed real donation",
                    "donator": "Alice",
                    "amount": 50,
                    "currency": "USD",
                    "paymentPlatform": "square",
                    "isReplay": True,
                },
                "feed",
            ),
            (
                "tts transport",
                {
                    "message": "Real TTS donation",
                    "donator": "Bob",
                    "amount": 50,
                    "currency": "USD",
                    "paymentPlatform": "paypal",
                },
                "tts",
            ),
        ]

        for label, payload, source in cases:
            with self.subTest(label=label):
                event = normalize_powerchat_payload(
                    payload,
                    source=source,
                    received_at="2026-07-05T10:01:00+00:00",
                )

                assert event is not None
                self.assertFalse(event["is_test"])
                self.assertEqual(
                    powerchat_totals([event]),
                    {
                        "money": [{"currency": "USD", "amount": 50.0}],
                        "units": [],
                    },
                )

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

    def test_sidecar_records_test_events_but_excludes_them_from_totals(self) -> None:
        with TemporaryDirectory() as tmp:
            sidecar = Path(tmp) / "stream.powerchat-events.json"
            real_event = normalize_powerchat_payload(
                {
                    "messageId": "real-1",
                    "message": "Real donation",
                    "donator": "Alice",
                    "amount": "5.50",
                    "currency": "usd",
                    "paymentPlatform": "square",
                },
                source="feed",
                received_at="2026-07-05T10:01:00+00:00",
            )
            test_event = normalize_powerchat_payload(
                {
                    "messageId": "test-1",
                    "message": "Test donation",
                    "donator": "Test User",
                    "amount": 50,
                    "currency": "gbp",
                    "paymentPlatform": "test",
                },
                source="feed",
                received_at="2026-07-05T10:02:00+00:00",
            )
            assert real_event is not None and test_event is not None

            write_powerchat_sidecar(
                sidecar,
                events=[real_event, test_event],
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )
            payload = load_powerchat_sidecar(sidecar)

        self.assertEqual(payload["event_count"], 2)
        self.assertEqual(payload["counted_event_count"], 1)
        self.assertEqual(payload["test_event_count"], 1)
        self.assertEqual(
            payload["totals"],
            {
                "money": [{"currency": "USD", "amount": 5.5}],
                "units": [],
            },
        )
        self.assertEqual(
            [event["is_test"] for event in payload["events"]],
            [False, True],
        )
        self.assertEqual(
            payload["events"][1]["raw"]["paymentPlatform"],
            "test",
        )

    def test_load_sidecar_repairs_legacy_test_event_and_stale_totals(self) -> None:
        with TemporaryDirectory() as tmp:
            sidecar = Path(tmp) / "stream.powerchat-events.json"
            sidecar.write_text(
                json.dumps(
                    {
                        "event_count": 1,
                        "totals": {
                            "money": [{"currency": "USD", "amount": 50.0}],
                            "units": [],
                        },
                        "events": [
                            {
                                "id": "legacy-test-1",
                                "dedupe_key": "id:legacy-test-1",
                                "kind": "money",
                                "source": "feed",
                                "received_at": "2026-07-05T10:01:00+00:00",
                                "offset_seconds": 60.0,
                                "donor": "Test User",
                                "platform": "Powerchat",
                                "message": "Legacy test alert",
                                "money_amount": 50.0,
                                "money_currency": "USD",
                                "unit_amount": None,
                                "unit": "",
                                "raw": {
                                    "id": "legacy-test-1",
                                    "message": "Legacy test alert",
                                    "donator": "Test User",
                                    "amount": 50,
                                    "paymentPlatform": "powerchat-test",
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            payload = load_powerchat_sidecar(sidecar)

        self.assertEqual(payload["event_count"], 1)
        self.assertEqual(payload["counted_event_count"], 0)
        self.assertEqual(payload["test_event_count"], 1)
        self.assertEqual(payload["totals"], {"money": [], "units": []})
        self.assertTrue(payload["events"][0]["is_test"])

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

    def test_recorder_preserves_identical_gift_multiplicity_within_window(self) -> None:
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
            payload = {"message": "Alice just gifted 5 Kicks on Kick"}

            first_tts = recorder.record_payload(
                payload,
                source="tts",
                received_at="2026-07-05T10:00:10+00:00",
            )
            first_feed = recorder.record_payload(
                payload,
                source="feed",
                received_at="2026-07-05T10:00:11+00:00",
            )

            # Rebuild from disk to prove the one-to-one pairing state survives
            # a service restart between two otherwise identical real gifts.
            recorder = PowerchatRecorder(
                sidecar_path=sidecar,
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
                stream_started_at="2026-07-05T10:00:00+00:00",
            )
            second_tts = recorder.record_payload(
                payload,
                source="tts",
                received_at="2026-07-05T10:00:20+00:00",
            )
            second_feed = recorder.record_payload(
                payload,
                source="feed",
                received_at="2026-07-05T10:00:21+00:00",
            )
            payload_on_disk = load_powerchat_sidecar(sidecar)

        self.assertTrue(first_tts)
        self.assertFalse(first_feed)
        self.assertTrue(second_tts)
        self.assertFalse(second_feed)
        self.assertEqual(payload_on_disk["event_count"], 2)
        self.assertEqual(payload_on_disk["totals"]["units"][0]["amount"], 10.0)
        self.assertEqual(
            [event["observed_sources"] for event in payload_on_disk["events"]],
            [["tts", "feed"], ["tts", "feed"]],
        )

    def test_duplicate_test_marker_updates_the_recorded_event(self) -> None:
        with TemporaryDirectory() as tmp:
            sidecar = Path(tmp) / "segment-001.powerchat-events.json"
            recorder = PowerchatRecorder(
                sidecar_path=sidecar,
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )
            base_payload = {
                "messageId": "shared-event-1",
                "message": "Test donation",
                "donator": "Test User",
                "amount": 50,
                "currency": "USD",
            }

            first = recorder.record_payload(
                {**base_payload, "paymentPlatform": "Powerchat"},
                source="tts",
                received_at="2026-07-05T10:01:00+00:00",
            )
            duplicate = recorder.record_payload(
                {**base_payload, "paymentPlatform": "test"},
                source="feed",
                received_at="2026-07-05T10:01:01+00:00",
            )
            payload = load_powerchat_sidecar(sidecar)

        self.assertTrue(first)
        self.assertFalse(duplicate)
        self.assertEqual(payload["event_count"], 1)
        self.assertEqual(payload["counted_event_count"], 0)
        self.assertEqual(payload["test_event_count"], 1)
        self.assertEqual(payload["totals"], {"money": [], "units": []})
        self.assertTrue(payload["events"][0]["is_test"])

    def test_sidecar_merge_preserves_a_later_test_marker(self) -> None:
        base_payload = {
            "messageId": "merged-event-1",
            "message": "Test donation",
            "donator": "Test User",
            "amount": 50,
            "currency": "USD",
        }
        first = normalize_powerchat_payload(
            {**base_payload, "paymentPlatform": "Powerchat"},
            source="tts",
            received_at="2026-07-05T10:01:00+00:00",
        )
        second = normalize_powerchat_payload(
            {**base_payload, "paymentPlatform": "powerchat-test"},
            source="feed",
            received_at="2026-07-05T10:01:01+00:00",
        )
        assert first is not None and second is not None

        merged = merge_powerchat_events([first], [second])

        self.assertEqual(len(merged), 1)
        self.assertTrue(merged[0]["is_test"])
        self.assertEqual(powerchat_totals(merged), {"money": [], "units": []})

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

    def test_copy_preserves_malformed_sidecar_for_recovery(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "segment-001.powerchat-events.json"
            media = root / "Stream Title [kick_oumb].mp4"
            source.write_text("{not valid json", encoding="utf-8")
            media.write_text("media", encoding="utf-8")

            target = copy_powerchat_segment_sidecar(source, media)
            source_exists = source.exists()

        self.assertIsNone(target)
        self.assertTrue(source_exists)

    def test_recorder_preserves_malformed_sidecar_and_persists_new_events(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            sidecar = root / "segment-001.powerchat-events.json"
            malformed = b"{not valid json"
            sidecar.write_bytes(malformed)
            recorder = PowerchatRecorder(
                sidecar_path=sidecar,
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )

            recorded = recorder.record_payload(
                "Alice just gifted 5 Kicks on Kick",
                source="tts",
            )
            payload = load_powerchat_sidecar(sidecar)
            recovery_files = list((root / ".powerchat-recovery").iterdir())
            recovered_payload = recovery_files[0].read_bytes()

        self.assertTrue(recorded)
        self.assertEqual(payload["event_count"], 1)
        self.assertEqual(len(recovery_files), 1)
        self.assertEqual(recovered_payload, malformed)

    def test_copy_preserves_sidecar_without_events_list(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "segment-001.powerchat-events.json"
            media = root / "Stream Title [kick_oumb].mp4"
            source.write_text('{"event_count": 0}', encoding="utf-8")
            media.write_text("media", encoding="utf-8")

            target = copy_powerchat_segment_sidecar(source, media)
            source_exists = source.exists()

        self.assertIsNone(target)
        self.assertTrue(source_exists)

    def test_copy_merges_existing_target_and_deduplicates_events(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "segment-001.powerchat-events.json"
            media = root / "Stream Title [kick_oumb].mp4"
            target = root / "Stream Title [kick_oumb].powerchat-events.json"
            media.write_text("media", encoding="utf-8")
            first = normalize_powerchat_payload(
                "Alice just gifted 5 Kicks on Kick",
                source="tts",
                received_at="2026-07-05T10:00:30+00:00",
            )
            second = normalize_powerchat_payload(
                "Bob just gifted 10 Kicks on Kick",
                source="tts",
                received_at="2026-07-05T10:01:30+00:00",
            )
            assert first is not None and second is not None
            write_powerchat_sidecar(
                target,
                events=[first],
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )
            write_powerchat_sidecar(
                source,
                events=[first, second],
                streamer_name="OUMB3rd",
                username="oumb",
                video_id="kick:oumb",
                segment_index=1,
            )

            copied = copy_powerchat_segment_sidecar(source, media)
            payload = load_powerchat_sidecar(target)
            source_exists = source.exists()

        self.assertEqual(copied, target)
        self.assertFalse(source_exists)
        self.assertEqual(payload["event_count"], 2)
        self.assertEqual(
            [event["donor"] for event in payload["events"]],
            ["Alice", "Bob"],
        )

    def test_powerchat_ws_url_normalizes_username(self) -> None:
        self.assertEqual(
            powerchat_ws_url(" OUMB ", suffix="_feed"),
            "wss://powerchat.live/oumb_feed",
        )


if __name__ == "__main__":
    unittest.main()
