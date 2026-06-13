from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest

from onlysavemevods.chat_render import chat_video_output_file
from onlysavemevods.config import BotConfig
from onlysavemevods.shot_audit import (
    ConsumedShotEvent,
    DonationEvent,
    HIGH_CONFIDENCE,
    LOW_CONFIDENCE,
    MEDIUM_CONFIDENCE,
    MotionSample,
    add_manual_visible_shot,
    add_media_to_project,
    compute_totals,
    create_project,
    default_shot_rules,
    delete_project,
    dedupe_consumed,
    dedupe_donations,
    estimate_consumed_shots_from_transcript,
    extract_paddle_ocr_text,
    frame_offsets_for_ranges,
    load_project,
    maybe_run_auto_shot_audit,
    parse_ocr_donation_text,
    parse_shot_rules_toml,
    render_markdown_report,
    run_shot_audit,
    visual_consumed_events_from_samples,
    visual_review_ranges,
    yolo_pose_device,
)


class ShotAuditTests(unittest.TestCase):
    def test_parse_ocr_donation_text_applies_default_rules(self) -> None:
        events = parse_ocr_donation_text(
            "Alice sent $21.00 LITTY AGAIN",
            42.0,
            default_shot_rules(),
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].amount, 21.0)
        self.assertEqual(events[0].currency, "USD")
        self.assertEqual(events[0].owed_shots, 2)
        self.assertEqual(events[0].username, "Alice")
        self.assertEqual(events[0].confidence, HIGH_CONFIDENCE)

    def test_dedupe_donations_merges_repeated_alert_frames(self) -> None:
        rules = default_shot_rules()
        first = parse_ocr_donation_text("Alice sent $21", 10.0, rules)[0]
        second = parse_ocr_donation_text("Alice sent $21.00 thanks", 13.0, rules)[0]

        deduped = dedupe_donations([first, second])

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].owed_shots, 2)
        self.assertIn("thanks", deduped[0].raw_text)

    def test_parse_shot_rules_toml_accepts_exact_and_range_rules(self) -> None:
        rules = parse_shot_rules_toml(
            """
            [[shot_rules]]
            amount = 10
            shots = 1
            label = "ten"

            [[shot_rules]]
            amount_min = 20
            amount_max = 25
            shots = 2
            label = "range"
            """
        )

        self.assertEqual(len(rules), 2)
        self.assertTrue(rules[0].matches(10.0, "USD"))
        self.assertFalse(rules[0].matches(10.5, "USD"))
        self.assertTrue(rules[1].matches(21.0, "USD"))

    def test_compute_totals_counts_high_confidence_and_manual_only(self) -> None:
        donations = [
            DonationEvent("d1", 1.0, 21.0, "USD", 2, "$21"),
            DonationEvent("d2", 2.0, 5.0, "USD", 1, "$5"),
        ]
        consumed = [
            ConsumedShotEvent("s1", 100.0, 1, HIGH_CONFIDENCE, "transcript", "cheers"),
            ConsumedShotEvent("s2", 120.0, 1, LOW_CONFIDENCE, "transcript", "jager"),
            ConsumedShotEvent("s3", 130.0, 1, "manual", "manual", "manual mark"),
        ]

        totals = compute_totals(donations, consumed)

        self.assertEqual(totals.owed_shots, 3)
        self.assertEqual(totals.machine_high_confidence_shots, 1)
        self.assertEqual(totals.manual_shots, 1)
        self.assertEqual(totals.counted_consumed_shots, 2)
        self.assertEqual(totals.unconfirmed_owed_shots, 1)

    def test_visual_motion_candidates_use_pending_owed_shot_count(self) -> None:
        donations = [
            DonationEvent("d1", 10.0, 21.0, "USD", 2, "$21 double"),
            DonationEvent("d2", 40.0, 5.0, "USD", 1, "$5 shot"),
        ]
        samples = [
            MotionSample(20.0, 0.01),
            MotionSample(24.0, 0.08),
            MotionSample(26.0, 0.09),
            MotionSample(45.0, 0.01),
        ]

        events = visual_consumed_events_from_samples(samples, donations, threshold=0.035)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source, "visual")
        self.assertEqual(events[0].confidence, MEDIUM_CONFIDENCE)
        self.assertEqual(events[0].count, 2)
        self.assertIn("pending owed shots", events[0].evidence)

    def test_yolo_pose_candidates_are_labeled_for_review(self) -> None:
        donations = [DonationEvent("d1", 10.0, 21.0, "USD", 2, "$21 double")]
        samples = [MotionSample(24.0, 0.85)]

        events = visual_consumed_events_from_samples(
            samples,
            donations,
            threshold=0.55,
            source="yolo_pose",
            evidence_label="YOLO pose hand-to-mouth candidate",
            review_hint="review for hand-to-mouth drinking motion",
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source, "yolo_pose")
        self.assertEqual(events[0].count, 2)
        self.assertIn("YOLO pose hand-to-mouth", events[0].evidence)

    def test_visual_review_offsets_cover_merged_donation_windows(self) -> None:
        donations = [
            DonationEvent("d1", 10.0, 21.0, "USD", 2, "$21 double"),
            DonationEvent("d2", 20.0, 5.0, "USD", 1, "$5 shot"),
        ]

        ranges = visual_review_ranges(donations, duration=60.0)
        offsets = frame_offsets_for_ranges(ranges, interval=10.0, max_frames=3)

        self.assertEqual(ranges, [(18.0, 60.0)])
        self.assertEqual(offsets, [18.0, 38.0, 58.0])

    def test_dedupe_consumed_merges_visual_and_transcript_evidence(self) -> None:
        transcript = ConsumedShotEvent(
            "s1",
            120.0,
            1,
            HIGH_CONFIDENCE,
            "transcript",
            "cheers",
            media_name="part1.mp4",
        )
        visual = ConsumedShotEvent(
            "s2",
            126.0,
            2,
            MEDIUM_CONFIDENCE,
            "visual",
            "visual motion candidate",
            media_name="part1.mp4",
        )

        events = dedupe_consumed([visual, transcript])

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].confidence, HIGH_CONFIDENCE)
        self.assertEqual(events[0].count, 2)
        self.assertEqual(events[0].source, "transcript+visual")
        self.assertIn("cheers", events[0].evidence)
        self.assertIn("visual motion", events[0].evidence)

    def test_extract_paddle_ocr_text_handles_common_result_shapes(self) -> None:
        result = [
            {"rec_texts": ["Alice sent $21", "take two"]},
            [[[[0, 0], [1, 1]], ("Bob tipped $5", 0.98)]],
        ]

        text = extract_paddle_ocr_text(result)

        self.assertIn("Alice sent $21", text)
        self.assertIn("take two", text)
        self.assertIn("Bob tipped $5", text)

    def test_yolo_pose_device_maps_cuda_alias(self) -> None:
        self.assertEqual(yolo_pose_device("cuda"), "0")
        self.assertIsNone(yolo_pose_device("auto"))

    def test_transcript_estimates_ignore_loose_jager_mentions(self) -> None:
        with TemporaryDirectory() as tmp:
            media = Path(tmp) / "Live [LIVEVIDEO01].mp4"
            media.write_text("media", encoding="utf-8")
            media.with_suffix(".srt").write_text(
                "1\n"
                "00:01:00,000 --> 00:01:02,000\n"
                "No Orange Jager, dude.\n\n"
                "2\n"
                "00:02:00,000 --> 00:02:02,000\n"
                "Jagermeister or suicide.\n\n"
                "3\n"
                "00:03:00,000 --> 00:03:02,000\n"
                "Let's take a shot, dude.\n\n",
                encoding="utf-8",
            )

            events = estimate_consumed_shots_from_transcript(media, [])

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].offset_seconds, 180.0)
        self.assertIn("take a shot", events[0].evidence)

    def test_auto_audit_waits_for_required_artifacts_then_saves_project(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media = root / "Live [LIVEVIDEO01].mp4"
            media.write_text("media", encoding="utf-8")
            config = BotConfig(
                state_dir=root / "state",
                download_dir=root,
                shot_audit_enabled=True,
                shot_audit_auto_run=True,
            )

            waiting = maybe_run_auto_shot_audit(
                config,
                video_id="LIVEVIDEO01",
                media_file=media,
            )
            media.with_suffix(".srt").write_text("subs", encoding="utf-8")
            media.with_suffix(".vtt").write_text("subs", encoding="utf-8")
            chat_video_output_file(media).write_text("chat video", encoding="utf-8")
            donation = DonationEvent("d1", 10.0, 21.0, "USD", 2, "$21")

            with patch(
                "onlysavemevods.shot_audit.detect_donation_events_from_video",
                return_value=[donation],
            ):
                ran = maybe_run_auto_shot_audit(
                    config,
                    video_id="LIVEVIDEO01",
                    media_file=media,
                    title="Live",
                )

            project = load_project(config, ran.project_id)

        self.assertFalse(waiting.ran)
        self.assertIn("Waiting for transcription", waiting.message)
        self.assertTrue(ran.ran)
        self.assertIsNotNone(project)
        assert project is not None
        self.assertEqual(project.donations[0].owed_shots, 2)
        self.assertIn("Owed shots: 2", render_markdown_report(project))

    def test_manual_visible_shot_updates_saved_project(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media = root / "Live [LIVEVIDEO01].mp4"
            media.write_text("media", encoding="utf-8")
            config = BotConfig(state_dir=root / "state", download_dir=root)
            donation = DonationEvent("d1", 10.0, 21.0, "USD", 2, "$21")

            with patch(
                "onlysavemevods.shot_audit.detect_donation_events_from_video",
                return_value=[donation],
            ):
                from onlysavemevods.shot_audit import run_shot_audit

                project = run_shot_audit(
                    config,
                    video_id="LIVEVIDEO01",
                    media_file=media,
                    title="Live",
                )

            updated = add_manual_visible_shot(
                config,
                project.project_id,
                offset_seconds=120.0,
                count=1,
                note="Clear on camera",
            )

        totals = compute_totals(updated.donations, updated.consumed)
        self.assertEqual(totals.manual_shots, 1)
        self.assertEqual(totals.unconfirmed_owed_shots, 1)

    def test_project_can_run_against_multiple_media_files(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "Part 1 [LIVEVIDEO01].mp4"
            second = root / "Part 2 [LIVEVIDEO01].mp4"
            first.write_text("media1", encoding="utf-8")
            second.write_text("media2", encoding="utf-8")
            config = BotConfig(
                state_dir=root / "state",
                download_dir=root,
                shot_audit_require_transcription=False,
                shot_audit_require_chat_video=False,
            )

            project = create_project(
                config,
                video_id="LIVEVIDEO01",
                media_file=first,
                title="Live",
            )
            project = add_media_to_project(
                config,
                project.project_id,
                video_id="LIVEVIDEO01",
                media_file=second,
                title="Live",
            )

            def fake_detect(_config: object, video_file: Path, _rules: object, **_kwargs: object) -> list[DonationEvent]:
                amount = 21.0 if video_file == first else 5.0
                owed = 2 if video_file == first else 1
                return [DonationEvent(f"d-{video_file.stem}", 10.0, amount, "USD", owed, "rule")]

            with (
                patch(
                    "onlysavemevods.shot_audit.detect_donation_events_from_video",
                    side_effect=fake_detect,
                ),
                patch("onlysavemevods.shot_audit.estimate_consumed_shots", return_value=[]),
            ):
                project = run_shot_audit(
                    config,
                    video_id="LIVEVIDEO01",
                    media_file=first,
                    title="Live",
                    force=True,
                )

        self.assertEqual(len(project.media_items), 2)
        self.assertEqual(len(project.donations), 2)
        self.assertEqual({event.media_name for event in project.donations}, {first.name, second.name})
        self.assertEqual(compute_totals(project.donations, project.consumed).owed_shots, 3)
        self.assertIn(first.name, render_markdown_report(project))
        self.assertIn(second.name, render_markdown_report(project))

    def test_delete_project_removes_audit_state_but_keeps_media(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            media = root / "Live [LIVEVIDEO01].mp4"
            media.write_text("media", encoding="utf-8")
            config = BotConfig(state_dir=root / "state", download_dir=root)
            project = create_project(
                config,
                video_id="LIVEVIDEO01",
                media_file=media,
                title="Live",
            )

            delete_project(config, project.project_id)
            deleted = load_project(config, project.project_id)
            media_exists = media.is_file()

        self.assertIsNone(deleted)
        self.assertTrue(media_exists)


if __name__ == "__main__":
    unittest.main()
