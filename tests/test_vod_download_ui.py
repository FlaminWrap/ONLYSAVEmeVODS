from html.parser import HTMLParser
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from onlysavemevods.config import BotConfig, StreamerConfig
from onlysavemevods.download_progress import (
    clear_all_download_progress,
    download_progress_path,
    record_download_progress,
)
from onlysavemevods.job_tracker import (
    clear_tracked_jobs,
    finish_tracked_job,
    start_tracked_job,
    update_tracked_job,
)
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore
from onlysavemevods.web import (
    queue_vod_download_job,
    render_admin_fragment_with_state,
    render_admin_page,
)


class DownloadProgressParser(HTMLParser):
    """Read recording progress separately from the processing jobs list."""

    def __init__(self) -> None:
        super().__init__()
        self.sections: list[dict] = []
        self.elements: list[dict] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        element = {"tag": tag, "attrs": attributes}
        if "data-download-progress" in attributes:
            element.update({"text": "", "bars": []})
            self.sections.append(element)
        if tag == "progress":
            for parent in self.elements:
                if "data-download-progress" in parent["attrs"]:
                    parent["bars"].append(attributes)
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
            self.elements.append(element)

    def handle_data(self, data: str) -> None:
        for element in self.elements:
            if "data-download-progress" in element["attrs"]:
                element["text"] += data

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.elements) - 1, -1, -1):
            if self.elements[index]["tag"] == tag:
                del self.elements[index:]
                break


class VodDownloadUiTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_tracked_jobs()
        clear_all_download_progress()
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.config = BotConfig(
            streamers={"OGGEEZERLIVE": StreamerConfig(sources=["@OGGEEZERLIVE"])},
            download_dir=root / "downloads",
            state_dir=root / "state",
            auto_redownload_failed_finalization=True,
        )
        self.stream = LiveStream(
            video_id="youtube:6WKBYU9Rg-c",
            url="https://www.youtube.com/watch?v=6WKBYU9Rg-c",
            title="Saved live recording",
            channel="OGGEEZERLIVE",
            platform="youtube",
            source="@OGGEEZERLIVE",
        )
        self.job_id = f"vod-download:{self.stream.video_id}"
        self.output = root / "downloads" / "vod-copy.%(ext)s"
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)
        self.addCleanup(clear_tracked_jobs)
        self.addCleanup(clear_all_download_progress)

    def make_failed_recording(self) -> None:
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, 1)
        self.assertTrue(self.state.mark_finalization_failed(self.stream.video_id))
        record_download_progress(
            self.stream.video_id,
            {"video": (700, 900), "audio": (120, 900)},
            segment_index=1,
            updated_at=0,
            progress_file=download_progress_path(self.config.state_dir),
        )

    def queue_vod(self, *, automatic: bool = False) -> None:
        if automatic:
            self.assertIsNotNone(
                self.state.begin_automatic_vod_recovery(
                    self.stream.video_id, str(self.output), now=1000
                )
            )
        with patch("onlysavemevods.web.Thread"):
            queue_vod_download_job(
                self.config,
                self.job_id,
                self.stream,
                self.stream.url,
                self.output,
                previous_status="finalization_failed" if self.state.get_stream(self.stream.video_id) else None,
                queued_message="Queued VOD copy",
                item="vod-copy",
                automatic=automatic,
            )

    def rendered_pages(self) -> list[str]:
        return [
            render_admin_page(self.config, "overview", {}),
            render_admin_page(
                self.config, "streamers", {"selected": ["OGGEEZERLIVE"]}
            ),
        ]

    def assert_overall_progress(self, html: str, progress: float | None) -> dict:
        parsed = DownloadProgressParser()
        parsed.feed(html)
        self.assertEqual(len(parsed.sections), 1)
        section = parsed.sections[0]
        self.assertIn("VOD download", section["text"])
        self.assertIn("Overall", section["text"])
        self.assertEqual(len(section["bars"]), 1)
        self.assertNotIn("Live-edge download", html)
        self.assertNotIn("fragments", section["text"])
        self.assertNotIn("30s+", section["text"])
        bar = section["bars"][0]
        self.assertIn("Overall", bar["aria-label"])
        if progress is None:
            self.assertNotIn("value", bar)
        else:
            self.assertAlmostEqual(float(bar["value"]) / float(bar["max"]), progress)
        return section

    def test_new_manual_vod_shows_one_overall_bar_on_both_current_pages(self) -> None:
        self.queue_vod()
        self.assertEqual(self.state.get_stream(self.stream.video_id).recording_kind, "vod")
        update_tracked_job(self.job_id, phase="Downloading", progress=0.42)

        for html in self.rendered_pages():
            self.assert_overall_progress(html, 0.42)

    def test_manual_failed_recording_copy_uses_vod_progress_despite_live_identity(self) -> None:
        self.make_failed_recording()
        for html in self.rendered_pages():
            self.assertIn(
                f'data-download-progress-slot="{self.stream.video_id}" hidden', html
            )
        self.queue_vod()
        self.assertEqual(self.state.get_stream(self.stream.video_id).recording_kind, "live")
        update_tracked_job(self.job_id, phase="Downloading", progress=0.37)

        for html in self.rendered_pages():
            self.assert_overall_progress(html, 0.37)

    def test_automatic_failed_recording_copy_uses_vod_progress_despite_live_identity(self) -> None:
        self.make_failed_recording()
        self.queue_vod(automatic=True)
        self.assertEqual(self.state.get_stream(self.stream.video_id).recording_kind, "live")
        update_tracked_job(self.job_id, phase="Downloading", progress=0.63)

        for html in self.rendered_pages():
            self.assert_overall_progress(html, 0.63)

    def test_queued_and_unknown_progress_use_one_overall_bar(self) -> None:
        self.queue_vod()
        update_tracked_job(self.job_id, status="queued", phase="Queued", progress=None)
        for html in self.rendered_pages():
            section = self.assert_overall_progress(html, None)
            self.assertIn("Queued", section["text"])

        update_tracked_job(self.job_id, status="running", phase="Downloading", progress=None)
        for html in self.rendered_pages():
            self.assert_overall_progress(html, None)

    def test_vod_waiting_for_job_has_overall_progress_without_live_edge_warnings(self) -> None:
        self.assertTrue(self.state.mark_vod_downloading(self.stream))

        for html in self.rendered_pages():
            self.assert_overall_progress(html, None)

    def test_other_job_kinds_do_not_replace_live_download_progress(self) -> None:
        self.state.mark_downloading(self.stream, 1)
        start_tracked_job(
            "transcription:test",
            kind="Transcription",
            video_id=self.stream.video_id,
            item="segment-001.mp4",
            progress=0.5,
        )

        for html in self.rendered_pages():
            parsed = DownloadProgressParser()
            parsed.feed(html)
            self.assertEqual(len(parsed.sections), 1)
            self.assertIn("Live-edge download", parsed.sections[0]["text"])
            self.assertEqual(len(parsed.sections[0]["bars"]), 2)
            self.assertNotIn("Overall", parsed.sections[0]["text"])

    def test_fragment_refresh_updates_vod_progress_and_removes_finished_bar(self) -> None:
        self.make_failed_recording()
        self.queue_vod()
        for page, params in (("overview", {}), ("streamers", {"selected": ["OGGEEZERLIVE"]})):
            with self.subTest(page=page):
                update_tracked_job(self.job_id, phase="Downloading", progress=0.25)
                before, before_revision, _ = render_admin_fragment_with_state(self.config, page, params)
                update_tracked_job(self.job_id, phase="Downloading", progress=0.75)
                after, after_revision, _ = render_admin_fragment_with_state(self.config, page, params)
                self.assert_overall_progress(before, 0.25)
                self.assert_overall_progress(after, 0.75)
                self.assertNotEqual(before_revision, after_revision)

        self.state.mark_vod_download_finished(self.stream.video_id)
        finish_tracked_job(self.job_id)
        for page, params in (("overview", {}), ("streamers", {"selected": ["OGGEEZERLIVE"]})):
            html, _, _ = render_admin_fragment_with_state(self.config, page, params)
            parsed = DownloadProgressParser()
            parsed.feed(html)
            self.assertEqual(parsed.sections, [])

    def test_failed_copy_removes_download_bar_and_does_not_restore_stale_live_bar(self) -> None:
        self.make_failed_recording()
        self.queue_vod()
        self.state.mark_vod_download_failed(
            self.stream.video_id, "VOD unavailable", restore_status="finalization_failed"
        )
        finish_tracked_job(self.job_id, status="failed", progress=None, message="VOD unavailable")

        for page, params in (("overview", {}), ("streamers", {"selected": ["OGGEEZERLIVE"]})):
            html, _, _ = render_admin_fragment_with_state(self.config, page, params)
            parsed = DownloadProgressParser()
            parsed.feed(html)
            self.assertEqual(parsed.sections, [])
            self.assertNotIn("Live-edge download", html)

    @unittest.skipUnless(shutil.which("node"), "Node is required to exercise dashboard refresh")
    def test_progress_refresh_keeps_updating_with_focused_controls_or_open_diagnostics(self) -> None:
        dashboard = (
            Path(__file__).parents[1]
            / "src"
            / "onlysavemevods"
            / "assets"
            / "dashboard.js"
        )
        script = r"""
const assert = require("node:assert/strict");
const fs = require("node:fs");
const source = fs.readFileSync(process.argv[1], "utf8");
const start = source.indexOf("  const fragmentInteractionBlocksReplacement =");
const end = source.indexOf('  document.querySelectorAll("[data-fragment-url]")', start);
assert.ok(start >= 0 && end > start, "dashboard refresh functions must exist");
const refreshSource = source.slice(start, end);

(async () => {
  const selector = "[data-download-progress], [data-download-progress-slot]";
  const transitions = [
    { initialSlot: false, incomingKind: "progress" },
    { initialSlot: true, incomingKind: "progress" },
    { initialSlot: false, incomingKind: "slot" },
    { initialSlot: false, incomingKind: "absent" },
  ];
  for (const interaction of ["focused", "diagnostics"]) {
    for (const { initialSlot, incomingKind } of transitions) {
      let replaced = 0;
      let removed = 0;
      const current = {
        dataset: initialSlot
          ? { downloadProgressSlot: "youtube:6WKBYU9Rg-c" }
          : { downloadProgress: "youtube:6WKBYU9Rg-c" },
        replaceWith(next) { replaced += 1; assert.equal(next, incoming); },
        remove() { removed += 1; },
      };
      const incoming = {
        dataset: incomingKind === "slot"
          ? { downloadProgressSlot: "youtube:6WKBYU9Rg-c" }
          : { downloadProgress: "youtube:6WKBYU9Rg-c" },
      };
      const region = {
        dataset: { fragmentUrl: "/?fragment=status", fragmentRevision: "old", fragmentStateRevision: "old-state" },
        matches(selector) { return selector === ":focus-within" && interaction === "focused"; },
        querySelector(selector) {
          return selector === "[data-file-diagnostics-loaded][open]" && interaction === "diagnostics" ? {} : null;
        },
        querySelectorAll(actualSelector) {
          assert.equal(actualSelector, selector);
          return [current];
        },
        set innerHTML(value) { assert.fail("refresh replaced the controls being used"); },
      };
      const document = {
        hidden: false,
        createElement(tag) {
          assert.equal(tag, "template");
          return {
            content: {
              querySelectorAll(actualSelector) {
                assert.equal(actualSelector, selector);
                return incomingKind === "absent" ? [] : [incoming];
              },
            },
          };
        },
      };
      const fetch = async () => ({
        ok: true,
        headers: { get(name) { return name === "X-Fragment-Revision" ? "new" : "new-state"; } },
        text: async () => "updated overall progress",
      });
      const refresh = new Function("document", "fetch", "fragmentRequests", refreshSource + "\nreturn refreshFragment;")(
        document, fetch, new WeakMap(),
      );
      await refresh(region);
      assert.equal(replaced, incomingKind === "absent" ? 0 : 1);
      assert.equal(removed, incomingKind === "absent" ? 1 : 0);
      assert.equal(region.dataset.fragmentRevision, "old");
    }
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
"""
        completed = subprocess.run(
            ["node", "-e", script, str(dashboard)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)


if __name__ == "__main__":
    unittest.main()
