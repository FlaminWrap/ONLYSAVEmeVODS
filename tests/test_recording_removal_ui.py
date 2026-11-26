from html.parser import HTMLParser
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.config import load_config
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore
from onlysavemevods.web import build_status_snapshot, dashboard_script, render_admin_page, render_status_html


class RemovalFormParser(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.removals = {}
        self.form = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "form":
            if self.form is not None:
                raise AssertionError("Unexpected nested form")
            self.form = (attributes, {})
        elif tag == "input" and self.form and attributes.get("name"):
            self.form[1][attributes["name"]] = attributes

    def handle_endtag(self, tag):
        if tag == "form" and self.form:
            if "delete_recordings" in self.form[1]:
                action = self.form[0]["action"]
                if action in self.removals:
                    raise AssertionError(f"Duplicate removal form for {action}")
                self.removals[action] = self.form
            self.form = None


class RecordingRemovalUiTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        config = Path(temp.name) / "config.toml"
        config.write_text('download_dir = "media"\nstate_dir = "state"\n'
                          '[streamers."Recorder"]\nsources = ["kick:recorder"]\n')
        self.config = load_config(config)
        stream = LiveStream(video_id="kick:123", channel="Recorder", url="https://kick.com/recorder",
                            title="Recorded stream", platform="kick", source="kick:recorder")
        state = StateStore(self.config.db_path)
        try:
            state.upsert_detected(stream)
            state.mark_ended(stream.video_id)
        finally:
            state.close()

    def check_removal_forms(self, html, *, dialog):
        forms = RemovalFormParser(html).removals
        self.assertEqual(set(forms), {"/streamers", "/delete-stream"})
        for route, (form, inputs) in forms.items():
            with self.subTest(route=route):
                self.assertEqual(form["method"], "post")
                option = inputs["delete_recordings"]
                self.assertEqual(option["type"], "checkbox")
                self.assertEqual(option["value"], "1")
                self.assertNotIn("checked", option)
                streamer = route == "/streamers"
                if streamer:
                    self.assertEqual(inputs["action"]["value"], "delete")
                    self.assertEqual(inputs["streamer_name"]["value"], "Recorder")
                    self.assertIn("still listed for this streamer", form["data-delete-message"])
                    self.assertIn("Also permanently delete recordings still listed for this streamer", html)
                else:
                    self.assertEqual(inputs["video_id"]["value"], "kick:123")
                self.assertEqual(inputs["confirm_delete"]["value"],
                                 "delete_streamer_recordings" if streamer else "delete_stream")
                self.assertIn("kept", form["data-keep-message"].lower())
                self.assertIn("permanently delete", form["data-delete-message"].lower())
                if dialog:
                    self.assertIn("data-recording-removal", form)
                    self.assertNotEqual(form["data-keep-label"], form["data-delete-label"])
                else:
                    self.assertIn("delete_recordings.checked", form["onsubmit"])
                    self.assertIn("deleteMessage", form["onsubmit"])
                    self.assertIn("keepMessage", form["onsubmit"])

    def test_modern_streamer_detail_offers_two_independent_removal_options(self):
        html = render_admin_page(self.config, "streamers", {"selected": ["Recorder"]})
        self.check_removal_forms(html, dialog=True)

    def test_legacy_initial_and_refreshed_cards_keep_the_same_removal_contract(self):
        html = render_status_html(build_status_snapshot(self.config))
        self.check_removal_forms(html, dialog=False)
        script = dashboard_script()
        encoded = script.split("const removalForms = ", 1)[1]
        templates, _ = json.JSONDecoder().raw_decode(encoded)
        refreshed = templates["streamer"].replace("__removal_target__", "Recorder")
        refreshed += templates["stream"].replace("__removal_target__", "kick:123")
        self.check_removal_forms(refreshed, dialog=False)
        self.assertIn("it may be detected again", templates["detected"])
        self.assertIn("downloading", templates["blocked"])
        self.assertNotIn("detected", templates["blocked"])
