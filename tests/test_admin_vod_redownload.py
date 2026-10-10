from html.parser import HTMLParser
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from onlysavemevods.config import BotConfig, StreamerConfig
from onlysavemevods.downloader import segment_directory
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore
from onlysavemevods.web import render_admin_page


class FormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.forms: list[dict[str, object]] = []
        self.buttons: list[dict[str, object]] = []
        self.dialogs: list[dict[str, object]] = []
        self.elements: list[dict[str, object]] = []
        self.current: dict[str, object] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        element = {"tag": tag, "attrs": attributes}
        button_row = next(
            (
                parent for parent in reversed(self.elements)
                if "button-row" in (parent["attrs"].get("class") or "").split()
            ),
            None,
        )
        dialog_id = next(
            (
                parent["attrs"].get("id") for parent in reversed(self.elements)
                if parent["tag"] == "dialog"
            ),
            None,
        )
        if tag == "form":
            self.current = {"attrs": attributes, "inputs": {}, "dialog_id": dialog_id}
            self.forms.append(self.current)
        elif tag == "input" and self.current is not None:
            self.current["inputs"][attributes.get("name")] = attributes.get("value")
        elif tag == "button":
            element.update(
                {"button_row": button_row, "dialog_id": dialog_id, "form": self.current, "text": ""}
            )
            self.buttons.append(element)
        elif tag == "dialog":
            self.dialogs.append(element)
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
            self.elements.append(element)

    def handle_data(self, data: str) -> None:
        for element in reversed(self.elements):
            if element["tag"] == "button":
                element["text"] += data
                break

    def handle_endtag(self, tag: str) -> None:
        if tag == "form":
            self.current = None
        for index in range(len(self.elements) - 1, -1, -1):
            if self.elements[index]["tag"] == tag:
                del self.elements[index:]
                break


class AdminVodRedownloadTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.config = BotConfig(
            streamers={"OGGEEZERLIVE": StreamerConfig(sources=["@OGGEEZERLIVE"])},
            download_dir=root / "downloads",
            state_dir=root / "state",
        )
        self.stream = LiveStream(
            video_id="youtube:6WKBYU9Rg-c",
            url="https://www.youtube.com/watch?v=6WKBYU9Rg-c",
            title="Live in Vegas",
            channel="OG GEEZER LIVE",
            source="@OGGEEZERLIVE",
        )
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, 1)
        directory = segment_directory(self.config, self.stream.video_id, self.stream.channel)
        directory.mkdir(parents=True)
        (directory / "segment-001.f140.m4a.part").write_bytes(b"saved audio")

    def render_streamer_page(self) -> str:
        return render_admin_page(
            self.config,
            "streamers",
            {"selected": ["OGGEEZERLIVE"]},
        )

    def test_failed_recording_has_vod_redownload_in_current_streamer_page(self) -> None:
        self.assertTrue(self.state.mark_finalization_failed(self.stream.video_id))

        html = self.render_streamer_page()
        forms = FormParser()
        forms.feed(html)
        redownloads = [
            form for form in forms.forms
            if form["inputs"].get("action") == "redownload"
        ]

        self.assertIn("Files, events, and actions", html)
        self.assertIn("Redownload from VOD", html)
        self.assertEqual(len(redownloads), 1)
        self.assertEqual(redownloads[0]["attrs"]["method"], "post")
        self.assertEqual(redownloads[0]["attrs"]["action"], "/vod-download")
        self.assertEqual(redownloads[0]["inputs"]["video_id"], self.stream.video_id)
        self.assertEqual(redownloads[0]["inputs"]["vod_url"], self.stream.url)

        redownload_button = next(
            button for button in forms.buttons if button["text"].strip() == "Redownload from VOD"
        )
        delete_button = next(
            button for button in forms.buttons if button["text"].strip() == "Delete stream"
        )
        self.assertIsNotNone(redownload_button["button_row"])
        self.assertIs(redownload_button["button_row"], delete_button["button_row"])
        self.assertEqual(redownload_button["attrs"]["type"], "button")
        dialog_id = redownload_button["attrs"]["data-open-dialog"]
        self.assertEqual(
            [
                dialog["attrs"]["id"] for dialog in forms.dialogs
                if dialog["attrs"].get("id") == dialog_id
            ],
            [dialog_id],
        )
        self.assertEqual(redownloads[0]["dialog_id"], dialog_id)
        submit_button = next(
            button for button in forms.buttons if button["text"].strip() == "Download VOD Copy"
        )
        self.assertIs(submit_button["form"], redownloads[0])
        self.assertEqual(submit_button["attrs"]["type"], "submit")
        cancel_button = next(
            button for button in forms.buttons
            if button["dialog_id"] == dialog_id and button["text"].strip() == "Cancel"
        )
        self.assertEqual(cancel_button["attrs"]["type"], "button")
        self.assertIn("data-close-dialog", cancel_button["attrs"])

    def test_completed_recording_has_vod_redownload_beside_delete(self) -> None:
        self.state.mark_ended(self.stream.video_id)

        forms = FormParser()
        forms.feed(self.render_streamer_page())
        redownload_button = next(
            button for button in forms.buttons if button["text"].strip() == "Redownload from VOD"
        )
        delete_button = next(
            button for button in forms.buttons if button["text"].strip() == "Delete stream"
        )
        self.assertIsNotNone(redownload_button["button_row"])
        self.assertIs(redownload_button["button_row"], delete_button["button_row"])

    def test_current_streamer_page_hides_vod_redownload_during_recording_and_merge(self) -> None:
        for status in ("downloading", "checking_after_exit", "stalled", "waiting_retry", "finalizing"):
            with self.subTest(status=status):
                self.state.conn.execute(
                    "UPDATE streams SET status = ? WHERE video_id = ?",
                    (status, self.stream.video_id),
                )
                self.state.conn.commit()

                html = self.render_streamer_page()

                self.assertIn(self.stream.title, html)
                self.assertNotIn("Redownload from VOD", html)
                self.assertNotIn('value="redownload"', html)


if __name__ == "__main__":
    unittest.main()
