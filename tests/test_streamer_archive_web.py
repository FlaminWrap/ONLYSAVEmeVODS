from dataclasses import asdict
from html.parser import HTMLParser
from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from urllib.parse import urlencode

from onlysavemevods.config import BotConfig, load_config
from onlysavemevods.downloader import segment_directory
from onlysavemevods.models import LiveStream
from onlysavemevods.state import StateStore
from onlysavemevods.web import (
    build_handler,
    config_file_revision,
    render_admin_fragment,
    render_admin_page,
    update_streamer_from_json,
)


class ArchivePageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.summary_names: list[str] = []
        self.forms: list[dict[str, object]] = []
        self.links: list[dict[str, str]] = []
        self.current_form: dict[str, object] | None = None
        self.current_link: dict[str, str] | None = None
        self.in_summary = False
        self.in_summary_heading = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "article" and "data-streamer-summary" in attributes:
            self.in_summary = True
        elif tag == "h2" and self.in_summary:
            self.in_summary_heading = True
            self.summary_names.append("")
        elif tag == "form":
            self.current_form = {"attrs": attributes, "inputs": {}}
            self.forms.append(self.current_form)
        elif tag == "input" and self.current_form is not None:
            self.current_form["inputs"][attributes.get("name")] = attributes.get("value")
        elif tag == "a":
            self.current_link = {"href": attributes.get("href") or "", "text": ""}
            self.links.append(self.current_link)

    def handle_data(self, data: str) -> None:
        if self.in_summary_heading:
            self.summary_names[-1] += data
        if self.current_link is not None:
            self.current_link["text"] += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "article":
            self.in_summary = False
        elif tag == "h2":
            self.in_summary_heading = False
        elif tag == "form":
            self.current_form = None
        elif tag == "a":
            self.current_link = None


class NonClosingBuffer(BytesIO):
    def close(self) -> None:
        pass


def post_streamer(config: BotConfig, payload: dict[str, object], *, as_json: bool = False) -> str:
    content_type = "application/json" if as_json else "application/x-www-form-urlencoded"
    body = json.dumps(payload).encode() if as_json else urlencode(payload).encode()
    request_body = (
        b"POST /streamers HTTP/1.1\r\n"
        b"Host: localhost:8080\r\n"
        b"Origin: http://localhost:8080\r\n"
        b"Sec-Fetch-Site: same-origin\r\n"
        + f"Content-Type: {content_type}\r\n".encode()
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )

    class Request:
        def __init__(self) -> None:
            self.input = NonClosingBuffer(request_body)
            self.output = NonClosingBuffer()

        def makefile(self, mode: str, buffering: int | None = None) -> NonClosingBuffer:
            return self.input

        def sendall(self, data: bytes) -> None:
            self.output.write(data)

    class Server:
        server_name = "localhost"
        server_port = 8080

    request = Request()
    build_handler(config)(request, ("127.0.0.1", 1), Server())
    return request.output.getvalue().decode("latin-1")


class StreamerArchiveWebTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config_path = self.root / "config.toml"
        self.config_path.write_text(
            'channels = []\ndownload_dir = "downloads"\nstate_dir = "state"\n'
            'web_enabled = true\nweb_host = "127.0.0.1"\nweb_port = 8080\n'
            '[streamers."Example"]\n'
            'sources = ["@Example", "kick:example"]\n'
            'download_dir_name = "Example VODs"\n'
            'powerchat_enabled = true\npowerchat_username = "example"\n'
            'timezone = "Europe/London"\n'
            '[streamers."Example".speaker_labels]\nSPEAKER_00 = "Example"\n'
            '[streamers."Example".post_stream]\ntranscribe_subtitles = false\n'
            '[streamers."Dormant"]\nsources = ["@Dormant"]\narchived = true\n',
            encoding="utf-8",
        )
        self.config = load_config(self.config_path)
        self.stream = LiveStream(
            video_id="youtube:ARCHIVE001",
            url="https://www.youtube.com/watch?v=ARCHIVE001",
            title="Saved recording",
            channel="Example",
            source="@Example",
        )
        self.state = StateStore(self.config.db_path)
        self.addCleanup(self.state.close)
        self.state.mark_downloading(self.stream, 1)
        self.state.mark_exited(self.stream.video_id, 0)
        self.state.mark_ended(self.stream.video_id)
        self.media = segment_directory(self.config, self.stream.video_id, self.stream.channel) / "segment-001.mp4"
        self.media.parent.mkdir(parents=True)
        self.media.write_bytes(b"existing recording")

    def parse_page(self, params: dict[str, list[str]] | None = None) -> ArchivePageParser:
        parser = ArchivePageParser()
        parser.feed(render_admin_page(self.config, "streamers", params or {}))
        return parser

    def test_form_archive_and_json_restore_preserve_settings_history_and_files(self) -> None:
        before = asdict(self.config.streamers["Example"])
        record = self.state.get_stream(self.stream.video_id)
        original_revision = config_file_revision(self.config)

        archived = post_streamer(
            self.config,
            {"action": "archive", "streamer_name": "Example", "return_to": "/streamers?view=archived"},
        )

        self.assertIn(" 303 See Other\r\n", archived)
        self.assertIn("Location: /streamers?view=archived\r\n", archived)
        self.assertTrue(self.config.streamers["Example"].archived)
        self.assertEqual(
            asdict(load_config(self.config_path).streamers["Example"]),
            {**before, "archived": True},
        )
        self.assertNotEqual(config_file_revision(self.config), original_revision)
        self.assertEqual(self.state.get_stream(self.stream.video_id), record)
        self.assertEqual(self.media.read_bytes(), b"existing recording")

        restored = post_streamer(
            self.config,
            {
                "action": "restore",
                "streamer_name": "Example",
                "revision": config_file_revision(self.config),
            },
            as_json=True,
        )

        self.assertIn(" 200 OK\r\n", restored)
        result = json.loads(restored.split("\r\n\r\n", 1)[1])
        self.assertTrue(result["ok"])
        self.assertEqual(result["saved"], ["archived"])
        self.assertEqual(result["restart_required"], [])
        self.assertEqual(asdict(self.config.streamers["Example"]), before)
        self.assertEqual(asdict(load_config(self.config_path).streamers["Example"]), before)
        self.assertEqual(self.state.get_stream(self.stream.video_id), record)
        self.assertEqual(self.media.read_bytes(), b"existing recording")

    def test_archive_json_rejects_stale_revision_and_unknown_streamer_without_writing(self) -> None:
        before = self.config_path.read_bytes()
        stale = post_streamer(
            self.config,
            {"action": "archive", "streamer_name": "Example", "revision": "stale"},
            as_json=True,
        )
        self.assertIn(" 409 Conflict\r\n", stale)
        self.assertEqual(self.config_path.read_bytes(), before)
        self.assertFalse(self.config.streamers["Example"].archived)

        for as_json in (False, True):
            with self.subTest(as_json=as_json):
                response = post_streamer(
                    self.config,
                    {"action": "archive", "streamer_name": "Missing"},
                    as_json=as_json,
                )
                self.assertIn(" 400 ", response)
                self.assertEqual(self.config_path.read_bytes(), before)

    def test_autosaving_basic_settings_does_not_restore_archived_streamer(self) -> None:
        result = update_streamer_from_json(
            self.config,
            {
                "streamer_name": "Dormant",
                "revision": config_file_revision(self.config),
                "values": {"timezone": "Europe/London", "sources": "@Dormant\nkick:dormant"},
            },
        )

        self.assertTrue(result["ok"])
        self.assertTrue(self.config.streamers["Dormant"].archived)
        saved = load_config(self.config_path).streamers["Dormant"]
        self.assertTrue(saved.archived)
        self.assertEqual(saved.timezone, "Europe/London")
        self.assertEqual(saved.sources, ["@Dormant", "kick:dormant"])

        response = post_streamer(
            self.config,
            {"action": "save", "streamer_name": "Dormant", "timezone": "UTC"},
        )
        self.assertIn(" 303 See Other\r\n", response)
        self.assertTrue(self.config.streamers["Dormant"].archived)
        self.assertTrue(load_config(self.config_path).streamers["Dormant"].archived)

    def test_streamer_list_and_refresh_filter_archive_with_navigation(self) -> None:
        active = self.parse_page()
        archived = self.parse_page({"view": ["archived"]})

        self.assertEqual(active.summary_names, ["Example"])
        self.assertEqual(archived.summary_names, ["Dormant"])
        self.assertTrue(any(
            link["href"] == "/streamers?view=archived"
            and "Archived" in link["text"] and "1" in link["text"]
            for link in active.links
        ))
        self.assertTrue(any(
            link["href"] == "/streamers"
            and "Active" in link["text"] and "1" in link["text"]
            for link in active.links
        ))
        self.assertTrue(any(link["href"] == "/streamers" for link in archived.links))
        for params, expected in (
            ({"fragment": ["list"]}, ["Example"]),
            ({"fragment": ["list"], "view": ["archived"]}, ["Dormant"]),
        ):
            with self.subTest(params=params):
                html, _revision = render_admin_fragment(self.config, "streamers", params)
                parser = ArchivePageParser()
                parser.feed(html)
                self.assertEqual(parser.summary_names, expected)

    def test_selected_archived_streamer_keeps_history_and_exposes_restore(self) -> None:
        post_streamer(self.config, {"action": "archive", "streamer_name": "Example"})

        html = render_admin_page(self.config, "streamers", {"selected": ["Example"]})
        parser = ArchivePageParser()
        parser.feed(html)

        self.assertIn("Archived", html)
        self.assertIn("monitoring", html.lower())
        self.assertIn(self.stream.title, html)
        self.assertIn("Restore streamer", html)
        restores = [form for form in parser.forms if form["inputs"].get("action") == "restore"]
        self.assertEqual(len(restores), 1)
        self.assertEqual(restores[0]["attrs"]["action"], "/streamers")
        self.assertEqual(restores[0]["inputs"]["streamer_name"], "Example")
        self.assertFalse(any(form["inputs"].get("action") == "archive" for form in parser.forms))

    def test_active_streamer_has_confirmed_archive_and_history_only_streamer_cannot_archive(self) -> None:
        parser = self.parse_page({"selected": ["Example"]})
        archives = [form for form in parser.forms if form["inputs"].get("action") == "archive"]
        self.assertEqual(len(archives), 1)
        self.assertEqual(archives[0]["attrs"]["method"], "post")
        self.assertEqual(archives[0]["attrs"]["action"], "/streamers")
        self.assertIn("data-confirm", archives[0]["attrs"])
        self.assertEqual(archives[0]["inputs"]["streamer_name"], "Example")

        orphan = LiveStream(
            video_id="youtube:HISTORY001",
            url="https://www.youtube.com/watch?v=HISTORY001",
            title="Older recording",
            channel="History only",
        )
        self.state.upsert_detected(orphan)
        self.state.mark_ended(orphan.video_id)
        parser = self.parse_page({"selected": ["History only"]})
        self.assertFalse(any(form["inputs"].get("action") in {"archive", "restore"} for form in parser.forms))

    def test_configured_streamer_cannot_archive_without_writable_config_path(self) -> None:
        self.config.config_path = None
        parser = self.parse_page({"selected": ["Example"]})

        self.assertFalse(any(
            form["inputs"].get("action") in {"archive", "restore"}
            for form in parser.forms
        ))

    def test_archiving_last_active_streamer_keeps_archive_view_available(self) -> None:
        _html, before_revision = render_admin_fragment(
            self.config, "streamers", {"fragment": ["list"]}
        )
        post_streamer(self.config, {"action": "archive", "streamer_name": "Example"})
        _html, after_revision = render_admin_fragment(
            self.config, "streamers", {"fragment": ["list"]}
        )

        self.assertNotEqual(before_revision, after_revision)
        active = self.parse_page()
        self.assertEqual(active.summary_names, [])
        self.assertTrue(any(
            link["href"] == "/streamers?view=archived"
            and "Archived" in link["text"] and "2" in link["text"]
            for link in active.links
        ))
        archived = self.parse_page({"view": ["archived"]})
        self.assertCountEqual(archived.summary_names, ["Example", "Dormant"])


if __name__ == "__main__":
    unittest.main()
