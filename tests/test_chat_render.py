from datetime import datetime, timezone
from concurrent.futures import Future
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import subprocess
import unittest
from unittest.mock import patch

from onlysavemevods.chat_render import (
    ASS_CHAT_TEXT_COLOR,
    ASS_EMOJI_COLORS,
    build_chat_panel_merge_command,
    build_chat_video_command,
    ChatEntry,
    ChatToken,
    CHAT_AUTHOR_COLORS,
    CHAT_PANEL_EMOJI_SIZE,
    CHAT_PANEL_FPS,
    CHAT_PANEL_TEXT_SIZE,
    choose_chat_render_nvenc_device,
    detect_nvidia_devices,
    EmojiImageCache,
    ffmpeg_supports_h264_nvenc,
    CHAT_MESSAGE_MAX_LINES,
    VideoProbeError,
    chat_bottom_padding,
    chat_entry_gap,
    chat_entry_height,
    chat_header_separator_y,
    chat_layout_for_video,
    chat_author_color,
    chat_video_output_file,
    ass_color_from_rgb,
    format_ass_time,
    load_chat_panel_fonts,
    log_chat_media_sync_diagnostics,
    panel_emoji_size,
    panel_emoji_y,
    panel_line_height,
    panel_text_y,
    parse_live_chat_file,
    render_chat_panel_video,
    render_chat_panel_frame,
    render_chat_ass,
    render_chat_video_file,
    resolve_chat_render_panel_workers,
    split_chat_panel_segments_for_animations,
    visible_chat_stack,
    wrap_panel_message_lines,
)


class ChatRenderTests(unittest.TestCase):
    def test_resolve_chat_render_panel_workers_uses_all_cpus(self) -> None:
        self.assertEqual(resolve_chat_render_panel_workers(0, cpu_count=16), 16)
        self.assertEqual(resolve_chat_render_panel_workers(0, cpu_count=2), 2)
        self.assertEqual(resolve_chat_render_panel_workers(0, cpu_count=None), 1)
        self.assertEqual(resolve_chat_render_panel_workers(1, cpu_count=16), 1)
        self.assertEqual(resolve_chat_render_panel_workers(6, cpu_count=16), 6)
        with self.assertRaises(ValueError):
            resolve_chat_render_panel_workers(-1, cpu_count=16)

    def test_choose_chat_render_nvenc_device_rotates_by_index(self) -> None:
        devices = ["0", "1", "2"]

        self.assertEqual(choose_chat_render_nvenc_device(devices, 0), "0")
        self.assertEqual(choose_chat_render_nvenc_device(devices, 1), "1")
        self.assertEqual(choose_chat_render_nvenc_device(devices, 2), "2")
        self.assertEqual(choose_chat_render_nvenc_device(devices, 3), "0")
        self.assertEqual(choose_chat_render_nvenc_device([], 0), "")

    def test_choose_chat_render_nvenc_device_hashes_non_numeric_key(self) -> None:
        devices = ["0", "1"]

        first = choose_chat_render_nvenc_device(devices, Path("/tmp/render-a.mp4"))
        second = choose_chat_render_nvenc_device(devices, Path("/tmp/render-a.mp4"))

        self.assertEqual(first, second)
        self.assertIn(first, devices)

    def test_detect_nvidia_devices_parses_nvidia_smi_output(self) -> None:
        class Result:
            returncode = 0
            stdout = "0, NVIDIA RTX 4090\n1, NVIDIA RTX 3090\n"
            stderr = ""

        with patch("onlysavemevods.chat_render.subprocess.run", return_value=Result()):
            devices = detect_nvidia_devices()

        self.assertEqual(devices, ["0: NVIDIA RTX 4090", "1: NVIDIA RTX 3090"])

    def test_ffmpeg_supports_h264_nvenc_reads_encoder_list(self) -> None:
        class Result:
            returncode = 0
            stdout = " V..... h264_nvenc NVIDIA NVENC H.264 encoder\n"
            stderr = ""

        with patch("onlysavemevods.chat_render.subprocess.run", return_value=Result()):
            self.assertTrue(ffmpeg_supports_h264_nvenc("ffmpeg"))

    def test_parse_live_chat_json_lines(self) -> None:
        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "segment-001.live_chat.json"
            chat_file.write_text(
                "\n".join(
                    [
                        (
                            '{"replayChatItemAction":{"videoOffsetTimeMsec":"1500",'
                            '"actions":[{"addChatItemAction":{"item":'
                            '{"liveChatTextMessageRenderer":{'
                            '"timestampUsec":"1779054300000000",'
                            '"authorName":{"simpleText":"Alice"},'
                            '"message":{"runs":[{"text":"Hello "},'
                            '{"emoji":{"shortcuts":[":wave:"]}}]}}}}}]}}'
                        ),
                        (
                            '{"replayChatItemAction":{"videoOffsetTimeMsec":"2600",'
                            '"actions":[{"addChatItemAction":{"item":'
                            '{"liveChatPaidMessageRenderer":{'
                            '"authorName":{"simpleText":"Bob"},'
                            '"purchaseAmountText":{"simpleText":"$5.00"},'
                            '"message":{"simpleText":"Nice stream"}}}}}]}}'
                        ),
                    ]
                ),
                encoding="utf-8",
            )

            entries = parse_live_chat_file(chat_file)

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].offset_seconds, 1.5)
        self.assertEqual(entries[0].timestamp_us, 1779054300000000)
        self.assertEqual(entries[0].author, "Alice")
        self.assertEqual(entries[0].message, "Hello 👋")
        self.assertEqual(entries[1].message, "$5.00 Nice stream")

    def test_parse_normalized_kick_live_chat_json(self) -> None:
        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "Kick [kick].live_chat.json"
            chat_file.write_text(
                json.dumps(
                    {
                        "platform": "kick",
                        "source": "kick:oumb",
                        "video_id": "kick:vod",
                        "messages": [
                            {
                                "id": "m1",
                                "created_at": "2026-07-05T02:18:24Z",
                                "offset_ms": 2000,
                                "author": "Alice",
                                "message": "hello Kick",
                                "badges": [],
                                "emotes": [],
                            },
                            {
                                "id": "m2",
                                "created_at": "2026-07-05T02:18:27Z",
                                "offset_ms": 5000,
                                "author": "Bob",
                                "message": "second",
                                "badges": [],
                                "emotes": [],
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            entries = parse_live_chat_file(chat_file)

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].offset_seconds, 2.0)
        self.assertEqual(entries[0].timestamp_us, 1783217904000000)
        self.assertEqual(entries[0].author, "Alice")
        self.assertEqual(entries[0].message, "hello Kick")
        self.assertEqual(entries[0].tokens[0].text, "hello Kick")

    def test_parse_normalized_kick_live_chat_converts_emote_placeholders_to_images(self) -> None:
        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "Kick [kick].live_chat.json"
            chat_file.write_text(
                json.dumps(
                    {
                        "platform": "kick",
                        "source": "kick:oumb",
                        "video_id": "kick:vod",
                        "messages": [
                            {
                                "id": "m1",
                                "created_at": "2026-07-05T02:18:24Z",
                                "offset_ms": 2000,
                                "author": "Alice",
                                "message": "hello [emote:4148074:HYPERCLAP] chat",
                                "badges": [],
                                "emotes": [
                                    {
                                        "id": "4148074",
                                        "name": "HYPERCLAP",
                                        "image_url": "https://files.kick.com/emotes/4148074/fullsize",
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            entries = parse_live_chat_file(chat_file)

        self.assertEqual(entries[0].message, "hello HYPERCLAP chat")
        self.assertEqual(len(entries[0].tokens), 3)
        self.assertEqual(entries[0].tokens[0].text, "hello ")
        self.assertTrue(entries[0].tokens[1].is_emoji)
        self.assertEqual(entries[0].tokens[1].text, " HYPERCLAP ")
        self.assertEqual(
            entries[0].tokens[1].image_url,
            "https://files.kick.com/emotes/4148074/fullsize",
        )
        self.assertEqual(entries[0].tokens[1].image_key, "kick-emote:4148074")
        self.assertEqual(entries[0].tokens[2].text, " chat")

    def test_parse_live_chat_resolves_youtube_emoji_shortcodes(self) -> None:
        with TemporaryDirectory() as tmp:
            chat_file = Path(tmp) / "segment-001.live_chat.json"
            chat_file.write_text(
                json.dumps(
                    {
                        "replayChatItemAction": {
                            "videoOffsetTimeMsec": "1500",
                            "actions": [
                                {
                                    "addChatItemAction": {
                                        "item": {
                                            "liveChatTextMessageRenderer": {
                                                "authorName": {"simpleText": "Alice"},
                                                "message": {
                                                    "runs": [
                                                        {
                                                            "text": (
                                                                "stop going live bruh "
                                                            )
                                                        },
                                                        {
                                                            "emoji": {
                                                                "emojiId": "UCksz/example",
                                                                "shortcuts": [
                                                                    ":face-red-droopy-eyes:"
                                                                ],
                                                                "searchTerms": [
                                                                    "face-red-droopy-eyes"
                                                                ],
                                                                "image": {
                                                                    "accessibility": {
                                                                        "accessibilityData": {
                                                                            "label": (
                                                                                "face-red-droopy-eyes"
                                                                            )
                                                                        }
                                                                    }
                                                                },
                                                                "isCustomEmoji": True,
                                                            }
                                                        },
                                                        {"text": " "},
                                                        {
                                                            "emoji": {
                                                                "emojiId": "🔥",
                                                                "shortcuts": [":fire:"],
                                                                "searchTerms": ["fire"],
                                                                "image": {
                                                                    "accessibility": {
                                                                        "accessibilityData": {
                                                                            "label": "🔥"
                                                                        }
                                                                    }
                                                                },
                                                            }
                                                        },
                                                    ]
                                                },
                                            }
                                        }
                                    }
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            entries = parse_live_chat_file(chat_file)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].message, "stop going live bruh [bruh] 🔥")
        self.assertEqual(len(entries[0].tokens), 4)
        self.assertTrue(entries[0].tokens[1].is_emoji)
        self.assertEqual(entries[0].tokens[1].image_url, "")
        self.assertTrue(entries[0].tokens[3].is_emoji)

    def test_chat_media_sync_diagnostics_warn_when_chat_ends_early(self) -> None:
        class Logger:
            def __init__(self) -> None:
                self.infos: list[tuple[object, ...]] = []
                self.warnings: list[tuple[object, ...]] = []

            def info(self, *args: object) -> None:
                self.infos.append(args)

            def warning(self, *args: object) -> None:
                self.warnings.append(args)

        logger = Logger()

        log_chat_media_sync_diagnostics(
            [
                ChatEntry(offset_seconds=0.0, author="Alice", message="first"),
                ChatEntry(offset_seconds=50.0, author="Bob", message="last"),
            ],
            100.0,
            media_file=Path("/tmp/live.mp4"),
            chat_file=Path("/tmp/live.live_chat.json"),
            logger=logger,  # type: ignore[arg-type]
        )

        self.assertTrue(logger.infos)
        self.assertTrue(logger.warnings)
        self.assertIn("Chat ends", str(logger.warnings[0][0]))

    def test_render_chat_ass_places_messages_in_right_panel(self) -> None:
        layout = chat_layout_for_video(1280, 720)
        entry = ChatEntry(
            offset_seconds=1.25,
            author="Alice",
            message="A message with {braces}",
        )
        ass = render_chat_ass(
            [entry],
            layout,
        )

        self.assertIn("PlayResX: 1760", ass)
        self.assertIn("PlayResY: 720", ass)
        self.assertIn("Live Chat", ass)
        separator_y = chat_header_separator_y(layout)
        self.assertIn("Dialogue: 2,0:00:00.00", ass)
        self.assertIn(
            f"m {layout.video_width} 0 l {layout.output_width} 0",
            ass,
        )
        self.assertIn("Dialogue: 3,0:00:00.00", ass)
        self.assertIn(
            f"m {layout.panel_x} {separator_y} "
            f"l {layout.output_width - layout.panel_padding_x} {separator_y}",
            ass,
        )
        self.assertIn("Dialogue: 4,0:00:00.00", ass)
        bottom_y = (
            layout.output_height
            - chat_bottom_padding(layout)
            - chat_entry_height(entry, layout)
        )
        self.assertIn(rf"\pos({layout.panel_x},{bottom_y})", ass)
        self.assertIn("Alice", ass)
        self.assertIn("A message with (braces)", ass)
        author_color = ass_color_from_rgb(chat_author_color("Alice"))
        self.assertIn(
            rf"{{\b1\c{author_color}}}Alice"
            rf"{{\b0\c{ASS_CHAT_TEXT_COLOR}}}\NA message",
            ass,
        )
        self.assertNotIn(r"\\N", ass)
        self.assertEqual(format_ass_time(1.25), "0:00:01.25")

    def test_chat_author_colors_are_stable_and_readable(self) -> None:
        alice_color = chat_author_color("Alice")

        self.assertEqual(alice_color, chat_author_color(" alice "))
        self.assertGreaterEqual(len(CHAT_AUTHOR_COLORS), 36)
        self.assertEqual(len(set(CHAT_AUTHOR_COLORS)), len(CHAT_AUTHOR_COLORS))
        for red, green, blue in CHAT_AUTHOR_COLORS:
            luminance = 0.2126 * red + 0.7152 * green + 0.0722 * blue
            self.assertGreaterEqual(luminance, 130)
        self.assertIn(alice_color, CHAT_AUTHOR_COLORS)
        self.assertRegex(ass_color_from_rgb(alice_color), r"^&H[0-9A-F]{6}&$")

    def test_render_chat_ass_places_kirkland_time_in_header(self) -> None:
        timestamp_us = int(
            datetime(2026, 5, 17, 21, 45, tzinfo=timezone.utc).timestamp()
            * 1_000_000
        )
        layout = chat_layout_for_video(1280, 720)
        ass = render_chat_ass(
            [
                ChatEntry(
                    offset_seconds=0,
                    author="Alice",
                    message="hello",
                    timestamp_us=timestamp_us,
                )
            ],
            layout,
        )

        self.assertIn("2:45 PM", ass)
        self.assertIn("2:46 PM", ass)
        self.assertIn("Dialogue: 5,0:00:00.00,0:01:00.00", ass)
        self.assertNotIn("Kirkland, WA", ass)
        self.assertIn(
            rf"\an9\pos({layout.output_width - layout.panel_padding_x},{layout.title_y})",
            ass,
        )
        self.assertIn(rf"\fs{layout.title_font_size}\b1}}2:45 PM", ass)

    def test_render_chat_ass_tints_standard_unicode_emoji(self) -> None:
        ass = render_chat_ass(
            [
                ChatEntry(
                    offset_seconds=1.25,
                    author="Alice",
                    message="Tyler killin the channel 🔥",
                )
            ],
            chat_layout_for_video(1280, 720),
        )

        self.assertIn(
            rf"{{\c{ASS_EMOJI_COLORS['🔥']}}}🔥{{\c{ASS_CHAT_TEXT_COLOR}}}",
            ass,
        )

    def test_render_chat_panel_frame_uses_emoji_images(self) -> None:
        with TemporaryDirectory() as tmp:
            from PIL import Image

            emoji_path = Path(tmp) / "emoji.png"
            Image.new("RGBA", (24, 24), (255, 0, 0, 255)).save(emoji_path)
            layout = chat_layout_for_video(1280, 720)
            entry = ChatEntry(
                offset_seconds=1.25,
                author="Alice",
                message="hello 🔥",
                tokens=(
                    ChatToken("hello "),
                    ChatToken(
                        " 🔥 ",
                        image_url=emoji_path.as_uri(),
                        image_key="fire",
                        is_emoji=True,
                    ),
                ),
            )

            frame = render_chat_panel_frame(
                [(entry, 120)],
                layout,
                cache=EmojiImageCache(Path(tmp) / "cache"),
            )

        pixels = frame.load()
        red_pixels = sum(
            1
            for y in range(frame.height)
            for x in range(frame.width)
            if pixels[x, y][0] > 200 and pixels[x, y][1] < 60 and pixels[x, y][2] < 60
        )
        self.assertGreater(red_pixels, 20)

    def test_emoji_cache_animates_gif_frames(self) -> None:
        with TemporaryDirectory() as tmp:
            from PIL import Image

            gif_path = Path(tmp) / "animated.gif"
            first = Image.new("RGBA", (12, 12), (255, 0, 0, 255))
            second = Image.new("RGBA", (12, 12), (0, 0, 255, 255))
            first.save(
                gif_path,
                save_all=True,
                append_images=[second],
                duration=80,
                loop=0,
            )

            cache = EmojiImageCache(Path(tmp) / "cache")
            image = cache.get(gif_path.as_uri(), "animated")
            second_image = cache.get_frame(gif_path.as_uri(), "animated", at_seconds=0.09)

        self.assertIsNotNone(image)
        assert image is not None
        pixel = image.getpixel((3, 3))
        self.assertGreater(pixel[0], 200)
        self.assertLess(pixel[2], 80)
        self.assertIsNotNone(second_image)
        assert second_image is not None
        second_pixel = second_image.getpixel((3, 3))
        self.assertLess(second_pixel[0], 80)
        self.assertGreater(second_pixel[2], 200)

    def test_render_chat_panel_frame_uses_animation_time_for_emoji_images(self) -> None:
        with TemporaryDirectory() as tmp:
            from PIL import Image

            gif_path = Path(tmp) / "animated.gif"
            first = Image.new("RGBA", (24, 24), (255, 0, 0, 255))
            second = Image.new("RGBA", (24, 24), (0, 0, 255, 255))
            first.save(
                gif_path,
                save_all=True,
                append_images=[second],
                duration=80,
                loop=0,
            )
            layout = chat_layout_for_video(1280, 720)
            entry = ChatEntry(
                offset_seconds=1.25,
                author="Alice",
                message="hello fire",
                tokens=(
                    ChatToken("hello "),
                    ChatToken(
                        " fire ",
                        image_url=gif_path.as_uri(),
                        image_key="animated-fire",
                        is_emoji=True,
                    ),
                ),
            )
            cache = EmojiImageCache(Path(tmp) / "cache")

            first_frame = render_chat_panel_frame(
                [(entry, 120)],
                layout,
                cache=cache,
                animation_time=0.0,
            )
            second_frame = render_chat_panel_frame(
                [(entry, 120)],
                layout,
                cache=cache,
                animation_time=0.09,
            )

        first_pixels = first_frame.load()
        second_pixels = second_frame.load()
        first_red_pixels = sum(
            1
            for y in range(first_frame.height)
            for x in range(first_frame.width)
            if first_pixels[x, y][0] > 200
            and first_pixels[x, y][1] < 60
            and first_pixels[x, y][2] < 60
        )
        second_blue_pixels = sum(
            1
            for y in range(second_frame.height)
            for x in range(second_frame.width)
            if second_pixels[x, y][0] < 60
            and second_pixels[x, y][1] < 60
            and second_pixels[x, y][2] > 200
        )
        self.assertGreater(first_red_pixels, 20)
        self.assertGreater(second_blue_pixels, 20)

    def test_chat_panel_animation_segments_split_only_when_images_are_visible(self) -> None:
        text_entry = ChatEntry(offset_seconds=0.0, author="Alice", message="hello")
        image_entry = ChatEntry(
            offset_seconds=0.0,
            author="Bob",
            message="wave",
            tokens=(
                ChatToken(
                    " wave ",
                    image_url="https://example.test/wave.gif",
                    image_key="wave",
                    is_emoji=True,
                ),
            ),
        )

        text_segments = split_chat_panel_segments_for_animations(
            [(0.0, 1.0, [(text_entry, 120)])],
            frame_seconds=0.25,
        )
        image_segments = split_chat_panel_segments_for_animations(
            [(0.0, 1.0, [(image_entry, 120)])],
            frame_seconds=0.25,
        )

        self.assertEqual(text_segments, [(0.0, 1.0, [(text_entry, 120)])])
        self.assertEqual(len(image_segments), 4)
        self.assertEqual(image_segments[0][0], 0.0)
        self.assertEqual(image_segments[-1][1], 1.0)

    def test_chat_panel_animation_segments_use_actual_animated_images_only(self) -> None:
        image_entry = ChatEntry(
            offset_seconds=0.0,
            author="Bob",
            message="wave",
            tokens=(
                ChatToken(
                    " wave ",
                    image_url="https://example.test/wave.gif",
                    image_key="wave",
                    is_emoji=True,
                ),
            ),
        )

        static_segments = split_chat_panel_segments_for_animations(
            [(0.0, 1.0, [(image_entry, 120)])],
            animation_steps={},
        )
        animated_segments = split_chat_panel_segments_for_animations(
            [(0.0, 0.2, [(image_entry, 120)])],
            animation_steps={("https://example.test/wave.gif", "wave"): 1 / CHAT_PANEL_FPS},
        )

        self.assertEqual(static_segments, [(0.0, 1.0, [(image_entry, 120)])])
        self.assertEqual(len(animated_segments), 12)
        self.assertEqual(animated_segments[0][0], 0.0)
        self.assertAlmostEqual(animated_segments[-1][1], 0.2)

    def test_render_chat_panel_video_parallel_frames_keep_concat_order(self) -> None:
        class InlineProcessPoolExecutor:
            def __init__(
                self,
                *,
                max_workers: int,
                initializer: object,
                initargs: tuple[object, ...],
            ) -> None:
                captured["workers"] = max_workers
                self.initializer = initializer
                self.initargs = initargs

            def __enter__(self) -> "InlineProcessPoolExecutor":
                assert callable(self.initializer)
                self.initializer(*self.initargs)
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def submit(self, fn: object, *args: object) -> Future[int]:
                future: Future[int] = Future()
                try:
                    assert callable(fn)
                    future.set_result(fn(*args))
                except BaseException as exc:
                    future.set_exception(exc)
                return future

        captured: dict[str, object] = {}
        real_run = subprocess.run

        def fake_run(command: list[str], *args: object, **kwargs: object) -> object:
            if command and command[0] == "fc-match":
                return real_run(command, *args, **kwargs)

            concat_file = Path(command[command.index("-i") + 1])
            captured["concat"] = concat_file.read_text(encoding="utf-8")
            Path(command[-1]).write_bytes(b"panel video")

            class Result:
                returncode = 0
                stdout = b""
                stderr = b""

            return Result()

        with TemporaryDirectory() as tmp:
            output_file = Path(tmp) / "panel.mp4"
            layout = chat_layout_for_video(320, 180, panel_width=160)
            entries = [
                ChatEntry(offset_seconds=0.0, author="Alice", message="first"),
                ChatEntry(offset_seconds=1.0, author="Bob", message="second"),
            ]

            with (
                patch("onlysavemevods.chat_render.ProcessPoolExecutor", InlineProcessPoolExecutor),
                patch("onlysavemevods.chat_render.subprocess.run", side_effect=fake_run),
            ):
                render_chat_panel_video(
                    entries,
                    layout,
                    output_file,
                    duration_seconds=2.0,
                    panel_workers=2,
                )

            concat = str(captured["concat"])
            self.assertEqual(captured["workers"], 2)
            self.assertLess(concat.index("frame-000000.png"), concat.index("frame-000001.png"))
            self.assertEqual(output_file.read_bytes(), b"panel video")

    def test_panel_emoji_uses_fixed_30px_size(self) -> None:
        layout = chat_layout_for_video(1280, 720)
        fonts = load_chat_panel_fonts(layout)
        y = 120

        self.assertEqual(layout.font_size, CHAT_PANEL_TEXT_SIZE)
        self.assertEqual(panel_emoji_size(layout), CHAT_PANEL_EMOJI_SIZE)
        self.assertEqual(panel_line_height(layout), CHAT_PANEL_EMOJI_SIZE + 2)
        self.assertEqual(panel_emoji_y(y, layout, fonts), y + 1)
        _left, text_top, _right, text_bottom = fonts.regular.getbbox("Ag")
        text_midpoint = panel_text_y(y, layout, fonts.regular) + text_top + (text_bottom - text_top) / 2
        emoji_midpoint = panel_emoji_y(y, layout, fonts) + CHAT_PANEL_EMOJI_SIZE / 2
        self.assertLessEqual(abs(text_midpoint - emoji_midpoint), 1)

    def test_render_chat_panel_allows_long_messages_more_than_two_lines(self) -> None:
        layout = chat_layout_for_video(1280, 720)
        fonts = load_chat_panel_fonts(layout)
        entry = ChatEntry(
            offset_seconds=1.25,
            author="@JustJackie_YT",
            message=(
                "You dont need to try cause you are just so cool. You read books "
                "and smoke cigars you must watch Andrew Huberman and take notes"
            ),
        )

        lines = wrap_panel_message_lines(entry, layout, fonts)

        self.assertGreater(len(lines), 2)
        self.assertLessEqual(len(lines), CHAT_MESSAGE_MAX_LINES)
        rendered_text = "".join(item.text for line in lines for item in line)
        self.assertIn("Huberman", rendered_text)
        self.assertNotIn("...", rendered_text)

    def test_chat_stack_starts_at_bottom_and_pushes_messages_up(self) -> None:
        layout = chat_layout_for_video(1280, 720)
        entries = [
            ChatEntry(
                offset_seconds=float(index),
                author=f"Author {index}",
                message=f"Message {index}",
            )
            for index in range(30)
        ]

        stack = visible_chat_stack(entries, layout)
        ass = render_chat_ass(entries, layout)

        bottom_y = (
            layout.output_height
            - chat_bottom_padding(layout)
            - chat_entry_height(entries[-1], layout)
        )
        self.assertGreater(len(stack), layout.row_count)
        self.assertEqual(stack[-1], (entries[-1], bottom_y))
        self.assertLess(stack[0][1], layout.row_top)
        self.assertNotIn("Author 0", [entry.author for entry, _ in stack])
        self.assertIn(
            rf"{{\pos({layout.panel_x},{bottom_y})}}"
            rf"{{\b1\c{ass_color_from_rgb(chat_author_color('Author 29'))}}}"
            "Author 29",
            ass,
        )
        self.assertNotIn(
            r"0:00:29.00,168:00:29.00,Chat,,0,0,0,,"
            rf"{{\pos({layout.panel_x},{bottom_y})}}"
            rf"{{\b1\c{ass_color_from_rgb(chat_author_color('Author 0'))}}}"
            "Author 0",
            ass,
        )

    def test_two_line_chat_messages_reserve_extra_height(self) -> None:
        layout = chat_layout_for_video(1280, 720)
        short = ChatEntry(offset_seconds=1, author="Alice", message="short")
        wrapped = ChatEntry(
            offset_seconds=2,
            author="Bob",
            message="this is a long enough message to wrap onto a second display line",
        )

        self.assertGreater(
            chat_entry_height(wrapped, layout),
            chat_entry_height(short, layout),
        )

        stack = visible_chat_stack([short, wrapped], layout)
        self.assertEqual(stack[-1][0], wrapped)
        self.assertEqual(stack[0][0], short)
        self.assertEqual(
            stack[-1][1] - stack[0][1],
            chat_entry_height(short, layout) + chat_entry_gap(layout),
        )

    def test_chat_video_command_keeps_original_file_separate(self) -> None:
        media_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID].mp4")
        ass_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID] - chat.ass")
        output_file = chat_video_output_file(media_file)
        layout = chat_layout_for_video(1280, 720)

        command = build_chat_video_command(
            "ffmpeg",
            media_file,
            ass_file,
            output_file,
            layout,
        )

        self.assertEqual(output_file, media_file.with_name("Live [ID] - chat.mp4"))
        self.assertEqual(command[0], "ffmpeg")
        self.assertIn(str(media_file), command)
        self.assertIn(str(output_file), command)
        self.assertIn("-filter_complex", command)
        filter_complex = command[command.index("-filter_complex") + 1]
        self.assertNotIn("scale=", filter_complex)
        self.assertIn("pad=1280:720:0:0:black", filter_complex)
        self.assertIn("color=c=0x111820:s=480x720", filter_complex)
        self.assertIn("hstack=inputs=2", filter_complex)
        self.assertIn("subtitles=filename=", filter_complex)
        self.assertIn("libx264", command)
        self.assertNotIn("h264_nvenc", command)

    def test_chat_panel_merge_command_keeps_source_video_size(self) -> None:
        media_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID].mp4")
        panel_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID].panel.mp4")
        output_file = chat_video_output_file(media_file)
        layout = chat_layout_for_video(1920, 1080)

        command = build_chat_panel_merge_command(
            "ffmpeg",
            media_file,
            panel_file,
            output_file,
            layout,
        )

        self.assertIn(str(media_file), command)
        self.assertIn(str(panel_file), command)
        filter_complex = command[command.index("-filter_complex") + 1]
        self.assertIn("pad=1920:1080:0:0:black", filter_complex)
        self.assertIn("hstack=inputs=2", filter_complex)
        self.assertNotIn("subtitles=filename=", filter_complex)

    def test_chat_video_command_can_use_nvenc_device(self) -> None:
        media_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID].mp4")
        ass_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID] - chat.ass")
        output_file = chat_video_output_file(media_file)

        command = build_chat_video_command(
            "ffmpeg",
            media_file,
            ass_file,
            output_file,
            use_nvenc=True,
            nvenc_device="0",
        )

        self.assertIn("h264_nvenc", command)
        self.assertIn("-cq", command)
        self.assertEqual(command[command.index("-cq") + 1], "23")
        self.assertIn("-gpu", command)
        self.assertEqual(command[command.index("-gpu") + 1], "0")
        self.assertNotIn("libx264", command)
        self.assertNotIn("-crf", command)

    def test_chat_panel_merge_command_can_use_nvenc_device(self) -> None:
        media_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID].mp4")
        panel_file = Path("/downloads/Example/LIVEVIDEO01/Live [ID].panel.mp4")
        output_file = chat_video_output_file(media_file)

        command = build_chat_panel_merge_command(
            "ffmpeg",
            media_file,
            panel_file,
            output_file,
            use_nvenc=True,
            nvenc_device="1",
        )

        self.assertIn("h264_nvenc", command)
        self.assertEqual(command[command.index("-cq") + 1], "23")
        self.assertEqual(command[command.index("-gpu") + 1], "1")

    def test_render_chat_video_file_overwrite_replaces_existing_output(self) -> None:
        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [ID].mp4"
            chat_file = Path(tmp) / "Live [ID].live_chat.json"
            output_file = chat_video_output_file(media_file)
            media_file.write_text("media", encoding="utf-8")
            output_file.write_text("old chat render", encoding="utf-8")
            chat_file.write_text(
                json.dumps(
                    {
                        "replayChatItemAction": {
                            "videoOffsetTimeMsec": "0",
                            "actions": [
                                {
                                    "addChatItemAction": {
                                        "item": {
                                            "liveChatTextMessageRenderer": {
                                                "timestampUsec": "1779054300000000",
                                                "authorName": {"simpleText": "Alice"},
                                                "message": {"simpleText": "hello"},
                                            }
                                        }
                                    }
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            class FakeProcess:
                returncode: int | None = None

                def __init__(self, command: list[str], **_kwargs: object) -> None:
                    self.command = command

                def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
                    self.returncode = 0
                    Path(self.command[-1]).write_text("new chat render", encoding="utf-8")
                    return b"", b""

            with (
                patch(
                    "onlysavemevods.chat_render.probe_video_dimensions",
                    side_effect=VideoProbeError("no probe"),
                ),
                patch("onlysavemevods.chat_render.LOGGER.exception"),
                patch("onlysavemevods.chat_render.subprocess.Popen", FakeProcess),
            ):
                result = render_chat_video_file(
                    media_file,
                    chat_file,
                    output_file=output_file,
                    overwrite=True,
                )

            self.assertEqual(result, output_file)
            self.assertEqual(output_file.read_text(encoding="utf-8"), "new chat render")

    def test_render_chat_video_file_keeps_waiting_while_output_grows(self) -> None:
        with TemporaryDirectory() as tmp:
            media_file = Path(tmp) / "Live [ID].mp4"
            chat_file = Path(tmp) / "Live [ID].live_chat.json"
            output_file = chat_video_output_file(media_file)
            media_file.write_text("media", encoding="utf-8")
            chat_file.write_text(
                json.dumps(
                    {
                        "replayChatItemAction": {
                            "videoOffsetTimeMsec": "0",
                            "actions": [
                                {
                                    "addChatItemAction": {
                                        "item": {
                                            "liveChatTextMessageRenderer": {
                                                "timestampUsec": "1779054300000000",
                                                "authorName": {"simpleText": "Alice"},
                                                "message": {"simpleText": "hello"},
                                            }
                                        }
                                    }
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            class FakeProcess:
                returncode: int | None = None

                def __init__(self, command: list[str], **_kwargs: object) -> None:
                    self.command = command
                    self.calls = 0

                def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
                    self.calls += 1
                    if self.calls == 1:
                        Path(self.command[-1]).write_bytes(b"partial output")
                        raise subprocess.TimeoutExpired(cmd=self.command, timeout=timeout)
                    self.returncode = 0
                    Path(self.command[-1]).write_bytes(b"finished output")
                    return b"", b""

                def kill(self) -> None:
                    raise AssertionError("active ffmpeg render should not be killed")

            with (
                patch(
                    "onlysavemevods.chat_render.probe_video_dimensions",
                    side_effect=VideoProbeError("no probe"),
                ),
                patch("onlysavemevods.chat_render.LOGGER.exception"),
                patch("onlysavemevods.chat_render.subprocess.Popen", FakeProcess),
                patch("onlysavemevods.chat_render.FFMPEG_OUTPUT_PROGRESS_POLL_SECONDS", 0.1),
            ):
                result = render_chat_video_file(
                    media_file,
                    chat_file,
                    output_file=output_file,
                    timeout_seconds=0.1,
                    overwrite=True,
                )

            self.assertEqual(result, output_file)
            self.assertEqual(output_file.read_bytes(), b"finished output")
