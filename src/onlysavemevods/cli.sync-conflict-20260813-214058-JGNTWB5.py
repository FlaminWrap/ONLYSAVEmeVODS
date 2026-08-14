from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

from . import __version__ as APP_VERSION
from .chat_render import (
    choose_chat_render_nvenc_device,
    log_nvenc_environment,
    render_chat_video_file,
)
from .config import (
    ConfigError,
    append_missing_config_values,
    load_config,
    monitored_sources,
    update_config_values,
)
from .daemon import OnlySaveMeVodsDaemon
from .log_buffer import RingBufferLogHandler
from .transcription import voice_detection_mode, voice_detection_speaker_summary
from .web import StatusWebServer
from .state import StateStore
from .watermark import (
    WATERMARK_STATUS_DONE,
    WatermarkError,
    detect_watermark,
    detection_result_to_dict,
    format_detection_text,
    require_watermark_secret,
)
from .youtube import YoutubeProbe, YtDlpRunner


VOICE_DETECTION_MODES = ("off", "auto", "range", "fixed")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    try:
        if args.command == "check":
            return check_command(args)
        if args.command == "update-config":
            return update_config_command(args)
        if args.command == "render-chat-file":
            return render_chat_file_command(args)
        if args.command == "detect-watermark":
            return detect_watermark_command(args)
        if args.command == "voice-detection":
            return voice_detection_command(args)
        if args.command == "run":
            return run_command(args)
        if args.command == "web":
            return web_command(args)
    except ConfigError as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 2
    except KeyboardInterrupt:
        return 130

    parser.print_help()
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="onlysavemevods",
        description="Automatically download live YouTube streams with yt-dlp.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase log verbosity.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {APP_VERSION}",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="Check configured channels and streamer sources once.")
    check.add_argument("--config", default="config.toml", help="Path to config TOML.")

    update_config = subparsers.add_parser(
        "update-config",
        help="Append missing settings from config.example.toml without overwriting existing values.",
    )
    update_config.add_argument("--config", default="config.toml", help="Path to config TOML.")
    update_config.add_argument(
        "--defaults",
        default="config.example.toml",
        help="Path to default config TOML to merge from.",
    )

    run = subparsers.add_parser("run", help="Run the continuous daemon.")
    run.add_argument("--config", default="config.toml", help="Path to config TOML.")

    render_chat = subparsers.add_parser("render-chat-file", help=argparse.SUPPRESS)
    render_chat.add_argument("--config", required=True, help="Path to config TOML.")
    render_chat.add_argument("--media", required=True, help="Finalized media file.")
    render_chat.add_argument("--chat", required=True, help="Live chat JSON file.")
    render_chat.add_argument("--output", required=True, help="Output chat video file.")
    render_chat.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace output if it already exists.",
    )

    detect_watermark_parser = subparsers.add_parser(
        "detect-watermark",
        help="Detect a private watermark in a suspect video file.",
    )
    detect_watermark_parser.add_argument("--config", default="config.toml", help="Path to config TOML.")
    detect_watermark_parser.add_argument("--media", required=True, help="Suspect media file.")
    detect_watermark_parser.add_argument(
        "--json",
        action="store_true",
        help="Write the detection result as JSON.",
    )

    voice_detection = subparsers.add_parser(
        "voice-detection",
        help="Show or update transcription voice detection settings.",
    )
    voice_detection_subparsers = voice_detection.add_subparsers(
        dest="voice_detection_action",
        required=True,
    )
    voice_detection_show = voice_detection_subparsers.add_parser(
        "show",
        help="Show current transcription voice detection settings.",
    )
    voice_detection_show.add_argument(
        "--config",
        default="config.toml",
        help="Path to config TOML.",
    )
    voice_detection_set = voice_detection_subparsers.add_parser(
        "set",
        help="Update transcription voice detection settings in config.toml.",
    )
    voice_detection_set.add_argument(
        "--config",
        default="config.toml",
        help="Path to config TOML.",
    )
    voice_detection_set.add_argument(
        "--mode",
        required=True,
        choices=VOICE_DETECTION_MODES,
        help="Voice detection mode: off, auto, range, or fixed.",
    )
    voice_detection_set.add_argument(
        "--min-speakers",
        type=positive_int_arg,
        help="Minimum speaker count for range mode.",
    )
    voice_detection_set.add_argument(
        "--max-speakers",
        type=positive_int_arg,
        help="Maximum speaker count for range mode.",
    )
    voice_detection_set.add_argument(
        "--speakers",
        type=positive_int_arg,
        help="Exact speaker count for fixed mode.",
    )
    voice_detection_set.add_argument(
        "--hf-token-env",
        help="Environment variable name containing the Hugging Face token.",
    )

    web = subparsers.add_parser("web", help="Run the read-only status web interface.")
    web.add_argument("--config", default="config.toml", help="Path to config TOML.")
    web.add_argument("--host", help="Host/IP to bind. Defaults to config web_host.")
    web.add_argument("--port", type=int, help="Port to bind. Defaults to config web_port.")

    return parser


def positive_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def validate_env_var_name(value: str) -> str:
    if not value:
        raise ConfigError("--hf-token-env must be a non-empty environment variable name")
    if value.startswith("hf_"):
        raise ConfigError(
            "--hf-token-env must be an environment variable name, not a token value"
        )
    first = value[0]
    if not (first.isalpha() or first == "_"):
        raise ConfigError("--hf-token-env must start with a letter or underscore")
    if not all(char.isalnum() or char == "_" for char in value):
        raise ConfigError("--hf-token-env may contain only letters, digits, and underscores")
    return value


def configure_logging(verbose: int, log_level: str = "INFO") -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    if verbose >= 1:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    buffer_handler = RingBufferLogHandler()
    buffer_handler.setLevel(logging.DEBUG)
    buffer_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logging.getLogger().addHandler(buffer_handler)


def check_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    configure_logging(args.verbose, config.log_level)
    probe = YoutubeProbe(
        YtDlpRunner(config.yt_dlp_path),
        channel_scan_limit=config.channel_scan_limit,
        discovery_probe_concurrency=config.discovery_probe_concurrency,
    )
    found = 0
    failures = 0

    sources = monitored_sources(config)
    if not sources:
        print("No channels or streamers configured.")
        return 0

    for channel in sources:
        print(f"Checking {channel}")
        try:
            streams = probe.discover_channel_live_streams(channel)
        except Exception as exc:
            failures += 1
            print(f"  ERROR: {exc}")
            continue

        if not streams:
            print("  No live streams detected.")
            continue

        for stream in streams:
            found += 1
            title = f" - {stream.title}" if stream.title else ""
            print(f"  LIVE {stream.video_id}{title}")
            print(f"       {stream.url}")

    if failures and not found:
        return 1
    return 0


def update_config_command(args: argparse.Namespace) -> int:
    added = append_missing_config_values(args.config, args.defaults)
    if added:
        print(f"Added missing config settings: {', '.join(added)}")
    else:
        print("Config already has all default settings.")
    return 0


def render_chat_file_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    configure_logging(args.verbose, config.log_level)
    output_file = Path(args.output)
    nvenc_device = choose_chat_render_nvenc_device(
        config.chat_render_nvenc_devices,
        output_file,
    )
    try:
        render_chat_video_file(
            Path(args.media),
            Path(args.chat),
            ffmpeg_path=config.ffmpeg_path,
            output_file=output_file,
            overwrite=args.overwrite,
            panel_workers=config.chat_render_panel_workers,
            use_nvenc=config.chat_render_use_nvenc,
            nvenc_device=nvenc_device,
        )
    except Exception:  # noqa: BLE001 - command should return a process failure.
        logging.getLogger(__name__).exception(
            "Unable to render chat video media=%s chat=%s output=%s",
            args.media,
            args.chat,
            args.output,
        )
        return 1
    return 0


def detect_watermark_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    configure_logging(args.verbose, config.log_level)
    media_file = Path(args.media)
    if not media_file.is_file():
        print(f"Suspect media file does not exist: {media_file}", file=sys.stderr)
        return 2

    try:
        secret = require_watermark_secret(config)
        state = StateStore(config.db_path)
        try:
            records = state.list_watermark_copies(
                statuses=[WATERMARK_STATUS_DONE],
                limit=5000,
            )
        finally:
            state.close()
        result = detect_watermark(
            media_file=media_file,
            records=records,
            secret=secret,
        )
    except WatermarkError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(detection_result_to_dict(result), indent=2, sort_keys=True))
    else:
        print(format_detection_text(result))
    return 0 if result.matched else 1


def voice_detection_command(args: argparse.Namespace) -> int:
    if args.voice_detection_action == "show":
        return voice_detection_show_command(args)
    if args.voice_detection_action == "set":
        return voice_detection_set_command(args)
    raise ConfigError("Unknown voice detection action")


def voice_detection_show_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    print(f"Voice detection: {voice_detection_mode(config)}")
    print(f"Speaker count: {voice_detection_speaker_summary(config)}")
    print(f"WhisperX diarization: {'enabled' if config.whisperx_diarize else 'disabled'}")
    if config.whisperx_hf_token_env:
        status = (
            "set"
            if os.environ.get(config.whisperx_hf_token_env, "").strip()
            else "not set"
        )
        print(f"Hugging Face token env: {config.whisperx_hf_token_env} ({status})")
    else:
        print("Hugging Face token env: disabled")
    return 0


def voice_detection_set_command(args: argparse.Namespace) -> int:
    updates = voice_detection_updates(args)
    changed = update_config_values(args.config, updates)
    config = load_config(args.config)
    if changed:
        print(f"Updated voice detection settings: {', '.join(changed)}")
    else:
        print("Voice detection settings already match requested values.")
    print(f"Voice detection: {voice_detection_mode(config)}")
    print(f"Speaker count: {voice_detection_speaker_summary(config)}")
    return 0


def voice_detection_updates(args: argparse.Namespace) -> dict[str, object]:
    updates: dict[str, object] = {}
    mode = args.mode
    if mode in {"off", "auto"} and (
        args.min_speakers is not None
        or args.max_speakers is not None
        or args.speakers is not None
    ):
        raise ConfigError(f"--mode {mode} does not accept speaker count options")

    if mode == "off":
        updates.update(
            whisperx_diarize=False,
            whisperx_min_speakers=0,
            whisperx_max_speakers=0,
        )
    elif mode == "auto":
        updates.update(
            whisperx_diarize=True,
            whisperx_min_speakers=0,
            whisperx_max_speakers=0,
        )
    elif mode == "fixed":
        if args.speakers is None:
            raise ConfigError("--mode fixed requires --speakers")
        if args.min_speakers is not None or args.max_speakers is not None:
            raise ConfigError(
                "--mode fixed uses --speakers, not --min-speakers or --max-speakers"
            )
        updates.update(
            whisperx_diarize=True,
            whisperx_min_speakers=args.speakers,
            whisperx_max_speakers=args.speakers,
        )
    elif mode == "range":
        if args.speakers is not None:
            raise ConfigError(
                "--mode range uses --min-speakers and/or --max-speakers, not --speakers"
            )
        min_speakers = args.min_speakers or 0
        max_speakers = args.max_speakers or 0
        if not min_speakers and not max_speakers:
            raise ConfigError(
                "--mode range requires --min-speakers and/or --max-speakers"
            )
        if min_speakers and max_speakers and min_speakers > max_speakers:
            raise ConfigError("--min-speakers must be less than or equal to --max-speakers")
        updates.update(
            whisperx_diarize=True,
            whisperx_min_speakers=min_speakers,
            whisperx_max_speakers=max_speakers,
        )
    else:
        raise ConfigError(f"Unknown voice detection mode: {mode}")

    if args.hf_token_env is not None:
        updates["whisperx_hf_token_env"] = validate_env_var_name(args.hf_token_env)
    return updates


def run_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    configure_logging(args.verbose, config.log_level)
    daemon = OnlySaveMeVodsDaemon(config)

    async def runner() -> None:
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, daemon.stop)
            except NotImplementedError:
                pass
        await daemon.run()

    asyncio.run(runner())
    return 0


def web_command(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    configure_logging(args.verbose, config.log_level)
    if config.render_live_chat_video or config.chat_render_use_nvenc:
        log_nvenc_environment(config.ffmpeg_path, config.chat_render_use_nvenc)
    server = StatusWebServer(config, host=args.host, port=args.port)
    server.start()
    try:
        while True:
            time.sleep(3600)
    finally:
        server.stop()


if __name__ == "__main__":
    sys.exit(main())
