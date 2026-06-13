# ONLYSAVEmeVODS

ONLYSAVEmeVODS watches YouTube channel stream pages and starts `yt-dlp` for every live
stream it finds. It supports multiple simultaneous streams from the same
channel and treats a `yt-dlp` exit as uncertain until YouTube has been checked
for the configured post-exit window.

## Quick Start

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e .
cp config.example.toml config.toml
```

Edit `config.toml`, then run a one-shot check:

```bash
.venv/bin/onlysavemevods check --config config.toml
```

Run continuously:

```bash
.venv/bin/onlysavemevods run --config config.toml
```

The daemon also starts a read-only local dashboard when `web_enabled = true`.
It shows stream status, storage totals, attention signals, and current segment
files. Channel and log tabs summarize the configured channels being archived
and recent in-process service logs, and the config tab shows the currently
loaded settings with sensitive yt-dlp arguments redacted. Finalized media files
and sidecars such as live chat and subtitles can be downloaded from the dashboard:

```text
http://127.0.0.1:8080/
http://127.0.0.1:8080/status.json
```

To run only the status page against an existing state database:

```bash
.venv/bin/onlysavemevods web --config config.toml
```

## Systemd

Install and enable the system service:

```bash
scripts/install-systemd.sh
```

The installer enables and restarts `onlysavemevods.service`, so rerunning it after an
update makes systemd pick up the newly installed code. It also appends any
missing top-level settings from the current `config.example.toml` to an existing
`config.toml` without overwriting your configured values.

By default the systemd installer deploys to `/opt/onlysavemevods`, creates a dedicated
`onlysavemevods` system user, and runs the service as that user instead of as root or
your login account. The application code, venv, and Deno runtime are root-owned;
only `downloads/`, `state/`, and `.cache/` are writable by the service user.
If the installer is run with no explicit install dir and detects an existing
legacy `/opt/ytdlbot` install and `/opt/onlysavemevods` does not already exist,
it stops/disables/removes `ytdlbot.service`, moves `/opt/ytdlbot` to
`/opt/onlysavemevods`, fixes writable directory ownership for the new service
user, and starts `onlysavemevods.service` against the existing config, state,
downloads, cache, venv, and Deno directories. If both install directories
already exist, it leaves `/opt/ytdlbot` untouched and uses `/opt/onlysavemevods`.

On AlmaLinux/RHEL-like systems, the installer will use `dnf` to install OS
dependencies where possible, including Python 3.11+, FFmpeg, DejaVu Sans fonts,
EPEL, and RPM Fusion. If NVIDIA PCI hardware is detected, it also enables RPM
Fusion nonfree and attempts to install the NVIDIA driver/CUDA runtime packages
needed for FFmpeg NVENC (`akmod-nvidia` and `xorg-x11-drv-nvidia-cuda`) only
when FFmpeg does not already advertise NVENC encoders and `nvidia-smi` is not
already installed. If FFmpeg already lists NVENC or `nvidia-smi` exists, the
installer treats the driver stack as user-managed and leaves NVIDIA driver
packages unchanged. When `nvidia-smi` exists but FFmpeg lacks `h264_nvenc`, the
installer may refresh FFmpeg and swap `ffmpeg-free` for the full RPM Fusion
`ffmpeg` package. DejaVu Sans is used by the rendered live chat panel for
consistent text layout. It installs `yt-dlp[default]` into the project venv, so
a system `yt-dlp` package is not required. It also installs a project-local Deno
runtime under `.deno/` because yt-dlp's current YouTube support uses EJS
challenge solver scripts with an external JavaScript runtime. If
`transcribe_subtitles = true` is set in `config.toml`, the installer also
installs `whisperx` into the project venv. Set `ONLYSAVEMEVODS_INSTALL_WHISPERX=1` to
force that install or `ONLYSAVEMEVODS_INSTALL_WHISPERX=0` to skip it.

To install somewhere other than `/opt/onlysavemevods`:

```bash
ONLYSAVEMEVODS_INSTALL_DIR=/srv/onlysavemevods scripts/install-systemd.sh
```

To skip OS package installation and only use what is already present:

```bash
ONLYSAVEMEVODS_SKIP_OS_DEPS=1 scripts/install-systemd.sh
```

To install OS packages but skip NVIDIA driver/NVENC package installation:

```bash
ONLYSAVEMEVODS_SKIP_NVIDIA_DEPS=1 scripts/install-systemd.sh
```

To skip Deno installation because you already provide a supported runtime on
`PATH`:

```bash
ONLYSAVEMEVODS_SKIP_DENO=1 scripts/install-systemd.sh
```

To install WhisperX even before transcription is enabled in `config.toml`:

```bash
ONLYSAVEMEVODS_INSTALL_WHISPERX=1 scripts/install-systemd.sh
```

To update a config file manually without changing existing values:

```bash
.venv/bin/onlysavemevods update-config --config config.toml --defaults config.example.toml
```

The old `ytdlbot` command, `python -m ytdlbot`, and `YTDLBOT_*` installer
environment variables remain available as compatibility aliases. Fresh installs
use the `onlysavemevods` names by default.
If installer output still says it installed `ytdlbot.service` or built
`ytdlbot==0.1.0`, the old installer script was run; run this updated
`scripts/install-systemd.sh` instead.

Inspect it:

```bash
sudo systemctl status onlysavemevods.service
journalctl -u onlysavemevods.service -f
```

Set `log_level = "DEBUG"` in `config.toml` and restart the service when you need
more detail about channel discovery, post-exit probes, resume decisions, and
finalization.

On the default config, the status page is available on the host at
`http://127.0.0.1:8080/`.

Uninstall the service without deleting config, state, or downloads:

```bash
scripts/uninstall-systemd.sh
```

## Notes

- Public YouTube streams only; no cookies are configured by default.
- Logging defaults to `INFO`. Set `log_level = "DEBUG"` in `config.toml`, or add
  `-v` when running the CLI manually, for verbose diagnostics.
- Dashboard downloads are limited to finalized files for streams already present
  in the state database, plus finalized live chat and subtitle sidecars.
  `.part`, fragment, and `.ytdl` files are not served.
- Discovery checks each channel's `/live` page first, then scans up to
  `channel_scan_limit` recent stream entries with up to
  `discovery_probe_concurrency` yt-dlp probes at once.
- New downloads are stored under `download_dir/<channel>/<video_id>/`. Existing
  in-progress `download_dir/<video_id>/` folders are reused so resumable partials
  are not abandoned after an update.
- Active and resumable files use stable `segment-001.*` names. When a stream is
  finally marked ended, finalized segment files are renamed to the video title
  and ID, for example `Live Title [VIDEOID].mp4`. If a continuation file is
  needed, it uses `Live Title [VIDEOID] - part 002`.
- Downloads use `--live-from-start`, `--continue`, `--part`, `--keep-fragments`,
  and `--no-playlist` by default. Keeping fragments costs extra disk space while
  a segment is active, but it gives the bot enough state to resume a format that
  yt-dlp accidentally finalized.
- Set `record_live_chat = true` to ask yt-dlp to write YouTube live chat with
  `--write-subs --sub-langs live_chat`. The bot keeps it as a sidecar
  `.live_chat.json` file and renames it with the finalized stream title and ID
  when the stream is marked ended. Chat is recorded by a separate yt-dlp sidecar
  process so the main download can start video/audio immediately instead of
  waiting behind live chat fragments. When live chat is enabled and no custom
  format is configured, the media process also passes
  `--format bestvideo*+bestaudio/best` so media download remains explicit.
- Set `render_live_chat_video = true` to also create a separate
  `Title [VIDEOID] - chat.mp4` after the stream ends. The original finalized
  media file is left untouched; the chat version is re-encoded with the video on
  the left and a rendered chat panel on the right. New chat messages appear at
  the bottom, older messages move upward, and messages leave only when pushed
  off the panel by newer chat. Emoji images referenced by the live chat JSON are
  cached locally and rendered into the panel when available. This option implies
  live chat recording. Set `chat_render_panel_workers` to control Python/Pillow
  panel frame workers: `0` uses all CPU cores, `1` renders serially, and higher
  values use that exact worker count. Set `chat_render_use_nvenc = true` to use
  NVIDIA NVENC for the FFmpeg chat video encode/merge stages. Leave
  `chat_render_nvenc_devices = []` to use FFmpeg's default GPU, set
  `chat_render_nvenc_devices = ["0"]` to pick one GPU, or set
  `chat_render_nvenc_devices = ["0", "1"]` to rotate chat renders across
  multiple GPUs. The systemd installer can install NVIDIA/NVENC packages on
  supported DNF systems when NVIDIA PCI hardware is detected. At runtime,
  ONLYSAVEmeVODS only detects NVIDIA GPUs and FFmpeg NVENC support and logs what it
  finds.
- Set `transcribe_subtitles = true` to run WhisperX after each stream is
  finalized. It writes speech subtitle/transcript sidecars next to the media
  file: `.srt`, `.vtt`, `.txt`, `.tsv`, and `.json`. The systemd installer
  installs WhisperX automatically when this setting is enabled; for manual
  installs, install it in the runtime environment and set `whisperx_path` if it
  is not on `PATH`. The defaults target an NVIDIA GPU
  with `whisperx_device = "cuda"`, `whisperx_model = "large-v3"`, and
  `whisperx_compute_type = "float16"`. Leave
  `transcription_max_concurrent = 1` for a single GPU. The systemd service
  stores Hugging Face, NLTK, and Matplotlib runtime caches under
  `/opt/onlysavemevods/.cache`.
- `whisperx_diarize = true` asks WhisperX/pyannote to label speakers as
  `SPEAKER_00`, `SPEAKER_01`, and so on. Diarization usually needs a Hugging
  Face token with the relevant pyannote model terms accepted; set the token in
  the environment variable named by `whisperx_hf_token_env` (`HF_TOKEN` by
  default). These labels identify recurring voices within a stream, but they do
  not prove real names. For regular casts, add a later name-resolution layer
  using known voice samples or transcript/context hints.
- The dashboard shows `Transcribe` for finalized media without subtitles and
  `Retranscribe` when `.srt`/`.vtt` sidecars already exist. Retranscription
  replaces only the WhisperX subtitle/transcript sidecars for that media file.
- Set `shot_audit_enabled = true` and `shot_audit_auto_run = true` to create
  machine-estimated donation/shot audit projects after a stream is finalized,
  chat is refreshed/rendered, and transcription is complete. The audit samples
  video frames, uses OCR to read on-screen donation alert/message overlays, not
  live chat messages, applies configurable shot rules, uses transcript
  heuristics for likely consumed shots, and can flag visual candidates in
  donation-following review windows for possible pouring/drinking. Tesseract is
  the default OCR backend; `shot_audit_ocr_backend = "paddleocr"` can use a
  PaddleOCR GPU install. The default visual backend is lightweight motion
  scoring; `shot_audit_vision_backend = "yolo_pose"` can use Ultralytics pose
  models on a configured CPU/CUDA device. The Python-side optional packages are
  exposed as extras: `.[shot-audit-ocr]`, `.[shot-audit-vision]`, or
  `.[shot-audit-gpu]`; install the matching PaddlePaddle CUDA wheel separately
  if your OCR backend needs GPU acceleration. Projects are saved under
  `state/shot-audits/` and appear in the dashboard's Shot Audit workspace,
  where you can attach multiple finalized media files to one project, jump from
  donation or consumed rows to the matching video timestamp, add or remove
  visible-shot corrections per media file, delete saved audit projects without
  deleting downloaded media, and download Markdown, JSON, review CSV, or parody
  reports. These reports are review aids, not legal proof.
- Set `watermark_enabled = true` to enable private, per-recipient invisible
  video watermark copies from the dashboard. Originals are left untouched.
  Before queueing or detecting copies, set the environment variable named by
  `watermark_secret_env` (default `ONLYSAVEMEVODS_WATERMARK_SECRET`) to a long random
  secret and keep it with your backups; detection requires both the secret and
  the SQLite copy records. One easy way to generate one is:

  ```bash
  export ONLYSAVEMEVODS_WATERMARK_SECRET="$(openssl rand -base64 48)"
  ```

  The systemd installer creates `${ONLYSAVEMEVODS_INSTALL_DIR:-/opt/onlysavemevods}/secrets.env`
  with a generated `ONLYSAVEMEVODS_WATERMARK_SECRET` if that file does not already
  exist, and the service loads it with `EnvironmentFile`. Back up this file with
  `config.toml` and `state/onlysavemevods.sqlite3`; losing the secret means old
  watermark copies cannot be detected. If the new default secret variable is
  unset, the app also checks the legacy `YTDLBOT_WATERMARK_SECRET` variable. To
  create or rotate it manually, put the generated value in that persistent
  environment file:

  ```ini
  ONLYSAVEMEVODS_WATERMARK_SECRET=replace-with-the-generated-secret
  ```

  Watermarked copies are written below each stream
  folder in `.watermarks/` and are served through a separate dashboard link.
  The detector is available from the dashboard and as
  `.venv/bin/onlysavemevods detect-watermark --config config.toml --media suspect.mp4`.
  Use a video slice when possible: 30-120 seconds is best, 10-30 seconds is
  usually enough, and screenshots are not supported for confident attribution.
- Planned reconnects are disabled by default with `reconnect_interval_seconds =
  0`. Set it above `0` only if you also want periodic forced reconnects after
  yt-dlp progress shows all active format downloads have caught up to the live
  edge. Planned reconnects terminate yt-dlp without graceful finalization,
  leaving `.part` files in place for `--continue`.
- Once the post-exit checks decide a stream has ended, leftover `.part` format
  files are finalized with FFmpeg and temporary `.ytdl`/fragment files are
  removed.
- If YouTube reports the video as private, removed, deleted, or otherwise
  terminally unavailable during a post-exit check, the bot stops checking and
  marks the stream ended immediately.
- If one format, such as audio, reaches the live edge and finalizes before the
  other format, a watchdog cuts that mixed segment quickly. When kept fragments
  are available, the bot turns the finalized format back into a resumable
  `.part` file and restarts the same segment with `--live-from-start` instead
  of jumping to the live edge. Once the stream is truly ended, mixed leftovers
  are muxed to the shortest track.
- `-k` is not used by default. Add it to `extra_yt_dlp_args` only if you want
  to debug or keep post-processing intermediates.
- `extra_yt_dlp_args` cannot include metadata-only or download-suppression flags
  such as `--skip-download`, `--simulate`, or `--dump-json`; those belong only
  in the bot's internal probe commands.
- Active live downloads do not use `--download-archive`; SQLite state prevents
  duplicate active processes without accidentally blocking a false-exit restart.
- The systemd installer only removes/replaces the root-owned app copy during
  updates; uninstalling the service leaves config, downloads, state, venv, Deno,
  and the `onlysavemevods` user in place.
- If `state/onlysavemevods.sqlite3` does not exist but the legacy
  `state/ytdlbot.sqlite3` file does, ONLYSAVEmeVODS uses the legacy database so
  existing state survives the rename.
