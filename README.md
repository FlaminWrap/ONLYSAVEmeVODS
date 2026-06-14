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

For a streamer with multiple channels, group them in `config.toml` so shared
settings only need to be maintained once:

```toml
[streamers."OUMB3rd"]
sources = ["@OUMB3rd", "https://www.youtube.com/@OUMB3rdVODS"]
download_dir_name = "OUMB3rd"

[streamers."OUMB3rd".voice_detection]
mode = "fixed"
speakers = 2

[streamers."OUMB3rd".speaker_labels]
SPEAKER_00 = "OUMB3rd"
SPEAKER_01 = "Guest"
```

The top-level `channels = [...]` list still works for simple setups. Streamer
`sources` currently use the same YouTube channel URL or `@handle` inputs as
`channels`.

Run continuously:

```bash
.venv/bin/onlysavemevods run --config config.toml
```

The daemon also starts a local dashboard when `web_enabled = true`. It shows
streamer cards with grouped sources, storage totals, attention signals, current
segment files, per-stream file/log/job tabs, and dashboard-triggered processing
work with phase and progress. Each streamer card has Settings and Voices actions
for shared source settings, voice detection, known voice samples, and speaker
attribution review. The About tab shows the app version and runtime details, and
the Config tab can save app settings back to `config.toml` while keeping
sensitive yt-dlp arguments redacted. Bind address and port changes are saved for
the next restart. Finalized media files and sidecars such as live chat and
subtitles can be downloaded from the dashboard:

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
scripts/install-almalinux.sh
```

On Debian or Ubuntu, you can use the distro-named entrypoints instead:

```bash
scripts/install-debian.sh
scripts/install-ubuntu.sh
```

The installer enables and restarts `onlysavemevods.service`, so rerunning it after an
update makes systemd pick up the newly installed code. It also appends any
missing top-level settings from the current `config.example.toml` to an existing
`config.toml` without overwriting your configured values.

By default the systemd installer deploys to `/opt/onlysavemevods`, creates a dedicated
`onlysavemevods` system user, and runs the service as that user instead of as root or
your login account. The application code, venv, and Deno runtime are root-owned;
only `config.toml`, `downloads/`, `state/`, and `.cache/` are writable by the
service user so the web UI can save settings without making app code writable.

The generic installer auto-detects `dnf` or `apt-get` for OS dependencies. On
Debian/Ubuntu systems, it uses `apt-get` to install systemd, curl, certificates,
unzip, DejaVu fonts, Python 3.11+ with venv support, and FFmpeg; the Ubuntu
script also enables the `universe` repository when available for FFmpeg. On
AlmaLinux/RHEL-like systems, the installer uses `dnf` where possible, including
Python 3.11+, FFmpeg, DejaVu Sans fonts, EPEL, and RPM Fusion.

For NVIDIA/NVENC, the AlmaLinux/RHEL path can install RPM Fusion NVIDIA
driver/CUDA runtime packages (`akmod-nvidia` and
`xorg-x11-drv-nvidia-cuda`) when needed. The Debian/Ubuntu path does not install
NVIDIA drivers automatically; install the distro-recommended NVIDIA driver and
encode packages yourself if you want NVENC chat rendering. If FFmpeg already
advertises NVENC encoders, the installer leaves the driver stack unchanged.

The installer installs `yt-dlp[default]` into the project venv, so a system
`yt-dlp` package is not required. It also installs a project-local Deno runtime
under `.deno/` because yt-dlp's current YouTube support uses EJS challenge
solver scripts with an external JavaScript runtime. If `transcribe_subtitles =
true` is set in `config.toml`, the installer also installs `whisperx` into the
project venv. Set `ONLYSAVEMEVODS_INSTALL_WHISPERX=1` to force that install or
`ONLYSAVEMEVODS_INSTALL_WHISPERX=0` to skip it. If `voice_match_enabled = true`,
the installer also installs the `onlysavemevods[voice-match]` extra for
pyannote-backed known-voice matching. That extra pins the shared Torch and
Hugging Face packages to the WhisperX-compatible stack. Set
`ONLYSAVEMEVODS_INSTALL_VOICE_MATCH=1` to force it or
`ONLYSAVEMEVODS_INSTALL_VOICE_MATCH=0` to skip it.

The installer also enables a nightly root-run Python dependency updater. It
refreshes the project venv, `yt-dlp[default]`, installed/enabled WhisperX, and
installed/enabled voice-match dependencies, but skips the run if the service is
recording, checking a recent exit, waiting
to retry, or running queued dashboard/watermark jobs. By default it runs at
`04:15` with up to `45m` randomized delay. If the service is active but the
local status endpoint cannot be read, the updater skips that run rather than
guessing. Disable or reschedule it at install time with:

```bash
ONLYSAVEMEVODS_ENABLE_PYTHON_UPDATER=0 scripts/install-almalinux.sh
ONLYSAVEMEVODS_PYTHON_UPDATE_CALENDAR='*-*-* 03:30:00' scripts/install-almalinux.sh
ONLYSAVEMEVODS_PYTHON_UPDATE_RANDOM_DELAY=20m scripts/install-almalinux.sh
```

To install somewhere other than `/opt/onlysavemevods`:

```bash
ONLYSAVEMEVODS_INSTALL_DIR=/srv/onlysavemevods scripts/install-almalinux.sh
```

To skip OS package installation and only use what is already present:

```bash
ONLYSAVEMEVODS_SKIP_OS_DEPS=1 scripts/install-almalinux.sh
```

To install OS packages but skip NVIDIA driver/NVENC package installation:

```bash
ONLYSAVEMEVODS_SKIP_NVIDIA_DEPS=1 scripts/install-almalinux.sh
```

To skip Deno installation because you already provide a supported runtime on
`PATH`:

```bash
ONLYSAVEMEVODS_SKIP_DENO=1 scripts/install-almalinux.sh
```

To install WhisperX even before transcription is enabled in `config.toml`:

```bash
ONLYSAVEMEVODS_INSTALL_WHISPERX=1 scripts/install-almalinux.sh
```

To install voice matching even before `voice_match_enabled = true` is present in
`config.toml`:

```bash
ONLYSAVEMEVODS_INSTALL_VOICE_MATCH=1 scripts/install-almalinux.sh
```

To update a config file manually without changing existing values:

```bash
.venv/bin/onlysavemevods update-config --config config.toml --defaults config.example.toml
```

Inspect it:

```bash
sudo systemctl status onlysavemevods.service
systemctl list-timers onlysavemevods-python-update.timer
journalctl -u onlysavemevods.service -f
journalctl -u onlysavemevods-python-update.service
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
  values use that exact worker count. Set `chat_render_timeout_seconds` to
  raise the one-hour render timeout for very long VODs, or `0` to disable that
  timeout. Set `chat_render_use_nvenc = true` to use
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
- Most app settings can be changed from the dashboard Config tab and are
  written back to `config.toml`. The Streamers tab can add, update, or delete
  grouped sources, and each streamer card keeps shared settings behind its
  Settings button. The running process reloads the saved values where possible;
  web bind address and port changes apply after restart.
- Voice detection for transcription is managed with WhisperX/pyannote
  diarization. Use the dashboard Config tab to update the default mode, use a
  streamer card's Settings button for shared streamer overrides, or use
  `onlysavemevods voice-detection show --config config.toml` and
  `onlysavemevods voice-detection set --config config.toml --mode auto` from the
  CLI. Modes are `off` for no speaker labels, `auto` to let WhisperX infer the
  count, `range` with `--min-speakers` and/or `--max-speakers`, and `fixed` with
  `--speakers N`. Streamer defaults are stored under
  `[streamers."Name".voice_detection]`; source-specific overrides can still be
  stored as `[channel_voice_detection."Channel Name"]` tables and take
  precedence. Diarization usually needs a Hugging Face token with the relevant
  pyannote model terms accepted; set the token in the environment variable named
  by `whisperx_hf_token_env` (`HF_TOKEN` by default).
- The dashboard has a Voices button on configured streamer cards. It manages
  `[streamers."Name".voices."Voice Name"]` profiles, uploaded samples under
  `state/voice_samples/<streamer>/<voice>/`, samples made from existing diarized
  transcript segments, and review of low-confidence matches. The systemd installer
  installs the optional matcher dependency when `voice_match_enabled = true`; for
  manual installs, use `.venv/bin/python -m pip install -e ".[voice-match]"`.
  If pip has already upgraded Torch or `huggingface-hub` too far, rerun that
  command so the WhisperX-compatible pins can downgrade them. When available, the matcher
  writes `<media>.voice-attribution.json` after WhisperX, auto-applies confident
  matches to `.srt`/`.vtt`, and leaves weak matches for review. Manual
  `[streamers."Name".speaker_labels]` and `[channel_speaker_labels."Channel Name"]`
  mappings still win because WhisperX `SPEAKER_00` IDs are per transcript.
- The dashboard Config tab also has a Speaker Names section. After WhisperX has
  produced a diarized `.json` sidecar, the dashboard lists detected labels such
  as `SPEAKER_00` and `SPEAKER_01` per streamer first, with source-specific
  overrides available for advanced cases. Save names there to write manual
  speaker-label mappings into `config.toml`; existing `.srt` and `.vtt` subtitles
  for that group are rewritten with names such as `Host: ...` or `Guest: ...`.
- The dashboard shows `Transcribe` for finalized media without subtitles and
  `Retranscribe` when `.srt`/`.vtt` sidecars already exist. Retranscription
  replaces only the WhisperX subtitle/transcript sidecars for that media file.
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
  watermark copies cannot be detected. To create or rotate it manually, put the
  generated value in that persistent environment file:

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
  are available, the bot turns finalized format files back into resumable
  `.part` files and restarts the same segment with `--live-from-start` instead
  of jumping to the live edge. If exact restore is not possible, continuation
  segments also use `--live-from-start` to prefer duplicates over missing
  content. Once the stream is truly ended, mixed leftovers are muxed to the
  shortest track.
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
