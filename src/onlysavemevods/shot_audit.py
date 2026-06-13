from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import csv
import hashlib
import io
import json
import logging
import re
import shutil
import subprocess
import tempfile
import time
import tomllib

from .chat_render import chat_video_output_file
from .config import BotConfig
from .transcription import transcription_outputs_exist


LOGGER = logging.getLogger(__name__)
AUDIT_DIR_NAME = "shot-audits"
PROJECT_FILENAME = "project.json"
DONATION_DEDUPE_WINDOW_SECONDS = 8.0
SHOT_DEDUPE_WINDOW_SECONDS = 25.0
DONATION_TO_SHOT_WINDOW_SECONDS = 15 * 60
DONATION_TTS_SUPPRESSION_SECONDS = 20.0
VISUAL_SHOT_SCAN_START_SECONDS = 8.0
VISUAL_SHOT_SCAN_WINDOW_SECONDS = 18 * 60
VISUAL_SHOT_EVENT_GAP_SECONDS = 8.0
VISUAL_MAX_INFERRED_SHOTS = 4
YOLO_POSE_DRINK_SCORE_THRESHOLD = 0.55
HIGH_CONFIDENCE = "high"
MEDIUM_CONFIDENCE = "medium"
LOW_CONFIDENCE = "low"
MANUAL_CONFIDENCE = "manual"
CONFIDENCE_ORDER = {
    LOW_CONFIDENCE: 1,
    MEDIUM_CONFIDENCE: 2,
    HIGH_CONFIDENCE: 3,
    MANUAL_CONFIDENCE: 4,
}

AMOUNT_RE = re.compile(
    r"(?P<symbol>[$£€])\s*(?P<symbol_amount>\d+(?:[.,]\d{1,2})?)"
    r"|(?P<code>USD|US\$|GBP|EUR)\s*\$?\s*(?P<code_amount>\d+(?:[.,]\d{1,2})?)",
    re.IGNORECASE,
)
DONATION_KEYWORD_RE = re.compile(
    r"\b(?:sent|donated|tipped|power\s*chat|powerchat|tts|amount|usd)\b",
    re.IGNORECASE,
)
USERNAME_RE = re.compile(
    r"(?P<name>@?[A-Za-z0-9_.\- ]{2,40})\s+"
    r"(?:sent|donated|tipped|power\s*chat|powerchat)",
    re.IGNORECASE,
)
SHOT_PHRASE_RE = re.compile(
    r"\b(?:taking|take|took|drink|drinking|drank|down|downing|did|doing|"
    r"pour|poured|pouring)\s+(?:a\s+|another\s+|the\s+|some\s+)?"
    r"(?:double\s+)?(?:shot|shots)\b"
    r"|\b(?:taking|take|took|drink|drinking|drank|down|downing|pour|poured|pouring)\s+"
    r"(?:a\s+|the\s+|some\s+)?(?:jager|jäger|jagermeister|jägermeister)\b"
    r"|\b(?:jager|jäger|jagermeister|jägermeister)\s+(?:shot|shots|double)\b"
    r"|\bcheers\b",
    re.IGNORECASE,
)
DOUBLE_RE = re.compile(r"\bdouble\b", re.IGNORECASE)
SHOT_NEGATION_RE = re.compile(
    r"\b(?:no|not|never|without|wont|won't|dont|don't|doesnt|doesn't|isnt|isn't|"
    r"aint|ain't)\b.{0,60}\b(?:shot|shots|drink|drinking|jager|jäger|jagermeister|jägermeister)\b",
    re.IGNORECASE,
)
ALERT_SUPPRESSION_RE = re.compile(
    r"\b(?:litty again|shots shots shots|i got 5 on it|take this you mall whore)\b",
    re.IGNORECASE,
)
TRANSCRIPT_TIME_RE = re.compile(
    r"(?P<start>(?:\d{1,2}:)?\d{1,2}:\d{2}[,.]\d{1,3})\s*-->\s*"
    r"(?P<end>(?:\d{1,2}:)?\d{1,2}:\d{2}[,.]\d{1,3})"
)
PADDLE_OCR_CACHE: dict[tuple[str, str], Any] = {}
YOLO_POSE_CACHE: dict[str, Any] = {}


class ShotAuditError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ShotRule:
    amount_min: float
    amount_max: float
    shots: int
    currency: str = "USD"
    label: str = ""

    def matches(self, amount: float, currency: str) -> bool:
        if self.currency and currency and self.currency.upper() != currency.upper():
            return False
        cents = round(amount * 100)
        return round(self.amount_min * 100) <= cents <= round(self.amount_max * 100)


@dataclass(frozen=True, slots=True)
class DonationEvent:
    event_id: str
    offset_seconds: float
    amount: float
    currency: str
    owed_shots: int
    rule_label: str
    username: str = ""
    message: str = ""
    raw_text: str = ""
    source: str = "ocr"
    confidence: str = MEDIUM_CONFIDENCE
    media_name: str = ""


@dataclass(frozen=True, slots=True)
class ConsumedShotEvent:
    event_id: str
    offset_seconds: float
    count: int
    confidence: str
    source: str
    evidence: str
    linked_donation_id: str = ""
    note: str = ""
    media_name: str = ""


@dataclass(frozen=True, slots=True)
class AuditTotals:
    owed_shots: int
    machine_high_confidence_shots: int
    manual_shots: int
    counted_consumed_shots: int
    unconfirmed_owed_shots: int
    donation_count: int
    consumed_event_count: int


@dataclass(frozen=True, slots=True)
class ShotAuditMedia:
    media_file: str
    video_id: str = ""
    title: str = ""
    channel: str = ""
    chat_file: str = ""
    chat_video_file: str = ""


@dataclass(frozen=True, slots=True)
class ShotAuditProject:
    project_id: str
    video_id: str
    title: str
    channel: str
    media_file: str
    chat_file: str = ""
    chat_video_file: str = ""
    status: str = "queued"
    message: str = ""
    created_at: str = ""
    updated_at: str = ""
    rules: list[ShotRule] = field(default_factory=list)
    donations: list[DonationEvent] = field(default_factory=list)
    consumed: list[ConsumedShotEvent] = field(default_factory=list)
    media_items: list[ShotAuditMedia] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class AutoAuditResult:
    ran: bool
    project_id: str = ""
    message: str = ""


@dataclass(frozen=True, slots=True)
class TranscriptCue:
    offset_seconds: float
    text: str


@dataclass(frozen=True, slots=True)
class MotionSample:
    offset_seconds: float
    score: float


def audit_root(config: BotConfig) -> Path:
    return config.state_dir / AUDIT_DIR_NAME


def project_dir(config: BotConfig, project_id: str) -> Path:
    return audit_root(config) / safe_project_id(project_id)


def project_file(config: BotConfig, project_id: str) -> Path:
    return project_dir(config, project_id) / PROJECT_FILENAME


def safe_project_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "audit"


def project_id_for(video_id: str, media_file: Path) -> str:
    digest = hashlib.sha1(str(media_file.resolve()).encode("utf-8")).hexdigest()[:10]
    prefix = safe_project_id(video_id or media_file.stem)[:48]
    return f"{prefix}-{digest}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_shot_rules() -> list[ShotRule]:
    return [
        ShotRule(21.0, 21.99, 2, "USD", "$21 double"),
        ShotRule(5.0, 5.99, 1, "USD", "$5 shot"),
        ShotRule(3.0, 3.99, 0, "USD", "$3 TTS only"),
    ]


def load_shot_rules(config: BotConfig) -> list[ShotRule]:
    if config.shot_audit_rules_file is None:
        return default_shot_rules()
    try:
        text = config.shot_audit_rules_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise ShotAuditError(
            f"Unable to read shot audit rules file: {config.shot_audit_rules_file}"
        ) from exc
    return parse_shot_rules_toml(text)


def parse_shot_rules_toml(text: str) -> list[ShotRule]:
    try:
        raw = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ShotAuditError(f"Invalid shot audit rules TOML: {exc}") from exc
    raw_rules = (
        raw.get("shot_rules")
        or raw.get("rules")
        or raw.get("shot_audit_rules")
        or []
    )
    if not isinstance(raw_rules, list):
        raise ShotAuditError("shot audit rules must be a TOML array")
    rules = [parse_shot_rule(item, index) for index, item in enumerate(raw_rules)]
    if not rules:
        raise ShotAuditError("shot audit rules file did not contain any rules")
    return rules


def parse_shot_rule(item: Any, index: int) -> ShotRule:
    if not isinstance(item, dict):
        raise ShotAuditError(f"shot rule {index} must be a table")
    if "amount" in item:
        amount = coerce_amount(item["amount"], f"shot rule {index} amount")
        amount_min = amount
        amount_max = amount + 0.009
    else:
        amount_min = coerce_amount(item.get("amount_min"), f"shot rule {index} amount_min")
        amount_max = coerce_amount(item.get("amount_max"), f"shot rule {index} amount_max")
    if amount_min < 0 or amount_max < amount_min:
        raise ShotAuditError(f"shot rule {index} has an invalid amount range")
    shots = item.get("shots")
    if not isinstance(shots, int) or isinstance(shots, bool) or shots < 0:
        raise ShotAuditError(f"shot rule {index} shots must be a non-negative integer")
    currency = str(item.get("currency") or "USD").upper()
    label = str(item.get("label") or f"{currency} {amount_min:g}-{amount_max:g}")
    return ShotRule(amount_min, amount_max, shots, currency, label)


def coerce_amount(value: Any, name: str) -> float:
    if isinstance(value, bool) or value is None:
        raise ShotAuditError(f"{name} must be a number")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "."))
        except ValueError as exc:
            raise ShotAuditError(f"{name} must be a number") from exc
    raise ShotAuditError(f"{name} must be a number")


def apply_rules(amount: float, currency: str, rules: list[ShotRule]) -> tuple[int, str]:
    for rule in rules:
        if rule.matches(amount, currency):
            return rule.shots, rule.label
    return 0, "no matching shot rule"


def parse_ocr_donation_text(
    text: str,
    offset_seconds: float,
    rules: list[ShotRule],
    *,
    source: str = "ocr",
) -> list[DonationEvent]:
    normalized = normalize_ocr_text(text)
    if not normalized:
        return []
    events: list[DonationEvent] = []
    for match in AMOUNT_RE.finditer(normalized):
        amount_text = match.group("symbol_amount") or match.group("code_amount") or ""
        try:
            amount = float(amount_text.replace(",", "."))
        except ValueError:
            continue
        currency = currency_from_match(match)
        owed_shots, rule_label = apply_rules(amount, currency, rules)
        confidence = HIGH_CONFIDENCE if DONATION_KEYWORD_RE.search(normalized) else MEDIUM_CONFIDENCE
        event_id = donation_event_id(offset_seconds, amount, currency, normalized)
        events.append(
            DonationEvent(
                event_id=event_id,
                offset_seconds=round(offset_seconds, 3),
                amount=amount,
                currency=currency,
                owed_shots=owed_shots,
                rule_label=rule_label,
                username=extract_username(normalized),
                message=normalized,
                raw_text=normalized,
                source=source,
                confidence=confidence,
            )
        )
    return events


def normalize_ocr_text(text: str) -> str:
    cleaned = text.replace("\x0c", " ")
    cleaned = cleaned.replace("＄", "$")
    cleaned = re.sub(r"[ \t\r\f\v]+", " ", cleaned)
    cleaned = re.sub(r"\n{2,}", "\n", cleaned)
    return cleaned.strip()


def currency_from_match(match: re.Match[str]) -> str:
    symbol = match.group("symbol") or ""
    code = (match.group("code") or "").upper()
    if symbol == "£" or code == "GBP":
        return "GBP"
    if symbol == "€" or code == "EUR":
        return "EUR"
    return "USD"


def extract_username(text: str) -> str:
    match = USERNAME_RE.search(text)
    if not match:
        return ""
    return " ".join(match.group("name").split()).strip(" :-")


def donation_event_id(offset_seconds: float, amount: float, currency: str, text: str) -> str:
    bucket = round(offset_seconds)
    normalized = re.sub(r"[^a-z0-9]+", "", text.casefold())[:120]
    digest = hashlib.sha1(
        f"{bucket}:{amount:.2f}:{currency}:{normalized}".encode("utf-8")
    ).hexdigest()[:12]
    return f"don_{digest}"


def dedupe_donations(
    events: list[DonationEvent],
    *,
    window_seconds: float = DONATION_DEDUPE_WINDOW_SECONDS,
) -> list[DonationEvent]:
    deduped: list[DonationEvent] = []
    for event in sorted(events, key=lambda item: item.offset_seconds):
        duplicate_index = find_duplicate_donation(deduped, event, window_seconds)
        if duplicate_index is None:
            deduped.append(event)
            continue
        existing = deduped[duplicate_index]
        deduped[duplicate_index] = merge_donations(existing, event)
    return deduped


def find_duplicate_donation(
    existing: list[DonationEvent],
    event: DonationEvent,
    window_seconds: float,
) -> int | None:
    cents = round(event.amount * 100)
    for index in range(len(existing) - 1, -1, -1):
        candidate = existing[index]
        if event.offset_seconds - candidate.offset_seconds > window_seconds:
            break
        if (
            candidate.media_name == event.media_name
            and candidate.currency == event.currency
            and round(candidate.amount * 100) == cents
        ):
            return index
    return None


def merge_donations(existing: DonationEvent, event: DonationEvent) -> DonationEvent:
    confidence = (
        HIGH_CONFIDENCE
        if HIGH_CONFIDENCE in {existing.confidence, event.confidence}
        else existing.confidence
    )
    raw_text = event.raw_text if len(event.raw_text) > len(existing.raw_text) else existing.raw_text
    username = existing.username or event.username
    return DonationEvent(
        event_id=existing.event_id,
        offset_seconds=existing.offset_seconds,
        amount=existing.amount,
        currency=existing.currency,
        owed_shots=max(existing.owed_shots, event.owed_shots),
        rule_label=existing.rule_label if existing.owed_shots >= event.owed_shots else event.rule_label,
        username=username,
        message=raw_text,
        raw_text=raw_text,
        source=existing.source,
        confidence=confidence,
        media_name=existing.media_name or event.media_name,
    )


def donation_for_media(event: DonationEvent, media_name: str) -> DonationEvent:
    if event.media_name == media_name:
        return event
    event_id = f"{event.event_id}_{hashlib.sha1(media_name.encode('utf-8')).hexdigest()[:8]}"
    return DonationEvent(
        event_id=event_id,
        offset_seconds=event.offset_seconds,
        amount=event.amount,
        currency=event.currency,
        owed_shots=event.owed_shots,
        rule_label=event.rule_label,
        username=event.username,
        message=event.message,
        raw_text=event.raw_text,
        source=event.source,
        confidence=event.confidence,
        media_name=media_name,
    )


def consumed_for_media(event: ConsumedShotEvent, media_name: str) -> ConsumedShotEvent:
    if event.media_name == media_name:
        return event
    event_id = f"{event.event_id}_{hashlib.sha1(media_name.encode('utf-8')).hexdigest()[:8]}"
    return ConsumedShotEvent(
        event_id=event_id,
        offset_seconds=event.offset_seconds,
        count=event.count,
        confidence=event.confidence,
        source=event.source,
        evidence=event.evidence,
        linked_donation_id=event.linked_donation_id,
        note=event.note,
        media_name=media_name,
    )


def detect_donation_events_from_video(
    config: BotConfig,
    video_file: Path,
    rules: list[ShotRule],
    *,
    logger: logging.Logger = LOGGER,
) -> list[DonationEvent]:
    _np, cv2 = optional_cv_dependencies()
    cap = cv2.VideoCapture(str(video_file))
    if not cap.isOpened():
        raise ShotAuditError(f"Unable to open video for shot audit OCR: {video_file}")
    try:
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        duration = frame_count / fps if fps > 0 and frame_count > 0 else 0.0
        interval = max(0.1, config.shot_audit_frame_interval_seconds)
        offsets = frame_offsets(duration, interval, config.shot_audit_max_ocr_frames)
        events: list[DonationEvent] = []
        started = time.monotonic()
        for index, offset in enumerate(offsets):
            cap.set(cv2.CAP_PROP_POS_MSEC, offset * 1000.0)
            ok, frame = cap.read()
            if not ok:
                continue
            text = ocr_frame(
                frame,
                cv2,
                config,
            )
            events.extend(
                parse_ocr_donation_text(
                    text,
                    offset,
                    rules,
                    source=ocr_source_name(config),
                )
            )
            if index and index % 50 == 0:
                logger.info(
                    "Shot audit OCR progress video=%s frames=%d/%d donations=%d",
                    video_file.name,
                    index,
                    len(offsets),
                    len(events),
                )
        deduped = dedupe_donations(events)
        logger.info(
            "Shot audit OCR completed video=%s frames=%d donations=%d elapsed=%.1fs",
            video_file.name,
            len(offsets),
            len(deduped),
            time.monotonic() - started,
        )
        return deduped
    finally:
        cap.release()


def frame_offsets(duration: float, interval: float, max_frames: int) -> list[float]:
    if duration <= 0:
        return [0.0]
    offsets: list[float] = []
    offset = 0.0
    while offset < duration and len(offsets) < max_frames:
        offsets.append(round(offset, 3))
        offset += interval
    return offsets


def ocr_frame(frame: Any, cv2: Any, config: BotConfig) -> str:
    if config.shot_audit_ocr_backend == "paddleocr":
        return paddle_ocr_frame(frame, cv2, config)
    return tesseract_ocr_frame(frame, cv2, config.shot_audit_tesseract_path)


def ocr_source_name(config: BotConfig) -> str:
    return f"onscreen_{config.shot_audit_ocr_backend}"


def tesseract_ocr_frame(frame: Any, cv2: Any, tesseract_path: str) -> str:
    roi = donation_alert_region(frame)
    gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
    height, width = gray.shape[:2]
    if width < 1200:
        scale = 1200 / max(1, width)
        gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    gray = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp:
        temp_path = Path(temp.name)
    try:
        cv2.imwrite(str(temp_path), gray)
        result = subprocess.run(
            [tesseract_path, str(temp_path), "stdout", "--psm", "6"],
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
    except FileNotFoundError as exc:
        raise ShotAuditError(f"Tesseract binary not found: {tesseract_path}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ShotAuditError("Tesseract timed out while reading donation alert text") from exc
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
    return result.stdout if result.returncode == 0 else ""


def paddle_ocr_frame(frame: Any, cv2: Any, config: BotConfig) -> str:
    roi = donation_alert_region(frame)
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp:
        temp_path = Path(temp.name)
    try:
        cv2.imwrite(str(temp_path), roi)
        engine = paddle_ocr_engine(config)
        if hasattr(engine, "predict"):
            result = engine.predict(str(temp_path))
        else:
            result = engine.ocr(str(temp_path), cls=False)
    except ImportError as exc:
        raise ShotAuditError(
            "PaddleOCR backend requested but paddleocr is not installed. "
            "Install PaddleOCR/PaddlePaddle or set shot_audit_ocr_backend = \"tesseract\"."
        ) from exc
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
    return "\n".join(extract_paddle_ocr_text(result))


def paddle_ocr_engine(config: BotConfig) -> Any:
    device = paddle_ocr_device(config.shot_audit_ocr_device)
    key = (device, config.shot_audit_ocr_backend)
    cached = PADDLE_OCR_CACHE.get(key)
    if cached is not None:
        return cached
    from paddleocr import PaddleOCR

    try:
        engine = PaddleOCR(device=device)
    except TypeError:
        use_gpu = device.startswith("gpu") or device.startswith("cuda")
        engine = PaddleOCR(use_gpu=use_gpu)
    PADDLE_OCR_CACHE[key] = engine
    return engine


def paddle_ocr_device(configured_device: str) -> str:
    requested = (configured_device or "auto").strip().casefold()
    if requested == "auto":
        return "gpu" if paddle_cuda_available() else "cpu"
    if requested == "cuda":
        return "gpu"
    return requested


def paddle_cuda_available() -> bool:
    try:
        import paddle
    except ImportError:
        return False
    return bool(getattr(paddle, "is_compiled_with_cuda", lambda: False)())


def extract_paddle_ocr_text(result: Any) -> list[str]:
    texts: list[str] = []
    collect_paddle_text(result, texts)
    return [text for text in texts if text.strip()]


def collect_paddle_text(value: Any, texts: list[str]) -> None:
    if value is None:
        return
    if hasattr(value, "to_dict"):
        collect_paddle_text(value.to_dict(), texts)
        return
    if isinstance(value, dict):
        for key in ("rec_texts", "texts"):
            raw = value.get(key)
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, str):
                        texts.append(item)
                return
        for key in ("text", "transcription"):
            raw = value.get(key)
            if isinstance(raw, str):
                texts.append(raw)
                return
        for item in value.values():
            collect_paddle_text(item, texts)
        return
    if isinstance(value, (list, tuple)):
        if len(value) == 2 and isinstance(value[0], str) and isinstance(value[1], (int, float)):
            texts.append(value[0])
            return
        for item in value:
            collect_paddle_text(item, texts)


def donation_alert_region(frame: Any) -> Any:
    height, width = frame.shape[:2]
    # In rendered chat videos the source media is on the left and chat is on the right.
    x2 = int(width * 0.78) if width >= 1500 else width
    y1 = int(height * 0.08)
    y2 = int(height * 0.86)
    return frame[y1:y2, 0:x2]


def optional_cv_dependencies() -> tuple[Any, Any]:
    try:
        import cv2
        import numpy as np
    except ImportError as exc:
        raise ShotAuditError(
            "Shot audit video analysis requires numpy and opencv-python-headless. "
            "Install the project dependencies before using shot audit features."
        ) from exc
    return np, cv2


def transcript_cues_for_media(media_file: Path) -> list[TranscriptCue]:
    for suffix in (".vtt", ".srt"):
        path = media_file.with_suffix(suffix)
        if path.is_file():
            try:
                return parse_timed_transcript(path.read_text(encoding="utf-8", errors="replace"))
            except OSError:
                return []
    return []


def parse_timed_transcript(text: str) -> list[TranscriptCue]:
    cues: list[TranscriptCue] = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        match = TRANSCRIPT_TIME_RE.search(lines[index])
        if not match:
            index += 1
            continue
        offset = timestamp_to_seconds(match.group("start"))
        index += 1
        cue_lines: list[str] = []
        while index < len(lines) and lines[index].strip():
            if not lines[index].strip().isdigit():
                cue_lines.append(lines[index].strip())
            index += 1
        text_value = re.sub(r"<[^>]+>", "", " ".join(cue_lines)).strip()
        if text_value:
            cues.append(TranscriptCue(offset, text_value))
        index += 1
    return cues


def timestamp_to_seconds(value: str) -> float:
    value = value.replace(",", ".")
    parts = value.split(":")
    seconds = float(parts[-1])
    minutes = int(parts[-2]) if len(parts) >= 2 else 0
    hours = int(parts[-3]) if len(parts) >= 3 else 0
    return hours * 3600 + minutes * 60 + seconds


def estimate_consumed_shots(
    media_file: Path,
    donations: list[DonationEvent],
    *,
    config: BotConfig | None = None,
    logger: logging.Logger = LOGGER,
) -> list[ConsumedShotEvent]:
    estimates = estimate_consumed_shots_from_transcript(media_file, donations)
    if config is not None and config.shot_audit_visual_detection_enabled:
        estimates.extend(
            detect_visual_consumed_shots_from_video(
                config,
                media_file,
                donations,
                logger=logger,
            )
        )
    return dedupe_consumed(estimates)


def estimate_consumed_shots_from_transcript(
    media_file: Path,
    donations: list[DonationEvent],
) -> list[ConsumedShotEvent]:
    cues = transcript_cues_for_media(media_file)
    estimates: list[ConsumedShotEvent] = []
    for cue in cues:
        if not SHOT_PHRASE_RE.search(cue.text):
            continue
        if SHOT_NEGATION_RE.search(cue.text):
            continue
        if ALERT_SUPPRESSION_RE.search(cue.text):
            continue
        if near_donation_alert(cue.offset_seconds, donations):
            continue
        linked = closest_prior_owed_donation(cue.offset_seconds, donations)
        count = 2 if DOUBLE_RE.search(cue.text) else 1
        confidence = consumed_confidence(cue.text, linked is not None)
        estimates.append(
            ConsumedShotEvent(
                event_id=consumed_event_id(cue.offset_seconds, cue.text),
                offset_seconds=round(cue.offset_seconds, 3),
                count=count,
                confidence=confidence,
                source="transcript",
                evidence=cue.text,
                linked_donation_id=linked.event_id if linked else "",
            )
        )
    return estimates


def detect_visual_consumed_shots_from_video(
    config: BotConfig,
    video_file: Path,
    donations: list[DonationEvent],
    *,
    logger: logging.Logger = LOGGER,
) -> list[ConsumedShotEvent]:
    owed_donations = [donation for donation in donations if donation.owed_shots > 0]
    if not owed_donations:
        return []
    if config.shot_audit_vision_backend == "yolo_pose":
        return detect_yolo_pose_consumed_shots_from_video(
            config,
            video_file,
            owed_donations,
            logger=logger,
        )
    try:
        _np, cv2 = optional_cv_dependencies()
    except ShotAuditError as exc:
        logger.info("Skipping visual shot detection: %s", exc)
        return []
    cap = cv2.VideoCapture(str(video_file))
    if not cap.isOpened():
        logger.info("Skipping visual shot detection; unable to open video=%s", video_file)
        return []
    try:
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        duration = frame_count / fps if fps > 0 and frame_count > 0 else 0.0
        ranges = visual_review_ranges(owed_donations, duration)
        offsets = frame_offsets_for_ranges(
            ranges,
            config.shot_audit_visual_frame_interval_seconds,
            config.shot_audit_max_visual_frames,
        )
        samples: list[MotionSample] = []
        previous_signature: Any | None = None
        previous_offset: float | None = None
        started = time.monotonic()
        for index, offset in enumerate(offsets):
            cap.set(cv2.CAP_PROP_POS_MSEC, offset * 1000.0)
            ok, frame = cap.read()
            if not ok:
                continue
            signature = visual_motion_signature(frame, cv2)
            if (
                previous_signature is not None
                and previous_offset is not None
                and offset - previous_offset <= config.shot_audit_visual_frame_interval_seconds * 2.5
            ):
                samples.append(
                    MotionSample(
                        offset_seconds=round(offset, 3),
                        score=visual_motion_score(previous_signature, signature, cv2),
                    )
                )
            previous_signature = signature
            previous_offset = offset
            if index and index % 100 == 0:
                logger.info(
                    "Shot audit visual progress video=%s frames=%d/%d",
                    video_file.name,
                    index,
                    len(offsets),
                )
        events = visual_consumed_events_from_samples(
            samples,
            owed_donations,
            threshold=config.shot_audit_visual_motion_threshold,
        )
        logger.info(
            "Shot audit visual completed video=%s frames=%d candidates=%d elapsed=%.1fs",
            video_file.name,
            len(offsets),
            len(events),
            time.monotonic() - started,
        )
        return events
    finally:
        cap.release()


def detect_yolo_pose_consumed_shots_from_video(
    config: BotConfig,
    video_file: Path,
    donations: list[DonationEvent],
    *,
    logger: logging.Logger = LOGGER,
) -> list[ConsumedShotEvent]:
    try:
        _np, cv2 = optional_cv_dependencies()
        model = yolo_pose_model(config.shot_audit_yolo_pose_model)
    except (ImportError, ShotAuditError) as exc:
        logger.info("Skipping YOLO pose shot detection: %s", exc)
        return []
    cap = cv2.VideoCapture(str(video_file))
    if not cap.isOpened():
        logger.info("Skipping YOLO pose shot detection; unable to open video=%s", video_file)
        return []
    try:
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        duration = frame_count / fps if fps > 0 and frame_count > 0 else 0.0
        ranges = visual_review_ranges(donations, duration)
        offsets = frame_offsets_for_ranges(
            ranges,
            config.shot_audit_visual_frame_interval_seconds,
            config.shot_audit_max_visual_frames,
        )
        samples: list[MotionSample] = []
        started = time.monotonic()
        device = yolo_pose_device(config.shot_audit_vision_device)
        for index, offset in enumerate(offsets):
            cap.set(cv2.CAP_PROP_POS_MSEC, offset * 1000.0)
            ok, frame = cap.read()
            if not ok:
                continue
            score = yolo_pose_drink_score(model, frame, device)
            if score > 0:
                samples.append(MotionSample(round(offset, 3), score))
            if index and index % 100 == 0:
                logger.info(
                    "Shot audit YOLO pose progress video=%s frames=%d/%d candidates=%d",
                    video_file.name,
                    index,
                    len(offsets),
                    len(samples),
                )
        events = visual_consumed_events_from_samples(
            samples,
            donations,
            threshold=YOLO_POSE_DRINK_SCORE_THRESHOLD,
            source="yolo_pose",
            evidence_label="YOLO pose hand-to-mouth candidate",
            review_hint="review for hand-to-mouth drinking motion, glass, or bottle",
        )
        logger.info(
            "Shot audit YOLO pose completed video=%s frames=%d candidates=%d elapsed=%.1fs",
            video_file.name,
            len(offsets),
            len(events),
            time.monotonic() - started,
        )
        return events
    finally:
        cap.release()


def yolo_pose_model(model_name: str) -> Any:
    cached = YOLO_POSE_CACHE.get(model_name)
    if cached is not None:
        return cached
    from ultralytics import YOLO

    model = YOLO(model_name)
    YOLO_POSE_CACHE[model_name] = model
    return model


def yolo_pose_device(configured_device: str) -> str | None:
    requested = (configured_device or "auto").strip()
    if not requested or requested.casefold() == "auto":
        return None
    return "0" if requested.casefold() == "cuda" else requested


def yolo_pose_drink_score(model: Any, frame: Any, device: str | None) -> float:
    kwargs: dict[str, Any] = {"verbose": False}
    if device is not None:
        kwargs["device"] = device
    results = model.predict(frame, **kwargs)
    return max((pose_result_drink_score(result, frame) for result in results), default=0.0)


def pose_result_drink_score(result: Any, frame: Any) -> float:
    keypoints = getattr(result, "keypoints", None)
    xy = tensor_like_to_numpy(getattr(keypoints, "xy", None))
    if xy is None or len(xy) == 0:
        return 0.0
    confidence = tensor_like_to_numpy(getattr(keypoints, "conf", None))
    height, width = frame.shape[:2]
    diagonal = max(1.0, (float(width) ** 2 + float(height) ** 2) ** 0.5)
    best = 0.0
    for person_index, person in enumerate(xy):
        if len(person) <= 10:
            continue
        nose = person[0]
        for wrist_index in (9, 10):
            if not pose_keypoint_confident(confidence, person_index, 0):
                continue
            if not pose_keypoint_confident(confidence, person_index, wrist_index):
                continue
            wrist = person[wrist_index]
            distance = ((float(nose[0] - wrist[0])) ** 2 + (float(nose[1] - wrist[1])) ** 2) ** 0.5
            score = max(0.0, 1.0 - distance / (diagonal * 0.12))
            best = max(best, score)
    return best


def tensor_like_to_numpy(value: Any) -> Any | None:
    if value is None:
        return None
    if hasattr(value, "detach"):
        value = value.detach()
    if hasattr(value, "cpu"):
        value = value.cpu()
    if hasattr(value, "numpy"):
        return value.numpy()
    return value


def pose_keypoint_confident(confidence: Any | None, person_index: int, keypoint_index: int) -> bool:
    if confidence is None:
        return True
    try:
        return float(confidence[person_index][keypoint_index]) >= 0.25
    except (IndexError, TypeError, ValueError):
        return True


def visual_review_ranges(
    donations: list[DonationEvent],
    duration: float,
) -> list[tuple[float, float]]:
    ranges: list[tuple[float, float]] = []
    for donation in sorted(donations, key=lambda item: item.offset_seconds):
        if donation.owed_shots <= 0:
            continue
        start = max(0.0, donation.offset_seconds + VISUAL_SHOT_SCAN_START_SECONDS)
        end = donation.offset_seconds + VISUAL_SHOT_SCAN_WINDOW_SECONDS
        if duration > 0:
            end = min(end, duration)
        if end > start:
            ranges.append((round(start, 3), round(end, 3)))
    return merge_time_ranges(ranges)


def merge_time_ranges(ranges: list[tuple[float, float]]) -> list[tuple[float, float]]:
    merged: list[tuple[float, float]] = []
    for start, end in sorted(ranges):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
            continue
        merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def frame_offsets_for_ranges(
    ranges: list[tuple[float, float]],
    interval: float,
    max_frames: int,
) -> list[float]:
    interval = max(0.1, interval)
    offsets: list[float] = []
    for start, end in ranges:
        offset = start
        while offset <= end:
            offsets.append(round(offset, 3))
            offset += interval
    if len(offsets) <= max_frames:
        return offsets
    step = max(1, (len(offsets) + max_frames - 1) // max_frames)
    return offsets[::step][:max_frames]


def visual_motion_signature(frame: Any, cv2: Any) -> Any:
    roi = visual_drink_region(frame)
    gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
    height, width = gray.shape[:2]
    if width > 240:
        scale = 240 / max(1, width)
        gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    elif width < 120:
        scale = 120 / max(1, width)
        gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
    return cv2.GaussianBlur(gray, (5, 5), 0)


def visual_drink_region(frame: Any) -> Any:
    height, width = frame.shape[:2]
    x1 = int(width * 0.16)
    x2 = int(width * 0.84)
    y1 = int(height * 0.10)
    y2 = int(height * 0.88)
    return frame[y1:y2, x1:x2]


def visual_motion_score(previous_signature: Any, signature: Any, cv2: Any) -> float:
    diff = cv2.absdiff(previous_signature, signature)
    changed = cv2.threshold(diff, 28, 255, cv2.THRESH_BINARY)[1]
    return float(cv2.countNonZero(changed)) / max(1, changed.shape[0] * changed.shape[1])


def visual_consumed_events_from_samples(
    samples: list[MotionSample | tuple[float, float]],
    donations: list[DonationEvent],
    *,
    threshold: float,
    source: str = "visual",
    evidence_label: str = "visual motion candidate",
    review_hint: str = "review for pour/drink near face or glass",
) -> list[ConsumedShotEvent]:
    normalized = [
        sample if isinstance(sample, MotionSample) else MotionSample(float(sample[0]), float(sample[1]))
        for sample in samples
    ]
    events: list[ConsumedShotEvent] = []
    current_cluster: list[MotionSample] = []
    for sample in normalized:
        if sample.score >= threshold:
            if (
                current_cluster
                and sample.offset_seconds - current_cluster[-1].offset_seconds
                > VISUAL_SHOT_EVENT_GAP_SECONDS
            ):
                event = visual_event_from_cluster(
                    current_cluster,
                    donations,
                    threshold,
                    source=source,
                    evidence_label=evidence_label,
                    review_hint=review_hint,
                )
                if event is not None:
                    events.append(event)
                current_cluster = []
            current_cluster.append(sample)
            continue
        if current_cluster:
            event = visual_event_from_cluster(
                current_cluster,
                donations,
                threshold,
                source=source,
                evidence_label=evidence_label,
                review_hint=review_hint,
            )
            if event is not None:
                events.append(event)
            current_cluster = []
    if current_cluster:
        event = visual_event_from_cluster(
            current_cluster,
            donations,
            threshold,
            source=source,
            evidence_label=evidence_label,
            review_hint=review_hint,
        )
        if event is not None:
            events.append(event)
    return dedupe_consumed(events)


def visual_event_from_cluster(
    cluster: list[MotionSample],
    donations: list[DonationEvent],
    threshold: float,
    *,
    source: str = "visual",
    evidence_label: str = "visual motion candidate",
    review_hint: str = "review for pour/drink near face or glass",
) -> ConsumedShotEvent | None:
    if not cluster:
        return None
    peak = max(cluster, key=lambda sample: sample.score)
    pending = pending_owed_shots(peak.offset_seconds, donations)
    linked = closest_prior_owed_donation(peak.offset_seconds, donations)
    count = min(VISUAL_MAX_INFERRED_SHOTS, max(1, pending or (linked.owed_shots if linked else 1)))
    confidence = MEDIUM_CONFIDENCE if linked and peak.score >= threshold * 1.5 else LOW_CONFIDENCE
    evidence_bits = [
        f"{evidence_label} score {peak.score:.3f}",
        review_hint,
    ]
    if pending:
        evidence_bits.append(f"inferred count {count} from pending owed shots")
    elif linked:
        evidence_bits.append(f"linked to prior donation owing {linked.owed_shots}")
    return ConsumedShotEvent(
        event_id=visual_consumed_event_id(peak.offset_seconds, peak.score, count),
        offset_seconds=round(peak.offset_seconds, 3),
        count=count,
        confidence=confidence,
        source=source,
        evidence="; ".join(evidence_bits),
        linked_donation_id=linked.event_id if linked else "",
    )


def pending_owed_shots(
    offset_seconds: float,
    donations: list[DonationEvent],
) -> int:
    return sum(
        donation.owed_shots
        for donation in donations
        if donation.owed_shots > 0
        and 0 <= offset_seconds - donation.offset_seconds <= DONATION_TO_SHOT_WINDOW_SECONDS
    )


def near_donation_alert(offset_seconds: float, donations: list[DonationEvent]) -> bool:
    return any(
        abs(offset_seconds - donation.offset_seconds) <= DONATION_TTS_SUPPRESSION_SECONDS
        for donation in donations
    )


def closest_prior_owed_donation(
    offset_seconds: float,
    donations: list[DonationEvent],
) -> DonationEvent | None:
    candidates = [
        donation
        for donation in donations
        if donation.owed_shots > 0
        and 0 <= offset_seconds - donation.offset_seconds <= DONATION_TO_SHOT_WINDOW_SECONDS
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda donation: donation.offset_seconds)


def consumed_confidence(text: str, linked: bool) -> str:
    lowered = text.casefold()
    strong = any(word in lowered for word in ("drank", "drinking", "took", "downed", "cheers"))
    if linked and strong:
        return HIGH_CONFIDENCE
    if linked:
        return MEDIUM_CONFIDENCE
    return LOW_CONFIDENCE


def consumed_event_id(offset_seconds: float, text: str) -> str:
    digest = hashlib.sha1(
        f"{round(offset_seconds)}:{text.casefold()[:120]}".encode("utf-8")
    ).hexdigest()[:12]
    return f"shot_{digest}"


def visual_consumed_event_id(offset_seconds: float, score: float, count: int) -> str:
    digest = hashlib.sha1(
        f"{round(offset_seconds)}:{score:.3f}:{count}".encode("utf-8")
    ).hexdigest()[:12]
    return f"visual_{digest}"


def dedupe_consumed(events: list[ConsumedShotEvent]) -> list[ConsumedShotEvent]:
    deduped: list[ConsumedShotEvent] = []
    for event in sorted(events, key=lambda item: (item.media_name, item.offset_seconds)):
        duplicate_index = None
        for index in range(len(deduped) - 1, -1, -1):
            existing = deduped[index]
            if existing.media_name != event.media_name:
                continue
            if event.offset_seconds - existing.offset_seconds > SHOT_DEDUPE_WINDOW_SECONDS:
                break
            if abs(event.offset_seconds - existing.offset_seconds) <= SHOT_DEDUPE_WINDOW_SECONDS:
                duplicate_index = index
                break
        if duplicate_index is None:
            deduped.append(event)
            continue
        deduped[duplicate_index] = merge_consumed(deduped[duplicate_index], event)
    return deduped


def merge_consumed(existing: ConsumedShotEvent, event: ConsumedShotEvent) -> ConsumedShotEvent:
    confidence = stronger_confidence(existing.confidence, event.confidence)
    return ConsumedShotEvent(
        event_id=existing.event_id,
        offset_seconds=min(existing.offset_seconds, event.offset_seconds),
        count=max(existing.count, event.count),
        confidence=confidence,
        source=combine_sources(existing.source, event.source),
        evidence=combine_evidence(existing.evidence, event.evidence),
        linked_donation_id=existing.linked_donation_id or event.linked_donation_id,
        note=existing.note or event.note,
        media_name=existing.media_name or event.media_name,
    )


def stronger_confidence(first: str, second: str) -> str:
    first_rank = CONFIDENCE_ORDER.get(first, 0)
    second_rank = CONFIDENCE_ORDER.get(second, 0)
    return second if second_rank > first_rank else first


def combine_sources(first: str, second: str) -> str:
    if first == second:
        return first
    sources: list[str] = []
    for source in [*first.split("+"), *second.split("+")]:
        source = source.strip()
        if source and source not in sources:
            sources.append(source)
    return "+".join(sources)


def combine_evidence(first: str, second: str) -> str:
    if not first:
        return second
    if not second or second == first:
        return first
    return f"{first} | {second}"


def create_project(
    config: BotConfig,
    *,
    video_id: str,
    media_file: Path,
    chat_file: Path | None = None,
    chat_video_file: Path | None = None,
    title: str = "",
    channel: str = "",
    status: str = "queued",
    message: str = "",
    rules: list[ShotRule] | None = None,
) -> ShotAuditProject:
    now = utc_now_iso()
    media_item = ShotAuditMedia(
        media_file=str(media_file),
        video_id=video_id,
        title=title,
        channel=channel,
        chat_file=str(chat_file) if chat_file else "",
        chat_video_file=str(chat_video_file) if chat_video_file else "",
    )
    project = ShotAuditProject(
        project_id=project_id_for(video_id, media_file),
        video_id=video_id,
        title=title,
        channel=channel,
        media_file=str(media_file),
        chat_file=str(chat_file) if chat_file else "",
        chat_video_file=str(chat_video_file) if chat_video_file else "",
        status=status,
        message=message,
        created_at=now,
        updated_at=now,
        rules=rules or load_shot_rules(config),
        media_items=[media_item],
    )
    save_project(config, project)
    return project


def add_media_to_project(
    config: BotConfig,
    project_id: str,
    *,
    video_id: str,
    media_file: Path,
    chat_file: Path | None = None,
    chat_video_file: Path | None = None,
    title: str = "",
    channel: str = "",
) -> ShotAuditProject:
    project = require_project(config, project_id)
    new_item = ShotAuditMedia(
        media_file=str(media_file),
        video_id=video_id,
        title=title,
        channel=channel,
        chat_file=str(chat_file) if chat_file else "",
        chat_video_file=str(chat_video_file) if chat_video_file else "",
    )
    items = project_media_items(project)
    new_path = Path(new_item.media_file).resolve()
    if any(Path(item.media_file).resolve() == new_path for item in items):
        return project
    updated = replace_project(
        project,
        message="Finalized media added to audit project",
        media_items=[*items, new_item],
    )
    save_project(config, updated)
    return updated


def delete_project(config: BotConfig, project_id: str) -> None:
    if not project_id or safe_project_id(project_id) != project_id:
        raise ShotAuditError("Invalid shot audit project id")
    project = load_project(config, project_id)
    if project is None:
        raise ShotAuditError(f"Shot audit project not found: {project_id}")
    root = audit_root(config).resolve()
    directory = project_dir(config, project.project_id).resolve()
    if directory == root or root not in directory.parents:
        raise ShotAuditError("Refusing to delete a path outside the shot audit state directory")
    try:
        shutil.rmtree(directory)
    except FileNotFoundError as exc:
        raise ShotAuditError(f"Shot audit project not found: {project_id}") from exc
    except OSError as exc:
        raise ShotAuditError(f"Unable to delete shot audit project: {project_id}") from exc


def run_shot_audit(
    config: BotConfig,
    *,
    video_id: str,
    media_file: Path,
    chat_file: Path | None = None,
    chat_video_file: Path | None = None,
    title: str = "",
    channel: str = "",
    force: bool = False,
    logger: logging.Logger = LOGGER,
) -> ShotAuditProject:
    rules = load_shot_rules(config)
    project_id = project_id_for(video_id, media_file)
    if not force:
        existing = load_project(config, project_id)
        if existing is not None and existing.status in {"done", "needs_review"}:
            return existing
    project = load_project(config, project_id)
    if project is None:
        project = create_project(
            config,
            video_id=video_id,
            media_file=media_file,
            chat_file=chat_file,
            chat_video_file=chat_video_file,
            title=title,
            channel=channel,
            status="running",
            message="Running OCR, visual, and transcript shot audit",
            rules=rules,
        )
    else:
        project = replace_project(
            project,
            status="running",
            message="Running OCR, visual, and transcript shot audit",
            rules=rules,
        )
        save_project(config, project)
    try:
        donations: list[DonationEvent] = []
        consumed: list[ConsumedShotEvent] = []
        for item in project_media_items(project):
            item_media = Path(item.media_file)
            if not item_media.is_file():
                logger.warning("Skipping missing shot audit media file: %s", item_media)
                continue
            item_chat_video = Path(item.chat_video_file) if item.chat_video_file else None
            source_video = (
                item_chat_video
                if item_chat_video is not None and item_chat_video.is_file()
                else item_media
            )
            media_name = item_media.name
            media_donations = [
                donation_for_media(event, media_name)
                for event in detect_donation_events_from_video(
                    config,
                    source_video,
                    rules,
                    logger=logger,
                )
            ]
            donations.extend(media_donations)
            consumed.extend(
                consumed_for_media(event, media_name)
                for event in estimate_consumed_shots(
                    item_media,
                    media_donations,
                    config=config,
                    logger=logger,
                )
            )
        donations = sorted(donations, key=lambda event: (event.media_name, event.offset_seconds))
        consumed = sorted(consumed, key=lambda event: (event.media_name, event.offset_seconds))
        project = replace_project(
            project,
            status=status_for_totals(donations, consumed),
            message="Shot audit completed",
            donations=donations,
            consumed=consumed,
        )
    except Exception as exc:  # noqa: BLE001 - audit projects should record failures.
        logger.exception("Shot audit failed for media=%s", media_file)
        project = replace_project(
            project,
            status="failed",
            message=str(exc) or exc.__class__.__name__,
        )
    save_project(config, project)
    return project


def maybe_run_auto_shot_audit(
    config: BotConfig,
    *,
    video_id: str,
    media_file: Path,
    chat_file: Path | None = None,
    chat_video_file: Path | None = None,
    title: str = "",
    channel: str = "",
    logger: logging.Logger = LOGGER,
) -> AutoAuditResult:
    if not config.shot_audit_enabled or not config.shot_audit_auto_run:
        return AutoAuditResult(False, message="Shot audit auto-run is disabled")
    if config.shot_audit_require_transcription and not transcription_outputs_exist(media_file):
        return AutoAuditResult(False, message="Waiting for transcription outputs")
    expected_chat_video = chat_video_file or chat_video_output_file(media_file)
    if config.shot_audit_require_chat_video and not expected_chat_video.is_file():
        return AutoAuditResult(False, message="Waiting for rendered chat video")
    project = run_shot_audit(
        config,
        video_id=video_id,
        media_file=media_file,
        chat_file=chat_file,
        chat_video_file=expected_chat_video if expected_chat_video.is_file() else None,
        title=title,
        channel=channel,
        logger=logger,
    )
    return AutoAuditResult(True, project.project_id, project.message)


def status_for_totals(
    donations: list[DonationEvent],
    consumed: list[ConsumedShotEvent],
) -> str:
    totals = compute_totals(donations, consumed)
    if totals.unconfirmed_owed_shots > 0 or any(
        event.confidence in {LOW_CONFIDENCE, MEDIUM_CONFIDENCE} for event in consumed
    ):
        return "needs_review"
    return "done"


def replace_project(project: ShotAuditProject, **changes: Any) -> ShotAuditProject:
    return ShotAuditProject(
        project_id=changes.get("project_id", project.project_id),
        video_id=changes.get("video_id", project.video_id),
        title=changes.get("title", project.title),
        channel=changes.get("channel", project.channel),
        media_file=changes.get("media_file", project.media_file),
        chat_file=changes.get("chat_file", project.chat_file),
        chat_video_file=changes.get("chat_video_file", project.chat_video_file),
        status=changes.get("status", project.status),
        message=changes.get("message", project.message),
        created_at=changes.get("created_at", project.created_at),
        updated_at=utc_now_iso(),
        rules=changes.get("rules", project.rules),
        donations=changes.get("donations", project.donations),
        consumed=changes.get("consumed", project.consumed),
        media_items=changes.get("media_items", project.media_items),
    )


def save_project(config: BotConfig, project: ShotAuditProject) -> None:
    directory = project_dir(config, project.project_id)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / PROJECT_FILENAME
    replacement = target.with_name(f"{target.name}.writing")
    replacement.write_text(
        json.dumps(project_to_dict(project), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    replacement.replace(target)


def load_project(config: BotConfig, project_id: str) -> ShotAuditProject | None:
    path = project_file(config, project_id)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return project_from_dict(payload)


def list_projects(config: BotConfig) -> list[ShotAuditProject]:
    root = audit_root(config)
    if not root.is_dir():
        return []
    projects: list[ShotAuditProject] = []
    for path in sorted(root.glob(f"*/{PROJECT_FILENAME}")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            projects.append(project_from_dict(payload))
    return sorted(projects, key=lambda project: project.updated_at, reverse=True)


def project_to_dict(project: ShotAuditProject) -> dict[str, Any]:
    return asdict(project) | {"totals": asdict(project_totals(project))}


def project_from_dict(payload: dict[str, Any]) -> ShotAuditProject:
    project = ShotAuditProject(
        project_id=str(payload.get("project_id") or ""),
        video_id=str(payload.get("video_id") or ""),
        title=str(payload.get("title") or ""),
        channel=str(payload.get("channel") or ""),
        media_file=str(payload.get("media_file") or ""),
        chat_file=str(payload.get("chat_file") or ""),
        chat_video_file=str(payload.get("chat_video_file") or ""),
        status=str(payload.get("status") or "queued"),
        message=str(payload.get("message") or ""),
        created_at=str(payload.get("created_at") or ""),
        updated_at=str(payload.get("updated_at") or ""),
        rules=[rule_from_dict(item) for item in payload.get("rules") or [] if isinstance(item, dict)],
        donations=[
            donation_from_dict(item)
            for item in payload.get("donations") or []
            if isinstance(item, dict)
        ],
        consumed=[
            consumed_from_dict(item)
            for item in payload.get("consumed") or []
            if isinstance(item, dict)
        ],
        media_items=[
            media_item_from_dict(item)
            for item in payload.get("media_items") or []
            if isinstance(item, dict)
        ],
    )
    if project.media_items:
        return project
    if not project.media_file:
        return project
    return replace_project(
        project,
        media_items=[
            ShotAuditMedia(
                media_file=project.media_file,
                video_id=project.video_id,
                title=project.title,
                channel=project.channel,
                chat_file=project.chat_file,
                chat_video_file=project.chat_video_file,
            )
        ],
    )


def rule_from_dict(payload: dict[str, Any]) -> ShotRule:
    return ShotRule(
        amount_min=float(payload.get("amount_min") or 0.0),
        amount_max=float(payload.get("amount_max") or 0.0),
        shots=int(payload.get("shots") or 0),
        currency=str(payload.get("currency") or "USD"),
        label=str(payload.get("label") or ""),
    )


def media_item_from_dict(payload: dict[str, Any]) -> ShotAuditMedia:
    return ShotAuditMedia(
        media_file=str(payload.get("media_file") or ""),
        video_id=str(payload.get("video_id") or ""),
        title=str(payload.get("title") or ""),
        channel=str(payload.get("channel") or ""),
        chat_file=str(payload.get("chat_file") or ""),
        chat_video_file=str(payload.get("chat_video_file") or ""),
    )


def donation_from_dict(payload: dict[str, Any]) -> DonationEvent:
    return DonationEvent(
        event_id=str(payload.get("event_id") or ""),
        offset_seconds=float(payload.get("offset_seconds") or 0.0),
        amount=float(payload.get("amount") or 0.0),
        currency=str(payload.get("currency") or "USD"),
        owed_shots=int(payload.get("owed_shots") or 0),
        rule_label=str(payload.get("rule_label") or ""),
        username=str(payload.get("username") or ""),
        message=str(payload.get("message") or ""),
        raw_text=str(payload.get("raw_text") or ""),
        source=str(payload.get("source") or "ocr"),
        confidence=str(payload.get("confidence") or MEDIUM_CONFIDENCE),
        media_name=str(payload.get("media_name") or ""),
    )


def consumed_from_dict(payload: dict[str, Any]) -> ConsumedShotEvent:
    return ConsumedShotEvent(
        event_id=str(payload.get("event_id") or ""),
        offset_seconds=float(payload.get("offset_seconds") or 0.0),
        count=int(payload.get("count") or 0),
        confidence=str(payload.get("confidence") or LOW_CONFIDENCE),
        source=str(payload.get("source") or ""),
        evidence=str(payload.get("evidence") or ""),
        linked_donation_id=str(payload.get("linked_donation_id") or ""),
        note=str(payload.get("note") or ""),
        media_name=str(payload.get("media_name") or ""),
    )


def project_media_items(project: ShotAuditProject) -> list[ShotAuditMedia]:
    if project.media_items:
        return list(project.media_items)
    if not project.media_file:
        return []
    return [
        ShotAuditMedia(
            media_file=project.media_file,
            video_id=project.video_id,
            title=project.title,
            channel=project.channel,
            chat_file=project.chat_file,
            chat_video_file=project.chat_video_file,
        )
    ]


def project_totals(project: ShotAuditProject) -> AuditTotals:
    return compute_totals(project.donations, project.consumed)


def compute_totals(
    donations: list[DonationEvent],
    consumed: list[ConsumedShotEvent],
) -> AuditTotals:
    owed = sum(event.owed_shots for event in donations)
    machine = sum(
        event.count
        for event in consumed
        if event.confidence == HIGH_CONFIDENCE and event.source != "manual"
    )
    manual = sum(event.count for event in consumed if event.source == "manual")
    counted = machine + manual
    return AuditTotals(
        owed_shots=owed,
        machine_high_confidence_shots=machine,
        manual_shots=manual,
        counted_consumed_shots=counted,
        unconfirmed_owed_shots=max(0, owed - counted),
        donation_count=len(donations),
        consumed_event_count=len(consumed),
    )


def add_manual_visible_shot(
    config: BotConfig,
    project_id: str,
    *,
    offset_seconds: float,
    count: int,
    note: str = "",
    media_name: str = "",
) -> ShotAuditProject:
    project = require_project(config, project_id)
    if count <= 0:
        raise ShotAuditError("Visible shot count must be positive")
    media_name = media_name.strip() or default_media_name(project)
    evidence = f"Manual visible mark at {format_offset(offset_seconds)}"
    event = ConsumedShotEvent(
        event_id=manual_event_id(offset_seconds, count, note),
        offset_seconds=round(max(0.0, offset_seconds), 3),
        count=count,
        confidence=MANUAL_CONFIDENCE,
        source="manual",
        evidence=evidence,
        note=note.strip(),
        media_name=media_name,
    )
    consumed = sorted(
        [*project.consumed, event],
        key=lambda item: (item.media_name, item.offset_seconds),
    )
    updated = replace_project(
        project,
        status=status_for_totals(project.donations, consumed),
        message="Manual visible-shot correction saved",
        consumed=consumed,
    )
    save_project(config, updated)
    return updated


def delete_consumed_event(
    config: BotConfig,
    project_id: str,
    event_id: str,
) -> ShotAuditProject:
    project = require_project(config, project_id)
    consumed = [event for event in project.consumed if event.event_id != event_id]
    updated = replace_project(
        project,
        status=status_for_totals(project.donations, consumed),
        message="Visible-shot event removed",
        consumed=consumed,
    )
    save_project(config, updated)
    return updated


def require_project(config: BotConfig, project_id: str) -> ShotAuditProject:
    project = load_project(config, project_id)
    if project is None:
        raise ShotAuditError(f"Shot audit project not found: {project_id}")
    return project


def default_media_name(project: ShotAuditProject) -> str:
    items = project_media_items(project)
    if not items:
        return ""
    return Path(items[0].media_file).name


def manual_event_id(offset_seconds: float, count: int, note: str) -> str:
    digest = hashlib.sha1(
        f"{time.time()}:{offset_seconds}:{count}:{note}".encode("utf-8")
    ).hexdigest()[:12]
    return f"manual_{digest}"


def render_markdown_report(project: ShotAuditProject) -> str:
    totals = project_totals(project)
    lines = [
        f"# Shot Audit: {project.title or project.video_id}",
        "",
        "This is a machine-estimated audit aid, not legal proof of fraud.",
        "",
        "## Totals",
        "",
        f"- Owed shots: {totals.owed_shots}",
        f"- Machine high-confidence consumed shots: {totals.machine_high_confidence_shots}",
        f"- Manual visible-shot corrections: {totals.manual_shots}",
        f"- Counted consumed shots: {totals.counted_consumed_shots}",
        f"- Unconfirmed owed shots: {totals.unconfirmed_owed_shots}",
        "",
        "## Donation Events",
        "",
        "| Media | Time | Amount | Owed | Rule | Confidence | Evidence |",
        "| --- | --- | ---: | ---: | --- | --- | --- |",
    ]
    if project.donations:
        for event in project.donations:
            lines.append(
                "| "
                f"{markdown_cell(event.media_name or '-')} | "
                f"{format_offset(event.offset_seconds)} | "
                f"{event.currency} {event.amount:.2f} | "
                f"{event.owed_shots} | "
                f"{markdown_cell(event.rule_label)} | "
                f"{event.confidence} | "
                f"{markdown_cell(event.raw_text[:180])} |"
            )
    else:
        lines.append("| - | - | - | - | - | - | No donation alerts detected |")
    lines.extend(
        [
            "",
            "## Consumed Shot Estimates",
            "",
            "| Media | Time | Count | Confidence | Source | Evidence |",
            "| --- | --- | ---: | --- | --- | --- |",
        ]
    )
    if project.consumed:
        for event in project.consumed:
            lines.append(
                "| "
                f"{markdown_cell(event.media_name or '-')} | "
                f"{format_offset(event.offset_seconds)} | "
                f"{event.count} | "
                f"{event.confidence} | "
                f"{event.source} | "
                f"{markdown_cell((event.note or event.evidence)[:180])} |"
            )
    else:
        lines.append("| - | - | - | - | - | No consumed-shot estimates detected |")
    return "\n".join(lines) + "\n"


def render_funny_report(project: ShotAuditProject) -> str:
    totals = project_totals(project)
    return (
        f"# Totally Not Serious Shot Audit: {project.title or project.video_id}\n\n"
        "Parody report. Numbers come from the machine audit, but this is not a legal accusation.\n\n"
        f"- Alleged Jager invoice: {totals.owed_shots} shots\n"
        f"- Machine says visibly/safely counted: {totals.counted_consumed_shots} shots\n"
        f"- Currently haunting the spreadsheet: {totals.unconfirmed_owed_shots} shots\n\n"
        "Verdict: the ledger would like a word, preferably with timestamps.\n"
    )


def render_review_csv(project: ShotAuditProject) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "kind",
            "media",
            "offset_seconds",
            "time",
            "count_or_owed",
            "amount",
            "currency",
            "confidence",
            "source",
            "evidence",
        ]
    )
    for donation in project.donations:
        writer.writerow(
            [
                "donation",
                donation.media_name,
                f"{donation.offset_seconds:.3f}",
                format_offset(donation.offset_seconds),
                donation.owed_shots,
                f"{donation.amount:.2f}",
                donation.currency,
                donation.confidence,
                donation.source,
                donation.raw_text,
            ]
        )
    for event in project.consumed:
        writer.writerow(
            [
                "consumed",
                event.media_name,
                f"{event.offset_seconds:.3f}",
                format_offset(event.offset_seconds),
                event.count,
                "",
                "",
                event.confidence,
                event.source,
                event.note or event.evidence,
            ]
        )
    return output.getvalue()


def markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def format_offset(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def is_path_under(path: Path, root: Path) -> bool:
    try:
        path_resolved = path.resolve(strict=True)
        root_resolved = root.resolve(strict=True)
    except OSError:
        return False
    return path_resolved == root_resolved or root_resolved in path_resolved.parents


def project_media_file(
    config: BotConfig,
    project: ShotAuditProject,
    media_index: int = 0,
) -> Path | None:
    items = project_media_items(project)
    if media_index < 0 or media_index >= len(items):
        return None
    path = Path(items[media_index].media_file)
    if not path.is_file() or not is_path_under(path, config.download_dir):
        return None
    return path


def project_media_files(config: BotConfig, project: ShotAuditProject) -> list[Path]:
    files: list[Path] = []
    for index, _item in enumerate(project_media_items(project)):
        media_file = project_media_file(config, project, index)
        if media_file is not None:
            files.append(media_file)
    return files
