from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
import asyncio
import hashlib
import json
import logging
import re
import tempfile
import uuid


LOGGER = logging.getLogger(__name__)
POWERCHAT_EVENT_SUFFIX = ".powerchat-events.json"
POWERCHAT_HOST = "powerchat.live"
POWERCHAT_SIDECAR_VERSION = 1
POWERCHAT_DEDUPE_WINDOW_SECONDS = 60
POWERCHAT_GIFT_RE = re.compile(
    r"^(?P<donor>.+?)\s+just\s+gifted\s+"
    r"(?P<amount>\d+(?:[.,]\d+)?)\s+"
    r"(?P<unit>[A-Za-z][A-Za-z0-9 _-]*?)\s+on\s+"
    r"(?P<platform>[A-Za-z][A-Za-z0-9 _-]*)\s*$",
    re.IGNORECASE,
)
POWERCHAT_CURRENCY_RE = re.compile(
    r"(?P<symbol>[$£€])\s*(?P<amount>\d+(?:,\d{3})*(?:\.\d+)?)"
)
CURRENCY_SYMBOLS = {
    "$": "USD",
    "£": "GBP",
    "€": "EUR",
}
EXPLICIT_ID_FIELDS = (
    "messageId",
    "message_id",
    "eventId",
    "event_id",
    "id",
    "paymentId",
    "payment_id",
    "transactionId",
    "transaction_id",
)


@dataclass(slots=True)
class PowerchatRecorder:
    sidecar_path: Path
    streamer_name: str
    username: str
    video_id: str
    segment_index: int
    stream_started_at: str = ""
    events: list[dict[str, Any]] = field(default_factory=list)
    _dedupe_keys: set[str] = field(default_factory=set, init=False)
    _preserve_invalid_sidecar: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        payload = _read_powerchat_sidecar(self.sidecar_path)
        if payload is None or not isinstance(payload.get("events"), list):
            if self.sidecar_path.is_file():
                try:
                    recovery_path = preserve_malformed_powerchat_sidecar(
                        self.sidecar_path
                    )
                except OSError as exc:
                    self._preserve_invalid_sidecar = True
                    LOGGER.error(
                        "Unable to preserve malformed Powerchat sidecar %s: %s",
                        self.sidecar_path,
                        exc,
                    )
                else:
                    LOGGER.warning(
                        "Moved malformed Powerchat sidecar %s to %s before recording new events",
                        self.sidecar_path,
                        recovery_path,
                    )
            payload = {}
        for raw_event in payload.get("events", []):
            if not isinstance(raw_event, dict):
                continue
            event = dict(raw_event)
            key = str(event.get("dedupe_key") or "").strip()
            if not key:
                key = powerchat_dedupe_key(event)
                event["dedupe_key"] = key
            event_keys = powerchat_observed_event_keys(event)
            if key.startswith("id:"):
                event_keys.append(key)
            if any(event_key in self._dedupe_keys for event_key in event_keys):
                for existing in self.events:
                    existing_keys = powerchat_observed_event_keys(existing)
                    existing_keys.append(str(existing.get("dedupe_key") or ""))
                    if set(event_keys) & set(existing_keys):
                        propagate_powerchat_test_marker(existing, event)
                        break
                continue
            self._dedupe_keys.update(event_keys)
            sources = powerchat_observed_sources(event)
            if sources:
                event["observed_sources"] = sources
            self.events.append(dict(event))

    def record_payload(
        self,
        payload: Any,
        *,
        source: str,
        received_at: str | None = None,
    ) -> bool:
        event = normalize_powerchat_payload(
            payload,
            source=source,
            received_at=received_at,
            stream_started_at=self.stream_started_at,
        )
        if event is None:
            return False
        key = str(event.get("dedupe_key") or "").strip()
        if key.startswith("id:") and key in self._dedupe_keys:
            for existing in self.events:
                existing_keys = powerchat_observed_event_keys(existing)
                existing_keys.append(str(existing.get("dedupe_key") or ""))
                if key in existing_keys and propagate_powerchat_test_marker(
                    existing, event
                ):
                    self.write()
                    break
            return False
        if self._pair_cross_source_duplicate(event):
            if key.startswith("id:"):
                self._dedupe_keys.add(key)
            self.write()
            return False
        if not key.startswith("id:"):
            event["dedupe_key"] = f"occurrence:{uuid.uuid4().hex}"
        sources = powerchat_observed_sources(event)
        if sources:
            event["observed_sources"] = sources
        self.events.append(event)
        if key.startswith("id:"):
            self._dedupe_keys.add(key)
        self.write()
        return True

    def _pair_cross_source_duplicate(self, event: dict[str, Any]) -> bool:
        source = str(event.get("source") or "").strip().casefold()
        if not source:
            return False
        pairing_key = powerchat_pairing_key(event)
        for existing in self.events:
            sources = powerchat_observed_sources(existing)
            if (
                source in sources
                or powerchat_pairing_key(existing) != pairing_key
                or not powerchat_events_within_dedupe_window(existing, event)
            ):
                continue
            propagate_powerchat_test_marker(existing, event)
            existing["observed_sources"] = [*sources, source]
            key = str(event.get("dedupe_key") or "").strip()
            if key.startswith("id:"):
                observed_keys = powerchat_observed_event_keys(existing)
                if key not in observed_keys:
                    observed_keys.append(key)
                existing["observed_event_keys"] = observed_keys
            return True
        return False

    def write(self) -> None:
        if not self.events or self._preserve_invalid_sidecar:
            return
        write_powerchat_sidecar(
            self.sidecar_path,
            events=self.events,
            streamer_name=self.streamer_name,
            username=self.username,
            video_id=self.video_id,
            segment_index=self.segment_index,
            stream_started_at=self.stream_started_at,
        )


def powerchat_event_file(media_file: Path) -> Path:
    return media_file.with_suffix(POWERCHAT_EVENT_SUFFIX)


def is_powerchat_event_file(name: str) -> bool:
    return name.endswith(POWERCHAT_EVENT_SUFFIX)


def normalize_powerchat_payload(
    payload: Any,
    *,
    source: str,
    received_at: str | None = None,
    stream_started_at: str = "",
) -> dict[str, Any] | None:
    if isinstance(payload, str):
        raw: dict[str, Any] = {"message": payload}
    elif isinstance(payload, dict):
        raw = dict(payload)
    else:
        return None

    received = received_at or first_string(raw, "createdAt", "created_at", "timestamp") or utc_now_iso()
    message = first_string(raw, "message", "text", "body", "subMessage")
    donor = first_string(raw, "donator", "donor", "displayName", "name")
    platform = first_string(raw, "paymentPlatform", "platform")
    explicit_id = first_string(raw, *EXPLICIT_ID_FIELDS)
    currency = first_string(raw, "currency", "fiat_currency", "fiatCurrency").upper()
    gift_match = POWERCHAT_GIFT_RE.match(message) if message else None

    money_amount: float | None = None
    money_currency = currency
    unit_amount: float | None = None
    unit = ""
    kind = "unknown"

    fiat = parse_number(raw.get("fiat_equivalent"))
    if fiat is None:
        fiat = parse_number(raw.get("fiatEquivalent"))
    if fiat is not None:
        money_amount = fiat
        money_currency = money_currency or "USD"
        kind = "money"
    elif not gift_match:
        amount = parse_number(raw.get("amount"))
        if amount is not None and (currency or platform):
            money_amount = amount
            money_currency = money_currency or "USD"
            kind = "money"

    if money_amount is None and message:
        if gift_match:
            donor = donor or gift_match.group("donor").strip()
            unit_amount = parse_number(gift_match.group("amount"))
            unit = gift_match.group("unit").strip()
            platform = platform or gift_match.group("platform").strip()
            kind = "unit" if unit_amount is not None else "unknown"
        else:
            currency_match = POWERCHAT_CURRENCY_RE.search(message)
            if currency_match:
                money_amount = parse_number(currency_match.group("amount"))
                money_currency = CURRENCY_SYMBOLS.get(currency_match.group("symbol"), "")
                kind = "money" if money_amount is not None and money_currency else "unknown"

    if not donor:
        donor = first_string(raw, "username", "user", "from")
    if not platform:
        font = first_string(raw, "customMessageFont")
        if font.startswith("tts-message-"):
            platform = font.removeprefix("tts-message-").strip().title()

    event = {
        "id": explicit_id or "",
        "dedupe_key": "",
        "source": source,
        "received_at": received,
        "offset_seconds": stream_offset_seconds(received, stream_started_at),
        "kind": kind,
        "donor": donor,
        "platform": normalize_platform_label(platform),
        "message": message,
        "money_amount": money_amount,
        "money_currency": money_currency,
        "unit_amount": unit_amount,
        "unit": unit,
        "raw": raw,
    }
    event["is_test"] = is_powerchat_test_event(event)
    event["dedupe_key"] = powerchat_dedupe_key(event)
    if not message and kind == "unknown" and not explicit_id:
        return None
    return event


def powerchat_dedupe_key(event: dict[str, Any]) -> str:
    explicit_id = str(event.get("id") or "").strip()
    if explicit_id:
        return f"id:{explicit_id}"
    return powerchat_payload_key(event, include_time_bucket=True)


def is_powerchat_test_event(event: dict[str, Any]) -> bool:
    """Return whether Powerchat identifies the payment provider as Test."""

    marker = event.get("is_test")
    if marker is True or (
        isinstance(marker, str)
        and marker.strip().casefold() in {"1", "true", "yes"}
    ):
        return True

    providers = [
        event.get("paymentPlatform"),
        event.get("paymentProvider"),
        event.get("payment_provider"),
        event.get("platform"),
    ]
    raw = event.get("raw")
    if isinstance(raw, dict):
        providers.extend(
            [
                raw.get("paymentPlatform"),
                raw.get("paymentProvider"),
                raw.get("payment_provider"),
            ]
        )
    return any(
        str(provider or "").strip().casefold() in {"test", "powerchat-test"}
        for provider in providers
    )


def propagate_powerchat_test_marker(
    target: dict[str, Any],
    candidate: dict[str, Any],
) -> bool:
    if not is_powerchat_test_event(candidate) or is_powerchat_test_event(target):
        return False
    target["is_test"] = True
    return True


def powerchat_pairing_key(event: dict[str, Any]) -> str:
    return powerchat_payload_key(event, include_time_bucket=False)


def powerchat_payload_key(
    event: dict[str, Any],
    *,
    include_time_bucket: bool,
) -> str:
    bits = {
        "kind": event.get("kind") or "unknown",
        "donor": event.get("donor") or "",
        "platform": event.get("platform") or "",
        "message": event.get("message") or "",
        "money_amount": event.get("money_amount"),
        "money_currency": event.get("money_currency") or "",
        "unit_amount": event.get("unit_amount"),
        "unit": event.get("unit") or "",
    }
    if include_time_bucket:
        bits["time_bucket"] = powerchat_dedupe_time_bucket(
            event.get("received_at")
        )
    digest = hashlib.sha1(json.dumps(bits, sort_keys=True).encode("utf-8")).hexdigest()
    return f"payload:{digest[:20]}"


def powerchat_observed_sources(event: dict[str, Any]) -> list[str]:
    raw_sources = event.get("observed_sources")
    candidates = raw_sources if isinstance(raw_sources, list) else []
    candidates = [*candidates, event.get("source")]
    sources: list[str] = []
    for value in candidates:
        source = str(value or "").strip().casefold()
        if source and source not in sources:
            sources.append(source)
    return sources


def powerchat_observed_event_keys(event: dict[str, Any]) -> list[str]:
    raw_keys = event.get("observed_event_keys")
    if not isinstance(raw_keys, list):
        return []
    keys: list[str] = []
    for value in raw_keys:
        key = str(value or "").strip()
        if key.startswith("id:") and key not in keys:
            keys.append(key)
    return keys


def powerchat_events_within_dedupe_window(
    first: dict[str, Any],
    second: dict[str, Any],
) -> bool:
    first_timestamp = parse_iso_datetime(str(first.get("received_at") or ""))
    second_timestamp = parse_iso_datetime(str(second.get("received_at") or ""))
    if first_timestamp is None or second_timestamp is None:
        return False
    return (
        abs((second_timestamp - first_timestamp).total_seconds())
        <= POWERCHAT_DEDUPE_WINDOW_SECONDS
    )


def powerchat_dedupe_time_bucket(value: Any) -> str:
    timestamp = parse_iso_datetime(str(value or ""))
    if timestamp is None:
        return ""
    bucket = int(timestamp.timestamp() // POWERCHAT_DEDUPE_WINDOW_SECONDS)
    return str(bucket)


def write_powerchat_sidecar(
    path: Path,
    *,
    events: Iterable[dict[str, Any]],
    streamer_name: str,
    username: str,
    video_id: str,
    segment_index: int | None = None,
    stream_started_at: str = "",
) -> None:
    event_list: list[dict[str, Any]] = []
    for event in events:
        normalized_event = dict(event)
        normalized_event["is_test"] = is_powerchat_test_event(normalized_event)
        event_list.append(normalized_event)
    test_event_count = sum(is_powerchat_test_event(event) for event in event_list)
    payload = {
        "version": POWERCHAT_SIDECAR_VERSION,
        "generated_at": utc_now_iso(),
        "streamer": streamer_name,
        "username": username,
        "video_id": video_id,
        "segment_index": segment_index,
        "stream_start": stream_started_at,
        "event_count": len(event_list),
        "counted_event_count": len(event_list) - test_event_count,
        "test_event_count": test_event_count,
        "totals": powerchat_totals(event_list),
        "events": event_list,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temp_path = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp_path.replace(path)


def load_powerchat_sidecar(path: Path) -> dict[str, Any]:
    payload = _read_powerchat_sidecar(path)
    return payload if payload is not None else {}


def _read_powerchat_sidecar(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return repair_powerchat_sidecar_payload(payload)


def preserve_malformed_powerchat_sidecar(path: Path) -> Path:
    """Move an unreadable sidecar aside without exposing it as segment media."""

    recovery_dir = path.parent / ".powerchat-recovery"
    recovery_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    recovery_path = recovery_dir / (
        f"{path.name}.{stamp}.{uuid.uuid4().hex}.malformed"
    )
    path.rename(recovery_path)
    return recovery_path


def repair_powerchat_sidecar_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw_events = payload.get("events")
    if not isinstance(raw_events, list):
        return payload

    stream_started_at = str(payload.get("stream_start") or "")
    events = [
        repair_powerchat_event(event, stream_started_at=stream_started_at)
        for event in raw_events
        if isinstance(event, dict)
    ]
    repaired = dict(payload)
    repaired["events"] = events
    repaired["event_count"] = len(events)
    repaired["test_event_count"] = sum(
        is_powerchat_test_event(event) for event in events
    )
    repaired["counted_event_count"] = (
        len(events) - repaired["test_event_count"]
    )
    repaired["totals"] = powerchat_totals(events)
    return repaired


def repair_powerchat_event(
    event: dict[str, Any],
    *,
    stream_started_at: str = "",
) -> dict[str, Any]:
    original = dict(event)
    original["is_test"] = is_powerchat_test_event(original)
    raw = event.get("raw")
    if not isinstance(raw, dict):
        return original

    source = str(event.get("source") or "feed")
    raw_timestamp = first_string(raw, "createdAt", "created_at", "timestamp")
    fallback_received_at = str(event.get("received_at") or "") or None
    repaired = normalize_powerchat_payload(
        raw,
        source=source,
        received_at=None if raw_timestamp else fallback_received_at,
        stream_started_at=stream_started_at,
    )
    if repaired is None or repaired.get("kind") == "unknown":
        return original

    if event.get("kind") not in {None, "", "unknown"}:
        return original

    merged = dict(event)
    for key in (
        "id",
        "dedupe_key",
        "source",
        "received_at",
        "kind",
        "donor",
        "platform",
        "message",
        "money_amount",
        "money_currency",
        "unit_amount",
        "unit",
        "is_test",
        "raw",
    ):
        merged[key] = repaired.get(key)
    merged["offset_seconds"] = (
        repaired.get("offset_seconds")
        if repaired.get("offset_seconds") is not None
        else event.get("offset_seconds")
    )
    return merged


def powerchat_totals(events: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    money: dict[str, float] = {}
    units: dict[tuple[str, str], float] = {}
    for event in events:
        if is_powerchat_test_event(event):
            continue
        if event.get("kind") == "money":
            amount = parse_number(event.get("money_amount"))
            currency = str(event.get("money_currency") or "").upper()
            if amount is not None and currency:
                money[currency] = money.get(currency, 0.0) + amount
        elif event.get("kind") == "unit":
            amount = parse_number(event.get("unit_amount"))
            unit = str(event.get("unit") or "").strip()
            platform = normalize_platform_label(str(event.get("platform") or ""))
            if amount is not None and unit:
                key = (platform, unit)
                units[key] = units.get(key, 0.0) + amount
    return {
        "money": [
            {"currency": currency, "amount": round(amount, 2)}
            for currency, amount in sorted(money.items())
        ],
        "units": [
            {"platform": platform, "unit": unit, "amount": round(amount, 2)}
            for (platform, unit), amount in sorted(units.items())
        ],
    }


def merge_powerchat_events(
    *event_groups: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for events in event_groups:
        for raw_event in events:
            event = dict(raw_event)
            key = str(event.get("dedupe_key") or "").strip()
            if not key:
                key = powerchat_dedupe_key(event)
                event["dedupe_key"] = key
            existing = seen.get(key)
            if existing is not None:
                propagate_powerchat_test_marker(existing, event)
                continue
            seen[key] = event
            merged.append(event)
    return merged


def copy_powerchat_segment_sidecar(
    source: Path,
    media_file: Path,
    *,
    streamer_name: str = "",
    username: str = "",
    video_id: str = "",
    segment_index: int | None = None,
    logger: logging.Logger = LOGGER,
) -> Path | None:
    if not source.is_file():
        return None
    payload = _read_powerchat_sidecar(source)
    if payload is None or not isinstance(payload.get("events"), list):
        logger.warning(
            "Unable to read Powerchat sidecar %s; preserving it for recovery",
            source,
        )
        return None
    events = [event for event in payload["events"] if isinstance(event, dict)]
    if not events:
        try:
            source.unlink(missing_ok=True)
        except OSError:
            logger.debug("Unable to remove empty Powerchat sidecar %s", source)
        return None
    target = powerchat_event_file(media_file)
    existing_payload: dict[str, Any] = {}
    if target.is_file():
        candidate = _read_powerchat_sidecar(target)
        if candidate is None or not isinstance(candidate.get("events"), list):
            try:
                recovery_path = preserve_malformed_powerchat_sidecar(target)
            except OSError as exc:
                logger.warning(
                    "Unable to preserve malformed Powerchat target %s: %s",
                    target,
                    exc,
                )
                return None
            logger.warning(
                "Moved malformed Powerchat target %s to %s before finalizing",
                target,
                recovery_path,
            )
        else:
            existing_payload = candidate
    existing_events = [
        event
        for event in existing_payload.get("events", [])
        if isinstance(event, dict)
    ]
    events = merge_powerchat_events(existing_events, events)
    try:
        write_powerchat_sidecar(
            target,
            events=events,
            streamer_name=(
                streamer_name
                or str(payload.get("streamer") or "")
                or str(existing_payload.get("streamer") or "")
            ),
            username=(
                username
                or str(payload.get("username") or "")
                or str(existing_payload.get("username") or "")
            ),
            video_id=(
                video_id
                or str(payload.get("video_id") or "")
                or str(existing_payload.get("video_id") or "")
            ),
            segment_index=segment_index,
            stream_started_at=str(
                payload.get("stream_start")
                or existing_payload.get("stream_start")
                or ""
            ),
        )
    except OSError as exc:
        logger.warning("Unable to write Powerchat sidecar %s: %s", target, exc)
        return None
    try:
        source.unlink()
    except OSError:
        logger.debug("Unable to remove temporary Powerchat sidecar %s", source)
    return target


async def run_powerchat_listener(
    username: str,
    recorder: PowerchatRecorder,
    *,
    host: str = POWERCHAT_HOST,
    connect: Callable[..., Any] | None = None,
    sleep: Callable[[float], Any] = asyncio.sleep,
    logger: logging.Logger = LOGGER,
    status_callback: Callable[[str, str], None] | None = None,
) -> None:
    username = username.strip().lower()
    if not username:
        return
    tasks = [
        asyncio.create_task(
            _listen_powerchat_socket(
                powerchat_ws_url(username, host=host, suffix="_feed"),
                source="feed",
                recorder=recorder,
                connect=connect,
                sleep=sleep,
                logger=logger,
                status_callback=status_callback,
            )
        ),
        asyncio.create_task(
            _listen_powerchat_socket(
                powerchat_ws_url(username, host=host, suffix=""),
                source="tts",
                recorder=recorder,
                connect=connect,
                sleep=sleep,
                logger=logger,
                status_callback=status_callback,
            )
        ),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        recorder.write()


async def _listen_powerchat_socket(
    url: str,
    *,
    source: str,
    recorder: PowerchatRecorder,
    connect: Callable[..., Any] | None,
    sleep: Callable[[float], Any],
    logger: logging.Logger,
    status_callback: Callable[[str, str], None] | None,
) -> None:
    backoff = 1.0
    while True:
        try:
            if connect is None:
                try:
                    import websockets  # type: ignore[import-not-found]
                except ImportError as exc:  # pragma: no cover - environment dependent.
                    raise RuntimeError(
                        "Install the websockets package to enable Powerchat listening."
                    ) from exc
                connect_callable = websockets.connect
            else:
                connect_callable = connect

            async with connect_callable(url) as websocket:
                emit_status(status_callback, f"Powerchat {source} connected", "info")
                logger.info("Powerchat %s connected url=%s", source, url)
                backoff = 1.0
                async for message in websocket:
                    recorded = handle_powerchat_socket_message(
                        message,
                        source=source,
                        recorder=recorder,
                    )
                    if recorded and recorder.events and recorder.events[-1].get("kind") == "unknown":
                        emit_status(
                            status_callback,
                            f"Powerchat {source} stored unrecognized support payload",
                            "warning",
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - websocket schemas/network errors should not stop recording.
            emit_status(
                status_callback,
                f"Powerchat {source} reconnecting after error: {exc}",
                "warning",
            )
            logger.warning("Powerchat %s listener error: %s", source, exc)
            await sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)


def handle_powerchat_socket_message(
    message: Any,
    *,
    source: str,
    recorder: PowerchatRecorder,
) -> bool:
    if isinstance(message, bytes):
        text = message.decode("utf-8", "replace")
    else:
        text = str(message)
    text = text.strip()
    if not text or text.lower() in {"ping", "pong"}:
        return False
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = {"message": text}
    return recorder.record_payload(payload, source=source)


def powerchat_ws_url(username: str, *, host: str = POWERCHAT_HOST, suffix: str = "") -> str:
    return f"wss://{host}/{username.strip().lower()}{suffix}"


def emit_status(
    callback: Callable[[str, str], None] | None,
    message: str,
    level: str,
) -> None:
    if callback is None:
        return
    try:
        callback(message, level)
    except Exception:  # pragma: no cover - status logging must never break listener.
        LOGGER.debug("Powerchat status callback failed", exc_info=True)


def first_string(payload: dict[str, Any], *names: str) -> str:
    for name in names:
        value = payload.get(name)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def parse_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"[^0-9.,-]+", "", text)
    if not text:
        return None
    if text.count(",") and text.count("."):
        text = text.replace(",", "")
    elif text.count(",") == 1 and text.count(".") == 0:
        left, right = text.split(",", 1)
        text = f"{left}.{right}" if len(right) != 3 else left + right
    else:
        text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def normalize_platform_label(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    known = {
        "kick": "Kick",
        "youtube": "YouTube",
        "twitch": "Twitch",
        "rumble": "Rumble",
        "powerchat": "Powerchat",
        "test": "Test",
        "powerchat-test": "Test",
        "paypal": "PayPal",
    }
    return known.get(text.casefold(), text[:1].upper() + text[1:])


def stream_offset_seconds(received_at: str, stream_started_at: str) -> float | None:
    received = parse_iso_datetime(received_at)
    started = parse_iso_datetime(stream_started_at)
    if received is None or started is None:
        return None
    return max(0.0, (received - started).total_seconds())


def parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
