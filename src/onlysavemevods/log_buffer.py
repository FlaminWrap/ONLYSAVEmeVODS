from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Lock
import logging


LOG_BUFFER_LIMIT = 500
LOG_MESSAGE_LIMIT = 4000


@dataclass(frozen=True, slots=True)
class LogEntry:
    created_at: float
    level: str
    logger: str
    message: str


_LOGS: deque[LogEntry] = deque(maxlen=LOG_BUFFER_LIMIT)
_LOCK = Lock()
_REVISION = 0


class RingBufferLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        global _REVISION
        try:
            message = truncate_log_message(self.format(record))
            entry = LogEntry(
                created_at=record.created,
                level=record.levelname,
                logger=record.name,
                message=message,
            )
            with _LOCK:
                _LOGS.append(entry)
                _REVISION += 1
        except Exception:
            self.handleError(record)


def get_recent_log_entries(limit: int = 200) -> list[LogEntry]:
    entries, _revision = get_recent_log_snapshot(limit)
    return entries


def get_recent_log_snapshot(limit: int = 200) -> tuple[list[LogEntry], int]:
    """Return entries and a revision from the same locked buffer snapshot."""

    with _LOCK:
        entries = list(_LOGS)
        revision = _REVISION
    if limit <= 0:
        return [], revision
    return entries[-limit:], revision


def clear_log_buffer() -> None:
    global _REVISION
    with _LOCK:
        _LOGS.clear()
        _REVISION += 1


def truncate_log_message(message: str) -> str:
    if len(message) <= LOG_MESSAGE_LIMIT:
        return message
    return f"{message[:LOG_MESSAGE_LIMIT]}... <truncated>"
