from __future__ import annotations

from dataclasses import dataclass, replace
from threading import Lock
import time


@dataclass(frozen=True, slots=True)
class TrackedJob:
    job_id: str
    kind: str
    status: str
    phase: str
    progress: float | None
    video_id: str
    item: str
    detail: str
    message: str
    started_at: float
    updated_at: float
    finished_at: float | None = None


_TRACKED_JOBS: dict[str, TrackedJob] = {}
_TRACKED_JOBS_LOCK = Lock()
MAX_TRACKED_JOBS = 200
FINAL_JOB_STATUSES = {"done", "failed", "interrupted"}


def start_tracked_job(
    job_id: str,
    *,
    kind: str,
    video_id: str,
    item: str,
    detail: str = "",
    phase: str = "Queued",
    message: str = "Queued",
    progress: float | None = 0.0,
) -> TrackedJob:
    now = time.time()
    job = TrackedJob(
        job_id=job_id,
        kind=kind,
        status="running",
        phase=phase,
        progress=progress,
        video_id=video_id,
        item=item,
        detail=detail,
        message=message,
        started_at=now,
        updated_at=now,
    )
    with _TRACKED_JOBS_LOCK:
        _TRACKED_JOBS[job_id] = job
        prune_tracked_jobs_locked()
    return job


def update_tracked_job(
    job_id: str,
    *,
    status: str | None = None,
    phase: str | None = None,
    progress: float | None = None,
    message: str | None = None,
    finished: bool = False,
) -> None:
    now = time.time()
    with _TRACKED_JOBS_LOCK:
        current = _TRACKED_JOBS.get(job_id)
        if current is None:
            return
        next_status = status if status is not None else current.status
        _TRACKED_JOBS[job_id] = replace(
            current,
            status=next_status,
            phase=phase if phase is not None else current.phase,
            progress=progress if progress is not None else current.progress,
            message=message if message is not None else current.message,
            updated_at=now,
            finished_at=now if finished else current.finished_at,
        )
        prune_tracked_jobs_locked()


def finish_tracked_job(
    job_id: str,
    *,
    status: str = "done",
    phase: str = "Complete",
    message: str = "Complete",
    progress: float | None = 1.0,
) -> None:
    update_tracked_job(
        job_id,
        status=status,
        phase=phase,
        progress=progress,
        message=message,
        finished=True,
    )


def list_tracked_jobs(limit: int = MAX_TRACKED_JOBS) -> list[TrackedJob]:
    with _TRACKED_JOBS_LOCK:
        jobs = sorted(
            _TRACKED_JOBS.values(),
            key=lambda job: (job.started_at or job.updated_at or 0.0, job.job_id),
            reverse=True,
        )
    return jobs[:limit]


def clear_tracked_jobs() -> None:
    with _TRACKED_JOBS_LOCK:
        _TRACKED_JOBS.clear()


def prune_tracked_jobs_locked() -> None:
    if len(_TRACKED_JOBS) <= MAX_TRACKED_JOBS:
        return
    ordered = sorted(
        _TRACKED_JOBS.values(),
        key=lambda job: job.updated_at or job.started_at,
        reverse=True,
    )
    keep = {job.job_id for job in ordered[:MAX_TRACKED_JOBS]}
    for job_id in list(_TRACKED_JOBS):
        if job_id not in keep:
            _TRACKED_JOBS.pop(job_id, None)
