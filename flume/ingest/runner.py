"""Auto-ingest state machine."""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flume.ingest.fingerprint import FileFingerprint, fingerprint_file, is_quiet
from flume.ingest.state import IngestStateStore, IngestStatus
from flume.sources import DiscoveredTranscript, TranscriptSource


@dataclass(frozen=True)
class IngestRequest:
    transcript: DiscoveredTranscript
    fingerprint: FileFingerprint


@dataclass(frozen=True)
class IngestOutcome:
    session_id: str | None = None
    trace_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


IngestFunction = Callable[[IngestRequest], IngestOutcome | None]


class IngestAct(StrEnum):
    """What one pass decided about one file.

    An enum because every member is written in one place and compared in
    another (`_summarize` counts them): as bare strings a typo on either
    side silently produces a zero count rather than an error.
    """

    INGESTED = "ingested"
    EMPTY = "empty"
    FAILED = "failed"
    WOULD_INGEST = "would_ingest"
    SKIP_ACTIVE = "skip_active"
    SKIP_UNCHANGED = "skip_unchanged"
    SKIP_VANISHED = "skip_vanished"


@dataclass(frozen=True)
class IngestAction:
    source_type: str
    path: Path
    action: IngestAct
    status: str
    fingerprint: str | None = None
    mtime_ns: int | None = None
    mtime: str | None = None
    size_bytes: int | None = None
    session_id: str | None = None
    trace_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "path": str(self.path),
            "action": self.action,
            "status": self.status,
            "fingerprint": self.fingerprint,
            "mtime_ns": self.mtime_ns,
            "mtime": self.mtime,
            "size_bytes": self.size_bytes,
            "session_id": self.session_id,
            "trace_id": self.trace_id,
            "metadata": self.metadata,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class IngestCycleSummary:
    source_type: str
    dry_run: bool
    discovered: int
    ingested: int
    empty: int
    failed: int
    skipped_active: int
    skipped_unchanged: int
    skipped_vanished: int
    would_ingest: int
    actions: list[IngestAction]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "dry_run": self.dry_run,
            "discovered": self.discovered,
            "ingested": self.ingested,
            "empty": self.empty,
            "failed": self.failed,
            "skipped_active": self.skipped_active,
            "skipped_unchanged": self.skipped_unchanged,
            "skipped_vanished": self.skipped_vanished,
            "would_ingest": self.would_ingest,
            "actions": [action.to_dict() for action in self.actions],
        }


def run_once(
    *,
    transcripts: TranscriptSource,
    store: IngestStateStore,
    ingester: IngestFunction,
    quiet_seconds: float,
    dry_run: bool = False,
    now: float | None = None,
) -> IngestCycleSummary:
    """Run one discover/checkpoint/ingest pass."""
    current = time.time() if now is None else now
    actions: list[IngestAction] = []

    for transcript in transcripts.discover():
        try:
            fingerprint = fingerprint_file(transcript.path)
        except OSError as exc:
            # A file can vanish between discovery and stat (app pruned it,
            # session dir renamed). Skip it; never abort the whole pass.
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action=IngestAct.SKIP_VANISHED,
                    status=IngestStatus.PENDING.value,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    metadata=dict(transcript.metadata),
                    reason=f"{type(exc).__name__}: {exc}",
                )
            )
            continue
        quiet = is_quiet(
            fingerprint,
            quiet_seconds=quiet_seconds,
            now=current,
        )
        existing = store.get(transcript.source_type, transcript.path)
        record = existing if dry_run else store.observe(transcript, fingerprint, now=current)
        changed = existing is None or existing.fingerprint != fingerprint.identity

        if not quiet:
            reason = f"file not quiet for {quiet_seconds:g}s"
            if not dry_run and record is not None:
                store.mark_active(record, now=current, reason=reason)
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action=IngestAct.SKIP_ACTIVE,
                    status=IngestStatus.ACTIVE.value,
                    fingerprint=fingerprint.identity,
                    mtime_ns=fingerprint.mtime_ns,
                    mtime=_mtime_iso(fingerprint),
                    size_bytes=fingerprint.size_bytes,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    metadata=dict(transcript.metadata),
                    reason=reason,
                )
            )
            continue

        if (
            record is not None
            and record.status in (IngestStatus.INGESTED, IngestStatus.EMPTY)
            and not changed
        ):
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action=IngestAct.SKIP_UNCHANGED,
                    status=record.status.value,
                    fingerprint=fingerprint.identity,
                    mtime_ns=fingerprint.mtime_ns,
                    mtime=_mtime_iso(fingerprint),
                    size_bytes=fingerprint.size_bytes,
                    session_id=record.session_id,
                    trace_id=record.trace_id,
                    metadata=record.metadata,
                    reason="fingerprint already ingested",
                )
            )
            continue

        if dry_run:
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action=IngestAct.WOULD_INGEST,
                    status=(record.status.value if record else IngestStatus.PENDING.value),
                    fingerprint=fingerprint.identity,
                    mtime_ns=fingerprint.mtime_ns,
                    mtime=_mtime_iso(fingerprint),
                    size_bytes=fingerprint.size_bytes,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    metadata=dict(transcript.metadata),
                    reason="dry-run",
                )
            )
            continue

        if record is None:  # pragma: no cover - observe returns a record.
            raise RuntimeError("missing ingest state record")

        ingesting = store.mark_ingesting(record, now=current)
        request = IngestRequest(transcript=transcript, fingerprint=fingerprint)
        try:
            outcome = ingester(request)
        except Exception as exc:  # noqa: BLE001 - persisted for retry visibility.
            error = f"{type(exc).__name__}: {exc}"
            store.mark_failed(ingesting, now=current, error=error)
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action=IngestAct.FAILED,
                    status=IngestStatus.FAILED.value,
                    fingerprint=fingerprint.identity,
                    mtime_ns=fingerprint.mtime_ns,
                    mtime=_mtime_iso(fingerprint),
                    size_bytes=fingerprint.size_bytes,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    metadata=dict(transcript.metadata),
                    reason=error,
                )
            )
            continue

        if outcome is None:
            # Parsed but produced no session. Not success: mark distinctly
            # so improved parsers can find these files and retry them.
            updated = store.mark_empty(ingesting, now=current)
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action=IngestAct.EMPTY,
                    status=IngestStatus.EMPTY.value,
                    fingerprint=fingerprint.identity,
                    mtime_ns=fingerprint.mtime_ns,
                    mtime=_mtime_iso(fingerprint),
                    size_bytes=fingerprint.size_bytes,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    metadata=dict(transcript.metadata),
                    reason="no session derived; retried when bytes change",
                )
            )
            continue

        updated = store.mark_ingested(
            ingesting,
            now=current,
            session_id=outcome.session_id,
            trace_id=outcome.trace_id,
        )
        actions.append(
            IngestAction(
                source_type=transcript.source_type,
                path=transcript.path,
                action=IngestAct.INGESTED,
                status=IngestStatus.INGESTED.value,
                fingerprint=fingerprint.identity,
                mtime_ns=fingerprint.mtime_ns,
                mtime=_mtime_iso(fingerprint),
                size_bytes=fingerprint.size_bytes,
                session_id=updated.session_id,
                trace_id=updated.trace_id,
                metadata=updated.metadata,
            )
        )

    return _summarize(transcripts.source_type, dry_run, actions)


def _summarize(
    source_type: str,
    dry_run: bool,
    actions: list[IngestAction],
) -> IngestCycleSummary:
    return IngestCycleSummary(
        source_type=source_type,
        dry_run=dry_run,
        discovered=len(actions),
        ingested=sum(1 for action in actions if action.action is IngestAct.INGESTED),
        empty=sum(1 for action in actions if action.action is IngestAct.EMPTY),
        failed=sum(1 for action in actions if action.action is IngestAct.FAILED),
        skipped_active=sum(1 for action in actions if action.action is IngestAct.SKIP_ACTIVE),
        skipped_unchanged=sum(
            1 for action in actions if action.action is IngestAct.SKIP_UNCHANGED
        ),
        skipped_vanished=sum(
            1 for action in actions if action.action is IngestAct.SKIP_VANISHED
        ),
        would_ingest=sum(1 for action in actions if action.action is IngestAct.WOULD_INGEST),
        actions=actions,
    )


def _mtime_iso(fingerprint: FileFingerprint) -> str:
    return datetime.fromtimestamp(
        fingerprint.mtime_seconds,
        tz=timezone.utc,
    ).isoformat()
