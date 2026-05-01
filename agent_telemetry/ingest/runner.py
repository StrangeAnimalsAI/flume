"""Auto-ingest state machine."""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_telemetry.ingest.fingerprint import FileFingerprint, fingerprint_file, is_quiet
from agent_telemetry.ingest.state import IngestStatus, SqliteIngestStateStore
from agent_telemetry.ingest.types import DiscoveredTranscript, TranscriptSource


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


@dataclass(frozen=True)
class IngestAction:
    source_type: str
    path: Path
    action: str
    status: str
    fingerprint: str | None = None
    session_id: str | None = None
    trace_id: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "path": str(self.path),
            "action": self.action,
            "status": self.status,
            "fingerprint": self.fingerprint,
            "session_id": self.session_id,
            "trace_id": self.trace_id,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class IngestCycleSummary:
    source_type: str
    dry_run: bool
    discovered: int
    ingested: int
    failed: int
    skipped_active: int
    skipped_unchanged: int
    would_ingest: int
    actions: list[IngestAction]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "dry_run": self.dry_run,
            "discovered": self.discovered,
            "ingested": self.ingested,
            "failed": self.failed,
            "skipped_active": self.skipped_active,
            "skipped_unchanged": self.skipped_unchanged,
            "would_ingest": self.would_ingest,
            "actions": [action.to_dict() for action in self.actions],
        }


def run_once(
    *,
    source: TranscriptSource,
    store: SqliteIngestStateStore,
    ingest: IngestFunction,
    quiet_seconds: float,
    dry_run: bool = False,
    now: float | None = None,
) -> IngestCycleSummary:
    """Run one discover/checkpoint/ingest pass."""
    current = time.time() if now is None else now
    actions: list[IngestAction] = []

    for transcript in source.discover():
        fingerprint = fingerprint_file(transcript.path)
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
                    action="skip_active",
                    status=IngestStatus.ACTIVE.value,
                    fingerprint=fingerprint.identity,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    reason=reason,
                )
            )
            continue

        if (
            record is not None
            and record.status == IngestStatus.INGESTED
            and not changed
        ):
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action="skip_unchanged",
                    status=IngestStatus.INGESTED.value,
                    fingerprint=fingerprint.identity,
                    session_id=record.session_id,
                    trace_id=record.trace_id,
                    reason="fingerprint already ingested",
                )
            )
            continue

        if dry_run:
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action="would_ingest",
                    status=(record.status.value if record else IngestStatus.PENDING.value),
                    fingerprint=fingerprint.identity,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    reason="dry-run",
                )
            )
            continue

        if record is None:  # pragma: no cover - observe returns a record.
            raise RuntimeError("missing ingest state record")

        ingesting = store.mark_ingesting(record, now=current)
        request = IngestRequest(transcript=transcript, fingerprint=fingerprint)
        try:
            outcome = ingest(request) or IngestOutcome()
        except Exception as exc:  # noqa: BLE001 - persisted for retry visibility.
            error = f"{type(exc).__name__}: {exc}"
            store.mark_failed(ingesting, now=current, error=error)
            actions.append(
                IngestAction(
                    source_type=transcript.source_type,
                    path=transcript.path,
                    action="failed",
                    status=IngestStatus.FAILED.value,
                    fingerprint=fingerprint.identity,
                    session_id=transcript.session_id,
                    trace_id=transcript.trace_id,
                    reason=error,
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
                action="ingested",
                status=IngestStatus.INGESTED.value,
                fingerprint=fingerprint.identity,
                session_id=updated.session_id,
                trace_id=updated.trace_id,
            )
        )

    return _summarize(source.source_type, dry_run, actions)


def _summarize(
    source_type: str,
    dry_run: bool,
    actions: list[IngestAction],
) -> IngestCycleSummary:
    return IngestCycleSummary(
        source_type=source_type,
        dry_run=dry_run,
        discovered=len(actions),
        ingested=sum(1 for action in actions if action.action == "ingested"),
        failed=sum(1 for action in actions if action.action == "failed"),
        skipped_active=sum(1 for action in actions if action.action == "skip_active"),
        skipped_unchanged=sum(
            1 for action in actions if action.action == "skip_unchanged"
        ),
        would_ingest=sum(1 for action in actions if action.action == "would_ingest"),
        actions=actions,
    )
