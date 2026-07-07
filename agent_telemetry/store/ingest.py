"""Ingest session files: raw archive first, then the analyzed store.

Two entry points:

- `store_ingest_function(source, store, archive)` returns an IngestFunction
  for the existing auto-ingest runner, so the same discover/quiet/fingerprint
  state machine drives either backend (OTLP→Langfuse or the local store).
- `ingest_path(store, source, path, metadata, archive)` ingests one file
  directly, used by the analyze CLI's `ingest` subcommand.

`source` resolves through the adapter registry, so it accepts either a
source name ("claude-code", "codex") or an unambiguous vendor alias
("anthropic", "openai"). Raw capture happens BEFORE parsing: even a file
the mapper cannot handle yet is preserved durably.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from agent_telemetry.ingest.runner import IngestOutcome, IngestRequest
from agent_telemetry.store.archive import RawArchive
from agent_telemetry.store.base import SessionStore
from agent_telemetry.store.bundle import bundle_from_spans
from agent_telemetry.store.registry import get_adapter


def ingest_path(
    store: SessionStore,
    source: str,
    path: Path,
    metadata: dict[str, Any] | None = None,
    *,
    archive: RawArchive | None = None,
) -> IngestOutcome | None:
    """Archive + map + extract + persist one session file.

    Returns None if the file yields no session (still archived)."""
    adapter = get_adapter(source)
    if adapter.probe is not None:
        probed = adapter.probe(path)
        metadata = {**probed, **(metadata or {})}
    try:
        spans = adapter.map_spans(path)
    except Exception:
        # Mapper failure must not cost us the raw data: archive under the
        # filename stem, then re-raise so the state machine records the
        # failure for retry once the mapper is fixed.
        _capture(archive, adapter.name, path.stem, path)
        raise
    root = spans[0] if spans else {}
    session_id = (root.get("attributes") or {}).get("session.id") or path.stem
    _capture(archive, adapter.name, session_id, path)

    if not spans:
        return None
    contents = adapter.extract_contents(path, session_id)
    bundle = bundle_from_spans(
        spans, contents=contents, file_path=path, metadata=metadata
    )
    if bundle is None:
        return None
    store.ingest_session(bundle)
    return IngestOutcome(
        session_id=bundle.session["session_id"],
        trace_id=bundle.session.get("trace_id"),
    )


def _capture(
    archive: RawArchive | None,
    source: str,
    session_id: str,
    path: Path,
) -> None:
    if archive is None:
        return
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        mtime_ns = None
    archive.capture(source, session_id, path, mtime_ns=mtime_ns)


def store_ingest_function(
    source: str,
    store: SessionStore,
    archive: RawArchive | None = None,
) -> Callable[[IngestRequest], IngestOutcome | None]:
    """Adapt `ingest_path` to the auto-ingest runner's IngestFunction."""

    def ingest(request: IngestRequest) -> IngestOutcome | None:
        metadata = dict(request.transcript.metadata or {})
        outcome = ingest_path(
            store, source, request.transcript.path, metadata, archive=archive
        )
        if outcome is None:
            # Empty/unparseable transcript: report ids so state is tracked.
            return IngestOutcome(
                session_id=request.transcript.session_id,
                trace_id=request.transcript.trace_id,
            )
        return outcome

    return ingest
