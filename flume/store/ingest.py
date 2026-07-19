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

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any, Callable

from flume.ingest.runner import IngestOutcome, IngestRequest
from flume.store.archive import RawArchive
from flume.store.base import SessionStore
from flume.store.bundle import PIPELINE_VERSION, bundle_from_spans
from flume.store.registry import get_adapter


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
        spans,
        contents=contents,
        file_path=path,
        metadata=metadata,
        raw_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
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


def rebuild_stale(
    store: SessionStore,
    archive: RawArchive,
    *,
    source: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Re-ingest sessions whose rows were built by an older pipeline.

    Bytes come from the original file when it still exists, else from the
    latest raw-archive version — so a rebuild works even after the vendor
    app pruned its transcripts. The session's stored metadata is replayed,
    preserving probe results (parent links, cwd) that a restored temp file
    could not re-derive."""
    stale = store.stale_sessions(PIPELINE_VERSION, source=source, limit=limit)
    if dry_run:
        return {
            "pipeline_version": PIPELINE_VERSION,
            "stale": len(stale),
            "dry_run": True,
            "sessions": [row["session_id"] for row in stale],
        }
    rebuilt: list[str] = []
    from_original = from_archive = 0
    missing_raw: list[str] = []
    failed: list[dict[str, str]] = []
    for row in stale:
        session_id = row["session_id"]
        metadata = json.loads(row["metadata"]) if row.get("metadata") else None
        if not row.get("source"):
            failed.append({"session_id": session_id, "error": "no source recorded"})
            continue
        try:
            original = Path(row["file_path"]) if row.get("file_path") else None
            if original is not None and original.is_file():
                ingest_path(store, row["source"], original, metadata, archive=archive)
                from_original += 1
            else:
                versions = archive.versions(session_id)
                if not versions:
                    missing_raw.append(session_id)
                    continue
                entry = versions[-1]
                with tempfile.TemporaryDirectory() as tmp:
                    # Keep the original filename: session_id fallback and
                    # source probes key off the path's stem/shape.
                    name = Path(entry.original_path).name or f"{session_id}.jsonl"
                    restored = archive.restore(entry, Path(tmp) / name)
                    ingest_path(store, row["source"], restored, metadata)
                from_archive += 1
            rebuilt.append(session_id)
        except Exception as exc:  # noqa: BLE001 - keep the batch going
            failed.append(
                {"session_id": session_id, "error": f"{type(exc).__name__}: {exc}"}
            )
    return {
        "pipeline_version": PIPELINE_VERSION,
        "stale": len(stale),
        "rebuilt": len(rebuilt),
        "from_original": from_original,
        "from_archive": from_archive,
        "missing_raw": missing_raw,
        "failed": failed,
    }
