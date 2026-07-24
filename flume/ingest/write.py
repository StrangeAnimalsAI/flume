"""The write path: archive raw bytes first, then map/extract/persist.

Three entry points:

- `ingest_path(store, adapter, path, ...)` ingests one file through an
  explicit `SourceAdapter`. Raw capture happens BEFORE parsing: even a
  file the mapper cannot handle yet is preserved durably.
- `store_ingest_function(adapter, store, archive)` wraps `ingest_path`
  as an IngestFunction so the discover/quiet/fingerprint state machine
  drives the store sink.
- `rebuild_stale(store, archive, ...)` re-ingests sessions built by an
  older pipeline version, resolving each session's recorded source name
  through the adapter registry.

The store engine never resolves adapters; callers pass one in (resolve
names with `flume.sources.get_adapter`).
"""
from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any, Callable

from flume.ingest.runner import IngestOutcome, IngestRequest
from flume.sources import SourceAdapter, get_adapter
from flume.store.archive import RawArchive
from flume.store.base import SessionStore
from flume.store.bundle import PIPELINE_VERSION, bundle_from_spans


def ingest_path(
    store: SessionStore,
    adapter: SourceAdapter,
    path: Path,
    metadata: dict[str, Any] | None = None,
    *,
    archive: RawArchive | None = None,
) -> IngestOutcome | None:
    """Archive + map + extract + persist one session file.

    Returns None if the file yields no session (still archived)."""
    # One read up front: the archived bytes and the recorded raw_sha256 are
    # guaranteed to be the same snapshot, even if the live file grows while
    # we parse. (Mappers re-read the path; the quiet-seconds gate bounds
    # that residual race.)
    data = path.read_bytes()
    raw_sha256 = hashlib.sha256(data).hexdigest()
    try:
        if adapter.probe is not None:
            probed = adapter.probe(path)
            metadata = {**probed, **(metadata or {})}
        spans = adapter.map_spans(path)
    except Exception:
        # Probe/mapper failure must not cost us the raw data: archive under
        # the filename stem, then re-raise so the state machine records the
        # failure for retry once the parser is fixed.
        _capture(archive, adapter.name, path.stem, path, data=data)
        raise
    root = spans[0] if spans else {}
    session_id = (root.get("attributes") or {}).get("session.id") or path.stem
    _capture(archive, adapter.name, session_id, path, data=data)

    if not spans:
        return None
    contents = adapter.extract_contents(path, session_id)
    bundle = bundle_from_spans(
        spans,
        contents=contents,
        file_path=path,
        metadata=metadata,
        raw_sha256=raw_sha256,
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
    *,
    data: bytes | None = None,
) -> None:
    if archive is None:
        return
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        mtime_ns = None
    archive.capture(source, session_id, path, mtime_ns=mtime_ns, data=data)


def store_ingest_function(
    adapter: SourceAdapter,
    store: SessionStore,
    archive: RawArchive | None = None,
) -> Callable[[IngestRequest], IngestOutcome | None]:
    """Adapt `ingest_path` to the auto-ingest runner's IngestFunction."""

    def ingest(request: IngestRequest) -> IngestOutcome | None:
        metadata = dict(request.transcript.metadata or {})
        # None (no session derived) propagates to the runner, which marks
        # the file EMPTY — distinct from success, retryable when parsers
        # improve. The raw bytes are already archived either way.
        return ingest_path(
            store, adapter, request.transcript.path, metadata, archive=archive
        )

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
    yielded_nothing: list[str] = []
    failed: list[dict[str, str]] = []
    for row in stale:
        session_id = row["session_id"]
        metadata = json.loads(row["metadata"]) if row.get("metadata") else None
        if not row.get("source"):
            failed.append({"session_id": session_id, "error": "no source recorded"})
            continue
        try:
            adapter = get_adapter(row["source"])
            original = Path(row["file_path"]) if row.get("file_path") else None
            if original is not None and original.is_file():
                outcome = ingest_path(
                    store, adapter, original, metadata, archive=archive
                )
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
                    outcome = ingest_path(store, adapter, restored, metadata)
                from_archive += 1
            if outcome is None:
                # The bytes no longer yield a session; the stale row was
                # NOT replaced. Report it instead of counting success.
                yielded_nothing.append(session_id)
            else:
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
        "yielded_nothing": yielded_nothing,
        "failed": failed,
    }
