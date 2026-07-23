"""Source-agnostic ingest pipeline: discovery, checkpoint state, write path.

Vendors live in `flume.sources`; this package drives them — the
discover/quiet/fingerprint state machine (`runner`), durable per-file
checkpoints (`state`), and the archive-then-persist write path (`write`).
"""
from __future__ import annotations

from flume.ingest.fingerprint import FileFingerprint, fingerprint_file, is_quiet
from flume.ingest.runner import IngestOutcome, IngestRequest, run_once
from flume.ingest.state import IngestStatus, SqliteIngestStateStore
from flume.ingest.write import ingest_path, rebuild_stale, store_ingest_function

__all__ = [
    "FileFingerprint",
    "IngestOutcome",
    "IngestRequest",
    "IngestStatus",
    "SqliteIngestStateStore",
    "fingerprint_file",
    "ingest_path",
    "is_quiet",
    "rebuild_stale",
    "run_once",
    "store_ingest_function",
]
