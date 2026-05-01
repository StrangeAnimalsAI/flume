"""Source-agnostic transcript auto-ingest framework."""
from __future__ import annotations

from agent_telemetry.ingest.fingerprint import FileFingerprint, fingerprint_file, is_quiet
from agent_telemetry.ingest.runner import IngestOutcome, IngestRequest, run_once
from agent_telemetry.ingest.state import IngestStatus, SqliteIngestStateStore
from agent_telemetry.ingest.types import DiscoveredTranscript, TranscriptSource

__all__ = [
    "DiscoveredTranscript",
    "FileFingerprint",
    "IngestOutcome",
    "IngestRequest",
    "IngestStatus",
    "SqliteIngestStateStore",
    "TranscriptSource",
    "fingerprint_file",
    "is_quiet",
    "run_once",
]
