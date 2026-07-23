"""Source-agnostic transcript auto-ingest framework."""
from __future__ import annotations

from flume.ingest.claude_code import ClaudeCodeTranscriptSource
from flume.ingest.codex import CodexRolloutSource
from flume.ingest.fingerprint import FileFingerprint, fingerprint_file, is_quiet
from flume.ingest.runner import IngestOutcome, IngestRequest, run_once
from flume.ingest.state import IngestStatus, SqliteIngestStateStore
from flume.ingest.types import DiscoveredTranscript, TranscriptSource

__all__ = [
    "ClaudeCodeTranscriptSource",
    "CodexRolloutSource",
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
