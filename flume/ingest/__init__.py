"""Source-agnostic transcript auto-ingest framework."""
from __future__ import annotations

from flume.ingest.claude_code import (
    ClaudeCodeTranscriptSource,
    ingest_claude_code_transcript,
)
from flume.ingest.codex import CodexRolloutSource, ingest_codex_rollout
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
    "ingest_claude_code_transcript",
    "ingest_codex_rollout",
    "is_quiet",
    "run_once",
]
