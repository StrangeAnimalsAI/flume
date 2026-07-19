"""Small source adapter contracts for transcript auto-ingest."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class DiscoveredTranscript:
    """A transcript candidate reported by a source adapter."""

    source_type: str
    path: Path
    session_id: str | None = None
    trace_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class TranscriptSource(Protocol):
    """Discovery-only source adapter interface.

    Future Codex and Claude adapters should keep this layer boring: enumerate
    candidate transcript files and expose optional ids when they are cheap to
    extract. Actual export/backfill remains a pluggable ingest function.
    """

    source_type: str

    def discover(self) -> Iterable[DiscoveredTranscript]:
        """Yield transcript files this source knows about."""
