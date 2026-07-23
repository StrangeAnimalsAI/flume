"""Fake source adapter used by tests and by the initial CLI skeleton."""
from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from flume.ingest.runner import IngestOutcome, IngestRequest
from flume.sources import DiscoveredTranscript


class FakeTranscriptSource:
    """Discover JSONL files under a caller-provided fixture directory."""

    source_type = "fake"

    def __init__(self, root: Path | str, *, pattern: str = "*.jsonl") -> None:
        self.root = Path(root).expanduser().resolve(strict=False)
        self.pattern = pattern

    def discover(self) -> Iterable[DiscoveredTranscript]:
        for path in sorted(self.root.rglob(self.pattern)):
            if not path.is_file():
                continue
            ids = _extract_ids(path)
            yield DiscoveredTranscript(
                source_type=self.source_type,
                path=path,
                session_id=ids.get("session_id"),
                trace_id=ids.get("trace_id"),
            )


def fake_ingest(request: IngestRequest) -> IngestOutcome:
    """Fixture ingest function.

    A JSON object containing `{"fake_ingest_error": true}` forces failure so
    retry transitions can be tested without touching real transcript sources
    or Langfuse.
    """
    for obj in _read_jsonl_objects(request.transcript.path):
        if obj.get("fake_ingest_error") is True:
            raise RuntimeError("fake ingest requested failure")
    return IngestOutcome(
        session_id=request.transcript.session_id,
        trace_id=request.transcript.trace_id,
    )


def _extract_ids(path: Path) -> dict[str, str | None]:
    for obj in _read_jsonl_objects(path):
        return {
            "session_id": _first_string(
                obj,
                "session_id",
                "session.id",
                ("payload", "session_id"),
                ("payload", "id"),
            ),
            "trace_id": _first_string(
                obj,
                "trace_id",
                "trace.id",
                ("payload", "trace_id"),
            ),
        }
    return {"session_id": path.stem, "trace_id": None}


def _read_jsonl_objects(path: Path) -> Iterable[dict[str, Any]]:
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def _first_string(obj: dict[str, Any], *keys: object) -> str | None:
    for key in keys:
        value: Any
        if isinstance(key, tuple):
            value = obj
            for part in key:
                if not isinstance(value, dict):
                    value = None
                    break
                value = value.get(part)
        else:
            value = obj.get(key)
        if isinstance(value, str) and value:
            return value
    return None
