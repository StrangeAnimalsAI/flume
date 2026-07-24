"""Harness transcript format: one JSON event per line.

Event types:
    session_meta   first line — session_id, model, effort, cwd, version
    user           a user prompt (initial or follow-up)
    assistant      one API turn — usage, duration_ms, content blocks
    tool_result    result of one tool_use (joined by tool_use_id)
    end            stop_reason + turn count

The file is the raw layer: the writer appends and flushes per event, so a
crashed run still leaves an ingestable prefix.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TranscriptWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._fh = path.open("a")
        os.chmod(path, 0o600)

    def write(self, event: dict[str, Any]) -> None:
        event.setdefault("ts", _now_iso())
        self._fh.write(json.dumps(event, default=str) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> "TranscriptWriter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def content_block_dict(block: Any) -> dict[str, Any]:
    """Serialize an SDK content block (or dict) for the transcript."""
    if isinstance(block, dict):
        return block
    kind = getattr(block, "type", None)
    if kind == "thinking":
        return {"type": "thinking", "thinking": block.thinking or ""}
    if kind == "text":
        return {"type": "text", "text": block.text}
    if kind == "tool_use":
        return {
            "type": "tool_use",
            "id": block.id,
            "name": block.name,
            "input": block.input,
        }
    return {"type": str(kind)}


def ts_ns(value: str | None) -> int | None:
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    return int(stamp.timestamp() * 1_000_000_000)
