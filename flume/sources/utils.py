"""Shared JSONL parsing helpers for source adapters.

Every supported agent writes sessions as JSON-lines files; these helpers
hold the line-level plumbing (tolerant parsing, timestamp conversion,
path enumeration) so each vendor module contains only format knowledge.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Shell commands that read/navigate the tree rather than do work. Vendor
# independent — every agent's shell tool runs the same coreutils. Matches
# the command word at a boundary (start, whitespace, quote, or separator)
# so `cargo test` does not match `cat`, while Codex's JSON-ish preview
# `{"cmd":"rg -n ..."}` does.
NAV_SHELL_RE = re.compile(
    r'(?:^|[\s"(;|&]|\\n)'
    r"(rg|grep|cat|sed|head|tail|find|tree|ls|wc|nl|repo-nav)\s"
)


def is_nav_shell(args_preview: str | None) -> bool:
    """True when a shell invocation is reading/navigating rather than working."""
    return bool(NAV_SHELL_RE.search(args_preview or ""))


# A single JSONL line larger than this is not analyzable content — it is a
# runaway tool output the agent wrote into its own transcript. Observed in
# the wild: a 4.5 GB Codex rollout of 976 lines, one of them several GB,
# which failed ingest outright with "OverflowError: string longer than
# INT_MAX bytes" (json rejects strings past INT_MAX) after buffering
# gigabytes to get there. Oversized lines are skipped and never held whole;
# the raw archive keeps the original bytes, which is what that layer is for.
MAX_LINE_BYTES = 64 * 1024 * 1024
_CHUNK_BYTES = 1 << 20


def iter_jsonl_lines(
    path: Path, *, max_bytes: int = MAX_LINE_BYTES
) -> Iterable[str]:
    """Yield non-blank lines, dropping any longer than `max_bytes`.

    Reads fixed-size chunks and discards an oversized line as it streams,
    so peak memory stays bounded regardless of how large one line is."""
    try:
        handle = path.open("rb")
    except FileNotFoundError:
        return
    with handle:
        pending: list[bytes] = []
        pending_size = 0
        skipping = False  # inside an oversized line, waiting for its newline
        for chunk in iter(lambda: handle.read(_CHUNK_BYTES), b""):
            start = 0
            while (newline := chunk.find(b"\n", start)) != -1:
                piece = chunk[start:newline]
                start = newline + 1
                if skipping:  # that newline ends the oversized line
                    skipping = False
                    pending, pending_size = [], 0
                    continue
                if pending_size + len(piece) > max_bytes:
                    pending, pending_size = [], 0
                    continue
                pending.append(piece)
                line = b"".join(pending).strip()
                pending, pending_size = [], 0
                if line:
                    yield line.decode("utf-8", "replace")
            tail = chunk[start:]
            if skipping:
                continue
            if pending_size + len(tail) > max_bytes:
                pending, pending_size, skipping = [], 0, True
                continue
            pending.append(tail)
            pending_size += len(tail)
        if not skipping and pending:
            line = b"".join(pending).strip()
            if line:
                yield line.decode("utf-8", "replace")


def iter_jsonl_objects(path: Path) -> Iterable[dict[str, Any]]:
    """Yield JSON objects from a JSONL file, skipping blank/invalid lines."""
    for line in iter_jsonl_lines(path):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read all JSON objects from a JSONL file; empty list when unreadable."""
    return list(iter_jsonl_objects(path))


def span_id(namespace: str, session_id: str, suffix: str) -> str:
    """Deterministic span id. The namespace is the source name, so ids from
    two sources can never collide even on an identical session id.

    The hash inputs and widths are a wire format: they are stored, and
    content rows join to spans on them. Changing either orphans every
    existing row, so treat this as frozen rather than an implementation
    detail."""
    return hashlib.sha256(
        f"{namespace}:{session_id}:{suffix}".encode()
    ).hexdigest()[:16]


def trace_id(namespace: str, session_id: str) -> str:
    """Deterministic trace id for one session. Frozen, as `span_id`."""
    return hashlib.sha256(f"{namespace}:{session_id}".encode()).hexdigest()[:32]


def iso_ts_ns(ts: Any) -> int | None:
    """ISO-8601 timestamp string -> unix nanoseconds, or None."""
    if not isinstance(ts, str) or not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    delta = dt - _EPOCH_UTC
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def result_text(content: Any) -> str:
    """Flatten a tool-result content value (string, block list, ...) to text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for blk in content:
            if isinstance(blk, str):
                parts.append(blk)
            elif isinstance(blk, dict):
                value = blk.get("text")
                if isinstance(value, str) and value:
                    parts.append(value)
        if parts:
            return "\n".join(parts)
    if isinstance(content, (list, dict)):
        return json_text(content)
    return ""


def json_text(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except (TypeError, ValueError):
        return str(value)


def jsonl_paths(root: Path, *, recursive: bool) -> Iterable[Path]:
    """Enumerate .jsonl files under a root (or the root itself if a file)."""
    if root.is_file() and root.suffix == ".jsonl":
        yield root
        return
    if not root.is_dir():
        return
    pattern = "**/*.jsonl" if recursive else "*.jsonl"
    yield from root.glob(pattern)


def unique_sorted(paths: Iterable[Path]) -> list[Path]:
    out: dict[str, Path] = {}
    for path in paths:
        if path.is_file():
            resolved = path.expanduser().resolve(strict=False)
            out[str(resolved)] = resolved
    return [out[key] for key in sorted(out)]


def as_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
