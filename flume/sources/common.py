"""Shared JSONL parsing helpers for source adapters.

Every supported agent writes sessions as JSON-lines files; these helpers
hold the line-level plumbing (tolerant parsing, timestamp conversion,
path enumeration) so each vendor module contains only format knowledge.
"""
from __future__ import annotations

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


def iter_jsonl_objects(path: Path) -> Iterable[dict[str, Any]]:
    """Yield JSON objects from a JSONL file, skipping blank/invalid lines."""
    try:
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    yield obj
    except FileNotFoundError:
        return


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read all JSON objects from a JSONL file; empty list on any OS error."""
    out: list[dict[str, Any]] = []
    try:
        text = path.read_text()
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


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
