"""Audit heuristics that need more than one SQL pass.

`script_clusters` finds throwaway code the agents keep rewriting: inline
python (heredocs, `python3 -c`) inside shell tool calls, fingerprinted by
imports + characteristic operations, grouped across sessions. A shape that
recurs in many sessions is a durable-tool candidate — the manual
"read the logs and find code that should be a real tool" workflow, as a
query.
"""
from __future__ import annotations

import collections
import re
from typing import Any

from flume.store.base import SessionStore

_SHELL_TOOLS = ["Bash", "exec_command"]
_SCRIPT_MARKERS = ("python3 -", "python -", "<<EOF", "<<'EOF'", "<<'PY'", "<< 'PY'")
_OPS = re.compile(
    r"\b(json\.load\w*|Counter|glob|read_text|splitlines|fetchall|groupby"
    r"|defaultdict|re\.findall|sqlite3)\b"
)


def script_clusters(
    store: SessionStore,
    *,
    source: str | None = None,
    since_ns: int | None = None,
    min_sessions: int = 3,
    limit: int = 30,
) -> list[dict[str, Any]]:
    sig_sessions: dict[tuple[str, str], set[str]] = collections.defaultdict(set)
    sig_example: dict[tuple[str, str], str] = {}

    # Single pass: one indexed fetch, marker filtering in Python (cheaper
    # than N LIKE table scans over multi-GB of argument text).
    for row in store.tool_argument_rows(
        tool_names=_SHELL_TOOLS, source=source, since_ns=since_ns
    ):
        text = row["text"]
        if not any(marker in text for marker in _SCRIPT_MARKERS):
            continue
        imports = sorted(set(re.findall(r"import (\w+)", text)))
        if not imports:
            continue
        ops = sorted(set(_OPS.findall(text)))
        sig = ("+".join(imports), "+".join(ops))
        sig_sessions[sig].add(row["session_id"])
        sig_example.setdefault(sig, text[:300])

    clusters = [
        {
            "sessions": len(session_ids),
            "imports": sig[0],
            "operations": sig[1],
            "example": sig_example[sig],
            "session_ids": sorted(session_ids)[:10],
        }
        for sig, session_ids in sig_sessions.items()
        if len(session_ids) >= min_sessions
    ]
    clusters.sort(key=lambda c: c["sessions"], reverse=True)
    return clusters[:limit]
