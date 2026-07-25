"""Hook-event extraction: nudges, denials, and injected feedback.

SOURCE-SPECIFIC: this reads Claude Code's hook markers. Hooks are a
Claude Code feature, not a universal agent concept, so `analyze hooks`
finds nothing for other sources — that is an empty result, not an error.
An agent with its own intervention mechanism would want a sibling module
parsing its markers, not a generalization of this one.

Claude Code writes every hook intervention into the transcript as a
tool_result error shaped like:

    PreToolUse:Read hook error: [python3 .../reread-guard.py]: message

so hook activity is recoverable from already-ingested sessions — no
schema change, no re-ingest. This module parses those markers and, for
the reread-guard specifically, judges compliance: after a denial, did
the agent leave the file alone (heeded) or read it again anyway via a
different range (bypassed)?

The reread-guard also appends denials to ~/.flume/
hook-events.jsonl at fire time; that log is the belt-and-braces raw
capture and joins to sessions on session_id. The store view here is the
primary query surface because it covers ALL hooks, not just ours.
"""
from __future__ import annotations

import re
from typing import Any

HOOK_MARKER = re.compile(
    r"(?P<event>[A-Za-z]+):(?P<matcher>[A-Za-z_|]+) hook "
    r"(?P<kind>error|feedback):?\s*"
    r"(?:\[(?P<command>[^\]]*)\]:?\s*)?"
    r"(?P<message>.*)",
    re.S,
)
_DENIED_FILE = re.compile(r"you already read (\S+?)(?:\s|$)")


def hook_events(
    store,
    *,
    since_ns: int | None = None,
    session_id: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Hook interventions extracted from tool results, newest first."""
    query = getattr(store, "_all", None)
    if query is None:
        raise TypeError("hook_events requires the sqlite backend")

    conditions = ["c.kind = 'tool_result'", "c.text LIKE '%hook error:%'"]
    params: list[Any] = []
    if since_ns is not None:
        conditions.append("s.started_at_ns >= ?")
        params.append(since_ns)
    if session_id is not None:
        conditions.append("c.session_id = ?")
        params.append(session_id)
    rows = query(
        f"""
        SELECT c.session_id, c.span_id, c.ts_ns, c.text,
               s.project, s.experiment, tc.name AS tool_name
        FROM contents c JOIN sessions s USING (session_id)
        LEFT JOIN tool_calls tc ON tc.span_id = c.span_id
        WHERE {' AND '.join(conditions)}
        ORDER BY c.ts_ns DESC LIMIT ?
        """,
        (*params, limit),
    )

    events = []
    for row in rows:
        match = HOOK_MARKER.search(row["text"])
        if not match:
            continue
        # A tool result that merely CONTAINS marker text (e.g. grep output
        # quoting a hook message) hangs off the wrong tool: a real
        # PreToolUse:Read denial is the result of a Read call.
        tool = row["tool_name"]
        if tool is not None and tool not in match.group("matcher").split("|"):
            continue
        command = (match.group("command") or "").strip()
        hook = command.split("/")[-1].split()[0] if command else "?"
        message = " ".join(match.group("message").split())
        event = {
            "session_id": row["session_id"],
            "span_id": row["span_id"],
            "project": row["project"],
            "experiment": row["experiment"],
            "ts_ns": row["ts_ns"],
            "event": match.group("event"),
            "matcher": match.group("matcher"),
            "hook": hook,
            "message": message[:200],
            "outcome": None,
        }
        if "reread-guard" in hook:
            event["outcome"] = _reread_outcome(store, row, message)
        events.append(event)
    return events


def _reread_outcome(store, row, message: str) -> str | None:
    """After a reread denial: 'heeded' (left the file alone) or 'bypassed'."""
    match = _DENIED_FILE.search(message)
    if not match or row["ts_ns"] is None:
        return None
    later = store._one(
        """
        SELECT COUNT(*) AS n FROM tool_calls
        WHERE session_id = ? AND name = 'Read' AND is_error = 0
            AND started_at_ns > ? AND args_preview LIKE ?
        """,
        (row["session_id"], row["ts_ns"], f"%{match.group(1)}%"),
    )
    return "bypassed" if (later or {}).get("n", 0) else "heeded"


def hooks_summary(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Per-hook rollup of the extracted events."""
    by_hook: dict[str, dict[str, Any]] = {}
    for event in events:
        slot = by_hook.setdefault(event["hook"], {
            "hook": event["hook"], "events": 0, "sessions": set(),
            "heeded": 0, "bypassed": 0,
            "first_ns": event["ts_ns"], "last_ns": event["ts_ns"],
        })
        slot["events"] += 1
        slot["sessions"].add(event["session_id"])
        if event["outcome"] in ("heeded", "bypassed"):
            slot[event["outcome"]] += 1
        for edge, pick in (("first_ns", min), ("last_ns", max)):
            if event["ts_ns"] is not None:
                slot[edge] = pick(slot[edge] or event["ts_ns"], event["ts_ns"])
    return [
        {**slot, "sessions": len(slot["sessions"])}
        for slot in sorted(by_hook.values(), key=lambda s: -s["events"])
    ]
