"""Attribute session wall time to navigation vs other work.

Method (validated against the July 2026 spend analysis): within one
session, consecutive turn timestamps bound a "cycle" — model generation
plus the tool calls issued in it. Each cycle's duration is split evenly
across the tool calls that started inside it and rolled up by class;
cycles with no tool call are pure generation. Cycles longer than
CYCLE_CAP_S are user-idle (the human walked away) and are excluded.

Turn `duration_ms` is unreliable in recent raw files (often 0), so this
works purely off `started_at_ns` point timestamps, which are always
present.

Requires the sqlite backend (uses store._all).
"""
from __future__ import annotations

import re
import statistics
from collections import defaultdict
from typing import Any

CYCLE_CAP_S = 300  # gaps past the prompt-cache TTL are user-idle, not work

NAV_TOOLS = {"Read", "Grep", "Glob", "LS"}
EDIT_TOOLS = {"Edit", "Write", "NotebookEdit"}
# Bash commands that navigate/read the tree rather than run work. Matches
# the command word at the start of the preview or after a separator, so
# `cargo test` does not match `cat`.
NAV_BASH_RE = re.compile(
    r'(?:^|[\s"(;|&]|\\n)(rg|grep|cat|sed|head|tail|find|tree|ls|wc|repo-nav)\s'
)


def classify_tool(name: str | None, args_preview: str | None) -> str:
    if name in NAV_TOOLS:
        return "navigation"
    if name == "Bash":
        if NAV_BASH_RE.search(args_preview or ""):
            return "navigation"
        return "bash-other"
    if name == "Agent":
        return "subagent"
    if name in EDIT_TOOLS:
        return "editing"
    return "other"


def session_nav_shares(
    store,
    *,
    source: str | None = "claude-code",
    since_ns: int | None = None,
    session_ids: list[str] | None = None,
    cap_s: int = CYCLE_CAP_S,
) -> list[dict[str, Any]]:
    """Per-session time attribution rows for top-level sessions.

    Sessions with fewer than two turns carry no measurable cycles and are
    omitted. `nav_share` is nav seconds / attributed active seconds."""
    query = getattr(store, "_all", None)
    if query is None:
        raise TypeError("session_nav_shares requires the sqlite backend")

    conditions = ["s.is_subagent = 0"]
    params: list[Any] = []
    if source:
        conditions.append("s.source = ?")
        params.append(source)
    if since_ns is not None:
        conditions.append("s.started_at_ns >= ?")
        params.append(since_ns)
    if session_ids is not None:
        if not session_ids:
            return []
        marks = ",".join("?" for _ in session_ids)
        conditions.append(f"s.session_id IN ({marks})")
        params.extend(session_ids)
    where = "WHERE " + " AND ".join(conditions)

    turns: dict[str, list[int]] = defaultdict(list)
    for row in query(
        f"""
        SELECT t.session_id, t.started_at_ns
        FROM turns t JOIN sessions s USING (session_id) {where}
        ORDER BY t.session_id, t.started_at_ns
        """,
        tuple(params),
    ):
        turns[row["session_id"]].append(row["started_at_ns"])

    calls: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in query(
        f"""
        SELECT t.session_id, t.started_at_ns, t.name, t.args_preview,
               t.result_chars
        FROM tool_calls t JOIN sessions s USING (session_id) {where}
        ORDER BY t.session_id, t.started_at_ns
        """,
        tuple(params),
    ):
        calls[row["session_id"]].append(row)

    meta = {
        row["session_id"]: row
        for row in query(
            f"SELECT s.session_id, s.project, s.experiment FROM sessions s {where}",
            tuple(params),
        )
    }

    out: list[dict[str, Any]] = []
    for session_id, stamps in turns.items():
        if len(stamps) < 2:
            continue
        by_class: dict[str, float] = defaultdict(float)
        nav_calls = 0
        session_calls = calls.get(session_id, [])
        cursor = 0
        for begin, end in zip(stamps, stamps[1:], strict=False):
            seconds = (end - begin) / 1e9
            if seconds <= 0:
                continue
            in_cycle = []
            while (
                cursor < len(session_calls)
                and session_calls[cursor]["started_at_ns"] < end
            ):
                if session_calls[cursor]["started_at_ns"] >= begin:
                    in_cycle.append(session_calls[cursor])
                cursor += 1
            if seconds > cap_s:
                continue  # user idle; calls are consumed but not timed
            if not in_cycle:
                by_class["generation"] += seconds
                continue
            share = seconds / len(in_cycle)
            for call in in_cycle:
                kind = classify_tool(call["name"], call["args_preview"])
                by_class[kind] += share
                if kind == "navigation":
                    nav_calls += 1
        active_s = sum(by_class.values())
        if active_s <= 0:
            continue
        info = meta.get(session_id, {})
        out.append(
            {
                "session_id": session_id,
                "project": info.get("project"),
                "experiment": info.get("experiment"),
                "active_s": round(active_s, 1),
                "nav_s": round(by_class["navigation"], 1),
                "nav_share": round(by_class["navigation"] / active_s, 4),
                "generation_s": round(by_class["generation"], 1),
                "nav_calls": nav_calls,
            }
        )
    out.sort(key=lambda r: -r["nav_share"])
    return out


def nav_summary(rows: list[dict[str, Any]], *, min_active_s: float = 300) -> dict[str, Any]:
    """Corpus rollup over per-session rows; short sessions are noise."""
    rows = [r for r in rows if r["active_s"] >= min_active_s]
    if not rows:
        return {"sessions": 0}
    shares = [r["nav_share"] for r in rows]
    return {
        "sessions": len(rows),
        "active_hours": round(sum(r["active_s"] for r in rows) / 3600, 1),
        "nav_hours": round(sum(r["nav_s"] for r in rows) / 3600, 1),
        "nav_share_median": round(statistics.median(shares), 4),
        "nav_share_mean": round(statistics.fmean(shares), 4),
        "nav_calls": sum(r["nav_calls"] for r in rows),
    }
