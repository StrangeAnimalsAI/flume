"""Experiment comparison: tagged sessions vs a pre-experiment baseline.

An experiment (see SqliteAnalyzedStore.create_experiment) tags sessions by
time window + scope. `compare_experiment` puts numbers side by side:

- baseline = sessions matching the same scope that STARTED in the
  `baseline_days` before the experiment began and carry no tag for it
- subagent sessions form their own pair of groups, so delegated work never
  dilutes the main-session numbers
- metrics chosen to answer "did the tooling change speed things up":
  navigation share of active time (the headline), bytes per nav call,
  nav calls per model request (batching), duplicate reads, cache hit,
  session shape

Deltas are directional evidence, not significance tests — session counts
here are small and workloads shift week to week. Read them with the n.
"""
from __future__ import annotations

import re
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

from flume.analysis.navtime import classify_tool, nav_summary, session_nav_shares

DAY_NS = 86_400 * 1_000_000_000
_FILE_ARG = re.compile(r'"file_path":\s*"([^"]+)"')
# Only claude-code tool calls and turns share a start timestamp; codex's don't.
_REQUEST_SOURCE = "claude-code"


def compare_experiment(
    store, name: str, *, baseline_days: int = 30
) -> dict[str, Any]:
    experiment = store.get_experiment(name)
    if experiment is None:
        raise KeyError(f"no experiment named {name!r}")

    groups = []
    for subagents in (False, True):
        suffix = " (subagents)" if subagents else ""
        groups.append(_group_metrics(
            store, f"baseline{suffix}",
            _baseline_ids(store, experiment, baseline_days, subagents=subagents),
            source=experiment["source"],
        ))
        groups.append(_group_metrics(
            store, f"experiment{suffix}",
            store.experiment_session_ids(name, subagents=subagents),
            source=experiment["source"],
        ))

    return {
        "experiment": experiment,
        "baseline_days": baseline_days,
        "groups": groups,
    }


def _baseline_ids(
    store, experiment: dict[str, Any], baseline_days: int, *, subagents: bool
) -> list[str]:
    conditions = ["s.is_subagent = ?",
                  "s.started_at_ns >= ?", "s.started_at_ns < ?",
                  "',' || COALESCE(s.experiment, '') || ',' NOT LIKE ?"]
    params: list[Any] = [
        int(subagents),
        experiment["started_at_ns"] - baseline_days * DAY_NS,
        experiment["started_at_ns"],
        f"%,{experiment['name']},%",
    ]
    if experiment["source"]:
        conditions.append("s.source = ?")
        params.append(experiment["source"])
    if experiment["project"]:
        conditions.append("s.project = ?")
        params.append(experiment["project"])
    if experiment["cwd"]:
        conditions.append("(s.cwd = ? OR substr(s.cwd, 1, ?) = ?)")
        params += [experiment["cwd"], len(experiment["cwd"]) + 1,
                   experiment["cwd"] + "/"]
    return [
        row["session_id"]
        for row in store.rows(
            f"SELECT s.session_id FROM sessions s WHERE {' AND '.join(conditions)}",
            tuple(params),
        )
    ]


def _group_metrics(
    store, label: str, session_ids: list[str], *, source: str | None
) -> dict[str, Any]:
    if not session_ids:
        return {"group": label, "sessions": 0}
    marks = ",".join("?" for _ in session_ids)

    shape = store.row(
        f"""
        SELECT COUNT(*) AS sessions,
               SUM(input_tokens) AS input_tokens,
               SUM(cache_read_tokens) AS cache_read,
               AVG(tool_call_count) AS avg_tools
        FROM sessions WHERE session_id IN ({marks})
        """,
        tuple(session_ids),
    )

    # A model request is the turn rows sharing COALESCE(request_id, span_id);
    # a tool call belongs to the request whose turn starts when it does.
    nav_calls = nav_chars = docnav_probes = 0
    nav_per_request: Counter[tuple[str, str]] = Counter()
    for row in store.rows(
        f"""
        SELECT t.session_id, t.name, t.args_preview, t.result_chars, s.source,
               r.request_key
        FROM tool_calls t JOIN sessions s USING (session_id)
        LEFT JOIN (
            SELECT session_id, started_at_ns,
                   MIN(COALESCE(request_id, span_id)) AS request_key
            FROM turns WHERE session_id IN ({marks})
            GROUP BY session_id, started_at_ns
        ) r ON r.session_id = t.session_id AND r.started_at_ns = t.started_at_ns
        WHERE t.session_id IN ({marks})
        """,
        tuple(session_ids) * 2,
    ):
        if "_docnav" in (row["args_preview"] or ""):
            docnav_probes += 1
        if (
            classify_tool(row["name"], row["args_preview"], row["source"])
            == "navigation"
        ):
            nav_calls += 1
            nav_chars += row["result_chars"] or 0
            if row["request_key"] is not None and row["source"] == _REQUEST_SOURCE:
                nav_per_request[(row["session_id"], row["request_key"])] += 1
    nav_requests = len(nav_per_request)

    duplicate_reads = (store.row(
        f"""
        SELECT COALESCE(SUM(n - 1), 0) AS dups FROM (
            SELECT COUNT(*) AS n FROM tool_calls
            WHERE session_id IN ({marks}) AND name = 'Read'
            GROUP BY session_id, args_hash HAVING n > 1
        )
        """,
        tuple(session_ids),
    ) or {}).get("dups", 0)

    # source=None widens the nav-share pull to every source in the group.
    # Sessions here were explicitly selected, so the noise floor is lower
    # than the corpus-level default.
    summary = nav_summary(
        session_nav_shares(store, source=source, session_ids=session_ids),
        min_active_s=60,
    )
    used_share = _read_used_share(store, session_ids)
    denominator = (shape["cache_read"] or 0) + (shape["input_tokens"] or 0)
    return {
        "group": label,
        "sessions": shape["sessions"],
        "measured": summary.get("sessions", 0),
        "nav_share_median": summary.get("nav_share_median"),
        "active_hours": summary.get("active_hours"),
        "nav_hours": summary.get("nav_hours"),
        "nav_calls": nav_calls,
        "kb_per_nav_call": round(nav_chars / nav_calls / 1000, 1) if nav_calls else None,
        "nav_calls_per_request": round(sum(nav_per_request.values()) / nav_requests, 2)
        if nav_requests else None,
        "nav_requests": nav_requests,
        "docnav_probes": docnav_probes,
        "duplicate_reads": duplicate_reads,
        "read_used_share": used_share,
        "cache_hit": round((shape["cache_read"] or 0) / denominator, 3)
        if denominator else None,
        "avg_tools_per_session": round(shape["avg_tools"] or 0, 1),
    }


def _read_used_share(store, session_ids: list[str]) -> float | None:
    """Median per-session share of read files that were later USED.

    Used = the file was subsequently edited in the session, or its basename
    appears in an assistant message (cited in reasoning or the answer).
    A proxy, derived entirely from ingested data: it under-credits
    reads that ruled something out and over-credits common basenames.
    _docnav map reads are excluded — they're lookup overhead, not targets."""
    shares = []
    for session_id in session_ids:
        reads = set()
        edited = set()
        for row in store.rows(
            """
            SELECT name, args_preview FROM tool_calls
            WHERE session_id = ?
                AND name IN ('Read', 'Edit', 'Write', 'NotebookEdit')
            """,
            (session_id,),
        ):
            match = _FILE_ARG.search(row["args_preview"] or "")
            if not match or "_docnav" in match.group(1):
                continue
            if row["name"] == "Read":
                reads.add(match.group(1))
            else:
                edited.add(match.group(1))
        if not reads:
            continue
        mentions = " ".join(
            row["text"]
            for row in store.rows(
                "SELECT text FROM contents WHERE session_id = ? "
                "AND kind = 'assistant_message'",
                (session_id,),
            )
        )
        used = sum(
            1 for path in reads
            if path in edited or Path(path).name in mentions
        )
        shares.append(used / len(reads))
    return round(statistics.median(shares), 3) if shares else None
