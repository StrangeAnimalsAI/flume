"""Experiment comparison: tagged sessions vs a pre-experiment baseline.

An experiment (see SqliteAnalyzedStore.create_experiment) tags sessions by
time window + scope. `compare_experiment` puts numbers side by side:

- baseline = sessions matching the same scope that STARTED in the
  `baseline_days` before the experiment began and carry no tag for it
- metrics chosen to answer "did the tooling change speed things up":
  navigation share of active time (the headline), bytes per nav call,
  duplicate reads, cache hit, session shape

Deltas are directional evidence, not significance tests — session counts
here are small and workloads shift week to week. Read them with the n.
"""
from __future__ import annotations

import re
import statistics
from pathlib import Path
from typing import Any

from flume.analysis.navtime import classify_tool, nav_summary, session_nav_shares

DAY_NS = 86_400 * 1_000_000_000
_FILE_ARG = re.compile(r'"file_path":\s*"([^"]+)"')


def compare_experiment(
    store, name: str, *, baseline_days: int = 30
) -> dict[str, Any]:
    experiment = store.get_experiment(name)
    if experiment is None:
        raise KeyError(f"no experiment named {name!r}")

    experiment_ids = store.experiment_session_ids(name)

    conditions = ["s.is_subagent = 0",
                  "s.started_at_ns >= ?", "s.started_at_ns < ?",
                  "',' || COALESCE(s.experiment, '') || ',' NOT LIKE ?"]
    params: list[Any] = [
        experiment["started_at_ns"] - baseline_days * DAY_NS,
        experiment["started_at_ns"],
        f"%,{name},%",
    ]
    if experiment["source"]:
        conditions.append("s.source = ?")
        params.append(experiment["source"])
    if experiment["project"]:
        conditions.append("s.project = ?")
        params.append(experiment["project"])
    baseline_ids = [
        row["session_id"]
        for row in store.rows(
            f"SELECT s.session_id FROM sessions s WHERE {' AND '.join(conditions)}",
            tuple(params),
        )
    ]

    return {
        "experiment": experiment,
        "baseline_days": baseline_days,
        "groups": [
            _group_metrics(store, "baseline", baseline_ids,
                           source=experiment["source"]),
            _group_metrics(store, "experiment", experiment_ids,
                           source=experiment["source"]),
        ],
    }


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

    nav_calls = nav_chars = 0
    for row in store.rows(
        f"""
        SELECT t.name, t.args_preview, t.result_chars, s.source
        FROM tool_calls t JOIN sessions s USING (session_id)
        WHERE t.session_id IN ({marks})
        """,
        tuple(session_ids),
    ):
        if (
            classify_tool(row["name"], row["args_preview"], row["source"])
            == "navigation"
        ):
            nav_calls += 1
            nav_chars += row["result_chars"] or 0

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
