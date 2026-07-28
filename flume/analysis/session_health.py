"""Per-session health: where the time, money, and redundant work went.

The store-wide analyses answer "how should I work"; this answers "how is this
one session going". The metrics were chosen from what the cross-session
analysis actually showed predicts trouble, not from what is easy to plot:

* Work done (tool calls) leads, not context occupancy. Redundant work tracks
  how much has been done, not how full the window is — controlling for ordinal
  position, context depth stops predicting repeats and at the top end reverses.
* Context is reported as peak occupancy *and* re-send factor. Peak alone is a
  stock; the bill and the latency track the flow through the window, and a
  session can sit at 40% forever while pushing far more than one that peaks at
  99%.
* Cache expiry is detected from the share of the prefix re-written, never from
  elapsed quiet time. The TTL is not recorded in any transcript, so a
  gap-threshold detector silently mis-fires: a 15-minute gap re-writes almost
  nothing under a 1h TTL and the whole prefix under a 5m one. Measured across
  the store, requests re-writing >=50% of their prefix follow an average 97
  minutes of quiet, while every other band averages under a minute.

Requires a SQL-capable store (`SqlReadable`); asserted up front.
"""
from __future__ import annotations

import statistics
from typing import Any

from flume.store.base import require_sql

# A gap longer than this between tool calls is a human being away, not the
# model working. Model turns run long, but not this long unattended.
IDLE_GAP_MS = 120_000

# Share of the prefix re-written that marks a cache miss rather than ordinary
# growth. See the module docstring for why this is a ratio, not a duration.
EXPIRY_WRITE_RATIO = 0.5

# A repeat this close to its original is a stutter (a retry, a failed edit
# reapplied); far apart it is re-derivation of something still in context.
# The two are different bugs and the distribution is bimodal, so report both.
STUTTER_GAP = 5
LONG_RANGE_GAP = 100


def session_health(store: object, session_id: str) -> dict[str, Any] | None:
    """Everything the single-session view needs, in one round trip."""
    sql = require_sql(store, "session health")

    session = sql.row(
        "SELECT session_id, source, surface, project, model, git_branch, "
        "started_at_ns, ended_at_ns, wall_ms, turn_count, tool_call_count, "
        "is_subagent, first_user_message "
        "FROM sessions WHERE session_id = ?",
        (session_id,),
    )
    if session is None:
        return None

    requests = sql.rows(
        "SELECT started_at_ns, context_tokens, input_tokens, output_tokens, "
        "cache_read_tokens, cache_creation_tokens, thinking_chars, block_count "
        "FROM turn_requests WHERE session_id = ? ORDER BY started_at_ns",
        (session_id,),
    )
    calls = sql.rows(
        "SELECT name, args_hash, args_preview, started_at_ns, ended_at_ns, "
        "is_error, result_chars "
        "FROM tool_calls WHERE session_id = ? ORDER BY started_at_ns",
        (session_id,),
    )

    return {
        "session": session,
        "headline": _headline(session, requests, calls),
        "time": _time_split(session, calls),
        "tools": _tool_costs(calls),
        "cache": _cache(requests),
        "redundancy": _redundancy(calls),
        "context_curve": _context_curve(session, requests),
    }


def _headline(session, requests, calls) -> dict[str, Any]:
    peak = max((r["context_tokens"] or 0 for r in requests), default=0)
    throughput = sum(r["context_tokens"] or 0 for r in requests)
    read = sum(r["cache_read_tokens"] or 0 for r in requests)
    repeats = _repeat_gaps(calls)
    errors = sum(1 for c in calls if c["is_error"])
    # cache_coverage_pct and error_pct stay in the payload — the API is a data
    # surface — but the view does not spend a headline slot on them: across
    # sessions with 6x spreads in re-send factor they varied 96.5-99.2% and
    # 1.0-3.0%, which is not a signal anyone can act on.
    return {
        "tool_calls": len(calls),
        "model_requests": len(requests),
        "wall_ms": session["wall_ms"] or 0,
        "active_ms": _time_split(session, calls)["active_ms"],
        "peak_context_tokens": peak,
        "context_throughput_tokens": throughput,
        # How many times the context was re-sent. The number the in-app
        # utilization meter cannot show you.
        "resend_factor": round(throughput / peak, 1) if peak else 0.0,
        "cache_coverage_pct": round(100.0 * read / throughput, 1) if throughput else 0.0,
        "redundancy_pct": round(100.0 * len(repeats) / len(calls), 1) if calls else 0.0,
        "error_pct": round(100.0 * errors / len(calls), 1) if calls else 0.0,
    }


def _time_split(session, calls) -> dict[str, Any]:
    """Where the session's *active* time went — tool execution vs model.

    Idle is measured but is not part of the split, because it is not the
    agent's time: a session left open overnight reports 97% idle, which says
    something about the human and nothing about the run. Across real sessions
    idle swamped everything (79-97%), squashing the only comparison that
    matters — tool versus model — into a sliver. `active_ms` is the honest
    denominator; `idle_ms` stays in the payload so wall clock still
    reconciles, and so "how much of the elapsed time was real" is answerable.

    Note what this cannot be: per-turn model latency. Claude Code transcripts
    carry duration_ms 0 on every turn and the mapper derives start from
    end - duration, collapsing both to one instant. So "model" here is the
    inter-call gap, which bundles genuine model time with harness overhead —
    enough to answer "is the model or the tooling slow", not enough to
    attribute precisely.
    """
    tool_ms = sum(
        (c["ended_at_ns"] - c["started_at_ns"]) // 1_000_000 for c in calls
    )
    model_ms = idle_ms = 0
    for prev, cur in zip(calls, calls[1:], strict=False):
        gap_ms = (cur["started_at_ns"] - prev["ended_at_ns"]) // 1_000_000
        if gap_ms <= 0:
            continue
        if gap_ms > IDLE_GAP_MS:
            idle_ms += gap_ms
        else:
            model_ms += gap_ms
    wall_ms = session["wall_ms"] or 0
    return {
        "wall_ms": wall_ms,
        "tool_ms": tool_ms,
        "model_ms": model_ms,
        "idle_ms": idle_ms,
        # The denominator the split is drawn against.
        "active_ms": tool_ms + model_ms,
        # Time before the first tool call and after the last, mostly.
        "unaccounted_ms": max(0, wall_ms - tool_ms - model_ms - idle_ms),
    }


def _tool_costs(calls) -> list[dict[str, Any]]:
    """Ranked by total wall time held, which is the actionable cut.

    Not call count (a hot tool may be fast) and not mean duration (one slow
    outlier is not a pattern). Total time held is what you can act on.
    """
    agg: dict[str, dict[str, Any]] = {}
    for call in calls:
        row = agg.setdefault(
            call["name"],
            {"name": call["name"], "calls": 0, "total_ms": 0, "errors": 0,
             "result_tokens": 0},
        )
        row["calls"] += 1
        row["total_ms"] += (call["ended_at_ns"] - call["started_at_ns"]) // 1_000_000
        row["errors"] += 1 if call["is_error"] else 0
        row["result_tokens"] += (call["result_chars"] or 0) // 4
    for row in agg.values():
        row["mean_ms"] = round(row["total_ms"] / row["calls"], 1)
    return sorted(agg.values(), key=lambda r: -r["total_ms"])


def _cache(requests) -> dict[str, Any]:
    written = sum(r["cache_creation_tokens"] or 0 for r in requests)
    read = sum(r["cache_read_tokens"] or 0 for r in requests)
    fresh = sum(r["input_tokens"] or 0 for r in requests)

    expiries = []
    for prev, cur in zip(requests, requests[1:], strict=False):
        context = cur["context_tokens"] or 0
        rewrote = cur["cache_creation_tokens"] or 0
        if context and rewrote / context >= EXPIRY_WRITE_RATIO:
            expiries.append(
                {
                    "at_ns": cur["started_at_ns"],
                    "quiet_ms": (cur["started_at_ns"] - prev["started_at_ns"])
                    // 1_000_000,
                    "rewrote_tokens": rewrote,
                }
            )
    return {
        "fresh_tokens": fresh,
        "read_tokens": read,
        "written_tokens": written,
        "expiries": expiries,
        "expiry_tokens": sum(e["rewrote_tokens"] for e in expiries),
    }


def _repeat_gaps(calls) -> list[dict[str, Any]]:
    """Byte-identical calls already made in this session, with their distance."""
    first_at: dict[tuple[str, str], int] = {}
    repeats: list[dict[str, Any]] = []
    for index, call in enumerate(calls):
        key = (call["name"], call["args_hash"])
        if key in first_at:
            repeats.append(
                {
                    "name": call["name"],
                    "args_preview": call["args_preview"],
                    "gap": index - first_at[key],
                    "at_index": index,
                }
            )
        else:
            first_at[key] = index
    return repeats


def _redundancy(calls) -> dict[str, Any]:
    repeats = _repeat_gaps(calls)
    gaps = [r["gap"] for r in repeats]
    return {
        "repeats": len(repeats),
        "stutters": sum(1 for g in gaps if g <= STUTTER_GAP),
        "long_range": sum(1 for g in gaps if g >= LONG_RANGE_GAP),
        "median_gap": round(statistics.median(gaps), 1) if gaps else None,
        "worst": sorted(repeats, key=lambda r: -r["gap"])[:10],
    }


def _context_curve(session, requests) -> list[dict[str, Any]]:
    """Occupancy over elapsed time — the series the session chart plots."""
    start = session["started_at_ns"] or 0
    return [
        {
            "elapsed_ms": (r["started_at_ns"] - start) // 1_000_000,
            "context_tokens": r["context_tokens"] or 0,
            "output_tokens": r["output_tokens"] or 0,
            "cache_creation_tokens": r["cache_creation_tokens"] or 0,
        }
        for r in requests
    ]
