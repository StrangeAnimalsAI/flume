"""Insight detectors: turn session data into ranked, actionable findings.

Each detector inspects a time window and emits findings shaped as:
    kind         stable detector id (e.g. "toolgap", "repeat_waste")
    fingerprint  stable key within the kind — recurring findings update the
                 same row (first_seen/last_seen/occurrences) instead of
                 duplicating, so trends are visible
    severity     1 = act now, 2 = worth fixing, 3 = watch
    title        one-line statement of the gap
    detail       evidence summary a reader can verify
    metric       the number that ranks it (unit in `unit`)
    action       the concrete suggested move (build X / fix Y / habit Z)

Currently requires the sqlite backend (uses raw SQL for a few detectors).
Severity heuristics are tuned for a single-user corpus; adjust freely.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flume.analysis.audit import script_clusters
from flume.analysis.navtime import nav_summary, session_nav_shares

Finding = dict[str, Any]

PREMIUM_MODELS = ("claude-fable-5", "claude-mythos", "claude-opus")
IDLE_GAP_NS = 300 * 1_000_000_000  # 5 min — prompt-cache TTL


def run_insights(
    store,
    *,
    since_ns: int | None = None,
    source: str | None = None,
    persist: bool = True,
) -> list[Finding]:
    """Run every detector, persist findings (deduped) unless persist=False,
    return them ranked.

    `source` restricts the scan to one source name; None means every source
    in the store. Detectors are source-agnostic — the patterns they look
    for (duplicate calls, idle gaps, navigation grind) are properties of
    agentic coding, not of any one vendor."""
    findings: list[Finding] = []
    for detector in (
        _toolgaps,
        _repeat_waste,
        _schema_loops,
        _error_hotspots,
        _context_floods,
        _idle_gap_churn,
        _marathon_sessions,
        _premium_grind,
        _docnav_ignored,
        _nav_share,
        _thinking_volume,
    ):
        findings.extend(detector(store, since_ns, source))
    findings.sort(key=lambda f: (f["severity"], -f["metric"]))
    if persist:
        store.upsert_findings(findings)
    return findings


def _finding(kind, fingerprint, severity, title, detail, metric, unit, action,
             evidence=None) -> Finding:
    return {
        "kind": kind, "fingerprint": fingerprint, "severity": severity,
        "title": title, "detail": detail, "metric": round(float(metric), 2),
        "unit": unit, "action": action,
        "evidence": json.dumps(evidence or {}, default=str)[:4000],
    }


# -- detectors ---------------------------------------------------------------


def _toolgaps(store, since_ns, source) -> list[Finding]:
    """Throwaway scripts rewritten across many sessions -> durable tools."""
    out = []
    for cluster in script_clusters(store, source=source, since_ns=since_ns, min_sessions=5)[:5]:
        n = cluster["sessions"]
        out.append(_finding(
            "toolgap", f"{cluster['imports']}|{cluster['operations']}",
            1 if n >= 30 else 2,
            f"Same inline script shape rewritten in {n} sessions "
            f"(imports: {cluster['imports']})",
            f"Example: {cluster['example'][:200]}",
            n, "sessions",
            "Build a durable CLI for this and register it in the repo's "
            "CLAUDE.md/AGENTS.md so agents stop re-deriving it.",
            {"session_ids": cluster["session_ids"]},
        ))
    return out


def _repeat_waste(store, since_ns, source) -> list[Finding]:
    """Byte-identical duplicate calls: provably zero-information re-work."""
    out = []
    by_tool: dict[str, dict[str, Any]] = {}
    for row in store.audit_repeats(source=source, since_ns=since_ns, limit=200):
        if not row.get("byte_identical"):
            continue
        slot = by_tool.setdefault(row["name"], {"calls": 0, "ms": 0, "groups": 0})
        slot["calls"] += row["calls"] - 1
        slot["ms"] += row["total_ms"] * (row["calls"] - 1) / max(row["calls"], 1)
        slot["groups"] += 1
    for tool, agg in sorted(by_tool.items(), key=lambda kv: -kv[1]["calls"]):
        if agg["calls"] < 5:
            continue
        out.append(_finding(
            "repeat_waste", tool, 2,
            f"{tool}: {agg['calls']} byte-identical duplicate calls "
            f"({agg['groups']} retry groups)",
            "Identical arguments returned identical bytes — zero new "
            f"information; ~{agg['ms'] / 60000:.0f} min of tool time.",
            agg["calls"], "wasted calls",
            "Retry loop or schema mismatch — fix the calling prompt/schema, "
            "or add a PreToolUse repeat-guard hook.",
        ))
    return out


def _schema_loops(store, since_ns, source) -> list[Finding]:
    """Subagents grinding against a StructuredOutput schema they never satisfy.

    Distinct from repeat_waste: each retry rephrases the payload (not
    byte-identical), so only the validation-error count exposes the loop.
    Root cause is usually an agent() prompt asking for different keys than
    the attached schema requires — the subagent follows the prompt (INT-1282)."""
    where, params = _scope(since_ns, source)
    rows = store._all(
        f"""
        SELECT s.project, COUNT(*) errors,
               COUNT(DISTINCT t.session_id) sessions,
               MAX(t.session_id) example_session
        FROM tool_calls t JOIN sessions s USING (session_id)
        {where} {_and(where)} t.name = 'StructuredOutput'
            AND t.is_error = 1
        GROUP BY s.project HAVING errors >= 10
        ORDER BY errors DESC LIMIT 5
        """, params)
    out = []
    for r in rows:
        sample = store._one(
            """
            SELECT c.text FROM contents c
            JOIN tool_calls t ON t.span_id = c.span_id
            JOIN sessions s USING (session_id)
            WHERE t.name = 'StructuredOutput' AND t.is_error = 1
                AND c.kind = 'tool_result' AND s.project IS ?
            ORDER BY c.ts_ns DESC LIMIT 1
            """, (r["project"],)) or {}
        out.append(_finding(
            "schema_loop", r["project"] or "?",
            1 if r["errors"] >= 50 else 2,
            f"StructuredOutput schema loops in {r['project'] or '?'}: "
            f"{r['errors']} validation failures across {r['sessions']} "
            "subagent(s)",
            f"Sample error: {(sample.get('text') or '')[:200]}",
            r["errors"], "failed calls",
            "Workflow agent() prompts must restate the schema's required "
            "keys verbatim; the subagent writes to the prompt, not the "
            "schema. Inspect: analyze show " + str(r["example_session"]),
        ))
    return out


def _error_hotspots(store, since_ns, source) -> list[Finding]:
    where, params = _scope(since_ns, source)
    rows = store._all(
        f"""
        SELECT t.name, COUNT(*) calls, SUM(t.is_error) errors
        FROM tool_calls t JOIN sessions s USING (session_id) {where}
        GROUP BY t.name HAVING calls >= 20 AND errors * 1.0 / calls > 0.10
        ORDER BY errors DESC LIMIT 5
        """, params)
    return [_finding(
        "error_hotspot", r["name"], 2,
        f"{r['name']} fails {r['errors'] * 100 // r['calls']}% of the time "
        f"({r['errors']}/{r['calls']} calls)",
        "Each failure is a wasted round trip plus context pollution.",
        r["errors"], "failed calls",
        "Inspect failing arguments in the store (tool_calls.is_error=1 join "
        "contents) and fix the schema, tool description, or environment.",
    ) for r in rows]


def _context_floods(store, since_ns, source) -> list[Finding]:
    where, params = _scope(since_ns, source)
    rows = store._all(
        f"""
        SELECT t.name, s.source, COUNT(*) n, SUM(t.result_chars) chars,
               MAX(t.result_chars) worst
        FROM tool_calls t JOIN sessions s USING (session_id)
        {where} {_and(where)} t.result_chars > 200000
        GROUP BY t.name, s.source ORDER BY chars DESC LIMIT 5
        """, params)
    return [_finding(
        "context_flood", f"{r['source']}:{r['name']}",
        1 if r["chars"] > 50_000_000 else 2,
        f"{r['name']} ({r['source']}): {r['n']} calls returned >200k chars "
        f"each ({r['chars'] / 1e6:.0f} MB total into context)",
        f"Worst single call: {r['worst'] / 1e6:.1f} MB. Every byte rides in "
        "context for the rest of the session and re-bills each turn.",
        r["chars"] / 1e6, "MB",
        "Bound the producing command (repo-nav / head / --max-count), or "
        "write output to a file and read ranges.",
    ) for r in rows]


def _idle_gap_churn(store, since_ns, source) -> list[Finding]:
    """Cache rewrites after >5-min idle gaps (prompt-cache TTL expiry)."""
    where, params = _scope(since_ns, source)
    row = store._one(
        f"""
        SELECT COUNT(*) gaps, COALESCE(SUM(next_cc), 0) rewrite_tokens
        FROM (
            SELECT t.cache_creation_tokens AS next_cc,
                   t.started_at_ns - LAG(t.ended_at_ns) OVER (
                       PARTITION BY t.session_id ORDER BY t.started_at_ns
                   ) AS gap
            FROM turns t JOIN sessions s USING (session_id)
            {where}
        ) WHERE gap > {IDLE_GAP_NS}
        """, params) or {}
    tokens = row.get("rewrite_tokens") or 0
    if tokens < 5_000_000:
        return []
    return [_finding(
        "idle_gap_churn", _fp(source),
        2 if tokens < 50_000_000 else 1,
        f"{row['gaps']} idle gaps >5min mid-session forced "
        f"{tokens / 1e6:.0f}M tokens of cache rewrites",
        "The prompt cache TTL is 5 minutes; resuming an idle session "
        "re-writes the whole context at a 1.25x premium.",
        tokens / 1e6, "M tokens",
        "End the session when stepping away; start fresh (or /clear) on "
        "return. Batch replies while a session is active.",
    )]


def _marathon_sessions(store, since_ns, source) -> list[Finding]:
    where, params = _scope(since_ns, source)
    rows = store._all(
        f"""
        SELECT session_id, project, turn_count, wall_ms,
               cache_read_tokens + input_tokens AS ctx_tokens
        FROM sessions s {where} {_and(where)} is_subagent = 0
            AND turn_count > 200
        ORDER BY cache_read_tokens DESC LIMIT 3
        """, params)
    return [_finding(
        "marathon_session", r["session_id"], 2,
        f"Marathon session in {r['project'] or '?'}: {r['turn_count']} turns, "
        f"{r['wall_ms'] / 3.6e6:.1f}h wall",
        f"{r['ctx_tokens'] / 1e9:.1f}B context tokens re-read; per-turn cost "
        "grows with everything accumulated since the first prompt.",
        r["ctx_tokens"] / 1e9, "B ctx tokens",
        "One task per session; split follow-up topics into fresh sessions "
        "— the per-prompt table (analyze commands) shows the seams.",
    ) for r in rows]


def _premium_grind(store, since_ns, source) -> list[Finding]:
    """Premium-model sessions doing high-volume mechanical tool work."""
    where, params = _scope(since_ns, source)
    premium = " OR ".join(f"model LIKE '{m}%'" for m in PREMIUM_MODELS)
    rows = store._all(
        f"""
        SELECT session_id, project, model, tool_call_count, output_tokens
        FROM sessions s {where} {_and(where)} is_subagent = 0
            AND ({premium}) AND tool_call_count > 150
        ORDER BY tool_call_count DESC LIMIT 3
        """, params)
    return [_finding(
        "premium_grind", r["session_id"], 3,
        f"{r['model']} session ran {r['tool_call_count']} tool calls "
        f"({r['project'] or '?'})",
        "High-volume tool loops on a premium model; much of this is "
        "typically exploration/mechanics a cheaper model handles.",
        r["tool_call_count"], "tool calls",
        "Delegate fan-out reads to the `scout` (haiku) subagent and "
        "well-specified edits to `mech` (sonnet); keep the premium model "
        "for judgment.",
    ) for r in rows]


def _docnav_ignored(store, since_ns, source) -> list[Finding]:
    """Sessions grinding navigation in repos that HAVE a _docnav index."""
    where, params = _scope(since_ns, source)
    sessions = store._all(
        f"""
        SELECT session_id, cwd, project, tool_call_count FROM sessions s
        {where} {_and(where)} tool_call_count > 30 AND cwd IS NOT NULL
        """, params)
    indexed = [s for s in sessions if (Path(s["cwd"]) / "_docnav").is_dir()]
    if not indexed:
        return []
    marks = ",".join("?" for _ in indexed)
    used = {r["session_id"] for r in store._all(
        f"""
        SELECT DISTINCT c.session_id FROM contents_fts
        JOIN contents c ON c.id = contents_fts.rowid
        WHERE contents_fts MATCH '"_docnav"'
            AND c.session_id IN ({marks})
        """, tuple(s["session_id"] for s in indexed))}
    ignored = [s for s in indexed if s["session_id"] not in used]
    if len(ignored) < 3:
        return []
    projects = sorted({s["project"] or "?" for s in ignored})
    return [_finding(
        "docnav_ignored", _fp(source), 3,
        f"{len(ignored)} of {len(indexed)} heavy sessions in indexed repos "
        "never consulted _docnav",
        f"Projects: {', '.join(projects[:5])}. The index exists but the "
        "agent navigated the tree instead.",
        len(ignored), "sessions",
        "Strengthen the CLAUDE.md nav instruction in those repos, or "
        "regenerate stale indexes (repo-nav index).",
        {"session_ids": [s["session_id"] for s in ignored[:10]]},
    )]


def _nav_share(store, since_ns, source) -> list[Finding]:
    """Share of active session time spent navigating code (cycle attribution).

    Measured 2026-07-08: ~33% median across May-July sessions — the single
    largest tool class, on par with pure generation. The fingerprint is
    fixed so re-runs update one row and the trend stays visible; experiment
    windows (analyze experiment compare) give the controlled before/after."""
    rows = session_nav_shares(store, source=source, since_ns=since_ns)
    summary = nav_summary(rows)
    if summary["sessions"] < 5:
        return []
    median = summary["nav_share_median"]
    if median < 0.15:
        return []
    worst = [r for r in rows if r["active_s"] >= 300][:3]
    worst_txt = "; ".join(
        f"{r['project'] or '?'} {r['nav_share']:.0%} of {r['active_s'] / 60:.0f}min"
        for r in worst
    )
    return [_finding(
        "nav_share", _fp(source),
        2 if median >= 0.30 else 3,
        f"Navigation eats {median:.0%} of active time (median of "
        f"{summary['sessions']} sessions; {summary['nav_hours']}h of "
        f"{summary['active_hours']}h)",
        f"Worst sessions: {worst_txt}.",
        median * 100, "% of active time",
        "Index the busiest repos (repo-nav index) and keep the CLAUDE.md "
        "nav contract fresh; run experiments (analyze experiment start) to "
        "verify tooling changes actually move this number.",
        {"nav_share_mean": summary["nav_share_mean"],
         "nav_calls": summary["nav_calls"],
         "worst_sessions": [r["session_id"] for r in worst]},
    )]


def _thinking_volume(store, since_ns, source) -> list[Finding]:
    """Premium output that is invisible reasoning, not deliverable.

    Measured 2026-07-09 (30d): ~91% of 63 MTok premium output was thinking
    (visible prose 1.8%, tool args 7%). Generation cycles run at healthy
    throughput (~230 tok/s, waiting negligible), so the 38% generation
    share of wall time is volume-driven: less thinking = less waiting AND
    less spend. Levers: effort level, delegation to non-premium models."""
    where, params = _scope(since_ns, source)
    premium = " OR ".join(f"t.model LIKE '{m}%'" for m in PREMIUM_MODELS)
    row = store._one(
        f"""
        SELECT SUM(t.output_tokens) out_tok, SUM(t.text_chars) text_chars
        FROM turns t JOIN sessions s USING (session_id)
        {where} {_and(where)} s.is_subagent = 0 AND ({premium})
        """, params) or {}
    out_tok = row.get("out_tok") or 0
    if out_tok < 1_000_000:
        return []
    visible = (row.get("text_chars") or 0) / (out_tok * 4)
    args_row = store._one(
        f"""
        SELECT SUM(LENGTH(c.text)) n FROM contents c
        JOIN sessions s USING (session_id)
        {where} {_and(where)} s.is_subagent = 0 AND c.kind = 'tool_arguments'
        """, params) or {}
    args_share = (args_row.get("n") or 0) / (out_tok * 4)
    invisible = max(0.0, 1 - visible - args_share)
    if invisible < 0.75:
        return []
    return [_finding(
        "thinking_volume", _fp(source),
        2 if invisible >= 0.85 else 3,
        f"{invisible:.0%} of {out_tok / 1e6:.0f} MTok premium output is "
        "invisible thinking",
        f"Visible prose {visible:.1%}, tool arguments {args_share:.1%}; "
        "the rest is reasoning tokens billed at premium output rates and "
        "emitted in real time (generation dominates session wall-clock).",
        invisible * 100, "% of output tokens",
        "Trim thinking volume where judgment isn't needed: delegate "
        "mechanical loops to scout/mech, and experiment with effortLevel "
        "(analyze experiment start effort-<level>) before/after.",
    )]


def _scope(since_ns, source=None) -> tuple[str, tuple]:
    """Shared detector scope: time window plus optional source restriction.

    Returns a complete WHERE clause (or "") and its params. Detectors that
    need further conditions append them with `_and(where)`."""
    clauses: list[str] = []
    params: list[Any] = []
    if since_ns is not None:
        clauses.append("s.started_at_ns >= ?")
        params.append(since_ns)
    if source is not None:
        clauses.append("s.source = ?")
        params.append(source)
    if not clauses:
        return "", ()
    return "WHERE " + " AND ".join(clauses), tuple(params)


def _fp(source: str | None) -> str:
    """Fingerprint for corpus-wide findings: scope-stable, vendor-neutral."""
    return source or "all-sources"


def _and(where: str) -> str:
    """Keyword for appending a condition to a possibly-empty WHERE clause."""
    return "AND" if where else "WHERE"
