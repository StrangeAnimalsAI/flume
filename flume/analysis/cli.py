"""Analysis CLI over the session store.

Designed for two readers: humans get aligned tables, agents get `--json`.
Every subcommand is a thin call into AnalyzedStore query methods, so any
backend that implements the interface gets the whole CLI for free.

Examples:
    flume analyze overview
    flume analyze sessions --source claude-code --limit 20
    flume analyze show <session-id>
    flume analyze thinking <session-id>
    flume analyze tools --since 7d
    flume analyze tokens --group-by model
    flume analyze search "navigat* codebase" --kind thinking
    flume analyze ingest --source claude-code --path ~/.claude/projects
    flume analyze experiment start docnav-all --hypothesis "..."
    flume analyze experiment compare docnav-all
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flume.store.base import (
    CONTENT_KINDS,
    DEFAULT_ANALYZED_STORE_URL,
    open_analyzed_store,
    require_sql,
)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 2
    with open_analyzed_store(args.analyzed_store_url) as store:
        result = args.func(store, args)
    if args.json:
        json.dump(result, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    else:
        _render(args.command, result)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flume analyze",
        description="Query the local agent session store.",
    )
    parser.add_argument(
        "--analyzed-store-url",
        default=None,
        help="Store URL (default: sqlite://~/.flume/store.sqlite3).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of tables (for agents / scripting).",
    )
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("overview", help="Corpus totals by source.")
    p.set_defaults(func=_cmd_overview)

    p = sub.add_parser("sessions", help="List top-level sessions, newest first.")
    p.add_argument("--source")
    p.add_argument("--surface")
    p.add_argument("--cwd", help="Substring filter on session cwd.")
    p.add_argument("--project", help="Exact project label (e.g. tools/flume).")
    p.add_argument("--since", help="Window like 24h, 7d, 30d.")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument(
        "--all",
        action="store_true",
        help="Include subagent/child sessions (hidden by default).",
    )
    p.set_defaults(func=_cmd_sessions)

    p = sub.add_parser(
        "commands", help="Per-prompt breakdown of one session (cost + time)."
    )
    p.add_argument("session_id")
    p.set_defaults(func=_cmd_commands)

    p = sub.add_parser("show", help="One session: turns, tools, economy.")
    p.add_argument("session_id")
    p.set_defaults(func=_cmd_show)

    p = sub.add_parser("thinking", help="Full thinking blocks for a session.")
    p.add_argument("session_id")
    p.set_defaults(func=_cmd_thinking)

    p = sub.add_parser("transcript", help="Full transcript contents for a session.")
    p.add_argument("session_id")
    p.add_argument(
        "--kind",
        action="append",
        choices=CONTENT_KINDS,
        help="Restrict to content kinds (repeatable).",
    )
    p.set_defaults(func=_cmd_transcript)

    p = sub.add_parser("tools", help="Tool economy: per-tool, repeats, slowest.")
    p.add_argument("--source")
    p.add_argument("--since", help="Window like 24h, 7d, 30d.")
    p.add_argument("--top", type=int, default=10)
    p.set_defaults(func=_cmd_tools)

    p = sub.add_parser("tokens", help="Token/cache economy rollups.")
    p.add_argument("--source")
    p.add_argument("--since", help="Window like 24h, 7d, 30d.")
    p.add_argument(
        "--group-by",
        default="source",
        choices=("source", "surface", "model", "session"),
    )
    p.set_defaults(func=_cmd_tokens)

    p = sub.add_parser("search", help="Full-text search across all content.")
    p.add_argument("query")
    p.add_argument(
        "--kind",
        action="append",
        choices=CONTENT_KINDS,
        help="Restrict to content kinds (repeatable).",
    )
    p.add_argument("--source")
    p.add_argument("--limit", type=int, default=25)
    p.set_defaults(func=_cmd_search)

    p = sub.add_parser(
        "insights",
        help="Run gap detectors: tooling, process, and design opportunities.",
    )
    p.add_argument("--since", default="7d", help="Window like 24h, 7d, 30d.")
    p.add_argument(
        "--source", default=None, help="Restrict to one source (default: all)."
    )
    p.add_argument(
        "--stored",
        action="store_true",
        help="List previously stored findings instead of re-running detectors.",
    )
    p.add_argument("--limit", type=int, default=25)
    p.set_defaults(func=_cmd_insights)

    p = sub.add_parser(
        "hooks",
        help="Hook interventions: nudges fired, denials, compliance.",
    )
    p.add_argument("--since", help="Window like 24h, 7d, 30d.")
    p.add_argument("--session", help="Only this session id.")
    p.add_argument("--limit", type=int, default=500)
    p.set_defaults(func=_cmd_hooks)

    p = sub.add_parser(
        "experiment",
        help="Track experiments: tag sessions in a window, compare metrics.",
    )
    exp_sub = p.add_subparsers(dest="experiment_command", required=True)
    ep = exp_sub.add_parser("start", help="Open an experiment window (tags sessions).")
    ep.add_argument("name")
    ep.add_argument("--hypothesis", help="What this experiment should prove.")
    ep.add_argument("--source", help="Scope: only sessions from this source.")
    ep.add_argument("--project", help="Scope: only this project label.")
    ep.add_argument(
        "--started",
        help="Window start: ISO date (2026-07-02) or lookback (7d). Default: now.",
    )
    ep.set_defaults(func=_cmd_experiment_start)
    ep = exp_sub.add_parser("stop", help="Close an experiment window.")
    ep.add_argument("name")
    ep.add_argument("--ended", help="Window end: ISO date or lookback. Default: now.")
    ep.set_defaults(func=_cmd_experiment_stop)
    ep = exp_sub.add_parser("list", help="All experiments with session counts.")
    ep.set_defaults(func=_cmd_experiment_list)
    ep = exp_sub.add_parser(
        "compare", help="Experiment sessions vs pre-experiment baseline."
    )
    ep.add_argument("name")
    ep.add_argument(
        "--baseline-days",
        type=int,
        default=30,
        help="Baseline window before the experiment start (default 30).",
    )
    ep.set_defaults(func=_cmd_experiment_compare)

    p = sub.add_parser("cost", help="Dollar cost of agent usage (cache-aware).")
    p.add_argument("--since", help="Window like 24h, 7d, 30d.")
    p.add_argument(
        "--source", default=None, help="Restrict to one source (default: all)."
    )
    p.add_argument(
        "--group-by", default="model", choices=("model", "day", "project", "session")
    )
    p.add_argument(
        "--as-model",
        default=None,
        help="What-if: reprice ALL usage at this model's rates (any priced model).",
    )
    p.set_defaults(func=_cmd_cost)

    p = sub.add_parser(
        "sql",
        help="Run a read-only SQL query over the store (exploratory analysis).",
        description="Read-only SELECT/WITH/PRAGMA/EXPLAIN over the store. Use the "
        "`tool_calls_ext` view for kind/vendor/result_tokens_est columns, e.g. "
        "\"SELECT kind, COUNT(*), SUM(result_tokens_est) FROM tool_calls_ext "
        "GROUP BY kind ORDER BY 3 DESC\".",
    )
    p.add_argument("query", help="A SELECT/WITH/PRAGMA/EXPLAIN statement.")
    p.add_argument("--limit", type=int, default=50, help="Cap rows rendered.")
    p.set_defaults(func=_cmd_sql)

    p = sub.add_parser("audit", help="Audit queries: repeats, bigreads, toolgaps.")
    audit_sub = p.add_subparsers(dest="audit_command", required=True)
    ap = audit_sub.add_parser(
        "repeats", help="Identical calls repeated in one session."
    )
    ap.add_argument("--source")
    ap.add_argument("--since", help="Window like 24h, 7d, 30d.")
    ap.add_argument("--limit", type=int, default=25)
    ap.set_defaults(func=_cmd_audit_repeats)
    ap = audit_sub.add_parser(
        "bigreads", help="Unranged Read calls with huge results."
    )
    ap.add_argument("--source")
    ap.add_argument("--since")
    ap.add_argument("--min-chars", type=int, default=50_000)
    ap.add_argument("--limit", type=int, default=25)
    ap.set_defaults(func=_cmd_audit_bigreads)
    ap = audit_sub.add_parser(
        "toolgaps", help="Recurring throwaway-script shapes (durable-tool candidates)."
    )
    ap.add_argument("--since")
    ap.add_argument("--min-sessions", type=int, default=3)
    ap.set_defaults(func=_cmd_audit_toolgaps)

    p = sub.add_parser("ingest", help="Ingest session files directly (no state db).")
    p.add_argument(
        "--source",
        required=True,
        help="Source name: claude-code, codex, harness.",
    )
    p.add_argument("--path", type=Path, required=True, help="File or directory.")
    p.add_argument(
        "--no-raw-store",
        action="store_true",
        help="Skip capturing original transcript bytes into the raw store.",
    )
    p.add_argument("--raw-store-url", default=None)
    p.set_defaults(func=_cmd_ingest)

    p = sub.add_parser(
        "rebuild",
        help="Re-ingest sessions built by an older pipeline (from the raw store).",
    )
    p.add_argument("--source")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--raw-store-url", default=None)
    p.set_defaults(func=_cmd_rebuild)

    p = sub.add_parser("sources", help="List registered source adapters.")
    p.set_defaults(func=_cmd_sources)

    p = sub.add_parser(
        "review",
        help="Assemble the periodic efficiency review (defaults from [review]).",
        description=(
            "Everything a periodic review needs, gathered without a model: new "
            "findings, recurring findings whose metric grew, active-experiment "
            "scoreboard, hook compliance. Pipe --json to whichever agent your "
            "scheduler runs."
        ),
    )
    p.add_argument("--since", default=None, help="Window (default: [review].since).")
    p.add_argument("--severity-max", type=int, default=None)
    p.add_argument("--growth", type=float, default=None,
                   help="Flag recurring findings whose metric grew by this fraction.")
    p.add_argument("--baseline-days", type=int, default=None)
    p.add_argument("--min-sessions", type=int, default=None)
    p.add_argument("--source", default=None)
    p.set_defaults(func=_cmd_review)

    p = sub.add_parser("raw", help="Raw store: stats, versions, restore.")
    raw_sub = p.add_subparsers(dest="raw_command", required=True)
    rp = raw_sub.add_parser("stats", help="Per-source blob counts and sizes.")
    rp.set_defaults(func=_cmd_raw_stats)
    rp = raw_sub.add_parser("versions", help="Archived versions of a session.")
    rp.add_argument("session_id")
    rp.set_defaults(func=_cmd_raw_versions)
    rp = raw_sub.add_parser("restore", help="Restore a session's raw file.")
    rp.add_argument("session_id")
    rp.add_argument("--out", type=Path, required=True)
    rp.add_argument(
        "--version", type=int, default=-1, help="Version index (default: latest)."
    )
    rp.set_defaults(func=_cmd_raw_restore)
    p.add_argument("--raw-store-url", default=None)

    p = sub.add_parser("retention", help="Show or enforce retention policy.")
    ret_sub = p.add_subparsers(dest="retention_command", required=True)
    rp = ret_sub.add_parser("show", help="Effective policy per source.")
    rp.set_defaults(func=_cmd_retention_show)
    rp = ret_sub.add_parser("run", help="Delete expired raw blobs and sessions.")
    rp.add_argument("--dry-run", action="store_true")
    rp.set_defaults(func=_cmd_retention_run)
    p.add_argument("--raw-store-url", default=None)
    p.add_argument("--config", type=Path, default=None)

    return parser


# -- commands ---------------------------------------------------------------


def _cmd_overview(store, _args) -> dict[str, Any]:
    return store.overview()


def _cmd_sessions(store, args) -> list[dict[str, Any]]:
    return store.list_sessions(
        source=args.source,
        surface=args.surface,
        cwd_like=args.cwd,
        project=args.project,
        since_ns=_since_ns(args.since),
        top_level_only=not args.all,
        limit=args.limit,
    )


def _cmd_commands(store, args) -> list[dict[str, Any]]:
    return store.session_commands(args.session_id)


def _cmd_show(store, args) -> dict[str, Any]:
    session = store.get_session(args.session_id)
    if session is None:
        raise SystemExit(f"session {args.session_id!r} not found")
    return session


def _cmd_thinking(store, args) -> list[dict[str, Any]]:
    return store.get_contents(args.session_id, kinds=["thinking"])


def _cmd_transcript(store, args) -> list[dict[str, Any]]:
    return store.get_contents(args.session_id, kinds=args.kind)


def _cmd_tools(store, args) -> dict[str, Any]:
    return store.tool_stats(
        source=args.source,
        since_ns=_since_ns(args.since),
        slowest=args.top,
        largest=args.top,
    )


def _cmd_tokens(store, args) -> list[dict[str, Any]]:
    return store.token_stats(
        source=args.source,
        since_ns=_since_ns(args.since),
        group_by=args.group_by,
    )


def _cmd_search(store, args) -> list[dict[str, Any]]:
    return store.search(
        args.query,
        kinds=args.kind,
        source=args.source,
        limit=args.limit,
    )


def _cmd_insights(store, args) -> list[dict[str, Any]]:
    if args.stored:
        return store.list_findings(limit=args.limit)
    from flume.analysis.insights import run_insights

    return run_insights(
        store, since_ns=_since_ns(args.since), source=args.source
    )[: args.limit]


def _cmd_hooks(store, args) -> dict[str, Any]:
    from flume.analysis.hooks import hook_events, hooks_summary

    _require_sqlite(store, "hooks")
    events = hook_events(
        store,
        since_ns=_since_ns(args.since),
        session_id=args.session,
        limit=args.limit,
    )
    return {"summary": hooks_summary(events), "events": events}


def _require_sqlite(store, feature: str):

    return require_sql(store, feature)


def _cmd_experiment_start(store, args) -> dict[str, Any]:
    _require_sqlite(store, "experiment")
    row = store.create_experiment(
        args.name,
        hypothesis=args.hypothesis,
        source=args.source,
        project=args.project,
        started_at_ns=_parse_when(args.started),
    )
    row["tagged_sessions"] = len(store.experiment_session_ids(args.name))
    return row


def _cmd_experiment_stop(store, args) -> dict[str, Any]:
    _require_sqlite(store, "experiment")
    row = store.end_experiment(args.name, _parse_when(args.ended))
    row["tagged_sessions"] = len(store.experiment_session_ids(args.name))
    return row


def _cmd_experiment_list(store, _args) -> list[dict[str, Any]]:
    _require_sqlite(store, "experiment")
    return store.list_experiments()


def _cmd_experiment_compare(store, args) -> dict[str, Any]:
    from flume.analysis.experiments import compare_experiment

    _require_sqlite(store, "experiment")
    try:
        return compare_experiment(store, args.name, baseline_days=args.baseline_days)
    except KeyError as exc:
        raise SystemExit(str(exc.args[0])) from None


def _parse_when(value: str | None) -> int | None:
    """None -> now (caller default); '7d' -> lookback; else ISO date/time."""
    if not value:
        return None
    match = re.fullmatch(r"(\d+)([hdw])", value.strip())
    if match:
        return _since_ns(value)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise SystemExit(
            f"bad time {value!r}; use ISO (2026-07-02[T14:00]) or lookback (7d)"
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return int(parsed.timestamp() * 1_000_000_000)




def _cmd_cost(store, args) -> list[dict[str, Any]]:
    # Cost is derived, not stored: price per-turn token splits at the turn's
    # own model rates (or --as-model rates for what-if).

    require_sql(store, "cost")
    since_ns = _since_ns(args.since)
    group_expr = {
        "model": "t.model",
        "day": "date(t.started_at_ns/1000000000, 'unixepoch')",
        "project": "s.project",
        "session": "t.session_id",
    }[args.group_by]
    clauses: list[str] = []
    params: list[Any] = []
    if args.source:
        clauses.append("s.source = ?")
        params.append(args.source)
    if since_ns is not None:
        clauses.append("s.started_at_ns >= ?")
        params.append(since_ns)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = store.rows(
        f"""
        SELECT {group_expr} AS grp, t.model AS model, COUNT(*) turns,
            SUM(t.input_tokens) i, SUM(t.output_tokens) o,
            SUM(t.cache_read_tokens) cr, SUM(t.cache_creation_tokens) cc
        FROM turns t JOIN sessions s USING (session_id)
        {where} GROUP BY {group_expr}, t.model
        """,
        tuple(params),
    )
    from flume.pricing import load_prices

    book = load_prices()
    forced = book.for_model(args.as_model) if args.as_model else None
    if args.as_model and forced is None:
        raise SystemExit(
            f"unpriced model {args.as_model!r}; known: {', '.join(book.known())}. "
            "Add it under [pricing] in ~/.flume/config.toml."
        )
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        price = forced or book.for_model(row["model"])
        bucket = buckets.setdefault(
            row["grp"] or "?",
            {"group": row["grp"], "turns": 0, "usd": 0.0, "usd_reads": 0.0,
             "usd_writes": 0.0, "usd_output": 0.0, "unpriced_turns": 0},
        )
        bucket["turns"] += row["turns"]
        if price is None:
            # Only count turns that actually consumed tokens. Sources record
            # placeholder turns with no usage — Claude Code writes
            # model="<synthetic>" for injected messages — and flagging those
            # as unpriced implies missing rates where there is nothing to
            # price.
            if any(row[k] or 0 for k in ("i", "o", "cr", "cc")):
                bucket["unpriced_turns"] += row["turns"]
            continue
        reads = (row["cr"] or 0) * price.cache_read / 1e6
        writes = (row["cc"] or 0) * price.cache_write / 1e6
        output = (row["o"] or 0) * price.output / 1e6
        raw = (row["i"] or 0) * price.input / 1e6
        bucket["usd_reads"] += reads
        bucket["usd_writes"] += writes
        bucket["usd_output"] += output
        bucket["usd"] += reads + writes + output + raw
    out = sorted(buckets.values(), key=lambda b: -b["usd"])
    for bucket in out:
        for key in ("usd", "usd_reads", "usd_writes", "usd_output"):
            bucket[key] = round(bucket[key], 2)
    return out


def _cmd_audit_repeats(store, args) -> list[dict[str, Any]]:
    return store.audit_repeats(
        source=args.source, since_ns=_since_ns(args.since), limit=args.limit
    )


def _cmd_audit_bigreads(store, args) -> list[dict[str, Any]]:
    return store.audit_whole_file_reads(
        source=args.source,
        since_ns=_since_ns(args.since),
        min_chars=args.min_chars,
        limit=args.limit,
    )


def _cmd_audit_toolgaps(store, args) -> list[dict[str, Any]]:
    from flume.analysis.audit import script_clusters

    return script_clusters(
        store, since_ns=_since_ns(args.since), min_sessions=args.min_sessions
    )


def _cmd_ingest(store, args) -> dict[str, Any]:
    from flume.ingest.write import ingest_path
    from flume.sources import get_adapter
    from flume.store.raw import open_raw_store

    adapter = get_adapter(args.source)
    source = adapter.name
    files = _expand(args.path, source)
    raw_store = None if args.no_raw_store else open_raw_store(args.raw_store_url)
    try:
        ingested, empty, failures = [], 0, []
        for f in files:
            try:
                outcome = ingest_path(store, adapter, f, raw_store=raw_store)
            except Exception as exc:  # noqa: BLE001 - keep batch going
                failures.append({"path": str(f), "error": f"{type(exc).__name__}: {exc}"})
                continue
            if outcome is None:
                empty += 1
            else:
                ingested.append(outcome.session_id)
    finally:
        if raw_store is not None:
            raw_store.close()
    return {
        "source": source,
        "files": len(files),
        "ingested": len(ingested),
        "empty": empty,
        "failed": failures,
        "raw_archived": not args.no_raw_store,
    }


def _cmd_rebuild(store, args) -> dict[str, Any]:
    from flume.ingest.write import rebuild_stale
    from flume.store.raw import open_raw_store

    with open_raw_store(args.raw_store_url) as raw_store:
        return rebuild_stale(
            store,
            raw_store,
            source=args.source,
            limit=args.limit,
            dry_run=args.dry_run,
        )


def _cmd_sql(_store, args) -> list[dict[str, Any]]:
    import sqlite3


    query = args.query.strip().rstrip(";")
    head = query.split(None, 1)[0].lower() if query else ""
    if head not in ("select", "with", "pragma", "explain"):
        raise SystemExit(
            "sql: only read-only SELECT/WITH/PRAGMA/EXPLAIN queries are allowed"
        )
    url = args.analyzed_store_url or DEFAULT_ANALYZED_STORE_URL
    path = url[len("sqlite://"):] if url.startswith("sqlite://") else url
    path = str(Path(path).expanduser())
    # main() already opened the store read-write (creating the view); this
    # separate read-only connection guarantees the query can't mutate.
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(r) for r in conn.execute(query).fetchall()]
    except sqlite3.Error as exc:
        raise SystemExit(f"sql error: {exc}") from None
    finally:
        conn.close()
    return rows[: args.limit]


def _cmd_review(store, args) -> dict[str, Any]:
    """CLI flags win; anything unset falls back to `[review]` in config."""
    from flume.analysis.review import load_review_config, run_review

    settings = load_review_config()

    def pick(flag, key, cast):
        return cast(settings[key]) if flag is None else flag

    return run_review(
        store,
        since_ns=_since_ns(pick(args.since, "since", str)),
        severity_max=pick(args.severity_max, "severity_max", int),
        growth=pick(args.growth, "growth", float),
        baseline_days=pick(args.baseline_days, "baseline_days", int),
        min_sessions=pick(args.min_sessions, "min_sessions", int),
        source=args.source,
    )


def _cmd_sources(_store, _args) -> list[dict[str, Any]]:
    from flume.sources import registered

    return [{"source": a.name} for a in registered()]


def _cmd_raw_stats(_store, args) -> list[dict[str, Any]]:
    from flume.store.raw import open_raw_store

    with open_raw_store(args.raw_store_url) as raw_store:
        return raw_store.stats()


def _cmd_raw_versions(_store, args) -> list[dict[str, Any]]:

    from flume.store.raw import open_raw_store

    with open_raw_store(args.raw_store_url) as raw_store:
        return [asdict(v) for v in raw_store.versions(args.session_id)]


def _cmd_raw_restore(_store, args) -> dict[str, Any]:
    from flume.store.raw import open_raw_store

    with open_raw_store(args.raw_store_url) as raw_store:
        versions = raw_store.versions(args.session_id)
        if not versions:
            raise SystemExit(f"no archived raw data for {args.session_id!r}")
        entry = versions[args.version]
        out = raw_store.restore(entry, args.out.expanduser())
    return {"restored": str(out), "sha256": entry.sha256, "bytes": entry.size_bytes}


def _cmd_retention_show(_store, args) -> dict[str, Any]:
    from flume.store.config import load_policy

    return load_policy(args.config).describe()


def _cmd_retention_run(store, args) -> dict[str, Any]:
    from flume.store.raw import open_raw_store
    from flume.store.config import load_policy
    from flume.store.retention import run_retention

    from flume.sources import registered

    with open_raw_store(args.raw_store_url) as raw_store:
        return run_retention(
            store=store,
            raw_store=raw_store,
            policy=load_policy(args.config),
            sources=[a.name for a in registered()],
            dry_run=args.dry_run,
        )


def _expand(path: Path, source: str) -> list[Path]:
    path = path.expanduser()
    if path.is_file():
        return [path]
    if path.is_dir():
        pattern = "**/rollout-*.jsonl" if source == "codex" else "**/*.jsonl"
        return sorted(path.glob(pattern))
    return []


# -- rendering ----------------------------------------------------------------


def _since_ns(window: str | None) -> int | None:
    if not window:
        return None
    from flume.store.config import parse_duration_ns

    try:
        ttl_ns = parse_duration_ns(window)
    except ValueError:
        raise SystemExit(
            f"bad --since {window!r}; use forms like 24h, 7d, 2w"
        ) from None
    if ttl_ns is None:
        return None
    return time.time_ns() - ttl_ns


def _render(command: str | None, result: Any) -> None:
    if command == "overview":
        totals = result["totals"] or {}
        print("== totals ==")
        for key, value in totals.items():
            print(f"  {key:24} {_fmt(key, value)}")
        print("== by source ==")
        _table(result["by_source"])
        return
    if command == "sessions":
        rows = [
            {
                "started": _ts(r["started_at_ns"]),
                "project": r.get("project") or "-",
                "source": r["source"] + ("*" if r.get("is_subagent") else ""),
                "session_id": r["session_id"],
                "kids": r.get("children") or 0,
                "turns": r["turn_count"],
                "tools": r["tool_call_count"],
                "out_tok": r["output_tokens"],
                "wall": _dur(r["wall_ms"]),
                "first_message": (r["first_user_message"] or "")[:60],
            }
            for r in result
        ]
        _table(rows)
        return
    if command == "commands":
        _table(
            [
                {
                    "asked": _ts(r["started_at_ns"]),
                    "took": _dur(r["duration_ms"]),
                    "turns": r["turns"],
                    "tools": r["tool_calls"],
                    "out_tok": r["output_tokens"],
                    "cache_read": r["cache_read_tokens"],
                    "prompt": r["prompt"][:70],
                }
                for r in result
            ]
        )
        return
    if command == "show":
        session = {k: v for k, v in result.items() if k not in ("turns", "tool_calls")}
        for key, value in session.items():
            print(f"  {key:24} {_fmt(key, value)}")
        print(f"== turns ({len(result['turns'])}) ==")
        _table(
            [
                {
                    "idx": t["turn_index"],
                    "start": _ts(t["started_at_ns"]),
                    "dur": _dur(t["duration_ms"]),
                    "in": t["input_tokens"],
                    "out": t["output_tokens"],
                    "cache_read": t["cache_read_tokens"],
                    "cache_new": t["cache_creation_tokens"],
                    "think_chars": t["thinking_chars"],
                }
                for t in result["turns"]
            ]
        )
        print(f"== tool calls ({len(result['tool_calls'])}) ==")
        _table(
            [
                {
                    "start": _ts(t["started_at_ns"]),
                    "tool": t["name"],
                    "dur": _dur(t["duration_ms"]),
                    "err": "x" if t["is_error"] else "",
                    "result_chars": t["result_chars"],
                    "args": (t["args_preview"] or "")[:60],
                }
                for t in result["tool_calls"]
            ]
        )
        return
    if command in ("thinking", "transcript"):
        for row in result:
            stamp = _ts(row["ts_ns"]) if row.get("ts_ns") else "?"
            print(f"--- [{row['kind']}] {stamp} (span {row['span_id'] or '-'}) ---")
            print(row["text"])
            print()
        if not result:
            print("(no content rows)")
        return
    if command == "tools":
        print("== per tool ==")
        _table(result["per_tool"])
        print("== repeated calls (same tool + same args in one session) ==")
        _table(result["repeated_calls"])
        print("== slowest ==")
        _table(result["slowest"])
        print("== largest results ==")
        _table(result["largest_results"])
        return
    if command == "tokens":
        _table(result)
        return
    if command == "sql":
        _table(result)
        return
    if command == "insights":
        sev_label = {1: "ACT NOW", 2: "WORTH FIXING", 3: "WATCH"}
        current = None
        for f in result:
            if f["severity"] != current:
                current = f["severity"]
                print(f"\n== {sev_label.get(current, current)} ==")
            trend = (
                f"  (seen {f['occurrences']}x since "
                f"{_ts(f['first_seen_ns'])[:10]})"
                if f.get("occurrences", 1) > 1 else ""
            )
            print(f"\n  [{f['kind']}] {f['title']}{trend}")
            if f.get("detail"):
                print(f"      {f['detail']}")
            if f.get("action"):
                print(f"      -> {f['action']}")
        if not result:
            print("(no findings — corpus is clean for this window)")
        return
    if command == "hooks":
        print("== per hook ==")
        _table(
            [
                {
                    "hook": s["hook"],
                    "events": s["events"],
                    "sessions": s["sessions"],
                    "heeded": s["heeded"] or "-",
                    "bypassed": s["bypassed"] or "-",
                    "first": _ts(s["first_ns"]),
                    "last": _ts(s["last_ns"]),
                }
                for s in result["summary"]
            ]
        )
        print("== recent events ==")
        _table(
            [
                {
                    "when": _ts(e["ts_ns"]),
                    "hook": e["hook"],
                    "on": f"{e['event']}:{e['matcher']}",
                    "outcome": e["outcome"] or "-",
                    "project": e["project"] or "-",
                    "message": e["message"][:60],
                }
                for e in result["events"][:20]
            ]
        )
        if not result["events"]:
            print("(no hook interventions recorded in this window)")
        return
    if command == "experiment":
        if isinstance(result, list):  # list
            _table(
                [
                    {
                        "name": r["name"],
                        "status": "active" if r["ended_at_ns"] is None else "ended",
                        "started": _ts(r["started_at_ns"]),
                        "ended": _ts(r["ended_at_ns"]),
                        "source": r["source"] or "-",
                        "project": r["project"] or "-",
                        "sessions": r["sessions"],
                        "hypothesis": (r["hypothesis"] or "")[:50],
                    }
                    for r in result
                ]
            )
            return
        if "groups" in result:  # compare
            exp = result["experiment"]
            span = f"{_ts(exp['started_at_ns'])} -> "
            span += _ts(exp["ended_at_ns"]) if exp["ended_at_ns"] else "(active)"
            print(f"  experiment {exp['name']}  [{span}]")
            if exp.get("hypothesis"):
                print(f"  hypothesis: {exp['hypothesis']}")
            print(f"  baseline: {result['baseline_days']}d before start\n")
            _table(result["groups"])
            groups = {g["group"]: g for g in result["groups"]}
            base, test = groups.get("baseline", {}), groups.get("experiment", {})
            if base.get("nav_share_median") and test.get("nav_share_median"):
                delta = test["nav_share_median"] - base["nav_share_median"]
                print(
                    f"\n  nav share: {base['nav_share_median']:.1%} -> "
                    f"{test['nav_share_median']:.1%} ({delta:+.1%})"
                )
            if min(base.get("measured") or 0, test.get("measured") or 0) < 10:
                print("  (small n — directional only, not significant)")
            return
        for key, value in result.items():  # start / stop
            print(f"  {key:24} {_fmt(key, value)}")
        return
    if command == "cost":
        _table(
            [
                {
                    "group": r["group"],
                    "turns": r["turns"],
                    "$total": f"${r['usd']:,.2f}",
                    "$reads": f"${r['usd_reads']:,.2f}",
                    "$writes": f"${r['usd_writes']:,.2f}",
                    "$output": f"${r['usd_output']:,.2f}",
                    "unpriced": r["unpriced_turns"] or "",
                }
                for r in result
            ]
        )
        total = sum(r["usd"] for r in result)
        print(f"\n  TOTAL: ${total:,.2f}")
        return
    if command == "search":
        for row in result:
            print(
                f"[{row['kind']}] {row['session_id']} "
                f"({row['source']}, {row.get('cwd') or '?'})"
            )
            print(f"    {row['snippet']}")
        if not result:
            print("(no matches)")
        return
    json.dump(result, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def _table(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("  (none)")
        return
    cols = list(rows[0].keys())
    widths = {
        c: max(len(str(c)), *(len(_cell(r.get(c))) for r in rows)) for c in cols
    }
    print("  " + "  ".join(str(c).ljust(widths[c]) for c in cols))
    for row in rows:
        print("  " + "  ".join(_cell(row.get(c)).ljust(widths[c]) for c in cols))


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value).replace("\n", " ")


def _fmt(key: str, value: Any) -> str:
    if value is None:
        return "-"
    if key.endswith("_at_ns") or key.endswith("_ns"):
        return _ts(value)
    if key.endswith("_ms"):
        return _dur(value)
    return str(value)


def _ts(ns: int | None) -> str:
    if not ns:
        return "-"
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _dur(ms: int | None) -> str:
    if ms is None:
        return "-"
    if ms < 1000:
        return f"{ms}ms"
    seconds = ms / 1000
    if seconds < 90:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 90:
        return f"{minutes:.1f}m"
    return f"{minutes / 60:.1f}h"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
