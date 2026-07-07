"""Analysis CLI over the session store.

Designed for two readers: humans get aligned tables, agents get `--json`.
Every subcommand is a thin call into SessionStore query methods, so any
backend that implements the interface gets the whole CLI for free.

Examples:
    agent-telemetry-analyze overview
    agent-telemetry-analyze sessions --source claude-code --limit 20
    agent-telemetry-analyze show <session-id>
    agent-telemetry-analyze thinking <session-id>
    agent-telemetry-analyze tools --since 7d
    agent-telemetry-analyze tokens --group-by model
    agent-telemetry-analyze search "navigat* codebase" --kind thinking
    agent-telemetry-analyze ingest --source claude-code --path ~/.claude/projects
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from agent_telemetry.store.base import CONTENT_KINDS, open_store


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 2
    with open_store(args.store_url) as store:
        result = args.func(store, args)
    if args.json:
        json.dump(result, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    else:
        _render(args.command, result)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-telemetry-analyze",
        description="Query the local agent session store.",
    )
    parser.add_argument(
        "--store-url",
        default=None,
        help="Store URL (default: sqlite://~/.agent-telemetry/store.sqlite3).",
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
    p.add_argument("--project", help="Exact project label (e.g. tools/agent-telemetry).")
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
        "--stored",
        action="store_true",
        help="List previously stored findings instead of re-running detectors.",
    )
    p.add_argument("--limit", type=int, default=25)
    p.set_defaults(func=_cmd_insights)

    p = sub.add_parser("cost", help="Dollar cost of Claude usage (cache-aware).")
    p.add_argument("--since", help="Window like 24h, 7d, 30d.")
    p.add_argument(
        "--group-by", default="model", choices=("model", "day", "project", "session")
    )
    p.add_argument(
        "--as-model",
        default=None,
        help="What-if: reprice ALL usage at this model's rates (e.g. claude-fable-5).",
    )
    p.set_defaults(func=_cmd_cost)

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
        help="Source name (claude-code, codex) or vendor alias (anthropic, openai).",
    )
    p.add_argument("--path", type=Path, required=True, help="File or directory.")
    p.add_argument(
        "--no-raw-archive",
        action="store_true",
        help="Skip capturing raw file copies into the archive.",
    )
    p.add_argument("--archive-url", default=None)
    p.set_defaults(func=_cmd_ingest)

    p = sub.add_parser("sources", help="List registered source adapters.")
    p.set_defaults(func=_cmd_sources)

    p = sub.add_parser("raw", help="Raw archive: stats, versions, restore.")
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
    for rp_parser in (p,):
        rp_parser.add_argument("--archive-url", default=None)

    p = sub.add_parser("retention", help="Show or enforce retention policy.")
    ret_sub = p.add_subparsers(dest="retention_command", required=True)
    rp = ret_sub.add_parser("show", help="Effective policy per source.")
    rp.set_defaults(func=_cmd_retention_show)
    rp = ret_sub.add_parser("run", help="Delete expired raw blobs and sessions.")
    rp.add_argument("--dry-run", action="store_true")
    rp.set_defaults(func=_cmd_retention_run)
    for rp_parser in (p,):
        rp_parser.add_argument("--archive-url", default=None)
        rp_parser.add_argument("--config", type=Path, default=None)

    return parser


# -- commands ---------------------------------------------------------------


def _cmd_overview(store, args) -> dict[str, Any]:
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
    from agent_telemetry.store.insights import run_insights

    return run_insights(store, since_ns=_since_ns(args.since))[: args.limit]


# $/MTok (input, output). Cache read = 0.1x input; cache write = 1.25x input
# (5-minute TTL). Prices as of 2026-07 — update when Anthropic pricing changes.
# Sonnet 5 intro pricing ($2/$10) runs through 2026-08-31; sticker is $3/$15.
_PRICES = {
    "claude-fable-5": (10.0, 50.0),
    "claude-mythos-5": (10.0, 50.0),
    "claude-opus-4-8": (5.0, 25.0),
    "claude-opus-4-7": (5.0, 25.0),
    "claude-opus-4-6": (5.0, 25.0),
    "claude-opus-4-5": (5.0, 25.0),
    "claude-sonnet-5": (2.0, 10.0),  # intro through 2026-08-31
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
}


def _price_for(model: str | None) -> tuple[float, float] | None:
    if not model:
        return None
    for key, price in _PRICES.items():
        if model.startswith(key):
            return price
    return None


def _cmd_cost(store, args) -> list[dict[str, Any]]:
    # Cost is derived, not stored: price per-turn token splits at the turn's
    # own model rates (or --as-model rates for what-if).
    session = getattr(store, "_all", None)
    if session is None:
        raise SystemExit("cost requires the sqlite backend")
    since_ns = _since_ns(args.since)
    group_expr = {
        "model": "t.model",
        "day": "date(t.started_at_ns/1000000000, 'unixepoch')",
        "project": "s.project",
        "session": "t.session_id",
    }[args.group_by]
    where = "WHERE s.source = 'claude-code'"
    params: list[Any] = []
    if since_ns is not None:
        where += " AND s.started_at_ns >= ?"
        params.append(since_ns)
    rows = store._all(
        f"""
        SELECT {group_expr} AS grp, t.model AS model, COUNT(*) turns,
            SUM(t.input_tokens) i, SUM(t.output_tokens) o,
            SUM(t.cache_read_tokens) cr, SUM(t.cache_creation_tokens) cc
        FROM turns t JOIN sessions s USING (session_id)
        {where} GROUP BY {group_expr}, t.model
        """,
        tuple(params),
    )
    forced = _price_for(args.as_model) if args.as_model else None
    if args.as_model and forced is None:
        raise SystemExit(f"unknown model {args.as_model!r}; known: {list(_PRICES)}")
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        price = forced or _price_for(row["model"])
        bucket = buckets.setdefault(
            row["grp"] or "?",
            {"group": row["grp"], "turns": 0, "usd": 0.0, "usd_reads": 0.0,
             "usd_writes": 0.0, "usd_output": 0.0, "unpriced_turns": 0},
        )
        bucket["turns"] += row["turns"]
        if price is None:
            bucket["unpriced_turns"] += row["turns"]
            continue
        p_in, p_out = price
        reads = (row["cr"] or 0) * p_in * 0.1 / 1e6
        writes = (row["cc"] or 0) * p_in * 1.25 / 1e6
        output = (row["o"] or 0) * p_out / 1e6
        raw = (row["i"] or 0) * p_in / 1e6
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
    from agent_telemetry.store.audit import script_clusters

    return script_clusters(
        store, since_ns=_since_ns(args.since), min_sessions=args.min_sessions
    )


def _cmd_ingest(store, args) -> dict[str, Any]:
    from agent_telemetry.store.archive import open_archive
    from agent_telemetry.store.ingest import ingest_path
    from agent_telemetry.store.registry import get_adapter

    source = get_adapter(args.source).name
    files = _expand(args.path, source)
    archive = None if args.no_raw_archive else open_archive(args.archive_url)
    try:
        ingested, empty, failures = [], 0, []
        for f in files:
            try:
                outcome = ingest_path(store, source, f, archive=archive)
            except Exception as exc:  # noqa: BLE001 - keep batch going
                failures.append({"path": str(f), "error": f"{type(exc).__name__}: {exc}"})
                continue
            if outcome is None:
                empty += 1
            else:
                ingested.append(outcome.session_id)
    finally:
        if archive is not None:
            archive.close()
    return {
        "source": source,
        "files": len(files),
        "ingested": len(ingested),
        "empty": empty,
        "failed": failures,
        "raw_archived": not args.no_raw_archive,
    }


def _cmd_sources(store, args) -> list[dict[str, Any]]:
    from agent_telemetry.store.registry import adapters

    return [{"source": a.name, "vendor": a.vendor} for a in adapters()]


def _cmd_raw_stats(store, args) -> list[dict[str, Any]]:
    from agent_telemetry.store.archive import open_archive

    with open_archive(args.archive_url) as archive:
        return archive.stats()


def _cmd_raw_versions(store, args) -> list[dict[str, Any]]:
    from dataclasses import asdict

    from agent_telemetry.store.archive import open_archive

    with open_archive(args.archive_url) as archive:
        return [asdict(v) for v in archive.versions(args.session_id)]


def _cmd_raw_restore(store, args) -> dict[str, Any]:
    from agent_telemetry.store.archive import open_archive

    with open_archive(args.archive_url) as archive:
        versions = archive.versions(args.session_id)
        if not versions:
            raise SystemExit(f"no archived raw data for {args.session_id!r}")
        entry = versions[args.version]
        out = archive.restore(entry, args.out.expanduser())
    return {"restored": str(out), "sha256": entry.sha256, "bytes": entry.size_bytes}


def _cmd_retention_show(store, args) -> dict[str, Any]:
    from agent_telemetry.store.config import load_policy

    return load_policy(args.config).describe()


def _cmd_retention_run(store, args) -> dict[str, Any]:
    from agent_telemetry.store.archive import open_archive
    from agent_telemetry.store.config import load_policy
    from agent_telemetry.store.retention import run_retention

    with open_archive(args.archive_url) as archive:
        return run_retention(
            store=store,
            archive=archive,
            policy=load_policy(args.config),
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
    match = re.fullmatch(r"(\d+)([hdw])", window.strip())
    if not match:
        raise SystemExit(f"bad --since {window!r}; use forms like 24h, 7d, 2w")
    value, unit = int(match.group(1)), match.group(2)
    seconds = value * {"h": 3600, "d": 86400, "w": 604800}[unit]
    return (int(time.time()) - seconds) * 1_000_000_000


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
