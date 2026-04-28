"""Parse Claude Code JSONL transcripts into OTel-shaped span dicts.

Pure data — no OTel SDK, no network. Each transcript maps to one trace with a
`claude_code.interaction` root, per-turn `claude_code.llm_request` children,
and per-tool-call `claude_code.tool` spans nested under the turn that issued
them. Span IDs are derived from session_id + event identity, so replays are
idempotent.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from agent_telemetry.backfill.langfuse import enrich_trace_attrs

Span = dict[str, Any]

# Match the 60 KB cap the native Claude Code OTel export applies with
# OTEL_LOG_TOOL_CONTENT=1, so backfill and live paths stay comparable.
_TOOL_PAYLOAD_MAX = 60_000


def jsonl_to_spans(path: Path) -> list[Span]:
    """Map one Claude Code JSONL transcript to a list of span dicts."""
    session_id = path.stem
    events = _read_events(path)
    trace_id = _trace_id(session_id)
    root_span_id = _span_id(session_id, "root")

    spans: list[Span] = []
    first_ns: int | None = None
    last_ns: int | None = None
    entrypoint: str | None = None
    version: str | None = None
    git_branch: str | None = None
    tool_pending: dict[str, dict[str, Any]] = {}
    # Track the last-emitted assistant turn span so a `turn_duration` event
    # that follows a cluster of assistant events can retro-attribute its ms
    # to that turn. The native Claude Code logger emits turn_duration AFTER
    # the assistant turn it measures (messageCount references the trailing
    # event count), so forward-attribution (the prior mapper behavior) loses
    # the final turn's duration when the session ends on a user/system tail.
    last_turn: Span | None = None

    def _apply_turn_duration(duration_ms: int) -> None:
        if duration_ms <= 0 or last_turn is None:
            return
        last_turn["attributes"]["claude_code.duration_ms"] = duration_ms
        # Preserve end_unix_nano; shift start backwards by duration.
        last_turn["start_unix_nano"] = (
            last_turn["end_unix_nano"] - duration_ms * 1_000_000
        )

    for ev in events:
        ts_ns = _ts_ns(ev.get("timestamp"))
        if ts_ns is not None:
            if first_ns is None:
                first_ns = ts_ns
            last_ns = ts_ns
        entrypoint = entrypoint or ev.get("entrypoint")
        version = version or ev.get("version")
        git_branch = git_branch or ev.get("gitBranch")

        t = ev.get("type")
        if t == "system" and ev.get("subtype") == "turn_duration":
            _apply_turn_duration(int(ev.get("durationMs") or 0))
            continue
        if t == "assistant" and ts_ns is not None:
            turn = _turn_span(ev, ts_ns, session_id, trace_id, root_span_id, 0)
            spans.append(turn)
            last_turn = turn
            _register_tool_uses(ev, ts_ns, turn["span_id"], tool_pending)
            continue
        if t == "user" and ts_ns is not None:
            spans.extend(
                _drain_tool_results(ev, ts_ns, session_id, trace_id, tool_pending)
            )

    if first_ns is None:
        return []

    root: Span = {
        "name": "claude_code.interaction",
        "trace_id": trace_id,
        "span_id": root_span_id,
        "parent_span_id": None,
        "start_unix_nano": first_ns,
        "end_unix_nano": last_ns,
        "attributes": {
            "source": "claude-code",
            "session.id": session_id,
            "entrypoint": entrypoint,
            "claude_code.version": version,
            "git.branch": git_branch,
        },
        "status": "OK",
    }
    return enrich_trace_attrs(
        [root, *spans],
        agent_source="claude-code",
        agent_family="claude-code",
    )


def _read_events(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def _ts_ns(ts: str | None) -> int | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    # Integer microsecond path — float `dt.timestamp() * 1e9` drops sub-ms
    # precision (e.g. .827 → .826999808), which then round-trips through
    # ClickHouse as a visible 1 ms drift. Using Unix epoch seconds as an int
    # plus the microsecond remainder keeps the value exact to the microsecond.
    from datetime import timezone
    epoch_utc = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt - epoch_utc
    return delta.days * 86_400_000_000_000 + delta.seconds * 1_000_000_000 + delta.microseconds * 1_000


def _trace_id(session_id: str) -> str:
    return hashlib.sha256(f"claude-code:{session_id}".encode()).hexdigest()[:32]


def _span_id(session_id: str, suffix: str) -> str:
    return hashlib.sha256(
        f"claude-code:{session_id}:{suffix}".encode()
    ).hexdigest()[:16]


def _turn_span(
    ev: dict[str, Any],
    ts_ns: int,
    session_id: str,
    trace_id: str,
    root_span_id: str,
    duration_ms: int,
) -> Span:
    msg = ev.get("message") or {}
    usage = msg.get("usage") or {}
    thinking_chars = 0
    text_chars = 0
    content = msg.get("content")
    if isinstance(content, list):
        for blk in content:
            if not isinstance(blk, dict):
                continue
            bt = blk.get("type")
            if bt == "thinking":
                thinking_chars += len(blk.get("thinking") or "")
            elif bt == "text":
                text_chars += len(blk.get("text") or "")
    turn_uuid = ev.get("uuid") or str(ts_ns)
    start_ns = ts_ns - duration_ms * 1_000_000
    return {
        "name": "claude_code.llm_request",
        "trace_id": trace_id,
        "span_id": _span_id(session_id, f"turn:{turn_uuid}"),
        "parent_span_id": root_span_id,
        "start_unix_nano": start_ns,
        "end_unix_nano": ts_ns,
        "attributes": {
            "gen_ai.system": "anthropic",
            "gen_ai.request.model": msg.get("model"),
            "gen_ai.response.id": msg.get("id"),
            "gen_ai.usage.input_tokens": usage.get("input_tokens") or 0,
            "gen_ai.usage.output_tokens": usage.get("output_tokens") or 0,
            "gen_ai.usage.cache_read_input_tokens": usage.get("cache_read_input_tokens") or 0,
            "gen_ai.usage.cache_creation_input_tokens": usage.get("cache_creation_input_tokens") or 0,
            "claude_code.thinking_chars": thinking_chars,
            "claude_code.text_chars": text_chars,
            "claude_code.duration_ms": duration_ms,
        },
        "status": "OK",
    }


def _register_tool_uses(
    ev: dict[str, Any],
    ts_ns: int,
    turn_span_id: str,
    tool_pending: dict[str, dict[str, Any]],
) -> None:
    content = (ev.get("message") or {}).get("content")
    if not isinstance(content, list):
        return
    for blk in content:
        if not isinstance(blk, dict) or blk.get("type") != "tool_use":
            continue
        tid = blk.get("id")
        if not tid:
            continue
        tool_pending[tid] = {
            "start_ns": ts_ns,
            "name": blk.get("name") or "?",
            "input": blk.get("input") or {},
            "parent_span_id": turn_span_id,
        }


def _drain_tool_results(
    ev: dict[str, Any],
    ts_ns: int,
    session_id: str,
    trace_id: str,
    tool_pending: dict[str, dict[str, Any]],
) -> Iterator[Span]:
    content = (ev.get("message") or {}).get("content")
    if not isinstance(content, list):
        return
    for blk in content:
        if not isinstance(blk, dict) or blk.get("type") != "tool_result":
            continue
        tid = blk.get("tool_use_id")
        if not isinstance(tid, str):
            continue
        pending = tool_pending.pop(tid, None)
        if not pending:
            continue
        clen = _result_chars(blk.get("content"))
        is_error = bool(blk.get("is_error"))
        yield {
            "name": "claude_code.tool",
            "trace_id": trace_id,
            "span_id": _span_id(session_id, f"tool:{tid}"),
            "parent_span_id": pending["parent_span_id"],
            "start_unix_nano": pending["start_ns"],
            "end_unix_nano": ts_ns,
            "attributes": {
                "tool.name": pending["name"],
                "tool.arguments": json.dumps(pending["input"])[:_TOOL_PAYLOAD_MAX],
                "tool.duration_ms": (ts_ns - pending["start_ns"]) // 1_000_000,
                "tool.is_error": is_error,
                "tool.result_chars": clen,
            },
            "status": "ERROR" if is_error else "OK",
        }


def _result_chars(content: Any) -> int:
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        return sum(
            len(str(x.get("text") or "")) for x in content if isinstance(x, dict)
        )
    return 0


def _cli(argv: list[str] | None = None) -> int:
    """Entry point: `python -m agent_telemetry.backfill.claude_code`.

    `--dry-run` prints the span-dicts as JSON to stdout (one document, pretty,
    stable key order) and does not import any OTel SDK machinery. Otherwise the
    spans are exported via OTLP-HTTP to `--endpoint`.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="python -m agent_telemetry.backfill.claude_code",
        description="Replay a Claude Code JSONL transcript as OTel spans.",
    )
    parser.add_argument(
        "--path",
        type=Path,
        required=True,
        help="Path to a .jsonl file, or a directory to glob *.jsonl from.",
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:4318/v1/traces",
        help="OTLP-HTTP traces endpoint (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dump span-dicts as JSON to stdout instead of exporting.",
    )
    args = parser.parse_args(argv)

    files = _expand_path(args.path)
    if not files:
        print(f"no .jsonl files found at {args.path}", file=sys.stderr)
        return 2

    all_spans: list[Span] = []
    for f in files:
        all_spans.extend(jsonl_to_spans(f))

    if args.dry_run:
        json.dump(all_spans, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0

    from agent_telemetry.backfill.otlp import export_spans_via_otlp
    export_spans_via_otlp(all_spans, args.endpoint, source="claude-code")
    return 0


def _expand_path(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(path.glob("*.jsonl"))
    if path.is_file():
        return [path]
    return []


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli())
