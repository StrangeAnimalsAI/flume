"""Harness: everything flume knows about its own agent transcript format.

Maps the harness JSONL format (see flume/harness/transcript.py, which
defines and writes it) to the shared span vocabulary
(`harness.interaction` / `.llm_request` / `.tool`) and extracts
full-fidelity content rows — including the thinking summaries that are the
whole point of the harness. Span ids are deterministic from session id +
event identity, matching the claude-code/codex convention. No discovery:
the harness is a push source — it ingests its own transcript when a run
ends.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from flume.sources import SourceAdapter
from flume.sources.utils import is_nav_shell, iso_ts_ns, iter_jsonl_lines
from flume.store.base import ContentKind, ContentRow

Span = dict[str, Any]


def _span_id(session_id: str, suffix: str) -> str:
    return hashlib.sha256(f"harness:{session_id}:{suffix}".encode()).hexdigest()[:16]


def _trace_id(session_id: str) -> str:
    return hashlib.sha256(f"harness:{session_id}".encode()).hexdigest()[:32]


def _read_events(path: Path) -> list[dict[str, Any]]:
    events = []
    for line in iter_jsonl_lines(path):
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def _meta(events: list[dict[str, Any]], path: Path) -> tuple[dict[str, Any], str]:
    meta = next((e for e in events if e.get("type") == "session_meta"), {})
    return meta, meta.get("session_id") or path.stem


def harness_to_spans(path: Path) -> list[Span]:
    events = _read_events(path)
    if not events:
        return []
    meta, session_id = _meta(events, path)
    trace_id = _trace_id(session_id)
    stamps = [t for t in (iso_ts_ns(e.get("ts")) for e in events) if t is not None]
    if not stamps:
        return []
    started, ended = min(stamps), max(stamps)

    root_id = _span_id(session_id, "root")
    root: Span = {
        "trace_id": trace_id,
        "span_id": root_id,
        "name": "harness.interaction",
        "start_unix_nano": started,
        "end_unix_nano": ended,
        "attributes": {
            "session.id": session_id,
            "source": "harness",
            "entrypoint": "harness",
            "gen_ai.request.model": meta.get("model"),
            "git.branch": meta.get("git_branch"),
        },
    }
    spans = [root]

    results_by_id = {
        e.get("tool_use_id"): e for e in events if e.get("type") == "tool_result"
    }
    turn_index = 0
    for event in events:
        if event.get("type") != "assistant":
            continue
        end = iso_ts_ns(event.get("ts"))
        if end is None:
            continue
        duration_ms = int(event.get("duration_ms") or 0)
        start = end - duration_ms * 1_000_000 if duration_ms else end
        usage = event.get("usage") or {}
        content = event.get("content") or []
        turn_id = _span_id(session_id, f"turn:{turn_index}")
        spans.append({
            "trace_id": trace_id,
            "span_id": turn_id,
            "parent_span_id": root_id,
            "name": "harness.llm_request",
            "start_unix_nano": start,
            "end_unix_nano": end,
            "attributes": {
                "gen_ai.request.model": event.get("model") or meta.get("model"),
                "gen_ai.usage.input_tokens": usage.get("input_tokens") or 0,
                "gen_ai.usage.output_tokens": usage.get("output_tokens") or 0,
                "gen_ai.usage.cache_read_input_tokens":
                    usage.get("cache_read_input_tokens") or 0,
                "gen_ai.usage.cache_creation_input_tokens":
                    usage.get("cache_creation_input_tokens") or 0,
                "turn.duration_ms": duration_ms,
                "turn.thinking_chars": sum(
                    len(b.get("thinking") or "")
                    for b in content if b.get("type") == "thinking"
                ),
                "turn.text_chars": sum(
                    len(b.get("text") or "")
                    for b in content if b.get("type") == "text"
                ),
            },
        })
        for block in content:
            if block.get("type") != "tool_use":
                continue
            tool_use_id = block.get("id") or ""
            result = results_by_id.get(tool_use_id, {})
            tool_end = iso_ts_ns(result.get("ts")) or end
            arguments = json.dumps(block.get("input") or {}, sort_keys=True)
            spans.append({
                "trace_id": trace_id,
                "span_id": _span_id(session_id, f"tool:{tool_use_id}"),
                "parent_span_id": turn_id,
                "name": "harness.tool",
                "start_unix_nano": end,
                "end_unix_nano": tool_end,
                "attributes": {
                    "tool.name": block.get("name") or "?",
                    "tool.arguments": arguments,
                    "tool.duration_ms": int(result.get("duration_ms") or 0),
                    "tool.is_error": bool(result.get("is_error")),
                    "tool.result_chars": len(result.get("output") or ""),
                },
            })
        turn_index += 1

    # SDK backend: per-turn usage is unavailable on the stream; the run's
    # totals ride on the end event. Attribute them to the last turn so
    # session rollups stay correct.
    turn_spans = [s for s in spans if s["name"] == "harness.llm_request"]
    end_usage = next(
        (e.get("usage") for e in events if e.get("type") == "end" and e.get("usage")),
        None,
    )
    if turn_spans and end_usage:
        keys = (
            "input_tokens", "output_tokens",
            "cache_read_input_tokens", "cache_creation_input_tokens",
        )
        recorded = sum(
            s["attributes"].get(f"gen_ai.usage.{k}") or 0
            for s in turn_spans for k in keys
        )
        if recorded == 0:
            for key in keys:
                turn_spans[-1]["attributes"][f"gen_ai.usage.{key}"] = (
                    end_usage.get(key) or 0
                )
    return spans


def extract_contents(path: Path, session_id: str) -> list[ContentRow]:
    events = _read_events(path)
    rows: list[ContentRow] = []
    seq = 0

    def add(span_id: str | None, kind: ContentKind, text: str, ts: int | None) -> None:
        nonlocal seq
        if not text:
            return
        rows.append(ContentRow(span_id=span_id, kind=kind, seq=seq, text=text, ts_ns=ts))
        seq += 1

    turn_index = 0
    for event in events:
        ts = iso_ts_ns(event.get("ts"))
        kind = event.get("type")
        if kind == "user":
            add(None, "user_message", (event.get("text") or "").strip(), ts)
        elif kind == "assistant":
            turn_span = _span_id(session_id, f"turn:{turn_index}")
            for block in event.get("content") or []:
                if block.get("type") == "thinking":
                    add(turn_span, "thinking", (block.get("thinking") or "").strip(), ts)
                elif block.get("type") == "text":
                    add(turn_span, "assistant_message", (block.get("text") or "").strip(), ts)
                elif block.get("type") == "tool_use":
                    tool_span = _span_id(session_id, f"tool:{block.get('id') or ''}")
                    add(
                        tool_span,
                        "tool_arguments",
                        json.dumps(block.get("input") or {}, sort_keys=True),
                        ts,
                    )
            turn_index += 1
        elif kind == "tool_result":
            tool_span = _span_id(session_id, f"tool:{event.get('tool_use_id') or ''}")
            add(tool_span, "tool_result", event.get("output") or "", ts)
    return rows


def probe(path: Path, *, max_lines: int = 200) -> dict[str, Any]:
    """Cheap pre-parse probe: cwd + harness version, and the CLI session id
    when one exists.

    The `claude-sdk` backend drives the Claude Agent SDK, which spawns Claude
    Code — so that run ALSO writes a transcript into ~/.claude/projects and
    gets pulled in as a separate `claude-code` session. Two records, one run.
    The backend already logs the id it spawned; surfacing it here as
    `cli_session_id` is what makes the pair linkable instead of silent.
    """
    out: dict[str, Any] = {}
    try:
        with path.open() as fh:
            for index, line in enumerate(fh):
                if index >= max_lines or ("cwd" in out and "cli_session_id" in out):
                    break
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                kind = event.get("type")
                if kind == "session_meta":
                    if isinstance(event.get("cwd"), str) and event["cwd"]:
                        out["cwd"] = event["cwd"]
                    if isinstance(event.get("harness_version"), str):
                        out["version"] = event["harness_version"]
                    if isinstance(event.get("backend"), str):
                        out["backend"] = event["backend"]
                elif kind == "cli_session":
                    cli_id = event.get("cli_session_id")
                    if isinstance(cli_id, str) and cli_id:
                        out["cli_session_id"] = cli_id
    except OSError:
        return out
    return out


def classify_tool(name: str | None, args_preview: str | None) -> str:
    """The harness exposes a single shell tool; the command text decides."""
    if name in ("bash", "Bash"):
        return "navigation" if is_nav_shell(args_preview) else "bash-other"
    return "other"


# The module owns its adapter: `flume.sources` names this module in its
# registry and imports it only when something resolves "harness".
ADAPTER = SourceAdapter(
    name="harness",
    vendor=None,  # set per run by the backend, not by the source
    map_spans=harness_to_spans,
    extract_contents=extract_contents,
    probe=probe,
    classify_tool=classify_tool,
)
