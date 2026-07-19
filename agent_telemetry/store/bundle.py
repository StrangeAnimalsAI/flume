"""Build a SessionBundle from mapper span dicts + extracted contents.

Source-agnostic: works off the span vocabulary both mappers share
(`*.interaction` root, `*.llm_request` turns, `*.tool` tool spans).
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from agent_telemetry.store.base import ContentRow, SessionBundle

Span = dict[str, Any]

# Provenance stamp written to every session row. Bump whenever backfill/*,
# store/extract.py, or this module change what they derive from raw bytes;
# `agent-telemetry-analyze rebuild --stale` then re-ingests older rows.
PIPELINE_VERSION = 1

_ARGS_PREVIEW_MAX = 500


def bundle_from_spans(
    spans: list[Span],
    *,
    contents: list[ContentRow],
    file_path: Path,
    metadata: dict[str, Any] | None = None,
    raw_sha256: str | None = None,
) -> SessionBundle | None:
    """Turn one session's span dicts into relational rows."""
    root = next(
        (s for s in spans if s.get("name", "").endswith(".interaction")), None
    )
    if root is None:
        return None
    meta = dict(metadata or {})
    attrs = root.get("attributes") or {}

    # Keyed by span_id: real transcripts can re-log an event with the same
    # uuid (API retries), which yields duplicate deterministic span ids.
    # Langfuse silently first-write-wins; here the LAST occurrence wins,
    # matching the re-log being the settled version of the event.
    turns_by_id: dict[str, dict[str, Any]] = {}
    tools_by_id: dict[str, dict[str, Any]] = {}
    for span in spans:
        name = span.get("name") or ""
        if name.endswith(".llm_request"):
            row = _turn_row(span)
            turns_by_id[row["span_id"]] = row
        elif name.endswith(".tool"):
            row = _tool_row(span)
            tools_by_id[row["span_id"]] = row
    turns = list(turns_by_id.values())
    tool_calls = list(tools_by_id.values())
    turns.sort(key=lambda t: (t["started_at_ns"], t["span_id"]))
    tool_calls.sort(key=lambda t: (t["started_at_ns"], t["span_id"]))
    for index, turn in enumerate(turns):
        turn.setdefault("turn_index", index)

    thinking_rows = [c for c in contents if c.kind == "thinking"]
    session_id = attrs.get("session.id") or file_path.stem
    started = int(root["start_unix_nano"])
    ended = int(root["end_unix_nano"])
    cwd = meta.get("cwd") or attrs.get("codex.cwd")
    surface = attrs.get("entrypoint") or meta.get("surface")
    is_subagent = bool(
        meta.get("is_subagent")
        or meta.get("is_sidechain")
        or surface == "subagent"
        or str(session_id).startswith("agent-")
    )
    session = {
        "session_id": session_id,
        "trace_id": root.get("trace_id"),
        "source": attrs.get("source"),
        "surface": surface,
        "cwd": cwd,
        "project": derive_project(cwd),
        "is_subagent": 1 if is_subagent else 0,
        "parent_session_id": meta.get("parent_session_id"),
        "git_branch": attrs.get("git.branch") or meta.get("git_branch"),
        "model": _first_model(turns) or attrs.get("gen_ai.request.model"),
        "version": attrs.get("claude_code.version")
        or attrs.get("codex.cli_version")
        or meta.get("version"),
        "started_at_ns": started,
        "ended_at_ns": ended,
        "wall_ms": max(0, (ended - started) // 1_000_000),
        "active_ms": sum(t["duration_ms"] or 0 for t in turns),
        "turn_count": len(turns),
        "tool_call_count": len(tool_calls),
        "input_tokens": sum(t["input_tokens"] for t in turns),
        "output_tokens": sum(t["output_tokens"] for t in turns),
        "cache_read_tokens": sum(t["cache_read_tokens"] for t in turns),
        "cache_creation_tokens": sum(t["cache_creation_tokens"] for t in turns),
        "reasoning_tokens": sum(t["reasoning_tokens"] for t in turns),
        "thinking_blocks": len(thinking_rows),
        "thinking_chars": sum(len(c.text) for c in thinking_rows),
        "first_user_message": _first_user_message(contents),
        "file_path": str(file_path),
        "raw_sha256": raw_sha256,
        "pipeline_version": PIPELINE_VERSION,
        "ingested_at_ns": time.time_ns(),
        "metadata": json.dumps(meta, sort_keys=True) if meta else None,
    }
    return SessionBundle(
        session=session,
        turns=turns,
        tool_calls=tool_calls,
        contents=contents,
    )


def _turn_row(span: Span) -> dict[str, Any]:
    attrs = span.get("attributes") or {}
    return {
        "span_id": span["span_id"],
        "turn_index": attrs.get("codex.turn_index"),
        "model": attrs.get("gen_ai.request.model"),
        "started_at_ns": int(span["start_unix_nano"]),
        "ended_at_ns": int(span["end_unix_nano"]),
        "duration_ms": int(attrs.get("claude_code.duration_ms") or 0)
        or max(0, (int(span["end_unix_nano"]) - int(span["start_unix_nano"])) // 1_000_000),
        "input_tokens": int(attrs.get("gen_ai.usage.input_tokens") or 0),
        "output_tokens": int(attrs.get("gen_ai.usage.output_tokens") or 0),
        "cache_read_tokens": int(attrs.get("gen_ai.usage.cache_read_input_tokens") or 0),
        "cache_creation_tokens": int(
            attrs.get("gen_ai.usage.cache_creation_input_tokens") or 0
        ),
        "reasoning_tokens": int(attrs.get("codex.reasoning_tokens") or 0),
        "thinking_chars": int(attrs.get("claude_code.thinking_chars") or 0),
        "text_chars": int(attrs.get("claude_code.text_chars") or 0),
    }


def _tool_row(span: Span) -> dict[str, Any]:
    attrs = span.get("attributes") or {}
    arguments = attrs.get("tool.arguments") or ""
    if not isinstance(arguments, str):
        arguments = json.dumps(arguments, sort_keys=True)
    return {
        "span_id": span["span_id"],
        "turn_span_id": span.get("parent_span_id"),
        "name": attrs.get("tool.name") or "?",
        "args_hash": hashlib.sha256(arguments.encode()).hexdigest()[:16],
        "args_preview": arguments[:_ARGS_PREVIEW_MAX],
        "started_at_ns": int(span["start_unix_nano"]),
        "ended_at_ns": int(span["end_unix_nano"]),
        "duration_ms": int(attrs.get("tool.duration_ms") or 0),
        "is_error": 1 if attrs.get("tool.is_error") else 0,
        "result_chars": int(attrs.get("tool.result_chars") or 0),
    }


def derive_project(cwd: Any) -> str | None:
    """Short project label from a session cwd.

    Strips $HOME and worktree noise, keeps the last two path segments:
    /home/alex/projects/tools/agent-telemetry -> tools/agent-telemetry.
    """
    if not isinstance(cwd, str) or not cwd:
        return None
    path = cwd
    home = str(Path.home())
    if path.startswith(home):
        path = path[len(home) :].lstrip("/")
    # A worktree cwd belongs to its repo, not to .claude/worktrees/<name>.
    path = path.split("/.claude/")[0].rstrip("/")
    if not path:
        return "~"
    parts = [p for p in path.split("/") if p]
    if parts and parts[0] == "Code":
        parts = parts[1:]
    return "/".join(parts[-2:]) if parts else "~"


def _first_model(turns: list[dict[str, Any]]) -> str | None:
    for turn in turns:
        model = turn.get("model")
        if isinstance(model, str) and model:
            return model
    return None


def _first_user_message(contents: list[ContentRow]) -> str | None:
    for row in contents:
        if row.kind == "user_message":
            text = row.text.strip()
            if text and not text.startswith("<"):  # skip harness/system tags
                return text[:300]
    return None
