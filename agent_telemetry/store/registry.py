"""Source-adapter registry: every vendor writes through the same protocol.

An adapter is three pure functions plus identity metadata. The whole
pipeline downstream — raw archive, bundle, session store, retention,
CLI, API, UI — is adapter-agnostic; "anthropic" vs "openai" is just an
argument. To add a vendor (Gemini, a custom harness, ...):

    1. Write `map_spans(path) -> list[Span]` producing the shared span
       vocabulary (`<name>.interaction` root, `<name>.llm_request` turns,
       `<name>.tool` tools — see agent_telemetry/backfill/ for examples).
    2. Write `extract_contents(path, session_id) -> list[ContentRow]` for
       the full-fidelity layer (thinking, messages, tool payloads).
    3. register(SourceAdapter(name=..., vendor=..., ...)).

Adapters resolve by source name or by vendor alias when unambiguous.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_telemetry.backfill.claude_code import jsonl_to_spans
from agent_telemetry.backfill.codex import rollout_to_spans
from agent_telemetry.store.base import ContentRow
from agent_telemetry.store.extract import (
    extract_claude_contents,
    extract_codex_contents,
)

Span = dict[str, Any]


@dataclass(frozen=True)
class SourceAdapter:
    name: str  # canonical source name, e.g. "claude-code"
    vendor: str  # model vendor, e.g. "anthropic"
    map_spans: Callable[[Path], list[Span]]
    extract_contents: Callable[[Path, str], list[ContentRow]]
    # Cheap pre-parse probe: hierarchy hints (parent_session_id,
    # is_subagent) from the file path or its first lines.
    probe: Callable[[Path], dict[str, Any]] | None = None


def probe_claude(path: Path) -> dict[str, Any]:
    import json

    out: dict[str, Any] = {}
    # Subagent transcripts live at .../<parent-session-id>/subagents/agent-*.jsonl
    parts = path.parts
    if "subagents" in parts:
        index = parts.index("subagents")
        if index >= 1:
            out["parent_session_id"] = parts[index - 1]
            out["is_subagent"] = True
    # cwd rides on individual events, not on any session header.
    try:
        with path.open() as fh:
            for _ in range(50):
                line = fh.readline()
                if not line:
                    break
                if '"cwd"' not in line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                cwd = event.get("cwd")
                if isinstance(cwd, str) and cwd:
                    out["cwd"] = cwd
                    break
    except OSError:
        pass
    return out


def probe_codex(path: Path) -> dict[str, Any]:
    # session_meta.source.subagent.thread_spawn.parent_thread_id links a
    # spawned Codex thread to its parent session.
    import json

    try:
        with path.open() as fh:
            for _ in range(5):
                line = fh.readline()
                if not line:
                    break
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("type") != "session_meta":
                    continue
                source = (event.get("payload") or {}).get("source")
                if isinstance(source, dict):
                    spawn = (source.get("subagent") or {}).get("thread_spawn") or {}
                    parent = spawn.get("parent_thread_id")
                    out: dict[str, Any] = {"is_subagent": True}
                    if isinstance(parent, str) and parent:
                        out["parent_session_id"] = parent
                    return out
                return {}
    except OSError:
        pass
    return {}


_ADAPTERS: dict[str, SourceAdapter] = {}


def register(adapter: SourceAdapter) -> None:
    _ADAPTERS[adapter.name] = adapter


def get_adapter(name_or_vendor: str) -> SourceAdapter:
    """Resolve an adapter by source name, or by vendor when unambiguous."""
    adapter = _ADAPTERS.get(name_or_vendor)
    if adapter is not None:
        return adapter
    by_vendor = [a for a in _ADAPTERS.values() if a.vendor == name_or_vendor]
    if len(by_vendor) == 1:
        return by_vendor[0]
    if len(by_vendor) > 1:
        names = ", ".join(sorted(a.name for a in by_vendor))
        raise ValueError(
            f"vendor {name_or_vendor!r} is ambiguous; use a source name: {names}"
        )
    known = ", ".join(sorted(_ADAPTERS))
    raise ValueError(f"unknown source {name_or_vendor!r}; known: {known}")


def adapters() -> list[SourceAdapter]:
    return sorted(_ADAPTERS.values(), key=lambda a: a.name)


register(
    SourceAdapter(
        name="claude-code",
        vendor="anthropic",
        map_spans=jsonl_to_spans,
        extract_contents=extract_claude_contents,
        probe=probe_claude,
    )
)
register(
    SourceAdapter(
        name="codex",
        vendor="openai",
        map_spans=rollout_to_spans,
        extract_contents=extract_codex_contents,
        probe=probe_codex,
    )
)
