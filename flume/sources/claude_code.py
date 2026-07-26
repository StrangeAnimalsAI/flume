"""Claude Code: everything flume knows about its transcript format.

Mapping (`jsonl_to_spans`): each transcript maps to one trace with a
`claude_code.interaction` root, per-turn `claude_code.llm_request` children,
and per-tool-call `claude_code.tool` spans nested under the turn that issued
them. Span IDs are derived from session_id + event identity, so replays are
idempotent. Extraction (`extract_contents`): a second full-fidelity pass over
the same file keyed by the same span ids. Discovery
(`ClaudeCodeTranscriptSource`): canonical JSONL locations under
~/.claude/projects, with a cheap metadata read and a `probe` for hierarchy
hints. Pure data — no network, no SDK.
"""
from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from flume.sources import DiscoveredTranscript, SourceAdapter
from flume.sources.utils import (
    as_string,
    is_nav_shell,
    iso_ts_ns,
    iter_jsonl_objects,
    json_text,
    jsonl_paths,
    read_jsonl,
    result_text,
    span_id,
    trace_id,
    unique_sorted,
)
from flume.store.base import ContentKind, ContentRow

Span = dict[str, Any]

# Match the 60 KB cap the native Claude Code OTel export applies with
# OTEL_LOG_TOOL_CONTENT=1, so backfill and live paths stay comparable.
_TOOL_PAYLOAD_MAX = 60_000
_TRANSCRIPT_ITEM_MAX = 4_000
_ROOT_TRANSCRIPT_ITEMS_MAX = 50
_ROOT_TRANSCRIPT_PREVIEW_MAX = 800


def jsonl_to_spans(path: Path) -> list[Span]:
    """Map one Claude Code JSONL transcript to a list of span dicts."""
    session_id = path.stem
    events = read_jsonl(path)
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
        last_turn["attributes"]["turn.duration_ms"] = duration_ms
        # Preserve end_unix_nano; shift start backwards by duration.
        last_turn["start_unix_nano"] = (
            last_turn["end_unix_nano"] - duration_ms * 1_000_000
        )

    for ev in events:
        ts_ns = iso_ts_ns(ev.get("timestamp"))
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

    root_start_ns = min([first_ns, *(s["start_unix_nano"] for s in spans)])
    root_end_ns = max([last_ns, *(s["end_unix_nano"] for s in spans)])
    root: Span = {
        "name": "claude_code.interaction",
        "trace_id": trace_id,
        "span_id": root_span_id,
        "parent_span_id": None,
        "start_unix_nano": root_start_ns,
        "end_unix_nano": root_end_ns,
        "attributes": {
            "source": "claude-code",
            "session.id": session_id,
            "entrypoint": entrypoint,
            "session.agent_version": version,
            "git.branch": git_branch,
        },
        "status": "OK",
    }
    return [root, *spans]


# Hash namespace for this source's span/trace ids. Stored and joined on,
# so it is frozen: changing it orphans every existing row.
_NAMESPACE = "claude-code"


def _trace_id(session_id: str) -> str:
    return trace_id(_NAMESPACE, session_id)


def trace_id_for_session(session_id: str) -> str:
    """Return the deterministic Claude Code trace id for a transcript session."""
    return _trace_id(session_id)


def _span_id(session_id: str, suffix: str) -> str:
    return span_id(_NAMESPACE, session_id, suffix)


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
    span: Span = {
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
            "turn.thinking_chars": thinking_chars,
            "turn.text_chars": text_chars,
            "turn.duration_ms": duration_ms,
        },
        "status": "OK",
    }
    return span


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
        arguments = pending["input"]
        yield {
            "name": "claude_code.tool",
            "trace_id": trace_id,
            "span_id": _span_id(session_id, f"tool:{tid}"),
            "parent_span_id": pending["parent_span_id"],
            "start_unix_nano": pending["start_ns"],
            "end_unix_nano": ts_ns,
            "attributes": {
                "tool.name": pending["name"],
                "tool.arguments": _json_text(arguments)[:_TOOL_PAYLOAD_MAX],
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


def _json_text(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except (TypeError, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Full-fidelity extraction
#
# The mapper above redacts thinking to counts and caps payloads; audits need
# the complete thought process and untruncated tool I/O. This second pass
# emits ContentRows keyed by the SAME deterministic span ids `_span_id`
# produces, so full text joins directly onto the metrics skeleton.


def _settled_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mirror the bundle's retry rule: a re-logged uuid (API retry)
    supersedes the earlier occurrence — last event wins, first-seen order —
    so extracted content stays consistent with the deduped span rows."""
    order: list[Any] = []
    settled: dict[Any, dict[str, Any]] = {}
    for index, ev in enumerate(events):
        key = ev.get("uuid") or ("#", index)
        if key not in settled:
            order.append(key)
        settled[key] = ev
    return [settled[key] for key in order]


def extract_contents(path: Path, session_id: str) -> list[ContentRow]:
    rows: list[ContentRow] = []
    seq = 0

    def add(span_id: str | None, kind: ContentKind, text: str, ts: int | None) -> None:
        nonlocal seq
        if not text:
            return
        rows.append(ContentRow(span_id=span_id, kind=kind, seq=seq, text=text, ts_ns=ts))
        seq += 1

    for ev in _settled_events(read_jsonl(path)):
        ts = iso_ts_ns(ev.get("timestamp"))
        t = ev.get("type")
        msg = ev.get("message")
        if not isinstance(msg, dict):
            continue
        content = msg.get("content")

        if t == "assistant":
            turn_uuid = ev.get("uuid") or (str(ts) if ts is not None else "")
            turn_span = _span_id(session_id, f"turn:{turn_uuid}")
            if isinstance(content, str):
                add(turn_span, "assistant_message", content.strip(), ts)
                continue
            if not isinstance(content, list):
                continue
            for blk in content:
                if not isinstance(blk, dict):
                    continue
                bt = blk.get("type")
                if bt == "thinking":
                    add(turn_span, "thinking", blk.get("thinking") or "", ts)
                elif bt == "text":
                    add(turn_span, "assistant_message", (blk.get("text") or "").strip(), ts)
                elif bt == "tool_use":
                    tid = blk.get("id")
                    if isinstance(tid, str):
                        add(
                            _span_id(session_id, f"tool:{tid}"),
                            "tool_arguments",
                            json_text(blk.get("input") or {}),
                            ts,
                        )
            continue

        if t == "user":
            if isinstance(content, str):
                add(None, "user_message", content.strip(), ts)
                continue
            if not isinstance(content, list):
                continue
            for blk in content:
                if isinstance(blk, str):
                    add(None, "user_message", blk.strip(), ts)
                    continue
                if not isinstance(blk, dict):
                    continue
                if blk.get("type") == "tool_result":
                    tid = blk.get("tool_use_id")
                    if isinstance(tid, str):
                        add(
                            _span_id(session_id, f"tool:{tid}"),
                            "tool_result",
                            result_text(blk.get("content")),
                            ts,
                        )
                elif blk.get("type") == "text":
                    add(None, "user_message", (blk.get("text") or "").strip(), ts)

    return rows


# ---------------------------------------------------------------------------
# Cheap pre-parse probe


def probe(path: Path) -> dict[str, Any]:
    """Hierarchy hints and cwd from the file path or its first lines."""
    out: dict[str, Any] = {}
    # Subagent transcripts live at .../<parent-session-id>/subagents/agent-*.jsonl
    parts = path.parts
    if "subagents" in parts:
        index = parts.index("subagents")
        if index >= 1:
            out["parent_session_id"] = parts[index - 1]
            out["is_subagent"] = True
    # cwd rides on individual events, not on any session header.
    for index, event in enumerate(iter_jsonl_objects(path)):
        if index >= 50:
            break
        cwd = event.get("cwd")
        if isinstance(cwd, str) and cwd:
            out["cwd"] = cwd
            break
    return out


# ---------------------------------------------------------------------------
# Discovery

DEFAULT_CLAUDE_PROJECTS_ROOT = Path.home() / ".claude" / "projects"


class ClaudeCodeTranscriptSource:
    """Discover canonical Claude Code JSONL transcripts under project roots."""

    source_type = "claude-code"

    def __init__(self, roots: Sequence[Path | str] | None = None) -> None:
        self.roots = tuple(
            Path(root).expanduser().resolve(strict=False)
            for root in (roots or (DEFAULT_CLAUDE_PROJECTS_ROOT,))
        )

    def discover(self) -> Iterable[DiscoveredTranscript]:
        for path in unique_sorted(self._candidate_paths()):
            metadata = read_transcript_metadata(path)
            session_id = path.stem
            metadata.setdefault("session_id", session_id)
            yield DiscoveredTranscript(
                source_type=self.source_type,
                path=path,
                session_id=session_id,
                trace_id=trace_id_for_session(session_id),
                metadata=metadata,
            )

    def _candidate_paths(self) -> Iterable[Path]:
        for root in self.roots:
            yield from jsonl_paths(root, recursive=True)


def read_transcript_metadata(path: Path, *, max_lines: int = 200) -> dict[str, Any]:
    """Extract cheap Claude Code metadata without running the full mapper."""
    metadata: dict[str, Any] = {"source": "claude-code"}
    for index, obj in enumerate(iter_jsonl_objects(path)):
        if index >= max_lines:
            break

        _merge_event_metadata(metadata, obj)
        _merge_message_metadata(metadata, obj.get("message"))
        if _has_enough_metadata(metadata):
            break

    return metadata


def _merge_event_metadata(metadata: dict[str, Any], obj: dict[str, Any]) -> None:
    event_session_id = as_string(obj.get("sessionId"))
    if event_session_id:
        metadata.setdefault("claude_session_id", event_session_id)

    for source_key, metadata_key in (
        ("cwd", "cwd"),
        ("version", "version"),
        ("gitBranch", "git_branch"),
        ("userType", "user_type"),
        ("permissionMode", "permission_mode"),
        ("promptId", "prompt_id"),
        ("agentId", "agent_id"),
        ("slug", "slug"),
    ):
        value = as_string(obj.get(source_key))
        if value:
            metadata.setdefault(metadata_key, value)

    entrypoint = as_string(obj.get("entrypoint"))
    if entrypoint:
        metadata.setdefault("entrypoint", entrypoint)
        metadata.setdefault("surface", entrypoint)

    is_sidechain = obj.get("isSidechain")
    if isinstance(is_sidechain, bool):
        metadata.setdefault("is_sidechain", is_sidechain)


def _merge_message_metadata(metadata: dict[str, Any], message: Any) -> None:
    if not isinstance(message, dict):
        return
    model = as_string(message.get("model"))
    if model:
        metadata.setdefault("model", model)


def _has_enough_metadata(metadata: dict[str, Any]) -> bool:
    return bool(
        metadata.get("claude_session_id")
        and metadata.get("entrypoint")
        and metadata.get("version")
        and metadata.get("model")
    )




# ---------------------------------------------------------------------------
# Tool vocabulary (for navigation-time attribution)

_NAV_TOOLS = {"Read", "Grep", "Glob", "LS"}
_EDIT_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
_SHELL_TOOLS = {"Bash", "BashOutput"}
_SUBAGENT_TOOLS = {"Agent", "Task"}


def classify_tool(name: str | None, args_preview: str | None) -> str:
    """Classify one tool call as navigation / editing / subagent / other."""
    if name in _NAV_TOOLS:
        return "navigation"
    if name in _SHELL_TOOLS:
        return "navigation" if is_nav_shell(args_preview) else "bash-other"
    if name in _SUBAGENT_TOOLS:
        return "subagent"
    if name in _EDIT_TOOLS:
        return "editing"
    return "other"


def make_source(roots=None, **_ignored) -> ClaudeCodeTranscriptSource:
    """Build this source's discovery. Keyword options come from the CLI's
    flags, or from a `[sources]` table for a config-declared source."""
    return ClaudeCodeTranscriptSource(roots)


# The module owns its adapter: `flume.sources` names this module in its
# registry and imports it only when something resolves "claude-code".
ADAPTER = SourceAdapter(
    name="claude-code",
    map_spans=jsonl_to_spans,
    extract_contents=extract_contents,
    probe=probe,
    classify_tool=classify_tool,
)
