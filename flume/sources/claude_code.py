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

import hashlib
import json
from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from flume.sources import DiscoveredTranscript
from flume.sources.common import (
    as_string,
    is_nav_shell,
    iso_ts_ns,
    iter_jsonl_lines,
    iter_jsonl_objects,
    json_text,
    jsonl_paths,
    read_jsonl,
    result_text,
    unique_sorted,
)
from flume.store.base import ContentRow

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
    transcript_items: list[dict[str, Any]] = []
    pending_input_items: list[dict[str, Any]] = []
    opaque_reasoning_items = 0
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
        event_items, reasoning_items = _event_transcript_items(ev, ts_ns)
        opaque_reasoning_items += reasoning_items
        if t == "assistant" and ts_ns is not None:
            transcript_items.extend(event_items)
            input_payload = _messages_payload(pending_input_items)
            output_payload = _messages_payload(event_items)
            turn = _turn_span(
                ev,
                ts_ns,
                session_id,
                trace_id,
                root_span_id,
                0,
                input_payload,
                output_payload,
            )
            spans.append(turn)
            last_turn = turn
            _register_tool_uses(ev, ts_ns, turn["span_id"], tool_pending)
            pending_input_items = []
            continue
        if t == "user" and ts_ns is not None:
            transcript_items.extend(event_items)
            pending_input_items.extend(
                item for item in event_items if item.get("direction") == "input"
            )
            spans.extend(
                _drain_tool_results(ev, ts_ns, session_id, trace_id, tool_pending)
            )

    if first_ns is None:
        return []

    root_start_ns = min([first_ns, *(s["start_unix_nano"] for s in spans)])
    root_end_ns = max([last_ns, *(s["end_unix_nano"] for s in spans)])
    root_input, root_output = _root_payloads(
        session_id,
        transcript_items,
        opaque_reasoning_items,
    )

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
        "input": root_input,
        "output": root_output,
        "status": "OK",
    }
    return [root, *spans]


def _read_events(path: Path) -> list[dict[str, Any]]:
    # Bounded streaming read: whole-file and single-line size are both
    # capped, so a runaway multi-GB line cannot take down the ingest.
    out: list[dict[str, Any]] = []
    for line in iter_jsonl_lines(path):
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


def trace_id_for_session(session_id: str) -> str:
    """Return the deterministic Claude Code trace id for a transcript session."""
    return _trace_id(session_id)


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
    input_payload: dict[str, Any],
    output_payload: dict[str, Any],
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
    if input_payload:
        span["input"] = input_payload
    if output_payload:
        span["output"] = output_payload
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
        display_output = _tool_result_output(blk.get("content"))
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
            "input": {
                "tool_use_id": tid,
                "name": pending["name"],
                "arguments": _bounded_json_value(arguments, _TOOL_PAYLOAD_MAX),
            },
            "output": _truncate_text(display_output, _TOOL_PAYLOAD_MAX),
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


def _event_transcript_items(
    ev: dict[str, Any],
    ts_ns: int | None,
) -> tuple[list[dict[str, Any]], int]:
    msg = ev.get("message")
    if not isinstance(msg, dict):
        return [], 0
    content = msg.get("content")
    event_type = ev.get("type")
    if event_type == "user":
        return _user_transcript_items(content, ts_ns), 0
    if event_type == "assistant":
        return _assistant_transcript_items(content, ts_ns)
    return [], 0


def _user_transcript_items(content: Any, ts_ns: int | None) -> list[dict[str, Any]]:
    if isinstance(content, str):
        text = content.strip()
        return [_message_item(ts_ns, "user", text, "claude_code.user")] if text else []
    if not isinstance(content, list):
        return []

    items: list[dict[str, Any]] = []
    for block in content:
        if isinstance(block, str):
            text = block.strip()
            if text:
                items.append(_message_item(ts_ns, "user", text, "claude_code.user"))
            continue
        if not isinstance(block, dict):
            continue
        if block.get("type") == "tool_result":
            tool_use_id = block.get("tool_use_id")
            if not isinstance(tool_use_id, str):
                continue
            items.append(
                {
                    "ts_ns": ts_ns,
                    "direction": "input",
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "is_error": bool(block.get("is_error")),
                    "output": _truncate_text(
                        _tool_result_output(block.get("content")),
                        _TOOL_PAYLOAD_MAX,
                    ),
                }
            )
            continue
        text = _content_text([block]).strip()
        if text:
            items.append(_message_item(ts_ns, "user", text, "claude_code.user"))
    return items


def _assistant_transcript_items(
    content: Any,
    ts_ns: int | None,
) -> tuple[list[dict[str, Any]], int]:
    if isinstance(content, str):
        text = content.strip()
        return (
            [_message_item(ts_ns, "assistant", text, "claude_code.assistant")]
            if text
            else []
        ), 0
    if not isinstance(content, list):
        return [], 0

    items: list[dict[str, Any]] = []
    opaque_reasoning_items = 0
    for block in content:
        if isinstance(block, str):
            text = block.strip()
            if text:
                items.append(
                    _message_item(ts_ns, "assistant", text, "claude_code.assistant")
                )
            continue
        if not isinstance(block, dict):
            continue

        block_type = block.get("type")
        if block_type in {"thinking", "reasoning", "redacted_thinking"}:
            opaque_reasoning_items += 1
            continue
        if block_type == "tool_use":
            tool_use_id = block.get("id")
            name = block.get("name")
            if not isinstance(tool_use_id, str) or not isinstance(name, str):
                continue
            items.append(
                {
                    "ts_ns": ts_ns,
                    "direction": "output",
                    "type": "tool_call",
                    "tool_use_id": tool_use_id,
                    "name": name,
                    "arguments": _bounded_json_value(
                        block.get("input") or {},
                        _TOOL_PAYLOAD_MAX,
                    ),
                }
            )
            continue

        text = _content_text([block]).strip()
        if text:
            items.append(
                _message_item(ts_ns, "assistant", text, "claude_code.assistant")
            )
    return items, opaque_reasoning_items


def _message_item(
    ts_ns: int | None,
    role: str,
    text: str,
    source: str,
) -> dict[str, Any]:
    return {
        "ts_ns": ts_ns,
        "direction": "input" if role == "user" else "output",
        "type": "message",
        "role": role,
        "source": source,
        "content": _truncate_text(text, _TRANSCRIPT_ITEM_MAX),
    }


def _messages_payload(items: list[dict[str, Any]]) -> dict[str, Any]:
    messages = [_strip_internal(item) for item in items]
    return {"messages": messages} if messages else {}


def _root_payloads(
    session_id: str,
    items: list[dict[str, Any]],
    opaque_reasoning_items: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    user_requests = [
        _strip_internal(item)
        for item in items
        if item["type"] == "message" and item["role"] == "user"
    ]
    assistant_messages = [
        _strip_internal(item)
        for item in items
        if item["type"] == "message" and item["role"] == "assistant"
    ]
    tool_calls = [item for item in items if item["type"] == "tool_call"]
    tool_results = [item for item in items if item["type"] == "tool_result"]

    root_input: dict[str, Any] = {
        "counts": {"user_requests": len(user_requests)},
        "session_id": session_id,
        "user_requests": _bounded_items(user_requests),
    }
    root_output: dict[str, Any] = {
        "counts": {
            "assistant_messages": len(assistant_messages),
            "tool_calls": len(tool_calls),
            "tool_outputs": len(tool_results),
            "opaque_reasoning_items": opaque_reasoning_items,
        },
        "session_id": session_id,
        "assistant_messages": _bounded_items(assistant_messages),
    }
    return root_input, root_output


def _bounded_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept = [_root_preview_item(item) for item in items[:_ROOT_TRANSCRIPT_ITEMS_MAX]]
    if len(items) <= _ROOT_TRANSCRIPT_ITEMS_MAX:
        return kept
    kept.append(
        {
            "type": "truncation_notice",
            "omitted_items": len(items) - _ROOT_TRANSCRIPT_ITEMS_MAX,
        }
    )
    return kept


def _root_preview_item(item: dict[str, Any]) -> dict[str, Any]:
    preview = dict(item)
    for key in ("content", "output"):
        value = preview.get(key)
        if isinstance(value, str) and len(value) > _ROOT_TRANSCRIPT_PREVIEW_MAX:
            preview[key] = _truncate_text(value, _ROOT_TRANSCRIPT_PREVIEW_MAX)
    return preview


def _strip_internal(item: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in item.items() if k not in {"direction", "ts_ns"}}


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
            continue
        if not isinstance(block, dict):
            continue
        if block.get("type") in {"thinking", "reasoning", "redacted_thinking"}:
            continue
        for key in ("text", "content", "input_text", "output_text"):
            value = block.get(key)
            if isinstance(value, str) and value:
                parts.append(value)
                break
    return "\n".join(parts)


def _tool_result_output(content: Any) -> str:
    if isinstance(content, str):
        return content
    text = _content_text(content)
    if text:
        return text
    if isinstance(content, (list, dict)):
        return _json_text(content)
    return ""


def _bounded_json_value(value: Any, max_chars: int) -> Any:
    encoded = _json_text(value)
    if len(encoded) <= max_chars:
        try:
            return json.loads(encoded)
        except json.JSONDecodeError:
            return encoded
    return {
        "truncated": True,
        "truncated_chars": len(encoded) - max_chars,
        "value": encoded[:max_chars],
    }


def _json_text(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except (TypeError, ValueError):
        return str(value)


def _truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + f"\n...[truncated {len(value) - max_chars} chars]"


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

    def add(span_id: str | None, kind: str, text: str, ts: int | None) -> None:
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
