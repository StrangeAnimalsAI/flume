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

from flume.backfill.langfuse import enrich_trace_attrs

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
            "claude_code.version": version,
            "git.branch": git_branch,
        },
        "input": root_input,
        "output": root_output,
        "status": "OK",
    }
    return enrich_trace_attrs(
        [root, *spans],
        agent_source="claude-code",
        agent_family="claude-code",
        agent_surface=entrypoint
        if isinstance(entrypoint, str) and entrypoint
        else None,
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
            "claude_code.thinking_chars": thinking_chars,
            "claude_code.text_chars": text_chars,
            "claude_code.duration_ms": duration_ms,
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


def _cli(argv: list[str] | None = None) -> int:
    """Entry point: `python -m flume.backfill.claude_code`.

    `--dry-run` prints the span-dicts as JSON to stdout (one document, pretty,
    stable key order) and does not import any OTel SDK machinery. Otherwise the
    spans are exported via OTLP-HTTP to `--endpoint`.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="python -m flume.backfill.claude_code",
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

    from flume.backfill.otlp import export_spans_via_otlp
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
