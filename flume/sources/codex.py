"""Codex: everything flume knows about its rollout format.

Mapping (`rollout_to_spans`): each rollout file maps to one trace with
a `codex.interaction` root, per-LLM-request `codex.llm_request` children, and
per-tool-call `codex.tool` spans nested under the turn that issued them. IDs
are derived from session_id + event identity, so replays are idempotent.
Extraction (`extract_contents`): a second full-fidelity pass keyed by the
same span ids (rollout `reasoning` items carry only encrypted blobs — no
plaintext thinking exists to extract; a source limitation, not a store
choice). Discovery (`CodexRolloutSource`): rollout locations under
~/.codex/sessions, plus a `probe` for sub-agent hierarchy hints.

# Rollout format (as observed in ~/.codex/sessions/2026/**/rollout-*.jsonl)

Each line is `{timestamp, type, payload}`. The `type` field is one of:

- `session_meta` (first line): carries `id` (session UUID), `originator`
  (e.g. "Codex Desktop"), `cli_version`, `source` ("vscode" or
  `{"subagent": {...}}` when the rollout belongs to a spawned sub-agent),
  `cwd`, `model_provider`. Forked sessions additionally carry
  `forked_from_id`; a desktop app fork writes two `session_meta` lines
  back-to-back (the parent then the fork).
- `turn_context`: `{turn_id, cwd, model, approval_policy, sandbox_policy,
  effort, collaboration_mode, ...}`. Snapshot of runtime config at the
  start of each user turn.
- `event_msg` with payload types:
  - `task_started` / `task_complete`: mark the boundaries of ONE user-
    prompted turn. A single task can issue many model responses.
  - `user_message`: the user's prompt text.
  - `agent_message`: a visible assistant chunk (`phase="commentary"`
    fragments plus a final full message).
  - `token_count`: emitted AFTER each model response; `info.last_token_usage`
    carries the usage for the just-completed call:
    `{input_tokens, cached_input_tokens, output_tokens,
      reasoning_output_tokens, total_tokens}`.
    The first `token_count` per task has a null `info` (pre-call snapshot).
  - `exec_command_end`: result of an `exec_command` tool, with
    `{call_id, command, cwd, stdout, stderr, aggregated_output, exit_code,
      duration: {secs, nanos}}`.
  - `patch_apply_end`: result of an `apply_patch` custom tool call, with
    `{call_id, stdout, stderr, success, changes}`.
- `response_item` with payload types:
  - `message` (role=developer/user/assistant): prompt/instruction chunks
    and the final assistant text. No message id.
  - `reasoning`: opaque encrypted reasoning blob. No plaintext, no tokens.
  - `function_call`: tool invocation, `{name, arguments (JSON string),
    call_id}`. Includes real tools (`exec_command`, `write_stdin`) and MCP
    tools (`mcp__server__tool`). Issued during a model response; ends at
    the matching `function_call_output`.
  - `function_call_output`: `{call_id, output (stringified)}`. The string
    format varies: exec_command wraps in a status header, MCP outputs are
    a JSON array of content blocks, custom tools return a JSON envelope.
  - `custom_tool_call` / `custom_tool_call_output`: `apply_patch` path. The
    input is a unified patch string; output is `{output, metadata:
    {exit_code, duration_seconds}}`.

# LLM-request granularity

One `task_started` → N model responses → `task_complete`. Each model response
terminates with a `token_count` event carrying `info.last_token_usage`. We
emit one `codex.llm_request` span per such `token_count`, spanning from the
previous boundary (prior token_count with usage, or task_started/first event)
to the current one. Tool spans attach to whichever turn contains their
function_call timestamp.

# Timestamps

RFC3339 with `Z`. We parse to UTC and use the integer-microsecond path to
avoid the float rounding bug caught by the Claude Code parity check (INT-436).

# Blind spots

- `codex exec` and `codex mcp-server` modes do not persist rollouts under
  `~/.codex/sessions/`. Backfill cannot see them.
- The `encrypted_content` on `reasoning` items is opaque; we surface only
  the reasoning_output_tokens count, not the text.
- Tool arguments on `exec_command` are the raw shell array; we JSON-encode
  them for `tool.arguments`. Large outputs are truncated to the same 60 KB
  cap Claude Code's native OTel export applies.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flume.sources import DiscoveredTranscript, SourceAdapter
from flume.sources.common import (
    as_string,
    is_nav_shell,
    iso_ts_ns,
    iter_jsonl_lines,
    iter_jsonl_objects,
    json_text,
    jsonl_paths,
    read_jsonl,
    unique_sorted,
)
from flume.store.base import ContentKind, ContentRow

Span = dict[str, Any]

# Match the 60 KB cap used by Claude Code's OTel log path.
_TOOL_PAYLOAD_MAX = 60_000
_TRANSCRIPT_ITEM_MAX = 4_000
_ROOT_TRANSCRIPT_ITEMS_MAX = 50
_ROOT_TRANSCRIPT_PREVIEW_MAX = 800


def rollout_to_spans(path: Path) -> list[Span]:
    """Map one Codex rollout JSONL to a list of span dicts."""
    events = _read_events(path)
    if not events:
        return []

    session_id = _session_id(events, path)
    trace_id = _trace_id(session_id)
    root_span_id = _span_id(session_id, "root")

    meta = _first_session_meta(events)
    originator = meta.get("originator")
    cli_version = meta.get("cli_version")
    cwd = meta.get("cwd")
    src_raw = meta.get("source")
    if isinstance(src_raw, dict):
        # Sub-agent rollouts: source is `{"subagent": {...}}`.
        session_source = "subagent"
    elif isinstance(src_raw, str):
        session_source = src_raw
    else:
        session_source = None
    model = _first_model(events)
    # Sessions can switch models mid-rollout (each user turn snapshots its
    # config in a turn_context). Track the current model so every turn is
    # attributed to the model that actually served it.
    current_model = model

    # First pass: collect LLM-request boundaries and tool call lifetimes.
    first_ns: int | None = None
    last_ns: int | None = None
    boundary_ns = _ts_ns(events[0].get("timestamp"))  # opening edge of first turn
    payload_boundary_ns = boundary_ns
    turn_boundaries: list[
        tuple[int, int, dict[str, Any], dict[str, Any], dict[str, Any], str | None]
    ] = []
    # (start_ns, end_ns, last_token_usage, input_payload, output_payload, model)

    pending_tools: dict[str, dict[str, Any]] = {}
    tool_spans: list[Span] = []
    transcript_items: list[dict[str, Any]] = []
    reasoning_items = 0

    for ev in events:
        ts_ns = _ts_ns(ev.get("timestamp"))
        if ts_ns is not None:
            if first_ns is None:
                first_ns = ts_ns
            last_ns = ts_ns
        if ev.get("type") == "turn_context":
            ctx_model = (ev.get("payload") or {}).get("model")
            if isinstance(ctx_model, str) and ctx_model:
                current_model = ctx_model
        p = ev.get("payload") or {}
        if not isinstance(p, dict):
            continue
        pt = p.get("type")
        if pt == "reasoning":
            reasoning_items += 1
        item = _transcript_item(p, ts_ns)
        if item is not None:
            transcript_items.append(item)

        if pt == "token_count":
            info = p.get("info") or {}
            last = info.get("last_token_usage") or {}
            if not last or not any(
                last.get(k) for k in ("input_tokens", "output_tokens")
            ):
                # Pre-call snapshot (null info, or zero tokens). Still advance
                # the boundary so we don't retroactively absorb it into the
                # first real turn.
                if ts_ns is not None:
                    boundary_ns = ts_ns
                continue
            if ts_ns is None or boundary_ns is None:
                continue
            input_payload, output_payload = _turn_payloads(
                transcript_items,
                payload_boundary_ns,
                ts_ns,
            )
            turn_boundaries.append(
                (boundary_ns, ts_ns, last, input_payload, output_payload, current_model)
            )
            boundary_ns = ts_ns
            payload_boundary_ns = ts_ns
            continue

        if pt == "function_call" and ts_ns is not None:
            call_id = p.get("call_id")
            if not isinstance(call_id, str):
                continue
            pending_tools[call_id] = {
                "start_ns": ts_ns,
                "name": p.get("name") or "?",
                "arguments": p.get("arguments") or "",
                "kind": "function_call",
            }
            continue

        if pt == "custom_tool_call" and ts_ns is not None:
            call_id = p.get("call_id")
            if not isinstance(call_id, str):
                continue
            pending_tools[call_id] = {
                "start_ns": ts_ns,
                "name": p.get("name") or "?",
                "arguments": p.get("input") or "",
                "kind": "custom_tool_call",
            }
            continue

        if pt in ("exec_command_end", "patch_apply_end") and ts_ns is not None:
            call_id = p.get("call_id")
            if not isinstance(call_id, str):
                continue
            entry = pending_tools.get(call_id)
            if entry is None:
                continue
            # Annotate with duration/exit info; the function_call_output will
            # close the span. If no function_call_output ever arrives (rare),
            # we still close on the end event below.
            if pt == "exec_command_end":
                entry["exit_code"] = p.get("exit_code")
                entry["end_ns"] = ts_ns
                dur = p.get("duration") or {}
                if isinstance(dur, dict):
                    secs = int(dur.get("secs") or 0)
                    nanos = int(dur.get("nanos") or 0)
                    entry["exec_duration_ns"] = secs * 1_000_000_000 + nanos
                entry["exec_output"] = p.get("aggregated_output") or ""
            else:  # patch_apply_end
                entry["success"] = bool(p.get("success"))
                entry["end_ns"] = ts_ns
                entry["exec_output"] = (p.get("stdout") or "") + (
                    p.get("stderr") or ""
                )
            continue

        if pt in ("function_call_output", "custom_tool_call_output") and ts_ns is not None:
            call_id = p.get("call_id")
            if not isinstance(call_id, str):
                continue
            entry = pending_tools.pop(call_id, None)
            if entry is None:
                continue
            tool_spans.append(
                _tool_span(
                    session_id,
                    trace_id,
                    call_id,
                    entry,
                    ts_ns,
                    p.get("output") or "",
                )
            )
            continue

    if first_ns is None:
        return []

    for call_id, entry in pending_tools.items():
        end_ns = entry.get("end_ns")
        if end_ns is None:
            continue
        tool_spans.append(
            _tool_span(
                session_id,
                trace_id,
                call_id,
                entry,
                end_ns,
                "",
            )
        )

    # Build turn spans now that boundaries are known.
    turn_spans: list[Span] = []
    for i, (
        start_ns,
        end_ns,
        last,
        input_payload,
        output_payload,
        turn_model,
    ) in enumerate(turn_boundaries):
        turn_spans.append(
            _turn_span(
                i,
                start_ns,
                end_ns,
                last,
                session_id,
                trace_id,
                root_span_id,
                turn_model,
                input_payload,
                output_payload,
            )
        )

    # Assign tool parent = the turn whose [start, end] contains the tool's
    # start_ns. Tools fire during a model response; the response terminates
    # with the next token_count, which is that turn's end boundary.
    for tool in tool_spans:
        start_ns = tool.pop("_parent_start_ns")
        parent = _find_turn_for(turn_spans, start_ns)
        tool["parent_span_id"] = parent["span_id"] if parent else root_span_id

    root_input, root_output = _root_payloads(
        session_id,
        transcript_items,
        reasoning_items,
    )
    root: Span = {
        "name": "codex.interaction",
        "trace_id": trace_id,
        "span_id": root_span_id,
        "parent_span_id": None,
        "start_unix_nano": first_ns,
        "end_unix_nano": last_ns,
        "attributes": {
            "source": "codex",
            "session.id": session_id,
            "entrypoint": session_source,
            "codex.originator": originator,
            "session.agent_version": cli_version,
            "session.cwd": cwd,
            "gen_ai.request.model": model,
            "codex.reasoning_items": reasoning_items,
        },
        "input": root_input,
        "output": root_output,
        "status": "OK",
    }

    # Sort tools by start to keep a stable order (deterministic for
    # idempotent byte-identical dry-runs).
    tool_spans.sort(key=lambda s: (s["start_unix_nano"], s["span_id"]))
    return [root, *turn_spans, *tool_spans]


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


def _first_session_meta(events: list[dict[str, Any]]) -> dict[str, Any]:
    for ev in events:
        if ev.get("type") == "session_meta":
            p = ev.get("payload") or {}
            if isinstance(p, dict):
                return p
    return {}


def _first_model(events: list[dict[str, Any]]) -> str | None:
    for ev in events:
        if ev.get("type") == "turn_context":
            p = ev.get("payload") or {}
            if isinstance(p, dict) and p.get("model"):
                return p.get("model")
    return None


def _session_id(events: list[dict[str, Any]], path: Path) -> str:
    # Prefer the session_meta.id — parent-fork sessions write multiple metas,
    # the first is the current session's own id. Fall back to filename stem.
    meta = _first_session_meta(events)
    sid = meta.get("id")
    if isinstance(sid, str) and sid:
        return sid
    return path.stem


def _ts_ns(ts: str | None) -> int | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    # Integer microsecond path — float `dt.timestamp() * 1e9` drops sub-ms
    # precision (INT-436 parity regression). Keep this in sync with the
    # Claude Code mapper.
    epoch_utc = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt - epoch_utc
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _trace_id(session_id: str) -> str:
    return hashlib.sha256(f"codex:{session_id}".encode()).hexdigest()[:32]


def trace_id_for_session(session_id: str) -> str:
    """Return the deterministic Codex trace id used by the rollout mapper."""
    return _trace_id(session_id)


def _span_id(session_id: str, suffix: str) -> str:
    return hashlib.sha256(
        f"codex:{session_id}:{suffix}".encode()
    ).hexdigest()[:16]


def _turn_span(
    index: int,
    start_ns: int,
    end_ns: int,
    last_usage: dict[str, Any],
    session_id: str,
    trace_id: str,
    root_span_id: str,
    model: str | None,
    input_payload: dict[str, Any],
    output_payload: dict[str, Any],
) -> Span:
    total_input = int(last_usage.get("input_tokens") or 0)
    cached = int(last_usage.get("cached_input_tokens") or 0)
    # OpenAI usage semantics: input_tokens INCLUDES the cached subset.
    # The store's vocabulary is exclusive (Anthropic-style): input_tokens
    # is the non-cached portion, cache reads ride separately — so cache-hit
    # math is uniform across sources. Verified empirically: cached never
    # exceeds input in raw rollouts (subset), while Claude cache reads
    # routinely exceed input (disjoint).
    input_tokens = max(total_input - cached, 0)
    output_tokens = int(last_usage.get("output_tokens") or 0)
    reasoning_tokens = int(last_usage.get("reasoning_output_tokens") or 0)
    span = {
        "name": "codex.llm_request",
        "trace_id": trace_id,
        "span_id": _span_id(session_id, f"turn:{index}"),
        "parent_span_id": root_span_id,
        "start_unix_nano": start_ns,
        "end_unix_nano": end_ns,
        "attributes": {
            "gen_ai.system": "openai",
            "gen_ai.request.model": model,
            # `cached_input_tokens` is a count of cached prompt tokens that
            # were READ on this request. Codex does not surface a separate
            # cache-creation count, so we leave that key unset.
            "gen_ai.usage.input_tokens": input_tokens,
            "gen_ai.usage.output_tokens": output_tokens,
            "gen_ai.usage.cache_read_input_tokens": cached,
            # Codex-specific: reasoning tokens are OUTPUT tokens spent in
            # private chain-of-thought. Namespaced so the generic
            # `gen_ai.usage.output_tokens` retains its cross-source meaning.
            "turn.reasoning_tokens": reasoning_tokens,
            "turn.index": index,
        },
        "status": "OK",
    }
    if input_payload:
        span["input"] = input_payload
    if output_payload:
        span["output"] = output_payload
    return span


def _tool_span(
    session_id: str,
    trace_id: str,
    call_id: str,
    entry: dict[str, Any],
    end_ns: int,
    output: Any,
) -> Span:
    result_chars = _result_chars(output, entry.get("exec_output"))
    is_error = _is_error(entry)
    start_ns = entry["start_ns"]
    # Prefer the exec_command_end duration when present (it's the real
    # process wall time; function_call_output can lag or be missing).
    exec_duration_ns = entry.get("exec_duration_ns")
    if exec_duration_ns:
        duration_ms = exec_duration_ns // 1_000_000
    else:
        duration_ms = max(0, (end_ns - start_ns) // 1_000_000)
    arguments = _coerce_args(entry["arguments"])[:_TOOL_PAYLOAD_MAX]
    display_output = _coerce_output(output, entry.get("exec_output"))[:_TOOL_PAYLOAD_MAX]
    return {
        "name": "codex.tool",
        "trace_id": trace_id,
        "span_id": _span_id(session_id, f"tool:{call_id}"),
        # parent_span_id set in second pass (needs turn ids)
        "_parent_start_ns": start_ns,
        "parent_span_id": None,
        "start_unix_nano": start_ns,
        "end_unix_nano": end_ns,
        "attributes": {
            "tool.name": entry["name"],
            "tool.arguments": arguments,
            "tool.duration_ms": duration_ms,
            "tool.is_error": is_error,
            "tool.result_chars": result_chars,
        },
        "input": {
            "call_id": call_id,
            "name": entry["name"],
            "arguments": _json_or_text(arguments),
        },
        "output": display_output,
        "status": "ERROR" if is_error else "OK",
    }


def _find_turn_for(turn_spans: list[Span], ts_ns: int) -> Span | None:
    for turn in turn_spans:
        if turn["start_unix_nano"] <= ts_ns <= turn["end_unix_nano"]:
            return turn
    # Fall back to the nearest turn by end time (tools issued at the very
    # end of the final model response can land after the last token_count).
    if turn_spans:
        after = [t for t in turn_spans if t["end_unix_nano"] >= ts_ns]
        if after:
            return after[0]
        return turn_spans[-1]
    return None


def _is_error(entry: dict[str, Any]) -> bool:
    if "exit_code" in entry and entry.get("exit_code") not in (0, None):
        return True
    if entry.get("kind") == "custom_tool_call" and entry.get("success") is False:
        return True
    return False


def _result_chars(output: Any, exec_output: Any) -> int:
    # Prefer the raw tool output string length. For exec_command the
    # `function_call_output.output` is a wrapper ("Wall time...", "Output:..."),
    # whereas `exec_command_end.aggregated_output` is the clean stdout/stderr
    # — we pick the larger of the two so the metric reflects actual bytes
    # the model consumed.
    def _len(x: Any) -> int:
        if isinstance(x, str):
            return len(x)
        if isinstance(x, (list, dict)):
            return len(json.dumps(x))
        return 0

    return max(_len(output), _len(exec_output))


def _coerce_args(args: Any) -> str:
    if isinstance(args, str):
        return args
    try:
        return json.dumps(args)
    except (TypeError, ValueError):
        return str(args)


def _coerce_output(output: Any, exec_output: Any) -> str:
    if isinstance(output, str) and output:
        return output
    if isinstance(exec_output, str) and exec_output:
        return exec_output
    if isinstance(output, (list, dict)):
        return json.dumps(output)
    if isinstance(exec_output, (list, dict)):
        return json.dumps(exec_output)
    return ""


def _transcript_item(payload: dict[str, Any], ts_ns: int | None) -> dict[str, Any] | None:
    pt = payload.get("type")
    if pt == "user_message":
        text = payload.get("message")
        if isinstance(text, str) and text:
            return _message_item(ts_ns, "user", text, "event_msg.user_message")
        return None
    if pt == "agent_message":
        text = payload.get("message")
        if isinstance(text, str) and text:
            item = _message_item(ts_ns, "assistant", text, "event_msg.agent_message")
            phase = payload.get("phase")
            if isinstance(phase, str) and phase:
                item["phase"] = phase
            return item
        return None
    if pt == "message":
        role = payload.get("role")
        if role not in ("user", "assistant"):
            return None
        text = _content_text(payload.get("content"))
        if not text:
            return None
        item = _message_item(ts_ns, role, text, "response_item.message")
        phase = payload.get("phase")
        if isinstance(phase, str) and phase:
            item["phase"] = phase
        return item
    if pt in ("function_call", "custom_tool_call"):
        call_id = payload.get("call_id")
        name = payload.get("name")
        args = payload.get("arguments") if pt == "function_call" else payload.get("input")
        if not isinstance(call_id, str) or not isinstance(name, str):
            return None
        return {
            "ts_ns": ts_ns,
            "direction": "output",
            "type": "tool_call",
            "call_id": call_id,
            "name": name,
            "arguments": _json_or_text(_coerce_args(args)[:_TOOL_PAYLOAD_MAX]),
        }
    if pt in ("function_call_output", "custom_tool_call_output"):
        call_id = payload.get("call_id")
        output = payload.get("output")
        if not isinstance(call_id, str):
            return None
        return {
            "ts_ns": ts_ns,
            "direction": "input",
            "type": "tool_result",
            "call_id": call_id,
            "output": _truncate_text(_coerce_output(output, None), _TOOL_PAYLOAD_MAX),
        }
    return None


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


def _turn_payloads(
    items: list[dict[str, Any]],
    start_ns: int | None,
    end_ns: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    selected = [
        item
        for item in items
        if item.get("ts_ns") is not None
        and (start_ns is None or int(item["ts_ns"]) > start_ns)
        and int(item["ts_ns"]) <= end_ns
    ]
    inputs = [_strip_internal(item) for item in selected if item["direction"] == "input"]
    outputs = [_strip_internal(item) for item in selected if item["direction"] == "output"]
    input_payload = {"messages": inputs} if inputs else {}
    output_payload = {"messages": outputs} if outputs else {}
    return input_payload, output_payload


def _root_payloads(
    session_id: str,
    items: list[dict[str, Any]],
    reasoning_items: int,
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
        "counts": {
            "user_requests": len(user_requests),
        },
        "session_id": session_id,
        "user_requests": _bounded_items(user_requests),
    }
    root_output: dict[str, Any] = {
        "counts": {
            "assistant_messages": len(assistant_messages),
            "tool_calls": len(tool_calls),
            "tool_outputs": len(tool_results),
            "opaque_reasoning_items": reasoning_items,
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
        if block.get("type") in {"reasoning", "encrypted_reasoning"}:
            continue
        for key in ("text", "content", "input_text", "output_text"):
            value = block.get(key)
            if isinstance(value, str) and value:
                parts.append(value)
                break
    return "\n".join(parts)


def _json_or_text(value: str) -> Any:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value


def _truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + f"\n...[truncated {len(value) - max_chars} chars]"


# ---------------------------------------------------------------------------
# Full-fidelity extraction


def extract_contents(path: Path, session_id: str) -> list[ContentRow]:
    rows: list[ContentRow] = []
    seq = 0

    def add(span_id: str | None, kind: ContentKind, text: str, ts: int | None) -> None:
        nonlocal seq
        if not text:
            return
        rows.append(ContentRow(span_id=span_id, kind=kind, seq=seq, text=text, ts_ns=ts))
        seq += 1

    for ev in read_jsonl(path):
        ts = iso_ts_ns(ev.get("timestamp"))
        p = ev.get("payload")
        if not isinstance(p, dict):
            continue
        pt = p.get("type")

        if pt == "user_message":
            text = p.get("message")
            if isinstance(text, str):
                add(None, "user_message", text.strip(), ts)
        elif pt == "agent_message":
            # Skip streaming "commentary" fragments; keep final messages.
            if p.get("phase") in (None, "final"):
                text = p.get("message")
                if isinstance(text, str):
                    add(None, "assistant_message", text.strip(), ts)
        elif pt in ("function_call", "custom_tool_call"):
            call_id = p.get("call_id")
            args = p.get("arguments") if pt == "function_call" else p.get("input")
            if isinstance(call_id, str):
                add(
                    _span_id(session_id, f"tool:{call_id}"),
                    "tool_arguments",
                    args if isinstance(args, str) else json_text(args),
                    ts,
                )
        elif pt in ("function_call_output", "custom_tool_call_output"):
            call_id = p.get("call_id")
            output = p.get("output")
            if isinstance(call_id, str):
                add(
                    _span_id(session_id, f"tool:{call_id}"),
                    "tool_result",
                    output if isinstance(output, str) else json_text(output),
                    ts,
                )

    return rows


# ---------------------------------------------------------------------------
# Cheap pre-parse probe


def probe(path: Path) -> dict[str, Any]:
    """Sub-agent hierarchy hints from the session_meta line."""
    # session_meta.source.subagent.thread_spawn.parent_thread_id links a
    # spawned Codex thread to its parent session.
    for index, event in enumerate(iter_jsonl_objects(path)):
        if index >= 5:
            break
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
    return {}


# ---------------------------------------------------------------------------
# Discovery

DEFAULT_CODEX_SESSIONS_ROOT = Path.home() / ".codex" / "sessions"
DEFAULT_CODEX_ARCHIVED_ROOT = Path.home() / ".codex" / "archived_sessions"


class CodexRolloutSource:
    """Discover Codex rollout JSONL files under session roots."""

    source_type = "codex"

    def __init__(
        self,
        roots: Sequence[Path | str] | None = None,
        *,
        include_archived: bool = False,
        archived_root: Path | str = DEFAULT_CODEX_ARCHIVED_ROOT,
    ) -> None:
        self.roots = tuple(
            Path(root).expanduser().resolve(strict=False)
            for root in (roots or (DEFAULT_CODEX_SESSIONS_ROOT,))
        )
        self.include_archived = include_archived
        self.archived_root = Path(archived_root).expanduser().resolve(strict=False)

    def discover(self) -> Iterable[DiscoveredTranscript]:
        for path in unique_sorted(self._candidate_paths()):
            metadata = read_rollout_metadata(path)
            session_id = as_string(metadata.get("session_id")) or path.stem
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
        if self.include_archived:
            yield from jsonl_paths(self.archived_root, recursive=False)


def read_rollout_metadata(path: Path, *, max_lines: int = 200) -> dict[str, Any]:
    """Extract cheap Codex metadata without running the full mapper."""
    metadata: dict[str, Any] = {}
    for index, obj in enumerate(iter_jsonl_objects(path)):
        if index >= max_lines:
            break
        payload = obj.get("payload")
        if not isinstance(payload, dict):
            continue

        if obj.get("type") == "session_meta":
            _merge_session_meta(metadata, payload)
            continue

        if obj.get("type") == "turn_context":
            model = payload.get("model")
            if isinstance(model, str) and model:
                metadata.setdefault("model", model)

        if metadata.get("session_id") and metadata.get("model"):
            break

    return metadata


def _merge_session_meta(metadata: dict[str, Any], payload: dict[str, Any]) -> None:
    session_id = payload.get("id")
    if isinstance(session_id, str) and session_id:
        metadata.setdefault("session_id", session_id)

    for source_key, metadata_key in (
        ("originator", "originator"),
        ("cli_version", "cli_version"),
        ("cwd", "cwd"),
        ("model_provider", "model_provider"),
    ):
        value = payload.get(source_key)
        if isinstance(value, str) and value:
            metadata.setdefault(metadata_key, value)

    surface = _surface(payload.get("source"))
    if surface:
        metadata.setdefault("source", surface)
        metadata.setdefault("surface", surface)


def _surface(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict):
        if "subagent" in value:
            return "subagent"
        for key, nested in value.items():
            if isinstance(key, str) and nested:
                return key
    return None




# ---------------------------------------------------------------------------
# Tool vocabulary (for navigation-time attribution)
#
# Codex works almost entirely through the shell: exec_command carries both
# navigation (`rg`, `cat`) and real work (`forge test`), so the command text
# decides. apply_patch is the edit primitive.

_SHELL_TOOLS = {"exec_command", "exec", "write_stdin", "shell"}
_EDIT_TOOLS = {"apply_patch"}
_SUBAGENT_TOOLS = {"spawn_agent", "wait_agent"}


def classify_tool(name: str | None, args_preview: str | None) -> str:
    """Classify one tool call as navigation / editing / subagent / other."""
    if name in _SHELL_TOOLS:
        return "navigation" if is_nav_shell(args_preview) else "bash-other"
    if name in _EDIT_TOOLS:
        return "editing"
    if name in _SUBAGENT_TOOLS:
        return "subagent"
    if name == "read_file":
        return "navigation"
    return "other"


def make_source(
    roots=None,
    include_archived: bool = False,
    archived_root=None,
    **_ignored,
) -> CodexRolloutSource:
    """Build this source's discovery. Keyword options come from the CLI's
    flags, or from a `[sources]` table for a config-declared source."""
    return CodexRolloutSource(
        roots,
        include_archived=bool(include_archived),
        archived_root=archived_root or DEFAULT_CODEX_ARCHIVED_ROOT,
    )


# The module owns its adapter: `flume.sources` names this module in its
# registry and imports it only when something resolves "codex".
ADAPTER = SourceAdapter(
    name="codex",
    vendor="openai",
    map_spans=rollout_to_spans,
    extract_contents=extract_contents,
    probe=probe,
    classify_tool=classify_tool,
)
