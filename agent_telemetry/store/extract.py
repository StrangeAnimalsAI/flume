"""Full-fidelity content extraction from raw session files.

The backfill mappers deliberately redact thinking text and cap payloads at
the 60 KB live-OTel limit so backfill and live Langfuse traces stay
comparable. The store has no such constraint — audits need the complete
internal thought process and untruncated tool I/O. These extractors walk
the raw files a second time and emit `ContentRow`s keyed by the SAME
deterministic span ids the mappers produce, so full text joins directly
onto the metrics skeleton.

Codex note: rollout `reasoning` items carry only encrypted blobs — there is
no plaintext to extract, so Codex sessions get messages and tool payloads
but no thinking rows. That is a source limitation, not a store choice.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from agent_telemetry.store.base import ContentRow


def _claude_span_id(session_id: str, suffix: str) -> str:
    # Mirrors agent_telemetry.backfill.claude_code._span_id — keep in sync.
    return hashlib.sha256(
        f"claude-code:{session_id}:{suffix}".encode()
    ).hexdigest()[:16]


def _codex_span_id(session_id: str, suffix: str) -> str:
    # Mirrors agent_telemetry.backfill.codex._span_id — keep in sync.
    return hashlib.sha256(f"codex:{session_id}:{suffix}".encode()).hexdigest()[:16]


def extract_claude_contents(path: Path, session_id: str) -> list[ContentRow]:
    rows: list[ContentRow] = []
    seq = 0

    def add(span_id: str | None, kind: str, text: str, ts_ns: int | None) -> None:
        nonlocal seq
        if not text:
            return
        rows.append(ContentRow(span_id=span_id, kind=kind, seq=seq, text=text, ts_ns=ts_ns))
        seq += 1

    for ev in _read_jsonl(path):
        ts_ns = _ts_ns(ev.get("timestamp"))
        t = ev.get("type")
        msg = ev.get("message")
        if not isinstance(msg, dict):
            continue
        content = msg.get("content")

        if t == "assistant":
            turn_uuid = ev.get("uuid") or (str(ts_ns) if ts_ns is not None else "")
            turn_span = _claude_span_id(session_id, f"turn:{turn_uuid}")
            if isinstance(content, str):
                add(turn_span, "assistant_message", content.strip(), ts_ns)
                continue
            if not isinstance(content, list):
                continue
            for blk in content:
                if not isinstance(blk, dict):
                    continue
                bt = blk.get("type")
                if bt == "thinking":
                    add(turn_span, "thinking", blk.get("thinking") or "", ts_ns)
                elif bt == "text":
                    add(turn_span, "assistant_message", (blk.get("text") or "").strip(), ts_ns)
                elif bt == "tool_use":
                    tid = blk.get("id")
                    if isinstance(tid, str):
                        add(
                            _claude_span_id(session_id, f"tool:{tid}"),
                            "tool_arguments",
                            _json_text(blk.get("input") or {}),
                            ts_ns,
                        )
            continue

        if t == "user":
            if isinstance(content, str):
                add(None, "user_message", content.strip(), ts_ns)
                continue
            if not isinstance(content, list):
                continue
            for blk in content:
                if isinstance(blk, str):
                    add(None, "user_message", blk.strip(), ts_ns)
                    continue
                if not isinstance(blk, dict):
                    continue
                if blk.get("type") == "tool_result":
                    tid = blk.get("tool_use_id")
                    if isinstance(tid, str):
                        add(
                            _claude_span_id(session_id, f"tool:{tid}"),
                            "tool_result",
                            _result_text(blk.get("content")),
                            ts_ns,
                        )
                elif blk.get("type") == "text":
                    add(None, "user_message", (blk.get("text") or "").strip(), ts_ns)

    return rows


def extract_codex_contents(path: Path, session_id: str) -> list[ContentRow]:
    rows: list[ContentRow] = []
    seq = 0

    def add(span_id: str | None, kind: str, text: str, ts_ns: int | None) -> None:
        nonlocal seq
        if not text:
            return
        rows.append(ContentRow(span_id=span_id, kind=kind, seq=seq, text=text, ts_ns=ts_ns))
        seq += 1

    for ev in _read_jsonl(path):
        ts_ns = _ts_ns(ev.get("timestamp"))
        p = ev.get("payload")
        if not isinstance(p, dict):
            continue
        pt = p.get("type")

        if pt == "user_message":
            text = p.get("message")
            if isinstance(text, str):
                add(None, "user_message", text.strip(), ts_ns)
        elif pt == "agent_message":
            # Skip streaming "commentary" fragments; keep final messages.
            if p.get("phase") in (None, "final"):
                text = p.get("message")
                if isinstance(text, str):
                    add(None, "assistant_message", text.strip(), ts_ns)
        elif pt in ("function_call", "custom_tool_call"):
            call_id = p.get("call_id")
            args = p.get("arguments") if pt == "function_call" else p.get("input")
            if isinstance(call_id, str):
                add(
                    _codex_span_id(session_id, f"tool:{call_id}"),
                    "tool_arguments",
                    args if isinstance(args, str) else _json_text(args),
                    ts_ns,
                )
        elif pt in ("function_call_output", "custom_tool_call_output"):
            call_id = p.get("call_id")
            output = p.get("output")
            if isinstance(call_id, str):
                add(
                    _codex_span_id(session_id, f"tool:{call_id}"),
                    "tool_result",
                    output if isinstance(output, str) else _json_text(output),
                    ts_ns,
                )

    return rows


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        text = path.read_text()
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _ts_ns(ts: Any) -> int | None:
    if not isinstance(ts, str) or not ts:
        return None
    from datetime import datetime, timezone

    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    epoch_utc = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt - epoch_utc
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _result_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for blk in content:
            if isinstance(blk, str):
                parts.append(blk)
            elif isinstance(blk, dict):
                value = blk.get("text")
                if isinstance(value, str) and value:
                    parts.append(value)
        if parts:
            return "\n".join(parts)
    if isinstance(content, (list, dict)):
        return _json_text(content)
    return ""


def _json_text(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except (TypeError, ValueError):
        return str(value)
