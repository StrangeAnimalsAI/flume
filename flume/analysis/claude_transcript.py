"""Source-level metrics for a Claude Code transcript.

The Langfuse parity check needs an independent view of a transcript: deriving
both sides from the OTel span mapper would hide mapper bugs.  This module reads
the canonical JSONL directly and intentionally has no dependency on the
backfill or store pipelines.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

_NAV_TOOLS = frozenset({"Read", "Grep", "Glob", "LS"})


def analyze_session(path: Path) -> dict[str, Any]:
    """Return the metrics used by the Langfuse parity report."""
    events = _read_events(path)
    timestamped = [
        (event, parsed)
        for event in events
        if (parsed := _parse_timestamp(event.get("timestamp"))) is not None
    ]

    first_ts = timestamped[0][0].get("timestamp") if timestamped else None
    last_ts = timestamped[-1][0].get("timestamp") if timestamped else None
    wall_time_s = 0.0
    if timestamped:
        wall_time_s = round((timestamped[-1][1] - timestamped[0][1]).total_seconds(), 1)

    tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_create": 0}
    turns_assistant = 0
    turns_user = 0
    thinking_chars = 0
    text_chars = 0
    entrypoints: Counter[str] = Counter()
    active_ms = 0
    pending_tools: dict[str, dict[str, Any]] = {}
    tool_calls: list[dict[str, Any]] = []

    for event in events:
        event_type = event.get("type")
        if event_type == "system" and event.get("subtype") == "turn_duration":
            active_ms += _int(event.get("durationMs"))
            continue

        entrypoint = event.get("entrypoint")
        if isinstance(entrypoint, str) and entrypoint:
            entrypoints[entrypoint] += 1

        message = event.get("message")
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        blocks = content if isinstance(content, list) else []

        if event_type == "assistant":
            turns_assistant += 1
            usage = message.get("usage")
            usage = usage if isinstance(usage, dict) else {}
            tokens["input"] += _int(usage.get("input_tokens"))
            tokens["output"] += _int(usage.get("output_tokens"))
            tokens["cache_read"] += _int(usage.get("cache_read_input_tokens"))
            tokens["cache_create"] += _int(usage.get("cache_creation_input_tokens"))
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "thinking":
                    thinking_chars += len(str(block.get("thinking") or ""))
                elif block_type == "text":
                    text_chars += len(str(block.get("text") or ""))
                elif block_type == "tool_use":
                    tool_id = block.get("id")
                    if isinstance(tool_id, str) and tool_id:
                        pending_tools[tool_id] = {
                            "name": block.get("name") or "?",
                            "input": block.get("input")
                            if isinstance(block.get("input"), dict)
                            else {},
                            "started_at": _parse_timestamp(event.get("timestamp")),
                        }

        elif event_type == "user":
            turns_user += 1
            ended_at = _parse_timestamp(event.get("timestamp"))
            for block in blocks:
                if not isinstance(block, dict) or block.get("type") != "tool_result":
                    continue
                tool_id = block.get("tool_use_id")
                if not isinstance(tool_id, str):
                    continue
                pending = pending_tools.pop(tool_id, None)
                if pending is None:
                    continue
                started_at = pending.pop("started_at")
                duration_s = 0.0
                if started_at is not None and ended_at is not None:
                    duration_s = max(0.0, (ended_at - started_at).total_seconds())
                tool_calls.append(
                    {
                        **pending,
                        "duration_s": duration_s,
                        "is_error": bool(block.get("is_error")),
                        "result_chars": _result_chars(block.get("content")),
                    }
                )

    by_tool: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "count": 0,
            "total_s": 0.0,
            "errors": 0,
            "total_result_chars": 0,
        }
    )
    repeats: Counter[str] = Counter()
    for call in tool_calls:
        stats = by_tool[call["name"]]
        stats["count"] += 1
        stats["total_s"] += call["duration_s"]
        stats["errors"] += int(call["is_error"])
        stats["total_result_chars"] += call["result_chars"]
        key = repeat_key(call["name"], call["input"])
        if key is not None:
            repeats[" | ".join(str(part) for part in key)] += 1

    total_input = tokens["input"] + tokens["cache_read"] + tokens["cache_create"]
    slowest = sorted(tool_calls, key=lambda call: -call["duration_s"])[:10]
    largest = sorted(tool_calls, key=lambda call: -call["result_chars"])[:10]
    return {
        "session_id": path.stem,
        "entrypoints": dict(entrypoints),
        "first_ts": first_ts,
        "last_ts": last_ts,
        "wall_time_s": wall_time_s,
        "active_time_s": round(active_ms / 1000.0, 1),
        "tool_out_chars": sum(call["result_chars"] for call in tool_calls),
        "nav_chars_all": sum(
            call["result_chars"] for call in tool_calls if is_nav_tool(call["name"])
        ),
        "turns_assistant": turns_assistant,
        "turns_user": turns_user,
        "tool_calls": len(tool_calls),
        "tool_errors": sum(bool(call["is_error"]) for call in tool_calls),
        "tokens": tokens,
        "cache_hit_ratio": round(tokens["cache_read"] / total_input, 4)
        if total_input
        else 0.0,
        "thinking_chars": thinking_chars,
        "text_out_chars": text_chars,
        "by_tool": {name: dict(stats) for name, stats in by_tool.items()},
        "repeats": {key: count for key, count in repeats.items() if count > 1},
        "slowest_tools": [_tool_summary(call, include_error=True) for call in slowest],
        "largest_results": [
            _tool_summary(call, include_error=False) for call in largest
        ],
    }


def repeat_key(name: str, arguments: dict[str, Any]) -> tuple[str, str] | None:
    """Stable identity for exact repeated calls."""
    try:
        encoded = json.dumps(arguments, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return None
    return name, encoded


def is_nav_tool(name: str) -> bool:
    return name in _NAV_TOOLS


def summarize_input(name: str, arguments: dict[str, Any]) -> str:
    """Return a compact, deterministic description for a tool call."""
    preferred_keys = {
        "Read": ("file_path", "path"),
        "Grep": ("pattern", "path"),
        "Glob": ("pattern", "path"),
        "LS": ("path",),
        "Bash": ("command",),
    }.get(name, ())
    parts = [str(arguments[key]) for key in preferred_keys if arguments.get(key)]
    if parts:
        return " ".join(parts)[:200]
    try:
        return json.dumps(arguments, sort_keys=True, separators=(",", ":"))[:200]
    except (TypeError, ValueError):
        return "{}"


def _tool_summary(call: dict[str, Any], *, include_error: bool) -> dict[str, Any]:
    result = {
        "name": call["name"],
        "duration_s": round(call["duration_s"], 2),
        "result_chars": call["result_chars"],
        "summary": summarize_input(call["name"], call["input"]),
    }
    if include_error:
        result["is_error"] = call["is_error"]
    return result


def _read_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open() as transcript:
        for line in transcript:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
    return events


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _result_chars(content: Any) -> int:
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        return sum(
            len(str(item.get("text") or ""))
            for item in content
            if isinstance(item, dict)
        )
    return 0
