"""Tests for the Claude Code JSONL → OTel span mapper.

Exercises the shape that downstream analysis depends on: one trace per
transcript, the root/turn/tool hierarchy, deterministic IDs, and the
attributes that reproduce `analyze_sessions.py` metrics (token breakdown,
duration, result_chars, is_error).
"""
from __future__ import annotations

import json
from pathlib import Path

from agent_telemetry.backfill.claude_code import jsonl_to_spans


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


def _fixture_events() -> list[dict]:
    # Mirrors the real Claude Code JSONL ordering: `turn_duration` emits AFTER
    # the assistant turn it measures (parity check INT-436 caught the
    # opposite-order assumption previously encoded here).
    return [
        {
            "type": "assistant",
            "uuid": "asst-1",
            "timestamp": "2026-04-20T10:00:01.500Z",
            "entrypoint": "cli",
            "version": "2.1.114",
            "gitBranch": "main",
            "message": {
                "id": "msg_01",
                "model": "claude-opus-4-7",
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cache_read_input_tokens": 900,
                    "cache_creation_input_tokens": 10,
                },
                "content": [
                    {"type": "thinking", "thinking": "abcdef"},
                    {"type": "text", "text": "hello world"},
                    {
                        "type": "tool_use",
                        "id": "tu_1",
                        "name": "Read",
                        "input": {"file_path": "/tmp/a.py"},
                    },
                ],
            },
        },
        # turn_duration for asst-1, emitted AFTER the turn it measures.
        {
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:01.510Z",
            "durationMs": 1500,
        },
        # Tool result matching tu_1, 2s after the tool_use.
        {
            "type": "user",
            "timestamp": "2026-04-20T10:00:03.500Z",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tu_1",
                        "is_error": False,
                        "content": "x" * 1024,
                    }
                ]
            },
        },
        # Second turn, no tool_use, error-free.
        {
            "type": "assistant",
            "uuid": "asst-2",
            "timestamp": "2026-04-20T10:00:04.500Z",
            "message": {
                "usage": {"input_tokens": 20, "output_tokens": 5},
                "content": [{"type": "text", "text": "done"}],
            },
        },
        {
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:04.510Z",
            "durationMs": 500,
        },
    ]


def test_root_span_covers_whole_session(tmp_path: Path) -> None:
    session_id = "abc-123"
    path = tmp_path / f"{session_id}.jsonl"
    _write_jsonl(path, _fixture_events())

    spans = jsonl_to_spans(path)
    root = spans[0]

    assert root["name"] == "claude_code.interaction"
    assert root["parent_span_id"] is None
    assert root["attributes"]["session.id"] == session_id
    assert root["attributes"]["entrypoint"] == "cli"
    assert root["attributes"]["claude_code.version"] == "2.1.114"
    assert root["attributes"]["git.branch"] == "main"
    assert root["attributes"]["source"] == "claude-code"
    assert root["attributes"]["langfuse.trace.metadata.agent_source"] == "claude-code"
    assert root["attributes"]["langfuse.trace.metadata.agent_family"] == "claude-code"
    assert root["attributes"]["langfuse.trace.tags"] == [
        "agent:claude-code",
        "family:claude-code",
    ]
    # Root covers the retroactively shifted first turn and the session tail.
    assert root["start_unix_nano"] < root["end_unix_nano"]


def test_root_start_covers_retroactively_shifted_turn(tmp_path: Path) -> None:
    events = [
        {
            "type": "assistant",
            "uuid": "asst-retro",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "message": {
                "usage": {"input_tokens": 10, "output_tokens": 5},
                "content": [{"type": "text", "text": "done"}],
            },
        },
        {
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:00.010Z",
            "durationMs": 1500,
        },
    ]
    path = tmp_path / "retro.jsonl"
    _write_jsonl(path, events)

    spans = jsonl_to_spans(path)
    root = spans[0]
    children = spans[1:]

    assert children
    assert root["start_unix_nano"] <= min(s["start_unix_nano"] for s in children)


def test_turn_span_carries_usage_and_duration(tmp_path: Path) -> None:
    path = tmp_path / "s.jsonl"
    _write_jsonl(path, _fixture_events())

    spans = jsonl_to_spans(path)
    turns = [s for s in spans if s["name"] == "claude_code.llm_request"]
    assert len(turns) == 2

    first = turns[0]
    attrs = first["attributes"]
    assert attrs["langfuse.trace.metadata.agent_source"] == "claude-code"
    assert attrs["langfuse.trace.metadata.agent_family"] == "claude-code"
    assert attrs["gen_ai.usage.input_tokens"] == 100
    assert attrs["gen_ai.usage.output_tokens"] == 50
    assert attrs["gen_ai.usage.cache_read_input_tokens"] == 900
    assert attrs["gen_ai.usage.cache_creation_input_tokens"] == 10
    assert attrs["gen_ai.request.model"] == "claude-opus-4-7"
    assert attrs["claude_code.thinking_chars"] == len("abcdef")
    assert attrs["claude_code.text_chars"] == len("hello world")
    assert attrs["claude_code.duration_ms"] == 1500
    # start = end - duration
    assert first["end_unix_nano"] - first["start_unix_nano"] == 1500 * 1_000_000


def test_tool_span_is_nested_under_turn_with_timing_and_result_chars(
    tmp_path: Path,
) -> None:
    path = tmp_path / "s.jsonl"
    _write_jsonl(path, _fixture_events())

    spans = jsonl_to_spans(path)
    tools = [s for s in spans if s["name"] == "claude_code.tool"]
    turns = [s for s in spans if s["name"] == "claude_code.llm_request"]
    assert len(tools) == 1
    tool = tools[0]

    assert tool["parent_span_id"] == turns[0]["span_id"]
    assert tool["attributes"]["tool.name"] == "Read"
    assert tool["attributes"]["tool.result_chars"] == 1024
    assert tool["attributes"]["tool.is_error"] is False
    # 2000 ms between tool_use (on assistant event) and tool_result.
    assert tool["attributes"]["tool.duration_ms"] == 2000
    assert "/tmp/a.py" in tool["attributes"]["tool.arguments"]


def test_error_tool_result_marks_span_status(tmp_path: Path) -> None:
    events = [
        {
            "type": "assistant",
            "uuid": "a",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "message": {
                "usage": {},
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tu_x",
                        "name": "Bash",
                        "input": {"command": "false"},
                    }
                ],
            },
        },
        {
            "type": "user",
            "timestamp": "2026-04-20T10:00:00.200Z",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tu_x",
                        "is_error": True,
                        "content": "boom",
                    }
                ]
            },
        },
    ]
    path = tmp_path / "e.jsonl"
    _write_jsonl(path, events)
    spans = jsonl_to_spans(path)
    tool = next(s for s in spans if s["name"] == "claude_code.tool")
    assert tool["status"] == "ERROR"
    assert tool["attributes"]["tool.is_error"] is True


def test_ids_are_deterministic(tmp_path: Path) -> None:
    path1 = tmp_path / "same.jsonl"
    path2 = tmp_path / "same.jsonl.copy"
    _write_jsonl(path1, _fixture_events())
    # Same session_id (filename stem) on both sides.
    path2 = tmp_path / "copy" / "same.jsonl"
    path2.parent.mkdir()
    _write_jsonl(path2, _fixture_events())

    a = jsonl_to_spans(path1)
    b = jsonl_to_spans(path2)
    assert [s["trace_id"] for s in a] == [s["trace_id"] for s in b]
    assert [s["span_id"] for s in a] == [s["span_id"] for s in b]


def test_empty_transcript_returns_empty(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("")
    assert jsonl_to_spans(path) == []


def test_unmatched_tool_result_is_dropped(tmp_path: Path) -> None:
    events = [
        {
            "type": "user",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "no-such-use",
                        "content": "orphan",
                    }
                ]
            },
        }
    ]
    path = tmp_path / "orphan.jsonl"
    _write_jsonl(path, events)
    spans = jsonl_to_spans(path)
    # Only the root interaction; no tool span for the orphan result.
    assert [s["name"] for s in spans] == ["claude_code.interaction"]
