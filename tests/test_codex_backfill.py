"""Tests for the Codex rollout → OTel span mapper.

Synthetic fixtures mirror the shapes observed in real
`~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl` files: session_meta first,
`task_started`/`task_complete` brackets, per-response `token_count` events
with `info.last_token_usage`, and `function_call`/`function_call_output`
pairs (plus `exec_command_end` for the shell path).
"""
from __future__ import annotations

import json
from pathlib import Path

from agent_telemetry.backfill.codex import rollout_to_spans


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


def _meta(session_id: str = "019d-session-uuid") -> dict:
    return {
        "timestamp": "2026-04-20T10:00:00.000Z",
        "type": "session_meta",
        "payload": {
            "id": session_id,
            "timestamp": "2026-04-20T10:00:00.000Z",
            "cwd": "/Users/james/Code/sample",
            "originator": "Codex Desktop",
            "cli_version": "0.124.0-alpha.2",
            "source": "vscode",
            "model_provider": "openai",
        },
    }


def _turn_context() -> dict:
    return {
        "timestamp": "2026-04-20T10:00:00.010Z",
        "type": "turn_context",
        "payload": {
            "turn_id": "turn-1",
            "cwd": "/Users/james/Code/sample",
            "model": "gpt-5.4",
            "effort": "xhigh",
        },
    }


def _fixture_events() -> list[dict]:
    return [
        _meta(),
        {
            "timestamp": "2026-04-20T10:00:00.005Z",
            "type": "event_msg",
            "payload": {
                "type": "task_started",
                "turn_id": "turn-1",
                "model_context_window": 258400,
            },
        },
        _turn_context(),
        {
            "timestamp": "2026-04-20T10:00:00.020Z",
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "do a thing"},
        },
        # Pre-call snapshot (null info) — not a boundary.
        {
            "timestamp": "2026-04-20T10:00:00.030Z",
            "type": "event_msg",
            "payload": {"type": "token_count", "info": None},
        },
        # First model response: reasoning + one exec_command tool_use.
        {
            "timestamp": "2026-04-20T10:00:01.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({"cmd": "ls"}),
                "call_id": "call_A",
            },
        },
        # Token_count ending the first model response — retro-attributes
        # function_call A to this turn.
        {
            "timestamp": "2026-04-20T10:00:01.500Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 100,
                        "cached_input_tokens": 80,
                        "output_tokens": 50,
                        "reasoning_output_tokens": 30,
                        "total_tokens": 150,
                    }
                },
            },
        },
        # exec_command_end arrives after the token_count, carrying the real
        # process wall time (here: 200 ms) and exit_code.
        {
            "timestamp": "2026-04-20T10:00:01.600Z",
            "type": "event_msg",
            "payload": {
                "type": "exec_command_end",
                "call_id": "call_A",
                "command": ["/bin/zsh", "-lc", "ls"],
                "stdout": "",
                "stderr": "",
                "aggregated_output": "a.txt\nb.txt\n",
                "exit_code": 0,
                "duration": {"secs": 0, "nanos": 200_000_000},
            },
        },
        # function_call_output closes the tool span at 1.7s.
        {
            "timestamp": "2026-04-20T10:00:01.700Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_A",
                "output": "Wall time: 0.2s\nOutput:\na.txt\nb.txt\n",
            },
        },
        # Second model response: one more tool_use, then the token_count
        # closing this turn.
        {
            "timestamp": "2026-04-20T10:00:02.100Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "mcp__linear__get_issue",
                "arguments": json.dumps({"id": "INT-1"}),
                "call_id": "call_B",
            },
        },
        {
            "timestamp": "2026-04-20T10:00:02.250Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_B",
                "output": '[{"type":"text","text":"issue details..."}]',
            },
        },
        {
            "timestamp": "2026-04-20T10:00:02.300Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 250,
                        "cached_input_tokens": 200,
                        "output_tokens": 20,
                        "reasoning_output_tokens": 5,
                        "total_tokens": 270,
                    }
                },
            },
        },
        {
            "timestamp": "2026-04-20T10:00:02.500Z",
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": "turn-1"},
        },
    ]


def test_root_span_covers_whole_rollout(tmp_path: Path) -> None:
    path = tmp_path / "rollout.jsonl"
    _write_jsonl(path, _fixture_events())
    spans = rollout_to_spans(path)

    root = spans[0]
    assert root["name"] == "codex.interaction"
    assert root["parent_span_id"] is None
    assert root["attributes"]["source"] == "codex"
    assert root["attributes"]["session.id"] == "019d-session-uuid"
    assert root["attributes"]["entrypoint"] == "vscode"
    assert root["attributes"]["codex.originator"] == "Codex Desktop"
    assert root["attributes"]["gen_ai.request.model"] == "gpt-5.4"
    assert root["start_unix_nano"] < root["end_unix_nano"]


def test_turn_span_carries_usage_and_reasoning(tmp_path: Path) -> None:
    path = tmp_path / "rollout.jsonl"
    _write_jsonl(path, _fixture_events())
    spans = rollout_to_spans(path)

    turns = [s for s in spans if s["name"] == "codex.llm_request"]
    assert len(turns) == 2

    first = turns[0]
    a = first["attributes"]
    assert a["gen_ai.system"] == "openai"
    assert a["gen_ai.request.model"] == "gpt-5.4"
    assert a["gen_ai.usage.input_tokens"] == 100
    assert a["gen_ai.usage.output_tokens"] == 50
    assert a["gen_ai.usage.cache_read_input_tokens"] == 80
    assert a["codex.reasoning_tokens"] == 30
    assert a["codex.turn_index"] == 0

    second = turns[1]
    assert second["attributes"]["gen_ai.usage.input_tokens"] == 250
    assert second["attributes"]["codex.reasoning_tokens"] == 5
    assert second["attributes"]["codex.turn_index"] == 1


def test_tool_spans_nest_under_correct_turn_with_exec_duration(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollout.jsonl"
    _write_jsonl(path, _fixture_events())
    spans = rollout_to_spans(path)

    turns = [s for s in spans if s["name"] == "codex.llm_request"]
    tools = [s for s in spans if s["name"] == "codex.tool"]
    assert len(tools) == 2
    by_name = {t["attributes"]["tool.name"]: t for t in tools}

    # exec_command fired during the first model response (before the first
    # token_count at 1.5s), so it parents to the first turn.
    assert by_name["exec_command"]["parent_span_id"] == turns[0]["span_id"]
    # exec_command_end carried a 200 ms real duration — that wins over the
    # function_call_output timestamp gap.
    assert by_name["exec_command"]["attributes"]["tool.duration_ms"] == 200
    assert by_name["exec_command"]["attributes"]["tool.is_error"] is False
    # result_chars prefers the larger of output vs. aggregated_output.
    assert (
        by_name["exec_command"]["attributes"]["tool.result_chars"]
        >= len("a.txt\nb.txt\n")
    )
    assert "ls" in by_name["exec_command"]["attributes"]["tool.arguments"]

    # MCP tool fired during the second model response → parents to turn 2.
    mcp = by_name["mcp__linear__get_issue"]
    assert mcp["parent_span_id"] == turns[1]["span_id"]
    assert mcp["attributes"]["tool.duration_ms"] == 150


def test_exec_error_sets_error_status(tmp_path: Path) -> None:
    events = [
        _meta("err-session"),
        {
            "timestamp": "2026-04-20T10:00:00.005Z",
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": "t"},
        },
        _turn_context(),
        {
            "timestamp": "2026-04-20T10:00:01.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({"cmd": "false"}),
                "call_id": "call_err",
            },
        },
        {
            "timestamp": "2026-04-20T10:00:01.500Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                    }
                },
            },
        },
        {
            "timestamp": "2026-04-20T10:00:01.600Z",
            "type": "event_msg",
            "payload": {
                "type": "exec_command_end",
                "call_id": "call_err",
                "command": ["/bin/zsh", "-lc", "false"],
                "aggregated_output": "",
                "exit_code": 1,
                "duration": {"secs": 0, "nanos": 5_000_000},
            },
        },
        {
            "timestamp": "2026-04-20T10:00:01.700Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_err",
                "output": "exit_code=1",
            },
        },
    ]
    path = tmp_path / "rollout-err.jsonl"
    _write_jsonl(path, events)
    spans = rollout_to_spans(path)
    tool = next(s for s in spans if s["name"] == "codex.tool")
    assert tool["attributes"]["tool.is_error"] is True
    assert tool["status"] == "ERROR"


def test_ids_are_deterministic(tmp_path: Path) -> None:
    p1 = tmp_path / "a.jsonl"
    p2 = tmp_path / "copy" / "a.jsonl"
    p2.parent.mkdir()
    _write_jsonl(p1, _fixture_events())
    _write_jsonl(p2, _fixture_events())
    a = rollout_to_spans(p1)
    b = rollout_to_spans(p2)
    assert [s["trace_id"] for s in a] == [s["trace_id"] for s in b]
    assert [s["span_id"] for s in a] == [s["span_id"] for s in b]


def test_empty_rollout_returns_empty(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("")
    assert rollout_to_spans(path) == []


def test_unmatched_tool_result_is_dropped(tmp_path: Path) -> None:
    events = [
        _meta("orphan-session"),
        _turn_context(),
        {
            "timestamp": "2026-04-20T10:00:01.500Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 1,
                        "output_tokens": 1,
                    }
                },
            },
        },
        {
            "timestamp": "2026-04-20T10:00:02.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "orphan-call",
                "output": "ghost",
            },
        },
    ]
    path = tmp_path / "orphan.jsonl"
    _write_jsonl(path, events)
    spans = rollout_to_spans(path)
    # Only the interaction and the one turn — no tool span for the orphan.
    assert {s["name"] for s in spans} == {"codex.interaction", "codex.llm_request"}
