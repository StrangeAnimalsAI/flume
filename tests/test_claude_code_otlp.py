"""Tests for the span-dict → OTel SDK Span adapter.

Runs entirely in-process: spans are pushed through a `SimpleSpanProcessor`
into an `InMemorySpanExporter`, so we can assert trace_id / span_id /
parent_span_id / timestamps / attributes match what the mapper emitted
without touching the network.
"""
from __future__ import annotations

import json
from pathlib import Path

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from flume.backfill.claude_code import jsonl_to_spans
from flume.backfill.otlp import export_span_dicts


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


def _fixture_events() -> list[dict]:
    return [
        {
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "durationMs": 1500,
        },
        {
            "type": "user",
            "uuid": "user-1",
            "timestamp": "2026-04-20T10:00:00.100Z",
            "message": {"role": "user", "content": "read /tmp/a.py"},
        },
        {
            "type": "assistant",
            "uuid": "asst-1",
            "timestamp": "2026-04-20T10:00:01.500Z",
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
                    {
                        "type": "tool_use",
                        "id": "tu_1",
                        "name": "Read",
                        "input": {"file_path": "/tmp/a.py"},
                    },
                ],
            },
        },
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
    ]


def _collect(span_dicts: list[dict]) -> list:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(resource=Resource.create({"service.name": "t"}))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    # The adapter only needs a processor, not the full tracer — we reuse the
    # provider's active processor so SimpleSpanProcessor gets every span.
    export_span_dicts(
        span_dicts,
        Resource.create({"service.name": "t", "source": "claude-code"}),
        provider._active_span_processor,
    )
    return list(exporter.get_finished_spans())


def test_adapter_preserves_ids_timestamps_and_hierarchy(tmp_path: Path) -> None:
    path = tmp_path / "abc.jsonl"
    _write_jsonl(path, _fixture_events())
    span_dicts = jsonl_to_spans(path)
    finished = _collect(span_dicts)

    assert len(finished) == len(span_dicts)

    by_name = {s.name: s for s in finished}
    root = by_name["claude_code.interaction"]
    turn = by_name["claude_code.llm_request"]
    tool = by_name["claude_code.tool"]

    root_dict = next(s for s in span_dicts if s["name"] == "claude_code.interaction")
    turn_dict = next(s for s in span_dicts if s["name"] == "claude_code.llm_request")
    tool_dict = next(s for s in span_dicts if s["name"] == "claude_code.tool")

    # Trace IDs: every span in the same trace, matching the mapper's hex.
    assert f"{root.context.trace_id:032x}" == root_dict["trace_id"]
    assert turn.context.trace_id == root.context.trace_id
    assert tool.context.trace_id == root.context.trace_id

    # Span IDs: exact hex from the mapper.
    assert f"{root.context.span_id:016x}" == root_dict["span_id"]
    assert f"{turn.context.span_id:016x}" == turn_dict["span_id"]
    assert f"{tool.context.span_id:016x}" == tool_dict["span_id"]

    # Parent wiring: root has no parent; turn's parent is root; tool's is turn.
    assert root.parent is None
    assert turn.parent is not None
    assert turn.parent.span_id == root.context.span_id
    assert tool.parent is not None
    assert tool.parent.span_id == turn.context.span_id

    # Timestamps: original nanoseconds, not `time.time_ns()`.
    assert root.start_time == root_dict["start_unix_nano"]
    assert root.end_time == root_dict["end_unix_nano"]
    assert turn.start_time == turn_dict["start_unix_nano"]
    assert tool.start_time == tool_dict["start_unix_nano"]
    assert tool.end_time == tool_dict["end_unix_nano"]

    # Attributes: key ones round-trip.
    assert root.attributes["langfuse.trace.metadata.agent_source"] == "claude-code"
    assert root.attributes["langfuse.trace.metadata.agent_family"] == "claude-code"
    assert tuple(root.attributes["langfuse.trace.tags"]) == (
        "agent:claude-code",
        "family:claude-code",
    )
    assert turn.attributes["gen_ai.usage.cache_read_input_tokens"] == 900
    assert tool.attributes["tool.name"] == "Read"
    assert tool.attributes["tool.result_chars"] == 1024

    # Sampled flag on the wire.
    assert root.context.trace_flags.sampled is True


def test_adapter_maps_payloads_to_langfuse_input_output_attrs(
    tmp_path: Path,
) -> None:
    path = tmp_path / "abc.jsonl"
    _write_jsonl(path, _fixture_events())
    finished = _collect(jsonl_to_spans(path))

    by_name = {span.name: span for span in finished}
    root = by_name["claude_code.interaction"]
    turn = by_name["claude_code.llm_request"]
    tool = by_name["claude_code.tool"]

    root_input = json.loads(root.attributes["langfuse.observation.input"])
    root_output = json.loads(root.attributes["langfuse.observation.output"])
    assert root.attributes["langfuse.trace.input"] == root.attributes[
        "langfuse.observation.input"
    ]
    assert root.attributes["langfuse.trace.output"] == root.attributes[
        "langfuse.observation.output"
    ]
    assert root_input["user_requests"][0]["content"] == "read /tmp/a.py"
    assert root_output["counts"]["tool_calls"] == 1

    turn_input = json.loads(turn.attributes["langfuse.observation.input"])
    turn_output = json.loads(turn.attributes["langfuse.observation.output"])
    assert turn_input["messages"][0]["content"] == "read /tmp/a.py"
    assert turn_output["messages"][0]["type"] == "tool_call"

    tool_input = json.loads(tool.attributes["langfuse.observation.input"])
    tool_output = json.loads(tool.attributes["langfuse.observation.output"])
    assert tool_input["arguments"] == {"file_path": "/tmp/a.py"}
    assert tool_output == "x" * 1024


def test_adapter_marks_error_status(tmp_path: Path) -> None:
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
    finished = _collect(jsonl_to_spans(path))
    tool = next(s for s in finished if s.name == "claude_code.tool")
    from opentelemetry.trace import StatusCode

    assert tool.status.status_code == StatusCode.ERROR
