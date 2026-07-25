"""OpenAI-compatible harness backend.

Exercised against a fake chat-completions server, so the test covers the
wire contract (tool-call round trip, usage normalization, transcript shape)
without needing a live model.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from flume.harness.openai_compat import run_openai_session
from flume.sources.harness import extract_contents, harness_to_spans


def _fake_server(responses: list[dict]):
    """Return a post() that replays canned chat-completions responses.

    Payloads are deep-copied on capture: the loop mutates its message list
    in place, and real transport serializes at call time, so a live
    reference would show later turns' state."""
    seen: list[dict] = []

    def post(url, payload, headers):
        seen.append(copy.deepcopy(payload))
        return responses[min(len(seen) - 1, len(responses) - 1)]

    post.seen = seen  # type: ignore[attr-defined]
    return post


def _message(content=None, tool_calls=None, reasoning=None, usage=None):
    message: dict = {"content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls
    if reasoning:
        message["reasoning_content"] = reasoning
    return {
        "model": "local-model",
        "choices": [{
            "message": message,
            "finish_reason": "tool_calls" if tool_calls else "stop",
        }],
        "usage": usage or {},
    }


def test_tool_call_round_trip_and_transcript_shape(tmp_path: Path) -> None:
    post = _fake_server([
        _message(
            content="Checking the tree.",
            reasoning="I should list files first.",
            tool_calls=[{
                "id": "call_1",
                "function": {"name": "bash", "arguments": json.dumps({"command": "echo hi"})},
            }],
            usage={"prompt_tokens": 100, "completion_tokens": 20,
                   "prompt_tokens_details": {"cached_tokens": 80}},
        ),
        _message(content="Done.", usage={"prompt_tokens": 50, "completion_tokens": 5}),
    ])

    path = run_openai_session(
        "list the files",
        model="local-model",
        transcript_dir=tmp_path,
        post=post,
        echo=False,
    )

    events = [json.loads(line) for line in path.read_text().splitlines()]
    kinds = [e["type"] for e in events]
    assert kinds == [
        "session_meta", "user", "assistant", "tool_result", "assistant", "end"
    ]

    meta = events[0]
    assert meta["backend"] == "openai"

    first = events[2]
    # OpenAI usage is inclusive-input (100 total, 80 cached) -> exclusive 20,
    # matching the store's vocabulary and the Codex mapper.
    assert first["usage"]["input_tokens"] == 20
    assert first["usage"]["cache_read_input_tokens"] == 80
    block_types = [b["type"] for b in first["content"]]
    assert block_types == ["thinking", "text", "tool_use"]

    result = events[3]
    assert result["is_error"] is False
    assert "hi" in result["output"]

    # The tool result was fed back as a `tool` role message on the next call.
    followup = post.seen[1]
    assert followup["messages"][-1]["role"] == "tool"
    assert followup["messages"][-1]["tool_call_id"] == "call_1"


def test_transcript_maps_through_the_harness_source_adapter(tmp_path: Path) -> None:
    """The payoff: a local-model session is analyzed like any other source."""
    post = _fake_server([
        _message(
            content="Answer.",
            reasoning="Some reasoning.",
            usage={"prompt_tokens": 10, "completion_tokens": 3},
        ),
    ])
    path = run_openai_session(
        "hello", model="local-model", transcript_dir=tmp_path, post=post, echo=False
    )

    spans = harness_to_spans(path)
    assert spans and spans[0]["name"] == "harness.interaction"
    turns = [s for s in spans if s["name"] == "harness.llm_request"]
    assert len(turns) == 1
    assert turns[0]["attributes"]["gen_ai.usage.input_tokens"] == 10

    session_id = spans[0]["attributes"]["session.id"]
    kinds = {c.kind for c in extract_contents(path, session_id)}
    # Reasoning a local model exposes is captured as thinking, same as
    # Anthropic's summaries — that is the whole point of the harness.
    assert "thinking" in kinds
    assert "assistant_message" in kinds


def test_arguments_that_are_not_json_still_run(tmp_path: Path) -> None:
    # Smaller models sometimes emit a bare command string, not JSON.
    post = _fake_server([
        _message(tool_calls=[{
            "id": "c1", "function": {"name": "bash", "arguments": "echo raw"}
        }]),
        _message(content="ok"),
    ])
    path = run_openai_session(
        "go", model="m", transcript_dir=tmp_path, post=post, echo=False
    )
    events = [json.loads(line) for line in path.read_text().splitlines()]
    result = next(e for e in events if e["type"] == "tool_result")
    assert "raw" in result["output"]
