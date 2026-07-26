"""Tests for the harness: agent loop tracing + the harness source adapter.

The load-bearing guarantee (INT-439): thinking summaries requested from the
API survive verbatim into the store's `thinking` content rows — the data
Claude Code transcripts stopped carrying ~May 2026.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from flume.harness.agent import run_session
from flume.ingest.write import ingest_path
from flume.sources import get_adapter
from flume.store.base import open_store

THINKING_1 = "The store schema suggests checking retention.py first."
THINKING_2 = "The config default explains the skipped blobs."


def _block(**kw) -> SimpleNamespace:
    return SimpleNamespace(**kw)


def _usage(inp=100, out=50, cr=900, cc=10) -> SimpleNamespace:
    return SimpleNamespace(
        input_tokens=inp,
        output_tokens=out,
        cache_read_input_tokens=cr,
        cache_creation_input_tokens=cc,
    )


class _StubStream:
    def __init__(self, message: SimpleNamespace) -> None:
        self._message = message
        self.text_stream = iter(())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_final_message(self):
        return self._message


class _StubClient:
    """Scripted responses; records the requests the loop sends."""

    def __init__(self, responses: list[SimpleNamespace]) -> None:
        self._responses = iter(responses)
        self.requests: list[dict] = []
        self.messages = SimpleNamespace(stream=self._stream)

    def _stream(self, **kwargs):
        # Snapshot: the loop mutates its messages list between calls.
        self.requests.append({**kwargs, "messages": list(kwargs["messages"])})
        return _StubStream(next(self._responses))


def _scripted_client() -> _StubClient:
    turn_1 = SimpleNamespace(
        model="claude-opus-4-8",
        stop_reason="tool_use",
        usage=_usage(),
        content=[
            _block(type="thinking", thinking=THINKING_1),
            _block(type="text", text="Checking the retention config."),
            _block(
                type="tool_use",
                id="tu_1",
                name="bash",
                input={"command": "echo retention"},
            ),
        ],
    )
    turn_2 = SimpleNamespace(
        model="claude-opus-4-8",
        stop_reason="end_turn",
        usage=_usage(inp=20, out=5, cr=950, cc=0),
        content=[
            _block(type="thinking", thinking=THINKING_2),
            _block(type="text", text="It skips archived_sessions by design."),
        ],
    )
    return _StubClient([turn_1, turn_2])


def test_run_session_requests_summarized_thinking(tmp_path: Path) -> None:
    client = _scripted_client()
    run_session("why?", transcript_dir=tmp_path, client=client, echo=False)

    for request in client.requests:
        assert request["thinking"] == {"type": "adaptive", "display": "summarized"}
        assert request["model"] == "claude-opus-4-8"
    # Tool results for a turn arrive in ONE user message.
    followup = client.requests[1]["messages"][-1]
    assert followup["role"] == "user"
    assert followup["content"][0]["tool_use_id"] == "tu_1"
    assert "retention" in followup["content"][0]["content"]


def test_transcript_captures_thinking_and_tools(tmp_path: Path) -> None:
    path = run_session(
        "why?", transcript_dir=tmp_path, client=_scripted_client(), echo=False
    )
    events = [json.loads(line) for line in path.read_text().splitlines()]
    kinds = [e["type"] for e in events]
    assert kinds == [
        "session_meta", "user", "assistant", "tool_result", "assistant", "end",
    ]
    assert events[2]["content"][0] == {"type": "thinking", "thinking": THINKING_1}
    assert events[3]["output"].strip() == "retention"
    assert events[5]["stop_reason"] == "end_turn"


def test_harness_transcript_ingests_with_thinking(tmp_path: Path) -> None:
    path = run_session(
        "why is retention skipping codex blobs?",
        transcript_dir=tmp_path,
        client=_scripted_client(),
        echo=False,
    )
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        outcome = ingest_path(store, get_adapter("harness"), path)
        assert outcome is not None
        session = store.get_session(outcome.session_id)
        thinking = store.get_contents(outcome.session_id, kinds=["thinking"])
        results = store.get_contents(outcome.session_id, kinds=["tool_result"])

    assert session is not None
    assert session["source"] == "harness"
    assert session["model"] == "claude-opus-4-8"
    assert session["turn_count"] == 2
    assert session["tool_call_count"] == 1
    assert session["input_tokens"] == 120
    assert session["cache_read_tokens"] == 1850
    assert session["thinking_blocks"] == 2
    assert session["first_user_message"] == "why is retention skipping codex blobs?"
    assert session["tool_calls"][0]["name"] == "bash"
    # The point of INT-439: thinking text lands in the store verbatim.
    assert [t["text"] for t in thinking] == [THINKING_1, THINKING_2]
    assert all(t["span_id"] for t in thinking)
    assert results[0]["text"].strip() == "retention"


# -- SDK backend --------------------------------------------------------------
# Duck-typed stand-ins: the backend dispatches on type names, so these
# mirror the claude-agent-sdk message/block classes without importing it.


class ThinkingBlock(SimpleNamespace):
    pass


class TextBlock(SimpleNamespace):
    pass


class ToolUseBlock(SimpleNamespace):
    pass


class ToolResultBlock(SimpleNamespace):
    pass


class AssistantMessage(SimpleNamespace):
    pass


class UserMessage(SimpleNamespace):
    pass


class ResultMessage(SimpleNamespace):
    pass


def _sdk_stream_factory():
    async def stream(prompt: str):
        yield AssistantMessage(
            model="claude-opus-4-8",
            content=[
                ThinkingBlock(thinking=THINKING_1),
                ToolUseBlock(id="tu_1", name="Bash", input={"command": "ls"}),
            ],
        )
        yield UserMessage(
            content=[
                ToolResultBlock(tool_use_id="tu_1", content="README.md", is_error=False)
            ]
        )
        yield AssistantMessage(
            model="claude-opus-4-8",
            content=[
                ThinkingBlock(thinking=THINKING_2),
                TextBlock(text="One file."),
            ],
        )
        yield ResultMessage(
            subtype="success",
            usage={
                "input_tokens": 300,
                "output_tokens": 80,
                "cache_read_input_tokens": 5000,
                "cache_creation_input_tokens": 40,
            },
        )

    return stream


def _run_sdk(tmp_path: Path) -> Path:
    import anyio

    from flume.harness.sdk_backend import run_sdk_session

    async def go() -> Path:
        return await run_sdk_session(
            "what files are here?",
            transcript_dir=tmp_path,
            query_fn=lambda prompt: _sdk_stream_factory()(prompt),
            echo=False,
        )

    return anyio.run(go)


def test_sdk_backend_ingests_thinking_and_attributes_totals(tmp_path: Path) -> None:
    path = _run_sdk(tmp_path)
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        outcome = ingest_path(store, get_adapter("harness"), path)
        assert outcome is not None
        assert outcome.session_id.startswith("harness-")
        session = store.get_session(outcome.session_id)
        thinking = store.get_contents(outcome.session_id, kinds=["thinking"])

    assert session is not None
    assert session["source"] == "harness"
    assert session["turn_count"] == 2
    assert session["tool_call_count"] == 1
    # Stream carries no per-turn usage; end totals land on the last turn.
    assert session["input_tokens"] == 300
    assert session["cache_read_tokens"] == 5000
    assert [t["text"] for t in thinking] == [THINKING_1, THINKING_2]
    assert session["tool_calls"][0]["name"] == "Bash"


def test_adapter_resolves_and_probes(tmp_path: Path) -> None:
    adapter = get_adapter("harness")
    assert adapter.name == "harness"

    path = run_session(
        "hi", transcript_dir=tmp_path, client=_scripted_client(), echo=False
    )
    assert adapter.probe is not None
    probed = adapter.probe(path)
    assert probed["cwd"]  # recorded from the run's working directory
    assert probed["version"]
    # The anthropic backend spawns nothing, so there is no CLI session to link.
    assert "cli_session_id" not in probed


def test_probe_links_the_claude_code_session_the_sdk_spawned(tmp_path: Path) -> None:
    """The claude-sdk backend spawns Claude Code, which writes its own
    transcript and is pulled in as a separate session. Probing must surface
    the id so the two records of one run can be told apart from two runs."""
    adapter = get_adapter("harness")
    path = tmp_path / "harness-x.jsonl"
    path.write_text(
        json.dumps({
            "type": "session_meta", "session_id": "harness-x",
            "backend": "sdk", "cwd": "/tmp/x", "harness_version": "1",
        }) + "\n"
        + json.dumps({"type": "user", "text": "hi"}) + "\n"
        + json.dumps({"type": "cli_session", "cli_session_id": "cc-999"}) + "\n"
    )
    assert adapter.probe is not None
    probed = adapter.probe(path)
    assert probed["cli_session_id"] == "cc-999"
    assert probed["backend"] == "sdk"
