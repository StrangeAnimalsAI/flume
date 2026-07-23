"""Tests for the pluggable session store: ingest fidelity + query surface.

The load-bearing guarantee: the store keeps FULL thinking text and
untruncated tool payloads — exactly the data the Langfuse path reduces to
counts/previews — and joins them to the same deterministic span ids the
mappers emit.
"""
from __future__ import annotations

import json
from pathlib import Path

from flume.ingest.write import ingest_path
from flume.sources import get_adapter
from flume.store.base import open_store

THINKING_1 = "I should read the file before editing; the bug is in parse()."
THINKING_2 = "The test failure suggests a missing null check in loader.py."
BIG_RESULT = "y" * 70_000  # over the 60 KB OTel cap — store must keep it whole


def _write_jsonl(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


def _claude_events() -> list[dict]:
    return [
        {
            "type": "user",
            "uuid": "user-1",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "version": "2.1.114",
            "gitBranch": "main",
            "cwd": "/Users/james/Code/demo",
            "message": {"role": "user", "content": "please fix the parser bug"},
        },
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
                    {"type": "thinking", "thinking": THINKING_1},
                    {"type": "text", "text": "Reading the file first."},
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
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:01.510Z",
            "durationMs": 1500,
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
                        "content": BIG_RESULT,
                    }
                ]
            },
        },
        {
            "type": "assistant",
            "uuid": "asst-2",
            "timestamp": "2026-04-20T10:00:04.500Z",
            "message": {
                "usage": {"input_tokens": 20, "output_tokens": 5},
                "content": [
                    {"type": "thinking", "thinking": THINKING_2},
                    {"type": "text", "text": "done"},
                ],
            },
        },
        {
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:04.510Z",
            "durationMs": 500,
        },
    ]


def _codex_events() -> list[dict]:
    return [
        {
            "timestamp": "2026-04-21T09:00:00.000Z",
            "type": "session_meta",
            "payload": {
                "id": "codex-sess-1",
                "originator": "Codex Desktop",
                "cli_version": "0.44.0",
                "cwd": "/Users/james/Code/demo",
                "source": "vscode",
            },
        },
        {
            "timestamp": "2026-04-21T09:00:00.100Z",
            "type": "turn_context",
            "payload": {"type": "turn_context", "model": "gpt-5.3-codex"},
        },
        {
            "timestamp": "2026-04-21T09:00:01.000Z",
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "run the tests"},
        },
        {
            "timestamp": "2026-04-21T09:00:02.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "call-1",
                "arguments": json.dumps({"command": ["pytest", "-q"]}),
            },
        },
        {
            "timestamp": "2026-04-21T09:00:04.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "call-1",
                "output": "3 passed",
            },
        },
        {
            "timestamp": "2026-04-21T09:00:05.000Z",
            "type": "event_msg",
            "payload": {"type": "agent_message", "message": "All tests pass."},
        },
        {
            "timestamp": "2026-04-21T09:00:05.500Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 200,
                        "cached_input_tokens": 150,
                        "output_tokens": 40,
                        "reasoning_output_tokens": 12,
                    }
                },
            },
        },
    ]


def _ingest_claude(store, tmp_path: Path, session_id: str = "sess-claude-1"):
    path = tmp_path / f"{session_id}.jsonl"
    _write_jsonl(path, _claude_events())
    return ingest_path(
        store, get_adapter("claude-code"), path, {"cwd": "/Users/james/Code/demo"}
    )


def _ingest_codex(store, tmp_path: Path):
    path = tmp_path / "rollout-2026-04-21T09-00-00-codex-sess-1.jsonl"
    _write_jsonl(path, _codex_events())
    return ingest_path(store, get_adapter("codex"), path)


def test_thinking_blocks_stored_in_full(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)
        thinking = store.get_contents("sess-claude-1", kinds=["thinking"])

    assert [t["text"] for t in thinking] == [THINKING_1, THINKING_2]
    # Thinking rows join to the turn spans the mapper emitted.
    assert all(t["span_id"] for t in thinking)


def test_tool_result_not_truncated(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)
        results = store.get_contents("sess-claude-1", kinds=["tool_result"])

    assert len(results) == 1
    assert results[0]["text"] == BIG_RESULT  # full 70 KB, no 60 KB cap


def test_session_rollup_metrics(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)
        session = store.get_session("sess-claude-1")

    assert session is not None
    assert session["source"] == "claude-code"
    assert session["surface"] == "cli"
    assert session["model"] == "claude-opus-4-7"
    assert session["cwd"] == "/Users/james/Code/demo"
    assert session["turn_count"] == 2
    assert session["tool_call_count"] == 1
    assert session["input_tokens"] == 120
    assert session["output_tokens"] == 55
    assert session["cache_read_tokens"] == 900
    assert session["cache_creation_tokens"] == 10
    assert session["active_ms"] == 2000
    assert session["thinking_blocks"] == 2
    assert session["first_user_message"] == "please fix the parser bug"
    assert len(session["turns"]) == 2
    assert len(session["tool_calls"]) == 1
    assert session["tool_calls"][0]["name"] == "Read"


def test_codex_ingest_and_reasoning_absent(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        outcome = _ingest_codex(store, tmp_path)
        assert outcome is not None
        session = store.get_session("codex-sess-1")
        thinking = store.get_contents("codex-sess-1", kinds=["thinking"])
        tool_args = store.get_contents("codex-sess-1", kinds=["tool_arguments"])

    assert session is not None
    assert session["source"] == "codex"
    assert session["reasoning_tokens"] == 12
    assert session["cache_read_tokens"] == 150
    assert thinking == []  # encrypted at the source; nothing to store
    assert json.loads(tool_args[0]["text"]) == {"command": ["pytest", "-q"]}


def test_reingest_replaces_rows(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)
        _ingest_claude(store, tmp_path)  # same session again

        assert store.overview()["totals"]["sessions"] == 1
        thinking = store.get_contents("sess-claude-1", kinds=["thinking"])
        assert len(thinking) == 2  # not duplicated


def test_duplicate_event_uuid_dedupes_to_last(tmp_path: Path) -> None:
    # Real transcripts can re-log an assistant event with the same uuid
    # (API retry); the deterministic span id collides. Last one wins.
    events = _claude_events()
    retry = json.loads(json.dumps(events[1]))
    retry["message"]["usage"]["output_tokens"] = 60
    events.insert(2, retry)
    path = tmp_path / "sess-dup.jsonl"
    _write_jsonl(path, events)

    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        assert ingest_path(store, get_adapter("claude-code"), path) is not None
        session = store.get_session("sess-dup")

    assert session is not None
    assert session["turn_count"] == 2  # not 3
    assert any(t["output_tokens"] == 60 for t in session["turns"])


def test_search_finds_thinking_text(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)
        _ingest_codex(store, tmp_path)

        hits = store.search("null check", kinds=["thinking"])
        assert len(hits) == 1
        assert hits[0]["session_id"] == "sess-claude-1"
        assert hits[0]["kind"] == "thinking"

        # Source filter narrows results.
        assert store.search("tests", source="codex")
        assert not store.search("null check", source="codex")


def test_tool_and_token_stats(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)
        _ingest_codex(store, tmp_path)

        tools = store.tool_stats()
        names = {row["name"] for row in tools["per_tool"]}
        assert names == {"Read", "exec_command"}

        tokens = {row["grp"]: row for row in store.token_stats(group_by="source")}
        assert tokens["claude-code"]["cache_read_tokens"] == 900
        # cache hit ratio = cache_read / (input + cache_read)
        assert abs(tokens["claude-code"]["cache_hit_ratio"] - 900 / 1020) < 0.001

        by_model = {row["grp"] for row in store.token_stats(group_by="model")}
        assert by_model == {"claude-opus-4-7", "gpt-5.3-codex"}


def test_session_hierarchy_and_project(tmp_path: Path) -> None:
    parent_id = "sess-parent-1"
    root = tmp_path / "projects" / "-Users-james-Code-demo"
    parent_path = root / f"{parent_id}.jsonl"
    child_path = root / parent_id / "subagents" / "agent-abc123.jsonl"
    parent_path.parent.mkdir(parents=True)
    child_path.parent.mkdir(parents=True)
    _write_jsonl(parent_path, _claude_events())
    _write_jsonl(child_path, _claude_events())

    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), parent_path, {"cwd": "/Users/james/Code/demo"})
        ingest_path(store, get_adapter("claude-code"), child_path, {"cwd": "/Users/james/Code/demo"})

        top = store.list_sessions(top_level_only=True)
        assert [s["session_id"] for s in top] == [parent_id]
        assert top[0]["children"] == 1
        assert top[0]["project"] == "demo"

        parent = store.get_session(parent_id)
        assert [c["session_id"] for c in parent["children"]] == ["agent-abc123"]
        assert parent["family"]["sessions"] == 2
        assert parent["family"]["output_tokens"] == 2 * parent["output_tokens"]

        child = store.get_session("agent-abc123")
        assert child["is_subagent"] == 1
        assert child["parent_session_id"] == parent_id


def test_session_commands_segmentation(tmp_path: Path) -> None:
    events = _claude_events()
    events.append(
        {
            "type": "user",
            "timestamp": "2026-04-20T10:01:00.000Z",
            "message": {"role": "user", "content": "now run the tests"},
        }
    )
    events.append(
        {
            "type": "assistant",
            "uuid": "asst-3",
            "timestamp": "2026-04-20T10:01:05.000Z",
            "message": {
                "usage": {"input_tokens": 30, "output_tokens": 7},
                "content": [{"type": "text", "text": "tests pass"}],
            },
        }
    )
    path = tmp_path / "sess-cmd.jsonl"
    _write_jsonl(path, events)

    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), path)
        commands = store.session_commands("sess-cmd")

    assert [c["prompt"] for c in commands] == [
        "please fix the parser bug",
        "now run the tests",
    ]
    assert commands[0]["turns"] == 2
    assert commands[0]["tool_calls"] == 1
    assert commands[0]["output_tokens"] == 55
    assert commands[1]["turns"] == 1
    assert commands[1]["output_tokens"] == 7
    assert commands[1]["duration_ms"] == 5000


def test_derive_project() -> None:
    from flume.store.bundle import derive_project

    assert derive_project("/Users/james/Code/tools/flume") == (
        "tools/flume"
    )
    assert derive_project(
        "/Users/james/Code/biz/security/crypto-analysis/.claude/worktrees/x-1"
    ) == "security/crypto-analysis"
    assert derive_project(None) is None
    # Labels must not depend on the analyzing machine's $HOME: a cwd under
    # any /Users/<name> or /home/<name> derives the same everywhere.
    assert derive_project("/Users/somebody/Code/demo") == "demo"
    assert derive_project("/home/alex/projects/tools/flume") == "tools/flume"
    assert derive_project("/Users/somebody") == "~"


def test_audit_repeats_flags_byte_identical(tmp_path: Path) -> None:
    # Two identical Read calls returning identical bytes -> provable waste.
    events = _claude_events()[:1]
    for i, (tu, result) in enumerate(
        [("tu_a", "same"), ("tu_b", "same"), ("tu_c", "different")]
    ):
        events.append(
            {
                "type": "assistant",
                "uuid": f"asst-{i}",
                "timestamp": f"2026-04-20T10:00:0{i + 1}.000Z",
                "message": {
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "content": [
                        {
                            "type": "tool_use",
                            "id": tu,
                            "name": "Grep",
                            "input": {"pattern": "foo"},
                        }
                    ],
                },
            }
        )
        events.append(
            {
                "type": "user",
                "timestamp": f"2026-04-20T10:00:0{i + 1}.500Z",
                "message": {
                    "content": [
                        {"type": "tool_result", "tool_use_id": tu, "content": result}
                    ]
                },
            }
        )
    path = tmp_path / "sess-rep.jsonl"
    _write_jsonl(path, events)

    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), path)
        repeats = store.audit_repeats()

    assert len(repeats) == 1
    assert repeats[0]["calls"] == 3
    assert repeats[0]["distinct_results"] == 2
    assert not repeats[0]["byte_identical"]


def test_audit_whole_file_reads(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)  # fixture Read has no offset, 70k result
        rows = store.audit_whole_file_reads(min_chars=50_000)
        assert len(rows) == 1
        assert rows[0]["result_chars"] == len(BIG_RESULT)
        assert store.audit_whole_file_reads(min_chars=100_000) == []


def test_insights_detect_and_persist(tmp_path: Path) -> None:
    from flume.analysis.insights import run_insights

    # Build a session with a byte-identical retry loop (6 identical calls).
    events = _claude_events()[:1]
    for i in range(6):
        events.append(
            {
                "type": "assistant",
                "uuid": f"asst-{i}",
                "timestamp": f"2026-04-20T10:00:0{i + 1}.000Z",
                "message": {
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"tu_{i}",
                            "name": "FlakyTool",
                            "input": {"q": "same"},
                        }
                    ],
                },
            }
        )
        events.append(
            {
                "type": "user",
                "timestamp": f"2026-04-20T10:00:0{i + 1}.500Z",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": f"tu_{i}",
                            "content": "identical result",
                        }
                    ]
                },
            }
        )
    path = tmp_path / "sess-flaky.jsonl"
    _write_jsonl(path, events)

    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), path)
        findings = run_insights(store)
        kinds = {f["kind"] for f in findings}
        assert "repeat_waste" in kinds
        waste = next(f for f in findings if f["kind"] == "repeat_waste")
        assert waste["fingerprint"] == "FlakyTool"
        assert waste["metric"] == 5  # 6 calls, 5 wasted

        # Re-run: same finding updates in place (occurrences bumps, no dup).
        run_insights(store)
        stored = store.list_findings(kind="repeat_waste")
        assert len(stored) == 1
        assert stored[0]["occurrences"] == 2


def test_insights_schema_loop_detects_distinct_payload_retries(
    tmp_path: Path,
) -> None:
    # A subagent rephrasing its StructuredOutput payload every attempt:
    # never byte-identical (invisible to repeat_waste), but every attempt
    # fails validation the same way.
    from flume.analysis.insights import run_insights

    schema_error = (
        "Output does not match required schema: "
        "root: must have required property 'key_facts'"
    )
    events = _claude_events()[:1]
    for i in range(12):
        events.append(
            {
                "type": "assistant",
                "uuid": f"asst-{i}",
                "timestamp": f"2026-04-20T10:00:{i + 10:02d}.000Z",
                "message": {
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"tu_{i}",
                            "name": "StructuredOutput",
                            "input": {"build_implication": f"attempt {i}"},
                        }
                    ],
                },
            }
        )
        events.append(
            {
                "type": "user",
                "timestamp": f"2026-04-20T10:00:{i + 10:02d}.500Z",
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": f"tu_{i}",
                            "is_error": True,
                            "content": schema_error,
                        }
                    ]
                },
            }
        )
    path = tmp_path / "sess-schema-loop.jsonl"
    _write_jsonl(path, events)

    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), path)
        findings = run_insights(store)

    loops = [f for f in findings if f["kind"] == "schema_loop"]
    assert len(loops) == 1
    assert loops[0]["metric"] == 12
    assert "key_facts" in loops[0]["detail"]
    # Not byte-identical, so the repeat_waste detector must NOT claim it.
    assert not any(
        f["kind"] == "repeat_waste" and f["fingerprint"] == "StructuredOutput"
        for f in findings
    )


def test_list_sessions_filters(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        _ingest_claude(store, tmp_path)
        _ingest_codex(store, tmp_path)

        assert len(store.list_sessions()) == 2
        assert len(store.list_sessions(source="codex")) == 1
        assert len(store.list_sessions(cwd_like="Code/demo")) == 2
        assert store.list_sessions(limit=1)[0]["source"] == "codex"  # newest first
