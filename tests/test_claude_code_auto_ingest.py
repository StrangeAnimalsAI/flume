from __future__ import annotations

import json
import os
import time
from pathlib import Path

from agent_telemetry.backfill.claude_code import trace_id_for_session
from agent_telemetry.ingest.claude_code import (
    ClaudeCodeTranscriptSource,
    ingest_claude_code_transcript,
    read_transcript_metadata,
)
from agent_telemetry.ingest.cli import main
from agent_telemetry.ingest.fingerprint import fingerprint_file
from agent_telemetry.ingest.runner import IngestOutcome, IngestRequest, run_once
from agent_telemetry.ingest.state import IngestStatus, SqliteIngestStateStore
from agent_telemetry.ingest.types import DiscoveredTranscript


NOW = 1_800_000_000.0


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def _age(path: Path, *, seconds: float, now: float = NOW) -> None:
    mtime_ns = int((now - seconds) * 1_000_000_000)
    os.utime(path, ns=(mtime_ns, mtime_ns))


def _claude_events(
    session_id: str = "session-123",
    *,
    entrypoint: str = "cli",
    is_sidechain: bool = False,
    agent_id: str | None = None,
) -> list[dict]:
    event_base = {
        "sessionId": session_id,
        "cwd": "/Users/james/Code/sample",
        "entrypoint": entrypoint,
        "version": "2.1.119",
        "gitBranch": "main",
        "isSidechain": is_sidechain,
        "userType": "external",
    }
    if agent_id:
        event_base["agentId"] = agent_id
        event_base["slug"] = "investigate-fixture"

    return [
        {
            **event_base,
            "type": "user",
            "uuid": "user-1",
            "parentUuid": None,
            "permissionMode": "acceptEdits",
            "promptId": "prompt-1",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "message": {"role": "user", "content": "Summarize this repo."},
        },
        {
            **event_base,
            "type": "assistant",
            "uuid": "asst-1",
            "parentUuid": "user-1",
            "requestId": "req-1",
            "timestamp": "2026-04-20T10:00:01.500Z",
            "message": {
                "id": "msg_01",
                "model": "claude-sonnet-4-5",
                "role": "assistant",
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cache_read_input_tokens": 25,
                    "cache_creation_input_tokens": 5,
                },
                "content": [
                    {"type": "text", "text": "I will inspect it."},
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
            **event_base,
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:01.510Z",
            "durationMs": 1500,
        },
        {
            **event_base,
            "type": "user",
            "uuid": "user-2",
            "parentUuid": "asst-1",
            "timestamp": "2026-04-20T10:00:03.500Z",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tu_1",
                        "is_error": False,
                        "content": "file contents",
                    }
                ],
            },
        },
    ]


def test_claude_code_source_discovers_project_transcripts_recursively(
    tmp_path: Path,
) -> None:
    projects = tmp_path / "projects"
    main = projects / "-Users-james-Code-tools" / "session-123.jsonl"
    subagent = (
        projects
        / "-Users-james-Code-tools"
        / "session-123"
        / "subagents"
        / "agent-a1.jsonl"
    )
    _write_jsonl(main, _claude_events("session-123", entrypoint="claude-desktop"))
    _write_jsonl(
        subagent,
        _claude_events(
            "session-123",
            entrypoint="cli",
            is_sidechain=True,
            agent_id="agent-a1",
        ),
    )
    _write_jsonl(projects / "-Users-james-Code-tools" / "ignored.txt", [{"type": "x"}])

    items = list(ClaudeCodeTranscriptSource([projects]).discover())
    by_name = {item.path.name: item for item in items}

    assert [str(item.path) for item in items] == sorted(
        str(item.path) for item in items
    )
    assert set(by_name) == {"agent-a1.jsonl", "session-123.jsonl"}
    assert by_name["session-123.jsonl"].session_id == "session-123"
    assert by_name["session-123.jsonl"].trace_id == trace_id_for_session(
        "session-123"
    )
    assert by_name["session-123.jsonl"].metadata["entrypoint"] == "claude-desktop"
    assert by_name["agent-a1.jsonl"].session_id == "agent-a1"
    assert by_name["agent-a1.jsonl"].trace_id == trace_id_for_session("agent-a1")
    assert by_name["agent-a1.jsonl"].metadata["claude_session_id"] == "session-123"
    assert by_name["agent-a1.jsonl"].metadata["agent_id"] == "agent-a1"
    assert by_name["agent-a1.jsonl"].metadata["is_sidechain"] is True


def test_claude_code_source_extracts_metadata_and_trace_id(tmp_path: Path) -> None:
    transcript = tmp_path / "projects" / "proj" / "session-123.jsonl"
    _write_jsonl(transcript, _claude_events("session-123", entrypoint="cli"))

    [item] = list(ClaudeCodeTranscriptSource([transcript]).discover())

    assert item.session_id == "session-123"
    assert item.trace_id == trace_id_for_session("session-123")
    assert item.metadata == {
        "claude_session_id": "session-123",
        "cwd": "/Users/james/Code/sample",
        "entrypoint": "cli",
        "git_branch": "main",
        "is_sidechain": False,
        "model": "claude-sonnet-4-5",
        "permission_mode": "acceptEdits",
        "prompt_id": "prompt-1",
        "session_id": "session-123",
        "source": "claude-code",
        "surface": "cli",
        "user_type": "external",
        "version": "2.1.119",
    }
    assert read_transcript_metadata(transcript)["model"] == "claude-sonnet-4-5"


def test_claude_code_dry_run_reports_mtime_fingerprint_metadata_and_reason(
    tmp_path: Path,
) -> None:
    transcript = tmp_path / "projects" / "proj" / "session-123.jsonl"
    _write_jsonl(transcript, _claude_events("session-123"))
    _age(transcript, seconds=20)

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        summary = run_once(
            source=ClaudeCodeTranscriptSource([transcript]),
            store=store,
            ingest=lambda _request: IngestOutcome(),
            quiet_seconds=5,
            dry_run=True,
            now=NOW,
        )
        records = store.list_records()

    [action] = summary.actions
    data = action.to_dict()
    assert summary.would_ingest == 1
    assert data["session_id"] == "session-123"
    assert data["trace_id"] == trace_id_for_session("session-123")
    assert data["fingerprint"].startswith("sha256:")
    assert data["mtime_ns"] is not None
    assert data["mtime"] is not None
    assert data["reason"] == "dry-run"
    assert data["metadata"]["entrypoint"] == "cli"
    assert records == []


def test_claude_code_active_file_is_not_ingested_by_default(tmp_path: Path) -> None:
    transcript = tmp_path / "projects" / "proj" / "session-123.jsonl"
    _write_jsonl(transcript, _claude_events("session-123", entrypoint="claude-desktop"))
    _age(transcript, seconds=1)
    calls: list[IngestRequest] = []

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        summary = run_once(
            source=ClaudeCodeTranscriptSource([transcript]),
            store=store,
            ingest=lambda request: calls.append(request) or IngestOutcome(),
            quiet_seconds=10,
            now=NOW,
        )
        record = store.get("claude-code", transcript)

    assert summary.skipped_active == 1
    assert calls == []
    assert record is not None
    assert record.status == IngestStatus.ACTIVE
    assert record.metadata["surface"] == "claude-desktop"


def test_claude_code_run_once_ingests_then_skips_unchanged(tmp_path: Path) -> None:
    transcript = tmp_path / "projects" / "proj" / "session-123.jsonl"
    _write_jsonl(transcript, _claude_events("session-123"))
    _age(transcript, seconds=20)
    calls: list[IngestRequest] = []

    def ingest(request: IngestRequest) -> IngestOutcome:
        calls.append(request)
        return IngestOutcome(trace_id="trace-from-export")

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        first = run_once(
            source=ClaudeCodeTranscriptSource([transcript]),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            now=NOW,
        )
        second = run_once(
            source=ClaudeCodeTranscriptSource([transcript]),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            dry_run=True,
            now=NOW + 1,
        )
        record = store.get("claude-code", transcript)

    assert first.ingested == 1
    assert second.skipped_unchanged == 1
    assert second.actions[0].to_dict()["reason"] == "fingerprint already ingested"
    assert len(calls) == 1
    assert record is not None
    assert record.status == IngestStatus.INGESTED
    assert record.session_id == "session-123"
    assert record.trace_id == "trace-from-export"
    assert record.metadata["entrypoint"] == "cli"


def test_ingest_claude_code_transcript_uses_existing_mapper_and_exporter(
    tmp_path: Path,
) -> None:
    transcript = tmp_path / "projects" / "proj" / "session-123.jsonl"
    _write_jsonl(transcript, _claude_events("session-123", entrypoint="claude-desktop"))
    discovered = DiscoveredTranscript(
        source_type="claude-code",
        path=transcript,
        session_id="session-123",
        trace_id=trace_id_for_session("session-123"),
    )
    exports: list[tuple[list[dict], str, str]] = []

    outcome = ingest_claude_code_transcript(
        IngestRequest(
            transcript=discovered,
            fingerprint=fingerprint_file(transcript),
        ),
        endpoint="http://collector.invalid/v1/traces",
        exporter=lambda spans, endpoint, source: exports.append(
            (spans, endpoint, source)
        ),
    )

    assert outcome.session_id == "session-123"
    assert outcome.trace_id == trace_id_for_session("session-123")
    [(spans, endpoint, source)] = exports
    assert endpoint == "http://collector.invalid/v1/traces"
    assert source == "claude-code"
    assert [span["name"] for span in spans] == [
        "claude_code.interaction",
        "claude_code.llm_request",
        "claude_code.tool",
    ]
    root = spans[0]
    assert root["attributes"]["source"] == "claude-code"
    assert root["attributes"]["entrypoint"] == "claude-desktop"
    assert root["attributes"]["langfuse.trace.metadata.agent_source"] == "claude-code"
    assert root["attributes"]["langfuse.trace.metadata.agent_family"] == "claude-code"
    assert (
        root["attributes"]["langfuse.trace.metadata.agent_surface"]
        == "claude-desktop"
    )
    assert root["attributes"]["langfuse.trace.tags"] == [
        "agent:claude-code",
        "family:claude-code",
        "surface:claude-desktop",
    ]


def test_cli_claude_code_dry_run_with_fixture_root(tmp_path: Path, capsys) -> None:
    transcript = tmp_path / "projects" / "proj" / "session-123.jsonl"
    _write_jsonl(transcript, _claude_events("session-123"))
    _age(transcript, seconds=20, now=time.time())
    state_db = tmp_path / "state.sqlite3"

    exit_code = main(
        [
            "--once",
            "--source",
            "claude-code",
            "--claude-root",
            str(transcript.parent),
            "--state-db",
            str(state_db),
            "--quiet-seconds",
            "5",
            "--dry-run",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["would_ingest"] == 1
    [action] = output["actions"]
    assert action["session_id"] == "session-123"
    assert action["path"] == str(transcript.resolve())
    assert action["fingerprint"].startswith("sha256:")
    assert action["mtime"] is not None
    assert action["reason"] == "dry-run"
    assert action["metadata"]["entrypoint"] == "cli"
    with SqliteIngestStateStore(state_db) as store:
        assert store.list_records() == []
