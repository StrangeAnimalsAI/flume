from __future__ import annotations

import json
import os
import time
from pathlib import Path

from flume.backfill.codex import trace_id_for_session
from flume.ingest.cli import main
from flume.ingest.codex import CodexRolloutSource, read_rollout_metadata
from flume.ingest.runner import IngestOutcome, IngestRequest, run_once
from flume.ingest.state import IngestStatus, SqliteIngestStateStore


NOW = 1_800_000_000.0


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def _age(path: Path, *, seconds: float, now: float = NOW) -> None:
    mtime_ns = int((now - seconds) * 1_000_000_000)
    os.utime(path, ns=(mtime_ns, mtime_ns))


def _rollout_events(session_id: str = "codex-session-1") -> list[dict]:
    return [
        {
            "timestamp": "2026-04-20T10:00:00.000Z",
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "cwd": "/Users/james/Code/sample",
                "originator": "Codex Desktop",
                "cli_version": "0.124.0-alpha.2",
                "source": "vscode",
                "model_provider": "openai",
            },
        },
        {
            "timestamp": "2026-04-20T10:00:00.010Z",
            "type": "turn_context",
            "payload": {
                "turn_id": "turn-1",
                "cwd": "/Users/james/Code/sample",
                "model": "gpt-5.4",
            },
        },
        {
            "timestamp": "2026-04-20T10:00:00.020Z",
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": "turn-1"},
        },
        {
            "timestamp": "2026-04-20T10:00:00.030Z",
            "type": "event_msg",
            "payload": {"type": "token_count", "info": None},
        },
        {
            "timestamp": "2026-04-20T10:00:01.000Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({"cmd": "pwd"}),
                "call_id": "call_A",
            },
        },
        {
            "timestamp": "2026-04-20T10:00:01.500Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "last_token_usage": {
                        "input_tokens": 100,
                        "cached_input_tokens": 80,
                        "output_tokens": 25,
                        "reasoning_output_tokens": 5,
                        "total_tokens": 125,
                    }
                },
            },
        },
        {
            "timestamp": "2026-04-20T10:00:01.600Z",
            "type": "event_msg",
            "payload": {
                "type": "exec_command_end",
                "call_id": "call_A",
                "aggregated_output": "/Users/james/Code/sample\n",
                "exit_code": 0,
                "duration": {"secs": 0, "nanos": 200_000_000},
            },
        },
        {
            "timestamp": "2026-04-20T10:00:01.700Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "call_A",
                "output": "Wall time: 0.2s\nOutput:\n/Users/james/Code/sample\n",
            },
        },
        {
            "timestamp": "2026-04-20T10:00:02.000Z",
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": "turn-1"},
        },
    ]


def test_codex_source_discovers_sessions_and_optional_archived(tmp_path: Path) -> None:
    sessions = tmp_path / "sessions"
    archived = tmp_path / "archived_sessions"
    current = sessions / "2026" / "05" / "01" / "rollout-current.jsonl"
    old = archived / "rollout-old.jsonl"
    _write_jsonl(current, _rollout_events("current-session"))
    _write_jsonl(old, _rollout_events("archived-session"))
    _write_jsonl(sessions / "not-json.txt", [{"ignored": True}])

    current_only = list(CodexRolloutSource([sessions]).discover())
    with_archived = list(
        CodexRolloutSource(
            [sessions],
            include_archived=True,
            archived_root=archived,
        ).discover()
    )

    assert [item.path for item in current_only] == [current.resolve()]
    assert [item.session_id for item in with_archived] == [
        "archived-session",
        "current-session",
    ]


def test_codex_source_extracts_metadata_and_trace_id(tmp_path: Path) -> None:
    rollout = tmp_path / "sessions" / "2026" / "05" / "01" / "rollout.jsonl"
    _write_jsonl(rollout, _rollout_events("session-123"))

    [item] = list(CodexRolloutSource([rollout]).discover())

    assert item.session_id == "session-123"
    assert item.trace_id == trace_id_for_session("session-123")
    assert item.metadata == {
        "cli_version": "0.124.0-alpha.2",
        "cwd": "/Users/james/Code/sample",
        "model": "gpt-5.4",
        "model_provider": "openai",
        "originator": "Codex Desktop",
        "session_id": "session-123",
        "source": "vscode",
        "surface": "vscode",
    }
    assert read_rollout_metadata(rollout)["model"] == "gpt-5.4"


def test_codex_dry_run_reports_mtime_fingerprint_metadata_and_reason(
    tmp_path: Path,
) -> None:
    rollout = tmp_path / "sessions" / "rollout.jsonl"
    _write_jsonl(rollout, _rollout_events("session-123"))
    _age(rollout, seconds=20)

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        summary = run_once(
            source=CodexRolloutSource([rollout]),
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
    assert data["fingerprint"].startswith("sha256:")
    assert data["mtime_ns"] is not None
    assert data["mtime"] is not None
    assert data["reason"] == "dry-run"
    assert data["metadata"]["originator"] == "Codex Desktop"
    assert records == []


def test_codex_active_file_is_not_ingested_by_default(tmp_path: Path) -> None:
    rollout = tmp_path / "sessions" / "rollout.jsonl"
    _write_jsonl(rollout, _rollout_events("session-123"))
    _age(rollout, seconds=1)
    calls: list[IngestRequest] = []

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        summary = run_once(
            source=CodexRolloutSource([rollout]),
            store=store,
            ingest=lambda request: calls.append(request) or IngestOutcome(),
            quiet_seconds=10,
            now=NOW,
        )
        record = store.get("codex", rollout)

    assert summary.skipped_active == 1
    assert calls == []
    assert record is not None
    assert record.status == IngestStatus.ACTIVE
    assert record.metadata["surface"] == "vscode"


def test_codex_run_once_ingests_then_skips_unchanged(tmp_path: Path) -> None:
    rollout = tmp_path / "sessions" / "rollout.jsonl"
    _write_jsonl(rollout, _rollout_events("session-123"))
    _age(rollout, seconds=20)
    calls: list[IngestRequest] = []

    def ingest(request: IngestRequest) -> IngestOutcome:
        calls.append(request)
        return IngestOutcome(trace_id="trace-from-export")

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        first = run_once(
            source=CodexRolloutSource([rollout]),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            now=NOW,
        )
        second = run_once(
            source=CodexRolloutSource([rollout]),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            dry_run=True,
            now=NOW + 1,
        )
        record = store.get("codex", rollout)

    assert first.ingested == 1
    assert second.skipped_unchanged == 1
    assert second.actions[0].to_dict()["reason"] == "fingerprint already ingested"
    assert second.actions[0].to_dict()["mtime"] is not None
    assert len(calls) == 1
    assert record is not None
    assert record.status == IngestStatus.INGESTED
    assert record.session_id == "session-123"
    assert record.trace_id == "trace-from-export"
    assert record.metadata["originator"] == "Codex Desktop"


def test_cli_codex_dry_run_with_fixture_root(tmp_path: Path, capsys) -> None:
    rollout = tmp_path / "sessions" / "rollout.jsonl"
    _write_jsonl(rollout, _rollout_events("session-123"))
    _age(rollout, seconds=20, now=time.time())
    state_db = tmp_path / "state.sqlite3"

    exit_code = main(
        [
            "--once",
            "--source",
            "codex",
            "--codex-root",
            str(rollout.parent),
            "--state-db",
            str(state_db),
            "--store-url",
            f"sqlite://{tmp_path}/store.sqlite3",
            "--no-raw-archive",
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
    assert action["path"] == str(rollout.resolve())
    assert action["fingerprint"].startswith("sha256:")
    assert action["mtime"] is not None
    assert action["reason"] == "dry-run"
    with SqliteIngestStateStore(state_db) as store:
        assert store.list_records() == []
