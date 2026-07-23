from __future__ import annotations

import json
import os
import time
from pathlib import Path

from flume.ingest.cli import main
from flume.ingest.fake import FakeTranscriptSource, fake_ingest
from flume.ingest.fingerprint import fingerprint_file, is_quiet
from flume.ingest.runner import IngestOutcome, IngestRequest, run_once
from flume.ingest.state import IngestStatus, SqliteIngestStateStore


NOW = 1_800_000_000.0


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def _age(path: Path, *, seconds: float, now: float = NOW) -> None:
    mtime_ns = int((now - seconds) * 1_000_000_000)
    os.utime(path, ns=(mtime_ns, mtime_ns))


def test_fingerprint_identity_uses_content_not_mtime(tmp_path: Path) -> None:
    path = tmp_path / "session.jsonl"
    _write_jsonl(path, [{"session_id": "s1"}])
    _age(path, seconds=20)

    original = fingerprint_file(path)
    _age(path, seconds=100)
    touched = fingerprint_file(path)

    assert touched.identity == original.identity
    assert touched.mtime_ns != original.mtime_ns

    _write_jsonl(path, [{"session_id": "s1"}, {"event": "new"}])
    _age(path, seconds=20)
    changed = fingerprint_file(path)

    assert changed.identity != original.identity


def test_quiet_file_detection_uses_mtime_window(tmp_path: Path) -> None:
    path = tmp_path / "session.jsonl"
    _write_jsonl(path, [{"session_id": "s1"}])
    _age(path, seconds=10)

    fingerprint = fingerprint_file(path)

    assert is_quiet(fingerprint, quiet_seconds=5, now=NOW) is True
    assert is_quiet(fingerprint, quiet_seconds=30, now=NOW) is False


def test_dry_run_reports_pending_without_state_or_ingest(tmp_path: Path) -> None:
    transcript = tmp_path / "fixtures" / "session.jsonl"
    _write_jsonl(transcript, [{"session_id": "s1", "trace_id": "t1"}])
    _age(transcript, seconds=20)
    calls: list[IngestRequest] = []

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        summary = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=lambda request: calls.append(request) or IngestOutcome(),
            quiet_seconds=5,
            dry_run=True,
            now=NOW,
        )

        assert summary.discovered == 1
        assert summary.would_ingest == 1
        assert calls == []
        assert store.list_records() == []


def test_quiet_file_ingests_once_then_skips_unchanged(tmp_path: Path) -> None:
    transcript = tmp_path / "fixtures" / "session.jsonl"
    _write_jsonl(transcript, [{"session_id": "s1", "trace_id": "t1"}])
    _age(transcript, seconds=20)
    calls: list[IngestRequest] = []

    def ingest(request: IngestRequest) -> IngestOutcome:
        calls.append(request)
        return IngestOutcome(trace_id="trace-from-ingest")

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        first = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            now=NOW,
        )
        second = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            now=NOW + 1,
        )
        record = store.get("fake", transcript)

    assert first.ingested == 1
    assert second.skipped_unchanged == 1
    assert len(calls) == 1
    assert record is not None
    assert record.status == IngestStatus.INGESTED
    assert record.session_id == "s1"
    assert record.trace_id == "trace-from-ingest"
    assert record.last_ingested_at is not None
    assert record.attempts == 1


def test_active_file_is_checkpointed_then_ingested_after_quiet(
    tmp_path: Path,
) -> None:
    transcript = tmp_path / "fixtures" / "session.jsonl"
    _write_jsonl(transcript, [{"session_id": "s1"}])
    _age(transcript, seconds=1)
    calls: list[IngestRequest] = []

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        active = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=lambda request: calls.append(request) or IngestOutcome(),
            quiet_seconds=10,
            now=NOW,
        )
        active_record = store.get("fake", transcript)

        _age(transcript, seconds=30)
        quiet = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=lambda request: calls.append(request) or IngestOutcome(),
            quiet_seconds=10,
            now=NOW,
        )
        quiet_record = store.get("fake", transcript)

    assert active.skipped_active == 1
    assert active_record is not None
    assert active_record.status == IngestStatus.ACTIVE
    assert active_record.last_ingested_at is None
    assert quiet.ingested == 1
    assert len(calls) == 1
    assert quiet_record is not None
    assert quiet_record.status == IngestStatus.INGESTED


def test_failure_is_persisted_and_retried(tmp_path: Path) -> None:
    transcript = tmp_path / "fixtures" / "session.jsonl"
    _write_jsonl(transcript, [{"session_id": "s1"}])
    _age(transcript, seconds=20)
    calls = 0

    def flaky(_request: IngestRequest) -> IngestOutcome:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary outage")
        return IngestOutcome()

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        failed = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=flaky,
            quiet_seconds=5,
            now=NOW,
        )
        failed_record = store.get("fake", transcript)
        retried = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=flaky,
            quiet_seconds=5,
            now=NOW + 1,
        )
        retried_record = store.get("fake", transcript)

    assert failed.failed == 1
    assert failed_record is not None
    assert failed_record.status == IngestStatus.FAILED
    assert failed_record.error == "RuntimeError: temporary outage"
    assert failed_record.attempts == 1
    assert retried.ingested == 1
    assert retried_record is not None
    assert retried_record.status == IngestStatus.INGESTED
    assert retried_record.error is None
    assert retried_record.attempts == 2


def test_changed_file_reingests_after_success(tmp_path: Path) -> None:
    transcript = tmp_path / "fixtures" / "session.jsonl"
    _write_jsonl(transcript, [{"session_id": "s1"}])
    _age(transcript, seconds=20)
    calls: list[str] = []

    def ingest(request: IngestRequest) -> IngestOutcome:
        calls.append(request.fingerprint.identity)
        return IngestOutcome()

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            now=NOW,
        )

        _write_jsonl(transcript, [{"session_id": "s1"}, {"event": "new"}])
        _age(transcript, seconds=20)
        changed = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=ingest,
            quiet_seconds=5,
            now=NOW + 1,
        )

    assert changed.ingested == 1
    assert len(calls) == 2
    assert calls[0] != calls[1]


def test_fake_ingest_can_mark_fixture_failure(tmp_path: Path) -> None:
    transcript = tmp_path / "fixtures" / "bad.jsonl"
    _write_jsonl(
        transcript,
        [{"session_id": "s1"}, {"fake_ingest_error": True}],
    )
    _age(transcript, seconds=20)

    with SqliteIngestStateStore(tmp_path / "state.sqlite3") as store:
        summary = run_once(
            source=FakeTranscriptSource(transcript.parent),
            store=store,
            ingest=fake_ingest,
            quiet_seconds=5,
            now=NOW,
        )
        record = store.get("fake", transcript)

    assert summary.failed == 1
    assert record is not None
    assert record.status == IngestStatus.FAILED
    assert record.error == "RuntimeError: fake ingest requested failure"


def test_cli_once_with_fake_source_persists_success(
    tmp_path: Path,
    capsys,
) -> None:
    transcript = tmp_path / "fixtures" / "session.jsonl"
    _write_jsonl(transcript, [{"session_id": "s1", "trace_id": "t1"}])
    _age(transcript, seconds=20, now=time.time())
    state_db = tmp_path / "state.sqlite3"

    exit_code = main(
        [
            "--once",
            "--source",
            "fake",
            "--fake-root",
            str(transcript.parent),
            "--state-db",
            str(state_db),
            "--store-url",
            f"sqlite://{tmp_path}/store.sqlite3",
            "--no-raw-archive",
            "--quiet-seconds",
            "5",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["ingested"] == 1
    with SqliteIngestStateStore(state_db) as store:
        record = store.get("fake", transcript)
    assert record is not None
    assert record.status == IngestStatus.INGESTED
