"""Tests for provenance columns and rebuild-from-raw.

The guarantee under test: every analyzed row records which raw bytes
(sha256) and which pipeline version produced it, and `rebuild`
can rebuild rows from the raw raw_store even after the vendor app pruned
the original transcript.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from flume.ingest.write import ingest_path, rebuild_stale
from flume.sources import get_adapter
from flume.store.raw import open_raw_store
from flume.store.base import open_analyzed_store
from flume.store.bundle import PIPELINE_VERSION


def _write_session(tmp_path: Path, session_id: str = "sess-1") -> Path:
    events = [
        {
            "type": "user",
            "uuid": "u1",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "cwd": "/Users/alex/Code/demo",
            "message": {"role": "user", "content": "hello"},
        },
        {
            "type": "assistant",
            "uuid": "a1",
            "timestamp": "2026-04-20T10:00:01.000Z",
            "message": {
                "usage": {"input_tokens": 5, "output_tokens": 3},
                "content": [{"type": "text", "text": "hi"}],
            },
        },
    ]
    path = tmp_path / f"{session_id}.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return path


def _mark_stale(tmp_path: Path, session_id: str) -> None:
    conn = sqlite3.connect(f"{tmp_path}/store.sqlite3")
    with conn:
        conn.execute(
            "UPDATE sessions SET pipeline_version = 0 WHERE session_id = ?",
            (session_id,),
        )
    conn.close()


def test_ingest_stamps_provenance(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), src)
        session = store.get_session("sess-1")

    assert session is not None
    assert session["pipeline_version"] == PIPELINE_VERSION
    assert session["raw_sha256"] == hashlib.sha256(src.read_bytes()).hexdigest()


def test_stale_sessions_and_overview_count(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), src)
        assert store.stale_sessions(PIPELINE_VERSION) == []
        assert store.overview()["totals"]["stale_sessions"] == 0

    _mark_stale(tmp_path, "sess-1")
    with open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        stale = store.stale_sessions(PIPELINE_VERSION)
        assert [row["session_id"] for row in stale] == ["sess-1"]
        assert store.overview()["totals"]["stale_sessions"] == 1


def test_rebuild_from_original_file(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with (
        open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_raw_store(f"file://{tmp_path}/raw") as raw_store,
    ):
        ingest_path(store, get_adapter("claude-code"), src, raw_store=raw_store)
    _mark_stale(tmp_path, "sess-1")

    with (
        open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_raw_store(f"file://{tmp_path}/raw") as raw_store,
    ):
        report = rebuild_stale(store, raw_store)
        assert report["rebuilt"] == 1
        assert report["from_original"] == 1
        session = store.get_session("sess-1")

    assert session is not None
    assert session["pipeline_version"] == PIPELINE_VERSION


def test_rebuild_from_archive_after_vendor_pruned(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with (
        open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_raw_store(f"file://{tmp_path}/raw") as raw_store,
    ):
        ingest_path(store, get_adapter("claude-code"), src, raw_store=raw_store)
    _mark_stale(tmp_path, "sess-1")
    src.unlink()  # vendor app pruned the transcript

    with (
        open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_raw_store(f"file://{tmp_path}/raw") as raw_store,
    ):
        report = rebuild_stale(store, raw_store)
        assert report["rebuilt"] == 1
        assert report["from_archive"] == 1
        session = store.get_session("sess-1")
        assert store.stale_sessions(PIPELINE_VERSION) == []

    assert session is not None
    assert session["pipeline_version"] == PIPELINE_VERSION
    assert session["first_user_message"] == "hello"
    # Metadata replay keeps probe-derived facts the temp file can't provide.
    assert session["cwd"] == "/Users/alex/Code/demo"


def test_rebuild_reports_missing_raw(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), src)  # no raw_store
    _mark_stale(tmp_path, "sess-1")
    src.unlink()

    with (
        open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_raw_store(f"file://{tmp_path}/raw") as raw_store,
    ):
        report = rebuild_stale(store, raw_store)

    assert report["rebuilt"] == 0
    assert report["missing_raw"] == ["sess-1"]


def test_rebuild_dry_run_changes_nothing(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with (
        open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_raw_store(f"file://{tmp_path}/raw") as raw_store,
    ):
        ingest_path(store, get_adapter("claude-code"), src, raw_store=raw_store)
    _mark_stale(tmp_path, "sess-1")

    with (
        open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_raw_store(f"file://{tmp_path}/raw") as raw_store,
    ):
        report = rebuild_stale(store, raw_store, dry_run=True)
        assert report == {
            "pipeline_version": PIPELINE_VERSION,
            "stale": 1,
            "dry_run": True,
            "sessions": ["sess-1"],
        }
        assert len(store.stale_sessions(PIPELINE_VERSION)) == 1
