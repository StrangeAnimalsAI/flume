"""Tests for the raw archive, retention cycle, and source-adapter registry."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from flume.ingest.write import ingest_path
from flume.sources import get_adapter
from flume.store.archive import open_archive
from flume.store.base import open_store
from flume.store.config import (
    RetentionPolicy,
    load_policy,
    parse_duration_ns,
)
from flume.store.retention import run_retention

ALL_SOURCES = ["claude-code", "codex", "harness"]

DAY_NS = 86_400_000_000_000


def _write_session(tmp_path: Path, session_id: str = "sess-1", extra: str = "") -> Path:
    events = [
        {
            "type": "user",
            "uuid": "u1",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "message": {"role": "user", "content": "hello" + extra},
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


# -- registry ---------------------------------------------------------------


def test_adapter_resolves_by_name_and_vendor() -> None:
    assert get_adapter("claude-code").vendor == "anthropic"
    assert get_adapter("openai").name == "codex"
    # Two anthropic-vendor sources (claude-code, harness): alias is ambiguous.
    with pytest.raises(ValueError, match="ambiguous"):
        get_adapter("anthropic")
    with pytest.raises(ValueError, match="unknown source"):
        get_adapter("gemini")


# -- archive ----------------------------------------------------------------


def test_capture_restore_roundtrip(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with open_archive(f"file://{tmp_path}/raw") as archive:
        entry = archive.capture("claude-code", "sess-1", src)
        assert entry is not None
        restored = archive.restore(entry, tmp_path / "restored.jsonl")

    assert restored.read_bytes() == src.read_bytes()


def test_capture_dedupes_identical_content_and_versions_changes(
    tmp_path: Path,
) -> None:
    src = _write_session(tmp_path)
    with open_archive(f"file://{tmp_path}/raw") as archive:
        assert archive.capture("claude-code", "sess-1", src) is not None
        assert archive.capture("claude-code", "sess-1", src) is None  # same bytes

        src.write_text(src.read_text() + "\n")  # file grew
        assert archive.capture("claude-code", "sess-1", src) is not None
        assert len(archive.versions("sess-1")) == 2


def test_ingest_path_archives_raw(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with (
        open_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_archive(f"file://{tmp_path}/raw") as archive,
    ):
        outcome = ingest_path(store, get_adapter("claude-code"), src, archive=archive)
        assert outcome is not None and outcome.session_id == "sess-1"
        assert len(archive.versions("sess-1")) == 1
        assert archive.stats()[0]["source"] == "claude-code"


def test_mapper_failure_still_archives_raw(tmp_path: Path, monkeypatch) -> None:
    import flume.sources as sources
    from flume.sources import SourceAdapter

    def boom(path: Path):
        raise OverflowError("string longer than INT_MAX bytes")

    monkeypatch.setitem(
        sources._ADAPTERS,
        "boom",
        SourceAdapter(name="boom", vendor="test", map_spans=boom,
                      extract_contents=lambda p, s: []),
    )
    src = _write_session(tmp_path, session_id="giant")
    with (
        open_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_archive(f"file://{tmp_path}/raw") as archive,
    ):
        with pytest.raises(OverflowError):
            ingest_path(store, get_adapter("boom"), src, archive=archive)
        assert len(archive.versions("giant")) == 1  # raw survived the crash


# -- retention config ---------------------------------------------------------


def test_parse_durations() -> None:
    assert parse_duration_ns("forever") is None
    assert parse_duration_ns("30d") == 30 * DAY_NS
    assert parse_duration_ns("2w") == 14 * DAY_NS
    assert parse_duration_ns("12h") == DAY_NS // 2
    with pytest.raises(ValueError):
        parse_duration_ns("30 days")


def test_load_policy_from_toml(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        """
[retention]
raw = "forever"
analyzed = "90d"

[retention.raw_overrides]
codex = "30d"
"""
    )
    policy = load_policy(config)
    assert policy.raw_ttl_ns("claude-code") is None
    assert policy.raw_ttl_ns("codex") == 30 * DAY_NS
    assert policy.analyzed_ttl_ns("claude-code") == 90 * DAY_NS


def test_missing_config_means_keep_forever(tmp_path: Path) -> None:
    policy = load_policy(tmp_path / "nope.toml")
    assert policy.raw_ttl_ns("claude-code") is None
    assert policy.analyzed_ttl_ns("codex") is None


# -- retention cycle ----------------------------------------------------------


def test_retention_deletes_expired_tiers_only(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with (
        open_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_archive(f"file://{tmp_path}/raw") as archive,
    ):
        ingest_path(store, get_adapter("claude-code"), src, archive=archive)
        entry = archive.versions("sess-1")[0]
        # Session ended 2026-04-20; blob captured "now" (test runtime).
        session = store.get_session("sess-1")
        assert session is not None

        policy = RetentionPolicy(
            raw_default_ns=None,  # raw: keep forever
            analyzed_default_ns=1 * DAY_NS,  # analyzed: 1 day
        )
        now_ns = session["ended_at_ns"] + 2 * DAY_NS  # 2 days after session end

        report = run_retention(
            store=store,
            archive=archive,
            policy=policy,
            sources=ALL_SOURCES,
            now_ns=now_ns,
            dry_run=True,
        )
        assert report["analyzed"]["claude-code"]["deleted"] == 1
        assert store.get_session("sess-1") is not None  # dry run deletes nothing

        report = run_retention(
            store=store, archive=archive, policy=policy, sources=ALL_SOURCES, now_ns=now_ns
        )
        assert report["analyzed"]["claude-code"]["deleted"] == 1
        assert store.get_session("sess-1") is None  # analyzed pruned
        assert archive.versions("sess-1") == [entry]  # raw kept forever


def test_retention_deletes_expired_raw_blobs(tmp_path: Path) -> None:
    src = _write_session(tmp_path)
    with (
        open_store(f"sqlite://{tmp_path}/store.sqlite3") as store,
        open_archive(f"file://{tmp_path}/raw") as archive,
    ):
        ingest_path(store, get_adapter("claude-code"), src, archive=archive)
        entry = archive.versions("sess-1")[0]
        blob = Path(f"{tmp_path}/raw/blobs") / entry.blob_path
        assert blob.is_file()

        policy = RetentionPolicy(raw_default_ns=1 * DAY_NS)
        now_ns = entry.captured_at_ns + 2 * DAY_NS

        report = run_retention(
            store=store, archive=archive, policy=policy, sources=ALL_SOURCES, now_ns=now_ns
        )
        assert report["raw"]["claude-code"]["deleted"] == 1
        assert archive.versions("sess-1") == []
        assert not blob.exists()
        # Analyzed tier untouched (default forever).
        assert store.get_session("sess-1") is not None


def test_archive_capture_sanitizes_hostile_session_ids(tmp_path: Path) -> None:
    from flume.store.archive import FsRawArchive

    src = tmp_path / "t.jsonl"
    src.write_text('{"x": 1}\n')
    archive = FsRawArchive(tmp_path / "raw")
    entry = archive.capture("claude-code", "../../../../tmp/escape", src)
    assert entry is not None
    blob = tmp_path / "raw" / "blobs" / entry.blob_path
    assert blob.resolve().is_relative_to((tmp_path / "raw" / "blobs").resolve())
    assert ".." not in entry.blob_path
