"""Tests for the tool taxonomy, the tool_calls_ext view, and `analyze sql`."""
from __future__ import annotations

from pathlib import Path

import pytest

from flume.analysis.cli import main
from flume.store.base import open_store
from flume.store.taxonomy import tool_kind, tool_vendor


def test_tool_kind_classification() -> None:
    assert tool_kind("Bash") == "shell"
    assert tool_kind("exec_command") == "shell"
    assert tool_kind("WebFetch") == "web"
    assert tool_kind("mcp__linear__save_issue") == "mcp"
    assert tool_kind("Read") == "file"
    assert tool_kind("Edit") == "edit"
    assert tool_kind("Agent") == "subagent"
    assert tool_kind("apply_patch") == "edit"
    assert tool_kind("SomethingNovel") == "other"
    assert tool_kind(None) == "other"


def test_tool_vendor_extraction() -> None:
    assert tool_vendor("mcp__linear__save_issue") == "linear"
    assert tool_vendor("mcp__095e-uuid-abc__d1_query") == "095e-uuid-abc"
    assert tool_vendor("Bash") is None


def _insert(store, span, name, chars) -> None:
    store._conn.execute(
        "INSERT INTO tool_calls (span_id, session_id, name, result_chars) "
        "VALUES (?, 'sess', ?, ?)", (span, name, chars))
    store._conn.commit()


def test_view_matches_python_and_estimates_tokens(tmp_path: Path) -> None:
    with open_store(f"sqlite://{tmp_path}/s.sqlite3") as store:
        _insert(store, "a", "mcp__linear__save_issue", 400)
        _insert(store, "b", "exec_command", 4000)
        rows = {r["name"]: r for r in store.rows(
            "SELECT name, kind, vendor, result_tokens_est FROM tool_calls_ext")}
    assert rows["mcp__linear__save_issue"]["kind"] == "mcp"
    assert rows["mcp__linear__save_issue"]["vendor"] == "linear"
    assert rows["exec_command"]["kind"] == "shell"
    assert rows["exec_command"]["result_tokens_est"] == 1000
    # SQL view and the Python classifier agree.
    for name, row in rows.items():
        assert row["kind"] == tool_kind(name)


def test_sql_command_is_read_only(tmp_path: Path, capsys) -> None:
    db = f"sqlite://{tmp_path}/s.sqlite3"
    with open_store(db) as store:
        _insert(store, "a", "Bash", 8)

    assert main(["--store-url", db, "--json", "sql",
                 "SELECT kind FROM tool_calls_ext"]) == 0
    assert "shell" in capsys.readouterr().out

    with pytest.raises(SystemExit, match="read-only"):
        main(["--store-url", db, "sql", "DELETE FROM sessions"])
