"""The SQL capability boundary between SessionStore and the analysis layer.

Analytics need ad-hoc SQL; the portable store contract does not provide it.
Rather than grow SessionStore with ~15 single-caller aggregations, analysis
declares `SqlReadable` explicitly. These tests pin that boundary: a
SQL-capable store satisfies it structurally, a store that only implements
SessionStore fails with a message naming the feature, and no caller reaches
for a private attribute to find out.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from flume.analysis.hooks import hook_events
from flume.analysis.insights import run_insights
from flume.analysis.navtime import session_nav_shares
from flume.store.base import (
    SqlReadable,
    StoreCapabilityError,
    require_sql,
)
from flume.store.sqlite import SqliteSessionStore


class _NoSqlStore:
    """A store shaped like a non-SQL backend (a remote API, say)."""

    def ingest_session(self, bundle): ...
    def overview(self): return {}
    def close(self): ...


def test_sqlite_satisfies_the_protocol_structurally(tmp_path: Path) -> None:
    with SqliteSessionStore(tmp_path / "s.sqlite3") as store:
        assert isinstance(store, SqlReadable)
        # And the surface is public — no private attribute access anywhere.
        assert store.rows("SELECT 1 AS n") == [{"n": 1}]
        assert store.row("SELECT 1 AS n") == {"n": 1}
        assert store.row("SELECT 1 AS n WHERE 0") is None


def test_a_store_without_sql_does_not_satisfy_it() -> None:
    assert not isinstance(_NoSqlStore(), SqlReadable)


def test_require_sql_names_the_feature_that_needs_it() -> None:
    with pytest.raises(StoreCapabilityError, match="cost"):
        require_sql(_NoSqlStore(), "cost")
    # And says which store fell short, so the message is actionable.
    with pytest.raises(StoreCapabilityError, match="_NoSqlStore"):
        require_sql(_NoSqlStore(), "cost")


def test_require_sql_passes_a_capable_store_through(tmp_path: Path) -> None:
    with SqliteSessionStore(tmp_path / "s.sqlite3") as store:
        assert require_sql(store, "anything") is store


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(lambda s: run_insights(s, persist=False), id="insights"),
        pytest.param(lambda s: session_nav_shares(s), id="navtime"),
        pytest.param(lambda s: hook_events(s), id="hooks"),
    ],
)
def test_analysis_entry_points_fail_clearly_without_sql(call) -> None:
    """The old failure mode was an AttributeError on a private helper."""
    with pytest.raises(StoreCapabilityError):
        call(_NoSqlStore())


def test_capability_error_is_a_typeerror() -> None:
    # Callers that already catch TypeError (the previous contract) keep working.
    assert issubclass(StoreCapabilityError, TypeError)
