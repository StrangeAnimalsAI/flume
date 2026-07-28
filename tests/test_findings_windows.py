"""Stored findings are per-window, and resolved ones leave the view.

The stored path exists so the dashboard reads a table instead of re-running
detectors that touch tens of MB. Two properties make that safe to trust:
a 7d and a 30d observation of the same fingerprint must not merge (or
"how long has this been true" is unanswerable), and a finding the latest run
no longer reports must disappear (or a fixed problem reads as current
forever).
"""
from __future__ import annotations

from pathlib import Path

from flume.store.base import open_analyzed_store


def _finding(fingerprint: str, *, severity: int = 2, metric: float = 1.0) -> dict:
    return {
        "kind": "repeat_waste",
        "fingerprint": fingerprint,
        "severity": severity,
        "title": f"finding {fingerprint}",
        "detail": "detail",
        "metric": metric,
        "unit": "calls",
        "action": "do the thing",
        "evidence": "{}",
    }


def test_windows_do_not_merge(tmp_path: Path) -> None:
    with open_analyzed_store(f"sqlite://{tmp_path}/s.sqlite3") as store:
        store.upsert_findings([_finding("f1")], window="7d")
        store.upsert_findings([_finding("f1")], window="30d")

        seven = store.list_findings(window="7d")
        thirty = store.list_findings(window="30d")
        assert len(seven) == 1 and len(thirty) == 1
        # Same fingerprint, two independent histories — not one seen twice.
        assert seven[0]["occurrences"] == 1
        assert thirty[0]["occurrences"] == 1


def test_repeat_in_same_window_accumulates(tmp_path: Path) -> None:
    with open_analyzed_store(f"sqlite://{tmp_path}/s.sqlite3") as store:
        for _ in range(3):
            store.upsert_findings([_finding("f1")], window="7d")
        stored = store.list_findings(window="7d")
        assert stored[0]["occurrences"] == 3
        assert stored[0]["first_seen_ns"] <= stored[0]["last_seen_ns"]


def test_unfiltered_list_still_sees_every_window(tmp_path: Path) -> None:
    with open_analyzed_store(f"sqlite://{tmp_path}/s.sqlite3") as store:
        store.upsert_findings([_finding("f1")], window="7d")
        store.upsert_findings([_finding("f1")], window="30d")
        assert len(store.list_findings()) == 2


def test_resolved_findings_are_swept(tmp_path: Path) -> None:
    """A finding the newest run didn't report must not linger."""
    import time

    with open_analyzed_store(f"sqlite://{tmp_path}/s.sqlite3") as store:
        store.upsert_findings(
            [_finding("gone"), _finding("stays")], window="7d"
        )
        assert len(store.list_findings(window="7d")) == 2

        stamp = time.time_ns()
        store.upsert_findings([_finding("stays")], window="7d")
        removed = store.clear_findings(window="7d", keep_seen_after_ns=stamp)

        assert removed == 1
        remaining = store.list_findings(window="7d")
        assert [f["fingerprint"] for f in remaining] == ["stays"]
        # The survivor keeps its history rather than being reset.
        assert remaining[0]["occurrences"] == 2


def test_sweep_is_scoped_to_its_window(tmp_path: Path) -> None:
    """A 7d refresh must not delete 30d rows it was never going to report."""
    import time

    with open_analyzed_store(f"sqlite://{tmp_path}/s.sqlite3") as store:
        store.upsert_findings([_finding("f1")], window="30d")
        stamp = time.time_ns()
        store.upsert_findings([_finding("f2")], window="7d")
        store.clear_findings(window="7d", keep_seen_after_ns=stamp)

        assert [f["fingerprint"] for f in store.list_findings(window="30d")] == ["f1"]


def test_legacy_rows_migrate_to_the_digest_window(tmp_path: Path) -> None:
    """Pre-window rows were all written by the daily `--since 7d` digest."""
    import sqlite3

    db = tmp_path / "s.sqlite3"
    with open_analyzed_store(f"sqlite://{db}") as store:
        store.upsert_findings([_finding("legacy")], window="7d")

    # Recreate the old shape: no window column, PK on (kind, fingerprint).
    conn = sqlite3.connect(db)
    with conn:
        conn.execute("DROP TABLE findings")
        conn.execute(
            "CREATE TABLE findings (kind TEXT NOT NULL, fingerprint TEXT NOT"
            " NULL, severity INTEGER NOT NULL, title TEXT NOT NULL, detail"
            " TEXT, metric REAL, unit TEXT, action TEXT, evidence TEXT,"
            " first_seen_ns INTEGER NOT NULL, last_seen_ns INTEGER NOT NULL,"
            " occurrences INTEGER NOT NULL DEFAULT 1,"
            " PRIMARY KEY (kind, fingerprint))"
        )
        conn.execute(
            "INSERT INTO findings VALUES ('repeat_waste','legacy',2,'t','d',"
            "1.0,'calls','a','{}',111,222,9)"
        )
    conn.close()

    with open_analyzed_store(f"sqlite://{db}") as store:
        rows = store.list_findings(window="7d")
        assert [r["fingerprint"] for r in rows] == ["legacy"]
        # History survives the rebuild — that is the whole point of keeping it.
        assert rows[0]["occurrences"] == 9
        assert rows[0]["first_seen_ns"] == 111
