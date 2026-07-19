"""Experiment tagging + comparison, and the nav-time attribution they rely on.

The load-bearing guarantees:
- sessions ingested inside an experiment's window/scope carry its tag,
  outside it they don't, and retagging is idempotent (re-ingest never
  loses a tag; stopping an experiment untags later sessions)
- nav-share attribution splits inter-turn cycles across the tool calls
  inside them, excludes user-idle gaps, and classes bash nav correctly
"""
from __future__ import annotations

from flume.store.base import ContentRow, SessionBundle
from flume.store.experiments import compare_experiment
from flume.store.navtime import classify_tool, session_nav_shares
from flume.store.sqlite import SqliteSessionStore

HOUR_NS = 3600 * 1_000_000_000
BASE_NS = 1_780_000_000 * 1_000_000_000  # 2026-06-08


def _bundle(session_id, started_ns, *, project="tools/demo", source="claude-code",
            turns=(), tool_calls=()):
    session = {
        "session_id": session_id,
        "source": source,
        "cwd": f"/Users/james/Code/{project}",
        "project": project,
        "is_subagent": 0,
        "started_at_ns": started_ns,
        "ended_at_ns": started_ns + HOUR_NS,
        "wall_ms": 3_600_000,
        "turn_count": len(turns),
        "tool_call_count": len(tool_calls),
        "input_tokens": 100,
        "output_tokens": 50,
        "cache_read_tokens": 900,
        "cache_creation_tokens": 10,
        "reasoning_tokens": 0,
        "thinking_blocks": 0,
        "thinking_chars": 0,
        "file_path": f"/tmp/{session_id}.jsonl",
        "pipeline_version": 1,
        "ingested_at_ns": started_ns,
    }
    return SessionBundle(
        session=session,
        turns=list(turns),
        tool_calls=list(tool_calls),
        contents=[
            ContentRow(span_id=None, kind="user_message", seq=0,
                       ts_ns=started_ns, text="do the thing"),
        ],
    )


def _turn(session_id, index, at_ns):
    return {
        "span_id": f"{session_id}-turn-{index}",
        "turn_index": index,
        "started_at_ns": at_ns,
        "ended_at_ns": at_ns,
        "duration_ms": 0,
        "input_tokens": 10,
        "output_tokens": 5,
        "cache_read_tokens": 0,
        "cache_creation_tokens": 0,
        "reasoning_tokens": 0,
        "thinking_chars": 0,
        "text_chars": 0,
    }


def _call(session_id, index, at_ns, name, args_preview, result_chars=1000):
    return {
        "span_id": f"{session_id}-tool-{index}",
        "turn_span_id": f"{session_id}-turn-{index}",
        "name": name,
        "args_hash": f"hash-{name}-{args_preview[:20]}",
        "args_preview": args_preview,
        "started_at_ns": at_ns,
        "ended_at_ns": at_ns + 1_000_000_000,
        "duration_ms": 1000,
        "is_error": 0,
        "result_chars": result_chars,
    }


def _store(tmp_path) -> SqliteSessionStore:
    return SqliteSessionStore(tmp_path / "store.sqlite3")


def _session_row(store, session_id) -> dict:
    row = store.get_session(session_id)
    assert row is not None, f"session {session_id} missing"
    return row


# -- experiment tagging -------------------------------------------------------


def test_ingest_tags_sessions_inside_experiment_window(tmp_path):
    with _store(tmp_path) as store:
        store.create_experiment(
            "docnav-all", hypothesis="indexing cuts nav time",
            started_at_ns=BASE_NS,
        )
        store.ingest_session(_bundle("in-window", BASE_NS + HOUR_NS))
        store.ingest_session(_bundle("before-window", BASE_NS - HOUR_NS))

        assert _session_row(store, "in-window")["experiment"] == "docnav-all"
        assert _session_row(store, "before-window")["experiment"] is None
        assert store.experiment_session_ids("docnav-all") == ["in-window"]


def test_scope_filters_restrict_tagging(tmp_path):
    with _store(tmp_path) as store:
        store.create_experiment(
            "scoped", project="tools/demo", source="claude-code",
            started_at_ns=BASE_NS,
        )
        store.ingest_session(_bundle("match", BASE_NS + 1))
        store.ingest_session(
            _bundle("other-project", BASE_NS + 2, project="biz/sketchup")
        )
        store.ingest_session(_bundle("other-source", BASE_NS + 3, source="codex"))

        assert _session_row(store, "match")["experiment"] == "scoped"
        assert _session_row(store, "other-project")["experiment"] is None
        assert _session_row(store, "other-source")["experiment"] is None


def test_retag_backfills_existing_sessions_and_stop_untags_later_ones(tmp_path):
    with _store(tmp_path) as store:
        store.ingest_session(_bundle("pre-existing", BASE_NS + HOUR_NS))
        # created AFTER ingest with a window that covers it -> retro-tagged
        store.create_experiment("retro", started_at_ns=BASE_NS)
        assert _session_row(store, "pre-existing")["experiment"] == "retro"

        store.end_experiment("retro", ended_at_ns=BASE_NS + 2 * HOUR_NS)
        store.ingest_session(_bundle("after-end", BASE_NS + 3 * HOUR_NS))
        assert _session_row(store, "after-end")["experiment"] is None
        # re-ingesting a tagged session keeps its tag (recomputed, not stored-once)
        store.ingest_session(_bundle("pre-existing", BASE_NS + HOUR_NS))
        assert _session_row(store, "pre-existing")["experiment"] == "retro"


def test_overlapping_experiments_join_names(tmp_path):
    with _store(tmp_path) as store:
        store.create_experiment("alpha", started_at_ns=BASE_NS)
        store.create_experiment("beta", started_at_ns=BASE_NS)
        store.ingest_session(_bundle("both", BASE_NS + 1))
        assert _session_row(store, "both")["experiment"] == "alpha,beta"
        assert store.experiment_session_ids("alpha") == ["both"]
        assert store.experiment_session_ids("beta") == ["both"]


def test_list_experiments_counts_sessions(tmp_path):
    with _store(tmp_path) as store:
        store.create_experiment("counted", started_at_ns=BASE_NS)
        store.ingest_session(_bundle("s1", BASE_NS + 1))
        store.ingest_session(_bundle("s2", BASE_NS + 2))
        rows = store.list_experiments()
        assert [(r["name"], r["sessions"]) for r in rows] == [("counted", 2)]


# -- nav-time attribution -----------------------------------------------------


def test_classify_tool_shapes():
    assert classify_tool("Read", None) == "navigation"
    assert classify_tool("Bash", '{"command": "rg -n pattern src"}') == "navigation"
    assert classify_tool("Bash", '{"command": "repo-nav search foo"}') == "navigation"
    assert classify_tool("Bash", '{"command": "cargo test"}') == "bash-other"
    # 'cat' must match as a command word, not inside 'cargo'/'concatenate'
    assert classify_tool("Bash", '{"command": "cat foo.py"}') == "navigation"
    assert classify_tool("Edit", None) == "editing"
    assert classify_tool("Agent", None) == "subagent"


def test_nav_share_splits_cycles_and_skips_idle(tmp_path):
    second = 1_000_000_000
    t0 = BASE_NS
    turns = [
        _turn("nav", 0, t0),
        _turn("nav", 1, t0 + 10 * second),   # cycle 1: 10s, one Read
        _turn("nav", 2, t0 + 20 * second),   # cycle 2: 10s, one Edit
        _turn("nav", 3, t0 + 30 * second),   # cycle 3: 10s, no tools (generation)
        _turn("nav", 4, t0 + 1000 * second), # cycle 4: 970s idle -> excluded
    ]
    tool_calls = [
        _call("nav", 0, t0 + 1 * second, "Read",
              '{"file_path": "/tmp/a.py"}', result_chars=5000),
        _call("nav", 1, t0 + 11 * second, "Edit", '{"file_path": "/tmp/a.py"}'),
    ]
    with _store(tmp_path) as store:
        store.ingest_session(
            _bundle("nav", t0, turns=turns, tool_calls=tool_calls)
        )
        rows = session_nav_shares(store)

    assert len(rows) == 1
    row = rows[0]
    assert row["session_id"] == "nav"
    assert row["active_s"] == 30.0          # idle cycle excluded
    assert row["nav_s"] == 10.0             # the Read cycle
    assert abs(row["nav_share"] - 1 / 3) < 0.001
    assert row["generation_s"] == 10.0
    assert row["nav_calls"] == 1


# -- comparison ---------------------------------------------------------------


def test_compare_experiment_baseline_vs_tagged(tmp_path):
    second = 1_000_000_000
    day_ns = 86_400 * second

    def _session(session_id, start, nav_heavy):
        turns = [_turn(session_id, i, start + i * 40 * second) for i in range(4)]
        name, args = ("Read", '{"file_path": "/x.py"}') if nav_heavy else (
            "Edit", '{"file_path": "/x.py"}')
        tool_calls = [
            _call(session_id, i, start + i * 40 * second + second, name, args)
            for i in range(3)
        ]
        return _bundle(session_id, start, turns=turns, tool_calls=tool_calls)

    with _store(tmp_path) as store:
        # baseline: nav-heavy sessions before the experiment starts
        store.ingest_session(_session("base-1", BASE_NS - 5 * day_ns, True))
        store.ingest_session(_session("base-2", BASE_NS - 4 * day_ns, True))
        store.create_experiment("tooling", started_at_ns=BASE_NS)
        store.ingest_session(_session("exp-1", BASE_NS + 1 * day_ns, False))

        result = compare_experiment(store, "tooling", baseline_days=30)

    groups = {g["group"]: g for g in result["groups"]}
    assert groups["baseline"]["sessions"] == 2
    assert groups["experiment"]["sessions"] == 1
    assert groups["baseline"]["nav_share_median"] == 1.0
    assert groups["experiment"]["nav_share_median"] == 0.0
    assert groups["baseline"]["nav_calls"] == 6
    assert groups["experiment"]["nav_calls"] == 0
    assert groups["experiment"]["cache_hit"] == 0.9


def test_read_used_share_derived_from_edits_and_mentions(tmp_path):
    from flume.store.experiments import _read_used_share

    second = 1_000_000_000
    bundle = _bundle(
        "utility", BASE_NS,
        turns=[_turn("utility", i, BASE_NS + i * 40 * second) for i in range(4)],
        tool_calls=[
            _call("utility", 0, BASE_NS + second, "Read",
                  '{"file_path": "/repo/edited.py"}'),
            _call("utility", 1, BASE_NS + 41 * second, "Read",
                  '{"file_path": "/repo/cited.py"}'),
            _call("utility", 2, BASE_NS + 81 * second, "Read",
                  '{"file_path": "/repo/deadend.py"}'),
            _call("utility", 3, BASE_NS + 121 * second, "Edit",
                  '{"file_path": "/repo/edited.py"}'),
        ],
    )
    bundle.contents.append(
        ContentRow(span_id=None, kind="assistant_message", seq=1,
                   ts_ns=BASE_NS + 122 * second,
                   text="The bug lives in cited.py; deadend was unrelated."),
    )
    with _store(tmp_path) as store:
        store.ingest_session(bundle)
        # edited.py used via Edit, cited.py via mention, deadend.py neither
        assert _read_used_share(store, ["utility"]) == round(2 / 3, 3)
        assert _read_used_share(store, ["missing"]) is None


def test_compare_unknown_experiment_raises(tmp_path):
    with _store(tmp_path) as store:
        try:
            compare_experiment(store, "nope")
        except KeyError:
            pass
        else:
            raise AssertionError("expected KeyError")
