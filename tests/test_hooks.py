"""Hook-event extraction: markers parsed, compliance judged correctly.

The transcript marker shape is what Claude Code actually writes (verified
against a live reread-guard denial, 2026-07-08):

    PreToolUse:Read hook error: [python3 .../reread-guard.py]: message
"""
from __future__ import annotations

from agent_telemetry.store.base import ContentRow, SessionBundle
from agent_telemetry.store.hooks import hook_events, hooks_summary
from agent_telemetry.store.sqlite import SqliteSessionStore

BASE_NS = 1_780_000_000 * 1_000_000_000
SECOND_NS = 1_000_000_000

DENIAL = (
    "PreToolUse:Read hook error: [python3 /Users/james/.claude/hooks/"
    "reread-guard.py]: reread-guard: you already read /repo/src/big.py "
    "this session and the file has not changed since — the content is in "
    "your context; use it from there."
)


def _session(session_id, *, denial_ts, later_read_of=None):
    contents = [
        ContentRow(span_id=f"{session_id}-t1", kind="tool_result",
                   seq=0, ts_ns=denial_ts, text=DENIAL),
    ]
    tool_calls = []
    if later_read_of:
        tool_calls.append({
            "span_id": f"{session_id}-read2",
            "turn_span_id": None,
            "name": "Read",
            "args_hash": "h2",
            "args_preview": f'{{"file_path": "{later_read_of}", "offset": 40}}',
            "started_at_ns": denial_ts + 5 * SECOND_NS,
            "ended_at_ns": denial_ts + 6 * SECOND_NS,
            "duration_ms": 1000,
            "is_error": 0,
            "result_chars": 900,
        })
    session = {
        "session_id": session_id,
        "source": "claude-code",
        "project": "tools/demo",
        "is_subagent": 0,
        "started_at_ns": denial_ts - 60 * SECOND_NS,
        "ended_at_ns": denial_ts + 60 * SECOND_NS,
        "turn_count": 2,
        "tool_call_count": len(tool_calls),
    }
    return SessionBundle(
        session=session, turns=[], tool_calls=tool_calls, contents=contents
    )


def test_marker_parsed_and_bypass_detected(tmp_path):
    with SqliteSessionStore(tmp_path / "s.sqlite3") as store:
        store.ingest_session(
            _session("bypasser", denial_ts=BASE_NS,
                     later_read_of="/repo/src/big.py")
        )
        store.ingest_session(_session("heeder", denial_ts=BASE_NS))
        events = hook_events(store)

    by_session = {e["session_id"]: e for e in events}
    event = by_session["bypasser"]
    assert event["event"] == "PreToolUse"
    assert event["matcher"] == "Read"
    assert event["hook"] == "reread-guard.py"
    assert event["message"].startswith("reread-guard: you already read")
    assert event["outcome"] == "bypassed"
    assert by_session["heeder"]["outcome"] == "heeded"


def test_summary_rolls_up_compliance(tmp_path):
    with SqliteSessionStore(tmp_path / "s.sqlite3") as store:
        store.ingest_session(
            _session("s1", denial_ts=BASE_NS, later_read_of="/repo/src/big.py")
        )
        store.ingest_session(_session("s2", denial_ts=BASE_NS + SECOND_NS))
        store.ingest_session(_session("s3", denial_ts=BASE_NS + 2 * SECOND_NS))
        summary = hooks_summary(hook_events(store))

    assert len(summary) == 1
    row = summary[0]
    assert row["hook"] == "reread-guard.py"
    assert row["events"] == 3
    assert row["sessions"] == 3
    assert row["heeded"] == 2
    assert row["bypassed"] == 1


def test_quoted_marker_in_bash_output_is_not_an_event(tmp_path):
    bundle = _session("greppy", denial_ts=BASE_NS)
    bundle.contents.append(
        ContentRow(span_id="greppy-bash1", kind="tool_result", seq=1,
                   ts_ns=BASE_NS + SECOND_NS,
                   text=f"$ grep 'hook error' transcript.jsonl\n{DENIAL}")
    )
    bundle.tool_calls.append({
        "span_id": "greppy-bash1", "turn_span_id": None, "name": "Bash",
        "args_hash": "hb", "args_preview": '{"command": "grep ..."}',
        "started_at_ns": BASE_NS + SECOND_NS,
        "ended_at_ns": BASE_NS + SECOND_NS, "duration_ms": 10,
        "is_error": 0, "result_chars": 300,
    })
    with SqliteSessionStore(tmp_path / "s.sqlite3") as store:
        store.ingest_session(bundle)
        events = hook_events(store)
    # only the real denial survives; the grep echo is filtered
    assert len(events) == 1
    assert events[0]["span_id"] == "greppy-t1"


def test_session_filter_and_empty_window(tmp_path):
    with SqliteSessionStore(tmp_path / "s.sqlite3") as store:
        store.ingest_session(_session("only", denial_ts=BASE_NS))
        assert hook_events(store, session_id="only")
        assert hook_events(store, session_id="other") == []
        assert hook_events(store, since_ns=BASE_NS + 10 * SECOND_NS) == []
