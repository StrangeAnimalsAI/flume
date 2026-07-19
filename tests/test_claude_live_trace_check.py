from __future__ import annotations

from flume.analysis.claude_live_trace_check import (
    ClaudeObservation,
    format_summary,
    summarize,
)


def _obs(
    trace_id: str,
    name: str,
    *,
    parent_id: str = "",
    session_id: str = "session-a",
) -> ClaudeObservation:
    return ClaudeObservation(
        trace_id=trace_id,
        trace_name=name,
        observation_id=f"obs-{trace_id}-{name}",
        name=name,
        parent_id=parent_id,
        session_id=session_id,
        source="claude-code-cli",
    )


def test_summarize_reports_clean_interaction_grouping() -> None:
    root = _obs("trace-1", "claude_code.interaction", session_id="session-a")
    child = _obs(
        "trace-1",
        "claude_code.llm_request",
        parent_id=root.observation_id,
        session_id="session-a",
    )

    summary = summarize([root, child])

    assert summary.has_grouping_issues is False
    assert summary.traces_missing_interaction == set()
    assert summary.orphan_child_observations == []
    assert summary.sessions_split_across_traces == {}
    assert "claude_live_grouping=ok" in format_summary(summary)


def test_summarize_reports_split_session_and_orphan_child_span() -> None:
    root = _obs("trace-1", "claude_code.interaction", session_id="session-a")
    child = _obs("trace-2", "claude_code.llm_request", session_id="session-a")

    summary = summarize([root, child])

    assert summary.has_grouping_issues is True
    assert summary.traces_missing_interaction == {"trace-2"}
    assert summary.orphan_child_observations == [child]
    assert summary.sessions_split_across_traces == {
        "session-a": {"trace-1", "trace-2"}
    }
    assert "claude_live_grouping=split_or_missing_roots" in format_summary(summary)
