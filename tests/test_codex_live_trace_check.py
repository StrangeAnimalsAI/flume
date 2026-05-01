from __future__ import annotations

from collections import Counter

from agent_telemetry.analysis.codex_live_trace_check import (
    CodexTrace,
    _is_codex_trace,
    _is_smoke_trace,
    format_summary,
)


def test_is_codex_trace_detects_name_and_langfuse_metadata() -> None:
    assert _is_codex_trace({"name": "codex.interaction"})
    assert _is_codex_trace({"name": "other", "metadata": {"agent_source": "codex"}})
    assert _is_codex_trace({"name": "other", "metadata": {"agent_family": "codex"}})
    assert _is_codex_trace({"name": "other", "tags": ["agent:codex"]})
    assert not _is_codex_trace(
        {"name": "claude_code.interaction", "metadata": {"agent_source": "claude-code"}}
    )


def test_is_smoke_trace_detects_synthetic_checks() -> None:
    assert _is_smoke_trace(
        {
            "metadata": {
                "attributes": {"session.id": "codex-smoke-2026-04-30"},
                "resourceAttributes": {"service.name": "codex-smoke"},
            }
        }
    )
    assert not _is_smoke_trace(
        {
            "metadata": {
                "attributes": {"session.id": "real-session"},
                "resourceAttributes": {"service.name": "codex_exec"},
            }
        }
    )


def test_format_summary_reports_presence_and_shape() -> None:
    summary = format_summary(
        [
            CodexTrace(
                trace_id="trace-1",
                name="codex.interaction",
                timestamp="2026-04-30T23:00:00Z",
                session_id="session-a",
                service_name="codex_exec",
                agent_source="codex",
                agent_family="codex",
                observation_names=Counter(
                    {"codex.interaction": 1, "codex.llm_request": 2, "codex.tool": 3}
                ),
            )
        ]
    )

    assert "codex_traces=1" in summary
    assert "session.id=session-a" in summary
    assert "service.name=codex_exec" in summary
    assert "agent_source=codex" in summary
    assert "codex.llm_request=2" in summary
    assert "codex_live_traces=present" in summary


def test_format_summary_reports_missing() -> None:
    summary = format_summary([])

    assert "codex_traces=0" in summary
    assert "codex_live_traces=missing" in summary
