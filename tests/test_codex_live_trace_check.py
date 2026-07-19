from __future__ import annotations

from collections import Counter

from flume.analysis.codex_live_trace_check import (
    CodexTrace,
    _analysis_counts,
    _is_codex_trace,
    _is_smoke_trace,
    format_summary,
)


def test_is_codex_trace_detects_name_and_langfuse_metadata() -> None:
    assert _is_codex_trace({"name": "codex.interaction"})
    assert _is_codex_trace({"name": "other", "metadata": {"agent_source": "codex"}})
    assert _is_codex_trace({"name": "other", "metadata": {"agent_family": "codex"}})
    assert _is_codex_trace({"name": "other", "tags": ["agent:codex"]})
    assert _is_codex_trace(
        {
            "name": "turn/start",
            "metadata": {
                "resourceAttributes": {"service.name": "codex-app-server"},
            },
        }
    )
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
                analysis_fields=Counter(
                    {
                        "session_id_observations": 1,
                        "turn_id_observations": 2,
                        "llm_request_candidates": 2,
                        "tool_call_candidates": 3,
                        "token_usage_observations": 2,
                        "duration_observations": 6,
                    }
                ),
                vocabulary_candidates=Counter(
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
    assert "session_id_observations" in summary
    assert "token_usage_observations" in summary
    assert "backfill_vocabulary_candidates:" in summary
    assert "codex_live_traces=present" in summary


def test_format_summary_reports_missing() -> None:
    summary = format_summary([])

    assert "codex_traces=0" in summary
    assert "codex_live_traces=missing" in summary


def test_analysis_counts_map_live_llm_tool_and_id_fields() -> None:
    fields, vocabulary = _analysis_counts(
        {
            "name": "mcp.tools.call",
            "type": "SPAN",
            "startTime": "2026-04-30T23:00:00Z",
            "endTime": "2026-04-30T23:00:01Z",
            "metadata": {
                "attributes": {
                    "tool.name": "linear_search",
                    "tool.call_id": "call_1",
                    "session.id": "session-a",
                    "turn.id": "turn-a",
                }
            },
        }
    )

    assert fields["session_id_observations"] == 1
    assert fields["turn_id_observations"] == 1
    assert fields["tool_call_candidates"] == 1
    assert fields["duration_observations"] == 1
    assert vocabulary == Counter({"codex.tool": 1})


def test_analysis_counts_detect_token_attrs_without_prompt_text() -> None:
    fields, vocabulary = _analysis_counts(
        {
            "name": "handle_responses",
            "type": "SPAN",
            "metadata": {
                "attributes": {
                    "gen_ai.usage.input_tokens": "207126",
                    "gen_ai.usage.cache_read.input_tokens": "203648",
                    "gen_ai.usage.output_tokens": "200",
                    "rpc.request_id": "not-a-prompt",
                }
            },
        }
    )

    assert fields["llm_request_candidates"] == 1
    assert fields["token_usage_observations"] == 1
    assert fields["prompt_payload_observations"] == 0
    assert vocabulary == Counter({"codex.llm_request": 1})


def test_analysis_counts_detect_prompt_payload_when_langfuse_has_it() -> None:
    fields, _ = _analysis_counts(
        {
            "name": "model_client.stream_responses_websocket",
            "type": "GENERATION",
            "input": [{"role": "user", "content": "do the thing"}],
            "metadata": {"attributes": {"model": "gpt-5.5", "turn_id": "turn-a"}},
        }
    )

    assert fields["llm_request_candidates"] == 1
    assert fields["turn_id_observations"] == 1
    assert fields["prompt_payload_observations"] == 1
