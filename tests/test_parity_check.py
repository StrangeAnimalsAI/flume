"""Tests for the parity check module.

Strategy: feed a small synthetic JSONL through the real span mapper, then
feed a synthetic Langfuse trace dict (matching what the HTTP API returns)
through the reconstruction. In the happy path, the two reports match; in the
drift path, we mutate the Langfuse dict and check the diff surfaces the drift.

No network: `fetch_trace` is bypassed — we call `reconstruct_report` directly
and feed its output into `diff_reports`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flume.analysis.claude_transcript import (
    analyze_session,
    is_nav_tool,
    repeat_key,
    summarize_input,
)
from flume.analysis.parity_check import (
    diff_reports,
    reconstruct_report,
    stale_ingest_diagnostics,
)


def _jsonl(path: Path) -> Path:
    """Tiny realistic JSONL: one assistant turn with a Read + a turn_duration."""
    events = [
        {
            "type": "user",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "message": {"role": "user", "content": "hello"},
        },
        {
            "type": "assistant",
            "uuid": "a1",
            "timestamp": "2026-04-20T10:00:01.000Z",
            "entrypoint": "cli",
            "message": {
                "model": "claude-opus-4-7",
                "id": "msg_1",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 20,
                    "cache_read_input_tokens": 400,
                    "cache_creation_input_tokens": 5,
                },
                "content": [
                    {"type": "text", "text": "hi"},
                    {
                        "type": "tool_use",
                        "id": "tu_1",
                        "name": "Read",
                        "input": {"file_path": "/tmp/x.py"},
                    },
                ],
            },
        },
        {
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:01.010Z",
            "durationMs": 1000,
        },
        {
            "type": "user",
            "timestamp": "2026-04-20T10:00:02.000Z",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tu_1",
                        "is_error": False,
                        "content": "x" * 100,
                    }
                ]
            },
        },
        {
            "type": "assistant",
            "uuid": "a2",
            "timestamp": "2026-04-20T10:00:03.000Z",
            "entrypoint": "cli",
            "message": {
                "usage": {"input_tokens": 2, "output_tokens": 7},
                "content": [{"type": "text", "text": "done"}],
            },
        },
        {
            "type": "system",
            "subtype": "turn_duration",
            "timestamp": "2026-04-20T10:00:03.010Z",
            "durationMs": 500,
        },
    ]
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return path


def _spans_to_langfuse_trace(spans: list[dict]) -> dict:
    """Convert mapper output to the shape `/api/public/traces/{id}` returns.

    We only fill the fields that `reconstruct_report` reads. Critically, all
    attribute values are stringified — Langfuse's OTLP ingestion serializes
    numeric OTel attributes back out as strings, and the parity code must
    cope with that.
    """
    from datetime import datetime, timezone

    def iso(ns: int) -> str:
        dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
        # ms precision, Z suffix.
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    root = next(s for s in spans if s["parent_span_id"] is None)

    observations = []
    for s in spans:
        stringy = {k: str(v) for k, v in (s["attributes"] or {}).items()}
        observations.append(
            {
                "id": s["span_id"],
                "traceId": s["trace_id"],
                "name": s["name"],
                "parentObservationId": s["parent_span_id"],
                "startTime": iso(s["start_unix_nano"]),
                "endTime": iso(s["end_unix_nano"]),
                "metadata": {"attributes": stringy},
                "type": "GENERATION"
                if s["name"] == "claude_code.llm_request"
                else "SPAN",
            }
        )

    return {
        "id": root["trace_id"],
        "timestamp": iso(root["start_unix_nano"]),
        "latency": (root["end_unix_nano"] - root["start_unix_nano"]) / 1e9,
        "observations": observations,
    }


def test_happy_path_reconstructs_exact_report(tmp_path: Path) -> None:
    from flume.backfill.claude_code import jsonl_to_spans

    jsonl = _jsonl(tmp_path / "sess.jsonl")
    spans = jsonl_to_spans(jsonl)
    trace = _spans_to_langfuse_trace(spans)

    local = analyze_session(jsonl)
    lf = reconstruct_report(trace, "sess", repeat_key, is_nav_tool, summarize_input)

    rows = diff_reports(local, lf)
    drifts = [r for r in rows if not r.ok]
    assert drifts == [], (
        f"unexpected drift: {[(r.metric, r.local, r.langfuse) for r in drifts]}"
    )


@pytest.mark.parametrize(
    "mutation,expect_metric",
    [
        ("tokens", "tokens.input"),
        ("tool_chars", "by_tool[Read].total_result_chars"),
        ("active_time", "active_time_s"),
    ],
)
def test_drift_is_detected(tmp_path: Path, mutation: str, expect_metric: str) -> None:
    """Mutate the Langfuse trace and verify `diff_reports` flags the metric."""
    from flume.backfill.claude_code import jsonl_to_spans

    jsonl = _jsonl(tmp_path / "sess.jsonl")
    spans = jsonl_to_spans(jsonl)
    trace = _spans_to_langfuse_trace(spans)

    if mutation == "tokens":
        # Corrupt one turn's input_tokens attribute.
        for o in trace["observations"]:
            if o["name"] == "claude_code.llm_request":
                o["metadata"]["attributes"]["gen_ai.usage.input_tokens"] = "999"
                break
    elif mutation == "tool_chars":
        for o in trace["observations"]:
            if o["name"] == "claude_code.tool":
                o["metadata"]["attributes"]["tool.result_chars"] = "0"
                break
    elif mutation == "active_time":
        # Zero out duration attrs and collapse span ranges → active_time_s=0.
        for o in trace["observations"]:
            if o["name"] == "claude_code.llm_request":
                o["metadata"]["attributes"]["claude_code.duration_ms"] = "0"
                o["startTime"] = o["endTime"]

    local = analyze_session(jsonl)
    lf = reconstruct_report(trace, "sess", repeat_key, is_nav_tool, summarize_input)
    rows = diff_reports(local, lf)

    drifted_metrics = {r.metric for r in rows if not r.ok}
    assert expect_metric in drifted_metrics, (
        f"expected drift on {expect_metric}, got drifts={drifted_metrics}"
    )


def test_langfuse_attrs_coerced_from_strings(tmp_path: Path) -> None:
    """Regression guard: OTLP/Langfuse returns numeric attrs as strings; the
    reconstruction must coerce, not silently read them as '0'."""
    from flume.backfill.claude_code import jsonl_to_spans

    jsonl = _jsonl(tmp_path / "sess.jsonl")
    spans = jsonl_to_spans(jsonl)
    trace = _spans_to_langfuse_trace(spans)  # already stringifies everything

    lf = reconstruct_report(trace, "sess", repeat_key, is_nav_tool, summarize_input)
    # Non-trivial values prove the strings were parsed.
    assert lf["tokens"]["input"] == 12  # 10 + 2
    assert lf["tokens"]["output"] == 27  # 20 + 7
    assert lf["by_tool"]["Read"]["total_result_chars"] == 100


def test_stale_ingest_diagnostic_flags_one_ms_timestamp_drift() -> None:
    diagnostics = stale_ingest_diagnostics(
        {
            "first_ts": "2026-04-20T10:00:00.827Z",
            "last_ts": "2026-04-20T10:00:03.010Z",
        },
        {
            "first_ts": "2026-04-20T10:00:00.826Z",
            "last_ts": "2026-04-20T10:00:03.010Z",
        },
    )

    assert any("first_ts differs by 1.000 ms" in d for d in diagnostics)
    assert any("langfuse_trace_reconcile" in d for d in diagnostics)


def test_stale_ingest_diagnostic_flags_missing_langfuse_end_time() -> None:
    diagnostics = stale_ingest_diagnostics(
        {
            "first_ts": "2026-04-20T10:00:00.000Z",
            "last_ts": "2026-04-20T10:00:03.010Z",
        },
        {"first_ts": "2026-04-20T10:00:00.000Z", "last_ts": None},
    )

    assert any(
        "last_ts is present locally but missing from Langfuse" in d for d in diagnostics
    )


def test_stale_ingest_diagnostic_ignores_non_timestamp_drift() -> None:
    diagnostics = stale_ingest_diagnostics(
        {"first_ts": "2026-04-20T10:00:00.000Z", "tokens": {"input": 10}},
        {"first_ts": "2026-04-20T10:00:00.000Z", "tokens": {"input": 999}},
    )

    assert diagnostics == []
