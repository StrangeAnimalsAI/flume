"""Insight detectors: per-detector assertions.

`test_store.py` covers repeat_waste and schema_loop end-to-end through real
ingest. This file drives the store directly so each remaining detector can
be pushed just over and just under its threshold, and covers the three
things the source-agnostic refactor changed:

- detectors scan every source unless scoped (they used to hardcode
  claude-code and silently drop everything else)
- "premium" is derived from the price table, not a vendor name list
- the agent-index detector matches configurable markers, not one tool's
  private directory
"""
from __future__ import annotations

from pathlib import Path

from flume.analysis.insights import run_insights
from flume.store.base import ContentRow, SessionBundle
from flume.store.sqlite import SqliteAnalyzedStore

SEC_NS = 1_000_000_000
BASE_NS = 1_780_000_000 * SEC_NS  # 2026-06-08


def _store(tmp_path: Path) -> SqliteAnalyzedStore:
    return SqliteAnalyzedStore(tmp_path / "store.sqlite3")


def _ingest(
    store,
    session_id: str,
    *,
    source: str = "claude-code",
    model: str = "claude-haiku-4-5",
    cwd: str = "/repo",
    project: str = "acme/app",
    tool_call_count: int = 0,
    turn_count: int = 1,
    output_tokens: int = 0,
    cache_read_tokens: int = 0,
    turns: list | None = None,
    tool_calls: list | None = None,
    contents: list | None = None,
    started_ns: int = BASE_NS,
) -> None:
    store.ingest_session(SessionBundle(
        session={
            "session_id": session_id,
            "source": source,
            "model": model,
            "cwd": cwd,
            "project": project,
            "is_subagent": 0,
            "started_at_ns": started_ns,
            "ended_at_ns": started_ns + 3600 * SEC_NS,
            "wall_ms": 3_600_000,
            "turn_count": turn_count,
            "tool_call_count": tool_call_count,
            "input_tokens": 100,
            "output_tokens": output_tokens,
            "cache_read_tokens": cache_read_tokens,
            "cache_creation_tokens": 0,
            "reasoning_tokens": 0,
            "thinking_blocks": 0,
            "thinking_chars": 0,
            "file_path": f"/tmp/{session_id}.jsonl",
            "pipeline_version": 1,
            "ingested_at_ns": started_ns,
        },
        turns=list(turns or []),
        tool_calls=list(tool_calls or []),
        contents=list(contents or []),
    ))


def _turn(session_id, index, at_ns, *, model="claude-haiku-4-5",
          output_tokens=0, text_chars=0, cache_creation_tokens=0, ended_ns=None):
    return {
        "span_id": f"{session_id}-turn-{index}",
        "turn_index": index,
        "model": model,
        "started_at_ns": at_ns,
        "ended_at_ns": ended_ns if ended_ns is not None else at_ns,
        "duration_ms": 0,
        "input_tokens": 10,
        "output_tokens": output_tokens,
        "cache_read_tokens": 0,
        "cache_creation_tokens": cache_creation_tokens,
        "reasoning_tokens": 0,
        "thinking_chars": 0,
        "text_chars": text_chars,
    }


def _call(session_id, index, name, *, is_error=0, result_chars=100,
          args_preview="{}", at_ns=BASE_NS):
    return {
        "span_id": f"{session_id}-tool-{index}",
        "turn_span_id": f"{session_id}-turn-0",
        "name": name,
        "args_hash": f"h-{session_id}-{index}",
        "args_preview": args_preview,
        "started_at_ns": at_ns,
        "ended_at_ns": at_ns + SEC_NS,
        "duration_ms": 1000,
        "is_error": is_error,
        "result_chars": result_chars,
    }


def _kinds(findings) -> set[str]:
    return {f["kind"] for f in findings}


def _one(findings, kind):
    matches = [f for f in findings if f["kind"] == kind]
    assert len(matches) == 1, f"expected one {kind}, got {len(matches)}"
    return matches[0]


# -- source scoping (the Phase 1 regression) ---------------------------------


def test_detectors_scan_every_source_by_default(tmp_path: Path) -> None:
    """The bug this replaced: detectors hardcoded claude-code, so a corpus
    that was mostly Codex reported almost nothing."""
    with _store(tmp_path) as store:
        _ingest(store, "cx", source="codex", tool_call_count=40,
                tool_calls=[_call("cx", i, "exec_command", is_error=1)
                            for i in range(25)])
        findings = run_insights(store, persist=False)
    hotspot = _one(findings, "error_hotspot")
    assert hotspot["fingerprint"] == "exec_command"


def test_source_scopes_the_scan(tmp_path: Path) -> None:
    with _store(tmp_path) as store:
        _ingest(store, "cx", source="codex", tool_call_count=40,
                tool_calls=[_call("cx", i, "exec_command", is_error=1)
                            for i in range(25)])
        _ingest(store, "cc", source="claude-code", tool_call_count=40,
                tool_calls=[_call("cc", i, "Bash", is_error=1)
                            for i in range(25)])

        both = {f["fingerprint"] for f in run_insights(store, persist=False)
                if f["kind"] == "error_hotspot"}
        codex_only = {f["fingerprint"] for f in
                      run_insights(store, source="codex", persist=False)
                      if f["kind"] == "error_hotspot"}
    assert both == {"exec_command", "Bash"}
    assert codex_only == {"exec_command"}


# -- premium tier derived from price -----------------------------------------


def _pricing_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(body)
    return path


def test_premium_grind_follows_price_not_vendor_name(
    tmp_path: Path, monkeypatch
) -> None:
    """A model flume has never heard of must still count as premium when it
    is priced like one — the old name list could not express that."""
    monkeypatch.setenv("FLUME_CONFIG", str(_pricing_config(tmp_path, """
[pricing]
"pricey-newcomer" = { input = 15.0, output = 75.0 }
"thrifty-newcomer" = { input = 0.2, output = 1.0 }
""")))
    with _store(tmp_path) as store:
        _ingest(store, "expensive", model="pricey-newcomer", tool_call_count=400)
        _ingest(store, "cheap", model="thrifty-newcomer", tool_call_count=400)
        findings = run_insights(store, persist=False)
    grind = _one(findings, "premium_grind")
    assert grind["fingerprint"] == "expensive"
    assert grind["metric"] == 400


def test_premium_grind_ignores_light_premium_sessions(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with _store(tmp_path) as store:
        # Premium model, but under the 150-call bar.
        _ingest(store, "light", model="claude-opus-5", tool_call_count=150)
        findings = run_insights(store, persist=False)
    assert "premium_grind" not in _kinds(findings)


def test_premium_threshold_is_configurable(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(_pricing_config(tmp_path, """
[pricing]
premium_output_threshold = 4.0
""")))
    with _store(tmp_path) as store:
        # Haiku is $5 output — premium only under a lowered threshold.
        _ingest(store, "haiku-heavy", model="claude-haiku-4-5",
                tool_call_count=400)
        findings = run_insights(store, persist=False)
    assert _one(findings, "premium_grind")["fingerprint"] == "haiku-heavy"


# -- agent-index detector (configurable markers) ------------------------------


def _repo_with(tmp_path: Path, name: str, marker: str) -> str:
    repo = tmp_path / name
    (repo / Path(marker).parent).mkdir(parents=True, exist_ok=True)
    (repo / marker).write_text("index\n")
    return str(repo)


def test_index_ignored_fires_for_any_configured_marker(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(_pricing_config(tmp_path, """
[insights]
index_markers = ["MYINDEX.md"]
""")))
    cwd = _repo_with(tmp_path, "repo", "MYINDEX.md")
    with _store(tmp_path) as store:
        for i in range(3):
            _ingest(store, f"blind-{i}", cwd=cwd, tool_call_count=50)
        findings = run_insights(store, persist=False)
    finding = _one(findings, "index_ignored")
    assert finding["metric"] == 3
    assert "MYINDEX.md" in finding["detail"]


def test_index_ignored_excludes_sessions_that_consulted_it(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(_pricing_config(tmp_path, """
[insights]
index_markers = ["MYINDEX.md"]
""")))
    cwd = _repo_with(tmp_path, "repo", "MYINDEX.md")
    with _store(tmp_path) as store:
        for i in range(2):
            _ingest(store, f"blind-{i}", cwd=cwd, tool_call_count=50)
        # This one read the index — it must not be counted as ignoring it,
        # which drops the total under the 3-session floor.
        _ingest(
            store, "reader", cwd=cwd, tool_call_count=50,
            contents=[ContentRow(span_id=None, kind="tool_arguments", seq=0,
                                 ts_ns=BASE_NS, text="cat MYINDEX.md")],
        )
        findings = run_insights(store, persist=False)
    assert "index_ignored" not in _kinds(findings)


def test_index_ignored_silent_without_an_index(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(_pricing_config(tmp_path, """
[insights]
index_markers = ["MYINDEX.md"]
""")))
    plain = tmp_path / "plain"
    plain.mkdir()
    with _store(tmp_path) as store:
        for i in range(5):
            _ingest(store, f"s-{i}", cwd=str(plain), tool_call_count=50)
        findings = run_insights(store, persist=False)
    assert "index_ignored" not in _kinds(findings)


# -- threshold detectors ------------------------------------------------------


def test_context_flood_needs_a_200k_char_result(tmp_path: Path) -> None:
    with _store(tmp_path) as store:
        _ingest(store, "flood", tool_call_count=2, tool_calls=[
            _call("flood", 0, "Bash", result_chars=250_000),
            _call("flood", 1, "Bash", result_chars=1_000),
        ])
        _ingest(store, "fine", tool_call_count=1, tool_calls=[
            _call("fine", 0, "Grep", result_chars=199_000),
        ])
        findings = run_insights(store, persist=False)
    flood = _one(findings, "context_flood")
    assert flood["fingerprint"] == "claude-code:Bash"


def test_marathon_session_needs_over_200_turns(tmp_path: Path) -> None:
    with _store(tmp_path) as store:
        _ingest(store, "marathon", turn_count=201, cache_read_tokens=10**9)
        _ingest(store, "normal", turn_count=200, cache_read_tokens=10**9)
        findings = run_insights(store, persist=False)
    assert _one(findings, "marathon_session")["fingerprint"] == "marathon"


def test_error_hotspot_needs_volume_and_a_10pct_rate(tmp_path: Path) -> None:
    with _store(tmp_path) as store:
        # 20 calls, 4 errors (20%) — fires.
        _ingest(store, "hot", tool_call_count=20, tool_calls=[
            _call("hot", i, "Flaky", is_error=1 if i < 4 else 0)
            for i in range(20)
        ])
        # 20 calls, 1 error (5%) — under the rate bar.
        _ingest(store, "ok", tool_call_count=20, tool_calls=[
            _call("ok", i, "Solid", is_error=1 if i < 1 else 0)
            for i in range(20)
        ])
        findings = run_insights(store, persist=False)
    assert _one(findings, "error_hotspot")["fingerprint"] == "Flaky"


def test_idle_gap_churn_counts_cache_rewrites_across_sources(
    tmp_path: Path
) -> None:
    """Was claude-code-only; a Codex corpus reported zero idle churn."""
    gap = 400 * SEC_NS  # over the 5-minute prompt-cache TTL
    turns = []
    at = BASE_NS
    for i in range(6):
        turns.append(_turn("idle", i, at, ended_ns=at + SEC_NS,
                           cache_creation_tokens=1_100_000))
        at += gap
    with _store(tmp_path) as store:
        _ingest(store, "idle", source="codex", turn_count=len(turns), turns=turns)
        findings = run_insights(store, persist=False)
    churn = _one(findings, "idle_gap_churn")
    assert churn["fingerprint"] == "all-sources"
    assert churn["metric"] > 5  # >5M tokens of rewrites


def test_idle_gap_churn_silent_below_5m_tokens(tmp_path: Path) -> None:
    turns = [
        _turn("quiet", i, BASE_NS + i * 400 * SEC_NS,
              ended_ns=BASE_NS + i * 400 * SEC_NS + SEC_NS,
              cache_creation_tokens=1000)
        for i in range(6)
    ]
    with _store(tmp_path) as store:
        _ingest(store, "quiet", turn_count=len(turns), turns=turns)
        findings = run_insights(store, persist=False)
    assert "idle_gap_churn" not in _kinds(findings)


def test_findings_are_ranked_by_severity_then_metric(tmp_path: Path) -> None:
    with _store(tmp_path) as store:
        _ingest(store, "flood", tool_call_count=1, tool_calls=[
            _call("flood", 0, "Bash", result_chars=300_000)])
        _ingest(store, "marathon", turn_count=300, cache_read_tokens=10**9)
        findings = run_insights(store, persist=False)
    severities = [f["severity"] for f in findings]
    assert severities == sorted(severities)


def test_persist_false_leaves_the_findings_table_untouched(
    tmp_path: Path
) -> None:
    """The web server runs detectors on a read-only store; persisting there
    would make a page refresh bump occurrence counts."""
    with _store(tmp_path) as store:
        _ingest(store, "marathon", turn_count=300, cache_read_tokens=10**9)
        assert run_insights(store, persist=False)
        assert store.list_findings() == []
        assert run_insights(store, persist=True)
        assert store.list_findings()
