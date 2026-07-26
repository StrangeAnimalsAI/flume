"""Cost reporting: pricing resolution and the unpriced signal.

The unpriced column is a call to action — "add a rate for this model" — so
it must only count turns that actually consumed tokens. Sources record
placeholder turns with no usage (Claude Code writes model="<synthetic>"
for injected messages), and counting those implied missing rates where
there was nothing to price.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from flume.analysis.cli import _cmd_cost
from flume.store.base import SessionBundle
from flume.store.sqlite import SqliteAnalyzedStore

BASE_NS = 1_780_000_000 * 1_000_000_000


def _args(**over):
    base = {"since": None, "source": None, "group_by": "model", "as_model": None,
            "json": True, "analyzed_store_url": None}
    base.update(over)
    return argparse.Namespace(**base)


def _turn(session_id, index, model, *, inp=0, out=0, cr=0, cc=0):
    return {
        "span_id": f"{session_id}-t{index}", "turn_index": index, "model": model,
        "started_at_ns": BASE_NS, "ended_at_ns": BASE_NS, "duration_ms": 0,
        "input_tokens": inp, "output_tokens": out, "cache_read_tokens": cr,
        "cache_creation_tokens": cc, "reasoning_tokens": 0,
        "thinking_chars": 0, "text_chars": 0,
    }


def _ingest(store, session_id, turns):
    store.ingest_session(SessionBundle(
        session={
            "session_id": session_id, "source": "claude-code", "is_subagent": 0,
            "started_at_ns": BASE_NS, "ended_at_ns": BASE_NS + 10**9,
            "turn_count": len(turns), "tool_call_count": 0,
            "pipeline_version": 1, "ingested_at_ns": BASE_NS,
        },
        turns=turns, tool_calls=[], contents=[],
    ))


def _by_group(rows):
    return {r["group"]: r for r in rows}


def test_priced_model_costs_are_cache_aware(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        # claude-opus-5 is $5/$25 per MTok; reads 0.1x input, writes 1.25x.
        _ingest(store, "s1", [_turn("s1", 0, "claude-opus-5",
                                    inp=1_000_000, out=1_000_000,
                                    cr=1_000_000, cc=1_000_000)])
        row = _by_group(_cmd_cost(store, _args()))["claude-opus-5"]
    assert row["usd_output"] == 25.0
    assert row["usd_reads"] == 0.5    # 1M * $5 * 0.1
    assert row["usd_writes"] == 6.25  # 1M * $5 * 1.25
    assert row["usd"] == 36.75        # + $5 raw input
    assert row["unpriced_turns"] == 0


def test_zero_token_placeholder_turns_are_not_flagged_unpriced(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        _ingest(store, "s1", [_turn("s1", 0, "<synthetic>")])
        row = _by_group(_cmd_cost(store, _args()))["<synthetic>"]
    assert row["turns"] == 1
    assert row["unpriced_turns"] == 0  # nothing to price
    assert row["usd"] == 0.0


def test_unknown_model_with_real_usage_is_flagged(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        _ingest(store, "s1", [_turn("s1", 0, "some-unpriced-model", inp=500)])
        row = _by_group(_cmd_cost(store, _args()))["some-unpriced-model"]
    assert row["unpriced_turns"] == 1


def test_config_pricing_makes_a_model_costable(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "config.toml"
    config.write_text('[pricing]\n"local-x" = { input = 2.0, output = 8.0 }\n')
    monkeypatch.setenv("FLUME_CONFIG", str(config))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        _ingest(store, "s1", [_turn("s1", 0, "local-x", inp=1_000_000, out=1_000_000)])
        row = _by_group(_cmd_cost(store, _args()))["local-x"]
    assert row["unpriced_turns"] == 0
    assert row["usd"] == 10.0  # $2 input + $8 output
