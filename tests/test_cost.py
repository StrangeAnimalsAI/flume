"""Cost reporting: pricing resolution and the unpriced signal.

The unpriced column is a call to action — "add a rate for this model" — so
it must only count turns that actually consumed tokens. Sources record
placeholder turns with no usage (Claude Code writes model="<synthetic>"
for injected messages), and counting those implied missing rates where
there was nothing to price.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from flume.cli.analyze import _cmd_cost
from flume.store.base import SessionBundle
from flume.store.sqlite import SqliteAnalyzedStore

BASE_NS = 1_780_000_000 * 1_000_000_000


def _args(**over):
    base = {"since": None, "source": None, "group_by": "model", "model": None,
            "as_model": None, "json": True, "analyzed_store_url": None}
    base.update(over)
    return argparse.Namespace(**base)


def _turn(session_id, index, model, *, inp=0, out=0, cr=0, cc=0, request_id=None,
          at=BASE_NS):
    return {
        "span_id": f"{session_id}-t{index}", "turn_index": index, "model": model,
        "request_id": request_id,
        "started_at_ns": at, "ended_at_ns": at, "duration_ms": 0,
        "input_tokens": inp, "output_tokens": out, "cache_read_tokens": cr,
        "cache_creation_tokens": cc, "reasoning_tokens": 0,
        "thinking_chars": 0, "text_chars": 0,
    }


def _ingest(store, session_id, turns, *, started=BASE_NS):
    store.ingest_session(SessionBundle(
        session={
            "session_id": session_id, "source": "claude-code", "is_subagent": 0,
            "started_at_ns": started, "ended_at_ns": started + 10**9,
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


def test_usage_repeated_on_each_block_of_a_request_is_billed_once(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        # Claude Code writes a thinking line and a text line for one response,
        # each carrying the same usage object.
        _ingest(store, "s1", [
            _turn("s1", i, "claude-opus-5", out=1_000_000, request_id="req_1")
            for i in range(2)
        ])
        row = _by_group(_cmd_cost(store, _args()))["claude-opus-5"]
    assert row["turns"] == 1
    assert row["usd_output"] == 25.0  # one request, not two


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


def _mixed(store):
    # opus-5: $25/MTok output (premium); haiku-4-5: $5 (not); gpt-x unpriced.
    _ingest(store, "s1", [
        _turn("s1", 0, "claude-opus-5", out=1_000_000),
        _turn("s1", 1, "claude-haiku-4-5", out=1_000_000),
        _turn("s1", 2, "claude-haiku-4-5", out=1_000_000),
        _turn("s1", 3, "gpt-x", out=1_000_000),
    ])


def test_rows_carry_their_share_of_turns_and_cost(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        _mixed(store)
        rows = _by_group(_cmd_cost(store, _args()))
    assert rows["claude-opus-5"]["pct_turns"] == 25.0
    assert rows["claude-haiku-4-5"]["pct_turns"] == 50.0
    assert rows["claude-opus-5"]["pct_usd"] == 71.4   # $25 of $35
    assert rows["claude-haiku-4-5"]["pct_usd"] == 28.6
    assert rows["gpt-x"]["pct_usd"] == 0.0


def test_model_prefix_restricts_the_subset_shares_are_of(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        _mixed(store)
        rows = _by_group(_cmd_cost(store, _args(model="claude")))
    assert set(rows) == {"claude-opus-5", "claude-haiku-4-5"}
    assert rows["claude-opus-5"]["pct_turns"] == 33.3
    assert rows["claude-haiku-4-5"]["pct_turns"] == 66.7


def test_premium_split_follows_the_model_that_ran(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        _mixed(store)
        [day] = _cmd_cost(store, _args(group_by="day"))
        # Repricing everything at opus rates must not make haiku premium.
        [repriced] = _cmd_cost(
            store, _args(group_by="day", as_model="claude-opus-5")
        )
    assert day["premium_turns"] == 1
    assert day["premium_usd"] == 25.0
    assert repriced["premium_turns"] == 1
    assert repriced["premium_usd"] == 25.0


def test_since_counts_recent_turns_of_a_session_that_started_earlier(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "absent.toml"))
    day = 86_400 * 10**9
    now = time.time_ns()
    with SqliteAnalyzedStore(tmp_path / "s.sqlite3") as store:
        # Opened eight days ago, still working yesterday.
        _ingest(store, "s1", [
            _turn("s1", 0, "claude-opus-5", out=1_000_000, at=now - 8 * day),
            _turn("s1", 1, "claude-opus-5", out=1_000_000, at=now - day),
        ], started=now - 8 * day)
        row = _by_group(_cmd_cost(store, _args(since="7d")))["claude-opus-5"]
    assert row["turns"] == 1
    assert row["usd"] == 25.0
