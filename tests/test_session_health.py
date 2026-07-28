"""Per-session health metrics.

Fixtures encode the shapes the metrics exist to catch: one API response split
across content blocks, a cache expiry, a long-range repeat, and a slow tool.
"""
from __future__ import annotations

import json
from pathlib import Path

from flume.analysis.session_health import (
    EXPIRY_WRITE_RATIO,
    _cache,
    session_health,
)
from flume.ingest.write import ingest_path
from flume.sources import get_adapter
from flume.store.base import open_analyzed_store

SESSION_ID = "sess-health-1"


def _assistant(uuid, request_id, ts, content, usage):
    return {
        "type": "assistant",
        "uuid": uuid,
        "requestId": request_id,
        "timestamp": ts,
        "message": {
            "role": "assistant",
            "id": f"msg_{request_id}",
            "model": "claude-opus-5",
            "content": content,
            "usage": usage,
        },
    }


def _tool_result(uuid, tool_use_id, ts, text):
    return {
        "type": "user",
        "uuid": uuid,
        "timestamp": ts,
        "message": {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": tool_use_id, "content": text}
            ],
        },
    }


def _events() -> list[dict]:
    read_a = {"type": "tool_use", "id": "t1", "name": "Read",
              "input": {"file_path": "/tmp/a.py"}}
    # Byte-identical to read_a — this is the repeat the detector must find.
    read_a_again = {**read_a, "id": "t3"}

    warm = {"input_tokens": 4, "output_tokens": 60,
            "cache_read_input_tokens": 30_000, "cache_creation_input_tokens": 500}
    # >=50% of the prefix re-written: a cache miss, not ordinary growth.
    cold = {"input_tokens": 4, "output_tokens": 60,
            "cache_read_input_tokens": 1_000, "cache_creation_input_tokens": 40_000}

    return [
        {
            "type": "user",
            "uuid": "u0",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "cwd": "/Users/alex/Code/demo",
            "message": {"role": "user", "content": "inspect the parser"},
        },
        _assistant("a1", "req_1", "2026-04-20T10:00:01.000Z",
                   [{"type": "thinking", "thinking": "read it first"}], warm),
        # Same requestId, second content block: must NOT bill twice.
        _assistant("a2", "req_1", "2026-04-20T10:00:02.000Z", [read_a], warm),
        _tool_result("u1", "t1", "2026-04-20T10:00:06.000Z", "def parse(): ..."),
        _assistant("a3", "req_2", "2026-04-20T10:00:07.000Z",
                   [{"type": "tool_use", "id": "t2", "name": "Bash",
                     "input": {"command": "pytest"}}], warm),
        _tool_result("u2", "t2", "2026-04-20T10:00:20.000Z", "1 failed"),
        # Long quiet, then the prefix is re-established.
        _assistant("a4", "req_3", "2026-04-20T11:30:00.000Z", [read_a_again], cold),
        _tool_result("u3", "t3", "2026-04-20T11:30:03.000Z", "def parse(): ..."),
    ]


def _health(tmp_path: Path):
    path = tmp_path / f"{SESSION_ID}.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in _events()) + "\n")
    with open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        ingest_path(store, get_adapter("claude-code"), path, {"cwd": "/tmp"})
        return session_health(store, SESSION_ID)


def test_unknown_session_returns_none(tmp_path: Path) -> None:
    with open_analyzed_store(f"sqlite://{tmp_path}/store.sqlite3") as store:
        assert session_health(store, "nope") is None


def test_headline_counts_requests_not_blocks(tmp_path: Path) -> None:
    head = _health(tmp_path)["headline"]
    # Four assistant lines, three billed requests.
    assert head["model_requests"] == 3
    assert head["tool_calls"] == 3


def test_resend_factor_uses_peak_not_last(tmp_path: Path) -> None:
    head = _health(tmp_path)["headline"]
    assert head["peak_context_tokens"] > 0
    # Three requests over a growing prefix: throughput must exceed peak.
    assert head["resend_factor"] > 1.0


def test_redundancy_finds_the_identical_call(tmp_path: Path) -> None:
    red = _health(tmp_path)["redundancy"]
    assert red["repeats"] == 1
    assert red["worst"][0]["name"] == "Read"


def test_cache_expiry_detected_by_write_ratio(tmp_path: Path) -> None:
    """The 40k re-write after quiet is an expiry; the 500-token ones are not."""
    cache = _health(tmp_path)["cache"]
    assert len(cache["expiries"]) == 1
    expiry = cache["expiries"][0]
    assert expiry["rewrote_tokens"] == 40_000
    assert expiry["quiet_ms"] > 60 * 60_000  # the ~90 minute gap
    assert cache["expiry_tokens"] == 40_000


def test_expiry_threshold_is_a_ratio_not_a_duration() -> None:
    """Guards the module's core claim: the TTL is unknowable, so use the ratio.

    A long quiet stretch under a 1h TTL re-writes almost nothing — a
    gap-threshold detector calls that an expiry and is wrong. A big re-write
    with no preceding gap is a real prefix invalidation and must be caught.
    Both directions are pinned here because both were plausible designs.
    """
    hour_ns = 3_600 * 1_000_000_000
    rows = [
        {"started_at_ns": 0, "context_tokens": 100_000, "input_tokens": 0,
         "cache_read_tokens": 99_000, "cache_creation_tokens": 1_000},
        # 2h quiet but the prefix survived: NOT an expiry.
        {"started_at_ns": 2 * hour_ns, "context_tokens": 100_000,
         "input_tokens": 0, "cache_read_tokens": 99_800,
         "cache_creation_tokens": 200},
        # Seconds later, most of the prefix re-written: IS an expiry.
        {"started_at_ns": 2 * hour_ns + 5_000_000_000,
         "context_tokens": 100_000, "input_tokens": 0,
         "cache_read_tokens": 10_000, "cache_creation_tokens": 90_000},
    ]
    expiries = _cache(rows)["expiries"]
    assert [e["rewrote_tokens"] for e in expiries] == [90_000]
    assert expiries[0]["quiet_ms"] < 10_000  # caught despite no quiet gap
    assert 0 < EXPIRY_WRITE_RATIO <= 1


def test_active_time_excludes_idle(tmp_path: Path) -> None:
    """Idle is the human's time, not the run's, and must not be a denominator.

    Real sessions run 79-97% idle, which would squash the tool-vs-model
    comparison — the only part that says anything about the agent — into a
    sliver of any bar drawn against wall clock.
    """
    health = _health(tmp_path)
    split, head = health["time"], health["headline"]
    assert split["active_ms"] == split["tool_ms"] + split["model_ms"]
    assert head["active_ms"] == split["active_ms"]
    # This fixture has a 90-minute gap; it must not be counted as work.
    assert split["idle_ms"] > 0
    assert split["active_ms"] < split["wall_ms"]


def test_time_split_accounts_for_wall_clock(tmp_path: Path) -> None:
    split = _health(tmp_path)["time"]
    total = (
        split["tool_ms"] + split["model_ms"] + split["idle_ms"]
        + split["unaccounted_ms"]
    )
    assert total == split["wall_ms"]
    assert split["idle_ms"] > 0  # the 90-minute gap


def test_tools_ranked_by_time_held(tmp_path: Path) -> None:
    tools = _health(tmp_path)["tools"]
    assert [t["name"] for t in tools][0] == "Bash"  # 13s vs Read's 4s and 3s
    assert tools[0]["calls"] == 1


def test_context_curve_is_elapsed_relative(tmp_path: Path) -> None:
    curve = _health(tmp_path)["context_curve"]
    assert curve[0]["elapsed_ms"] >= 0
    assert [p["elapsed_ms"] for p in curve] == sorted(
        p["elapsed_ms"] for p in curve
    )
