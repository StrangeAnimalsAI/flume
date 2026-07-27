"""Usage must be counted once per billed API request, not once per turn row.

Claude Code writes one transcript line per assistant content block (a thinking
block, then text, then each tool_use) and stamps the *same* `usage` object on
every one. Summing turn rows therefore double-counts every multi-block response
— measured at ~52% inflation on real claude-code data before this was fixed.

Two layers of defence:

* `test_split_response_*` — a synthetic transcript pinning the contract.
* `test_archived_transcripts_reconcile` — a differential check against the raw
  archive: a second, deliberately naive reimplementation of the token maths,
  run over real captured bytes. It is the closest thing to a ground-truth
  oracle available without an API round trip, and it is what catches a
  regression in the mapper that a hand-written fixture wouldn't anticipate.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from flume.sources.claude_code import extract_contents, jsonl_to_spans
from flume.store.bundle import bundle_from_spans

_USAGE = {
    "input_tokens": 7,
    "output_tokens": 500,
    "cache_read_input_tokens": 20_000,
    "cache_creation_input_tokens": 1_000,
}


def _split_response_events() -> list[dict]:
    """One user turn, then ONE API response written as three lines."""
    events: list[dict] = [
        {
            "type": "user",
            "uuid": "user-1",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "message": {"role": "user", "content": "hello"},
        }
    ]
    # Same requestId on every line — this is what the real format does.
    for i, block in enumerate(
        [
            {"type": "thinking", "thinking": "pondering"},
            {"type": "text", "text": "here is the answer"},
            {"type": "tool_use", "id": "t1", "name": "Read", "input": {"file_path": "/a"}},
        ]
    ):
        events.append(
            {
                "type": "assistant",
                "uuid": f"asst-{i}",
                "requestId": "req_ONE",
                "timestamp": f"2026-04-20T10:00:0{i + 1}.000Z",
                "message": {
                    "role": "assistant",
                    "id": "msg_1",
                    "model": "claude-opus-5",
                    "content": [block],
                    "usage": _USAGE,
                },
            }
        )
    return events


def _bundle(tmp_path: Path, events: list[dict], stem: str = "session"):
    path = tmp_path / f"{stem}.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    spans = jsonl_to_spans(path)
    return bundle_from_spans(
        spans, contents=extract_contents(path, path.stem), file_path=path
    )


def test_split_response_carries_request_id(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, _split_response_events())
    assert bundle is not None
    assert len(bundle.turns) == 3, "one row per content block is intended"
    assert {t["request_id"] for t in bundle.turns} == {"req_ONE"}


def test_split_response_bills_usage_once(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, _split_response_events())
    assert bundle is not None
    session = bundle.session
    # Three rows, one billed request: the session must not triple-count.
    assert session["output_tokens"] == _USAGE["output_tokens"]
    assert session["input_tokens"] == _USAGE["input_tokens"]
    assert session["cache_read_tokens"] == _USAGE["cache_read_input_tokens"]
    assert session["cache_creation_tokens"] == _USAGE["cache_creation_input_tokens"]


def test_turn_index_is_populated(tmp_path: Path) -> None:
    """_turn_row always writes the key with None, so setdefault was a no-op."""
    bundle = _bundle(tmp_path, _split_response_events())
    assert bundle is not None
    assert [t["turn_index"] for t in bundle.turns] == [0, 1, 2]


def _naive_usage(raw: str) -> dict[str, int]:
    """Independent reimplementation: dedupe on requestId, take each once.

    Deliberately shares no code with the pipeline — that is the whole point.
    """
    per_request: dict[str, dict[str, int]] = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") != "assistant":
            continue
        message = event.get("message") or {}
        usage = message.get("usage") or {}
        key = event.get("requestId") or event.get("uuid")
        prior = per_request.get(key, {})
        per_request[key] = {
            field: max(prior.get(field, 0), int(usage.get(field) or 0))
            for field in (
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_creation_input_tokens",
            )
        }
    totals = {k: 0 for k in ("input_tokens", "output_tokens",
                             "cache_read_input_tokens", "cache_creation_input_tokens")}
    for row in per_request.values():
        for field, value in row.items():
            totals[field] += value
    return totals


@pytest.mark.skipif(
    not (Path.home() / ".flume" / "raw").is_dir(),
    reason="no local raw archive to reconcile against",
)
def test_archived_transcripts_reconcile(tmp_path: Path) -> None:
    """Re-derive usage from archived bytes and compare to the pipeline's rows.

    Compares like-for-like: the *archived snapshot*, not the live transcript on
    disk. A session captured mid-flight has lines the archive doesn't, so
    reading the live file here would produce spurious mismatches.
    """
    from flume.store.base import open_analyzed_store
    from flume.store.raw import open_raw_store

    sample = int(os.environ.get("FLUME_PARITY_SAMPLE", "25"))
    checked = 0
    with open_analyzed_store(None, readonly=True) as store:
        session_ids = [
            row["session_id"]
            for row in store.list_sessions(source="claude-code", limit=sample * 4)
        ]
    if not session_ids:
        pytest.skip("no claude-code sessions in the local store")

    with open_raw_store(None) as raw_store:
        for session_id in session_ids:
            versions = raw_store.versions(session_id)
            if not versions:
                continue
            restored = tmp_path / "raw.jsonl"
            raw_store.restore(versions[-1], restored)
            raw = restored.read_text()
            if '"type":"assistant"' not in raw.replace(" ", ""):
                continue
            bundle = _bundle(tmp_path, [json.loads(x) for x in raw.splitlines() if x.strip()])
            if bundle is None:
                continue
            expected = _naive_usage(raw)
            session = bundle.session
            assert session["output_tokens"] == expected["output_tokens"], session_id
            assert session["cache_read_tokens"] == expected["cache_read_input_tokens"], session_id
            checked += 1
            if checked >= sample:
                break
    if checked == 0:
        pytest.skip("no claude-code transcripts in the archive")
