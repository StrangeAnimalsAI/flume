"""Bounded JSONL reading.

Found in production: a 4.5 GB Codex rollout of 976 lines — one of them
several GB — failed ingest outright with "OverflowError: string longer than
INT_MAX bytes", because json rejects strings past INT_MAX and the reader
buffered gigabytes to get there. These pin the fix: oversized lines are
skipped without ever being held whole, and everything around them still
parses.
"""
from __future__ import annotations

import json
from pathlib import Path

from flume.sources.utils import iter_jsonl_lines, read_jsonl


def _write(path: Path, lines: list[str]) -> Path:
    path.write_text("\n".join(lines) + "\n")
    return path


def test_reads_ordinary_lines(tmp_path: Path) -> None:
    p = _write(tmp_path / "a.jsonl", ['{"a": 1}', '{"b": 2}'])
    assert [json.loads(x) for x in iter_jsonl_lines(p)] == [{"a": 1}, {"b": 2}]


def test_blank_lines_are_dropped(tmp_path: Path) -> None:
    p = _write(tmp_path / "b.jsonl", ['{"a": 1}', "", "   ", '{"b": 2}'])
    assert len(list(iter_jsonl_lines(p))) == 2


def test_oversized_line_is_skipped_and_neighbours_survive(tmp_path: Path) -> None:
    """The production shape: a runaway line between good ones."""
    huge = '{"big": "' + "x" * 5000 + '"}'
    p = _write(tmp_path / "c.jsonl", ['{"a": 1}', huge, '{"b": 2}'])
    got = [json.loads(x) for x in iter_jsonl_lines(p, max_bytes=1000)]
    assert got == [{"a": 1}, {"b": 2}]


def test_oversized_line_spanning_many_chunks(tmp_path: Path) -> None:
    # Larger than the 1 MiB read chunk, so the skip must persist across reads.
    huge = "y" * (3 * 1024 * 1024)
    p = _write(tmp_path / "d.jsonl", ['{"a": 1}', huge, '{"b": 2}'])
    got = [json.loads(x) for x in iter_jsonl_lines(p, max_bytes=64 * 1024)]
    assert got == [{"a": 1}, {"b": 2}]


def test_consecutive_oversized_lines(tmp_path: Path) -> None:
    huge = "z" * 5000
    p = _write(tmp_path / "e.jsonl", [huge, huge, '{"ok": true}', huge])
    assert [json.loads(x) for x in iter_jsonl_lines(p, max_bytes=1000)] == [{"ok": True}]


def test_oversized_final_line_without_trailing_newline(tmp_path: Path) -> None:
    (tmp_path / "f.jsonl").write_text('{"a": 1}\n' + "q" * 5000)
    assert [json.loads(x) for x in
            iter_jsonl_lines(tmp_path / "f.jsonl", max_bytes=1000)] == [{"a": 1}]


def test_final_line_without_trailing_newline_is_kept(tmp_path: Path) -> None:
    (tmp_path / "g.jsonl").write_text('{"a": 1}\n{"b": 2}')
    assert len(list(iter_jsonl_lines(tmp_path / "g.jsonl"))) == 2


def test_missing_file_yields_nothing(tmp_path: Path) -> None:
    assert list(iter_jsonl_lines(tmp_path / "nope.jsonl")) == []
    assert read_jsonl(tmp_path / "nope.jsonl") == []


def test_invalid_utf8_does_not_raise(tmp_path: Path) -> None:
    (tmp_path / "h.jsonl").write_bytes(b'{"a": 1}\n\xff\xfe bad bytes\n{"b": 2}\n')
    # Undecodable bytes become replacement chars, fail JSON, and are skipped;
    # the surrounding valid lines still parse.
    assert read_jsonl(tmp_path / "h.jsonl") == [{"a": 1}, {"b": 2}]


def test_read_jsonl_skips_non_dict_and_malformed_rows(tmp_path: Path) -> None:
    p = _write(tmp_path / "i.jsonl", ['{"a": 1}', "[1,2,3]", "not json", '{"b": 2}'])
    assert read_jsonl(p) == [{"a": 1}, {"b": 2}]
