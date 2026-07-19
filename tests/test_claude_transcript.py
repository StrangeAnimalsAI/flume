from __future__ import annotations

import json
from pathlib import Path

from agent_telemetry.analysis.claude_transcript import (
    analyze_session,
    repeat_key,
    summarize_input,
)


def test_analyze_session_tolerates_partial_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "partial.jsonl"
    events = [
        {
            "type": "assistant",
            "timestamp": "2026-07-01T10:00:00Z",
            "entrypoint": "cli",
            "message": {
                "usage": {"input_tokens": "4", "output_tokens": 2},
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "Read",
                        "input": {"file_path": "/tmp/example.py"},
                    }
                ],
            },
        },
        {
            "type": "user",
            "timestamp": "2026-07-01T10:00:01Z",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": ["not", "hashable"],
                        "content": "ignored",
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-1",
                        "content": [{"type": "text", "text": "hello"}],
                    },
                ]
            },
        },
    ]
    path.write_text(
        "not json\n" + "\n".join(json.dumps(event) for event in events) + "\n"
    )

    report = analyze_session(path)

    assert report["tokens"] == {
        "input": 4,
        "output": 2,
        "cache_read": 0,
        "cache_create": 0,
    }
    assert report["tool_calls"] == 1
    assert report["tool_out_chars"] == 5
    assert report["by_tool"]["Read"]["total_s"] == 1.0


def test_tool_identity_and_summary_are_deterministic() -> None:
    left = {"offset": 20, "file_path": "/tmp/example.py"}
    right = {"file_path": "/tmp/example.py", "offset": 20}

    assert repeat_key("Read", left) == repeat_key("Read", right)
    assert summarize_input("Read", left) == "/tmp/example.py"
