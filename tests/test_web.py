"""HTTP-level tests for the dashboard API.

The server had no test coverage at all while it was a hand-rolled
BaseHTTPRequestHandler — every check went through the store directly, so
routing, query coercion, and status codes were unverified. TestClient makes
that cheap, so the things a browser actually depends on are pinned here.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from flume.ingest.write import ingest_path
from flume.sources import get_adapter
from flume.store.base import open_analyzed_store

fastapi = pytest.importorskip("fastapi", reason="needs flume[web]")
from fastapi.testclient import TestClient  # noqa: E402

from flume.web.server import create_app  # noqa: E402

SESSION_ID = "sess-web-1"


def _events() -> list[dict]:
    usage = {
        "input_tokens": 5,
        "output_tokens": 120,
        "cache_read_input_tokens": 9_000,
        "cache_creation_input_tokens": 800,
    }
    return [
        {
            "type": "user",
            "uuid": "user-1",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "cwd": "/Users/alex/Code/demo",
            "message": {"role": "user", "content": "read the parser"},
        },
        {
            "type": "assistant",
            "uuid": "asst-1",
            "requestId": "req_A",
            "timestamp": "2026-04-20T10:00:02.000Z",
            "message": {
                "role": "assistant",
                "id": "msg_1",
                "model": "claude-opus-5",
                "content": [{"type": "thinking", "thinking": "look at parse()"}],
                "usage": usage,
            },
        },
        {
            "type": "assistant",
            "uuid": "asst-2",
            "requestId": "req_A",
            "timestamp": "2026-04-20T10:00:03.000Z",
            "message": {
                "role": "assistant",
                "id": "msg_1",
                "model": "claude-opus-5",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "Read",
                        "input": {"file_path": "/tmp/parser.py"},
                    }
                ],
                "usage": usage,
            },
        },
        {
            "type": "user",
            "uuid": "user-2",
            "timestamp": "2026-04-20T10:00:04.000Z",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-1",
                        "content": "def parse(): ...",
                    }
                ],
            },
        },
    ]


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    store_url = f"sqlite://{tmp_path}/store.sqlite3"
    path = tmp_path / f"{SESSION_ID}.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in _events()) + "\n")
    with open_analyzed_store(store_url) as store:
        ingest_path(store, get_adapter("claude-code"), path, {"cwd": "/tmp"})
    return TestClient(create_app(store_url))


def test_index_serves_the_ui(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_openapi_schema_is_published(client: TestClient) -> None:
    """The API advertises itself as agent-consumable; make that true."""
    schema = client.get("/api/openapi.json").json()
    assert "/api/sessions/{session_id}" in schema["paths"]


def test_list_and_fetch_session(client: TestClient) -> None:
    listed = client.get("/api/sessions").json()
    assert [s["session_id"] for s in listed] == [SESSION_ID]

    session = client.get(f"/api/sessions/{SESSION_ID}").json()
    # One billed request across two content blocks — not counted twice.
    assert session["output_tokens"] == 120


def test_unknown_session_is_404_not_500(client: TestClient) -> None:
    assert client.get("/api/sessions/nope").status_code == 404


def test_limit_is_validated(client: TestClient) -> None:
    """Hand-rolled int(...) coercion turned bad input into a 500."""
    assert client.get("/api/sessions", params={"limit": "abc"}).status_code == 422
    assert client.get("/api/sessions", params={"limit": 0}).status_code == 422


def test_repeated_query_param_collects(client: TestClient) -> None:
    response = client.get(
        f"/api/sessions/{SESSION_ID}/contents",
        params=[("kind", "thinking"), ("kind", "tool_result")],
    )
    kinds = {row["kind"] for row in response.json()}
    assert kinds <= {"thinking", "tool_result"}
    assert "thinking" in kinds


def test_no_cors_header_is_exposed(client: TestClient) -> None:
    """Transcripts must not be readable cross-origin, even from localhost."""
    response = client.get(
        "/api/overview", headers={"Origin": "http://evil.example"}
    )
    assert "access-control-allow-origin" not in {
        k.lower() for k in response.headers
    }


def test_static_mount_refuses_traversal(client: TestClient) -> None:
    """The one route that reads arbitrary paths off disk."""
    response = client.get("/assets/../../../store/sqlite.py")
    assert response.status_code in (403, 404)
