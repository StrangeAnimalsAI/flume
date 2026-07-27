"""Exercise the app through a real uvicorn server, not TestClient.

TestClient drives the ASGI app in-process and, for one request at a time,
tends to reuse a single worker thread. That hid a bug where every request
failed in production: a `Depends(...)` generator opened the SQLite connection
on one threadpool thread and the endpoint used it on another, which sqlite3
refuses. Eight green TestClient tests, 500 on every real request.

So this boots the actual server. It is the only test here that does, and it
exists specifically to catch thread-affinity regressions in the store handling.
"""
from __future__ import annotations

import json
import socket
import threading
import time
from pathlib import Path

import pytest

from flume.ingest.write import ingest_path
from flume.sources import get_adapter
from flume.store.base import open_analyzed_store

pytest.importorskip("fastapi", reason="needs flume[web]")
pytest.importorskip("uvicorn", reason="needs flume[web]")

import httpx  # noqa: E402
import uvicorn  # noqa: E402

from flume.web.server import create_app  # noqa: E402

SESSION_ID = "sess-thread-1"


def _events() -> list[dict]:
    return [
        {
            "type": "user",
            "uuid": "u0",
            "timestamp": "2026-04-20T10:00:00.000Z",
            "entrypoint": "cli",
            "message": {"role": "user", "content": "hello"},
        },
        {
            "type": "assistant",
            "uuid": "a1",
            "requestId": "req_1",
            "timestamp": "2026-04-20T10:00:01.000Z",
            "message": {
                "role": "assistant",
                "id": "msg_1",
                "model": "claude-opus-5",
                "content": [{"type": "text", "text": "hi"}],
                "usage": {
                    "input_tokens": 3,
                    "output_tokens": 20,
                    "cache_read_input_tokens": 1_000,
                    "cache_creation_input_tokens": 100,
                },
            },
        },
    ]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def live_server(tmp_path: Path):
    store_url = f"sqlite://{tmp_path}/store.sqlite3"
    path = tmp_path / f"{SESSION_ID}.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in _events()) + "\n")
    with open_analyzed_store(store_url) as store:
        ingest_path(store, get_adapter("claude-code"), path, {"cwd": "/tmp"})

    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(store_url),
            host="127.0.0.1",
            port=port,
            log_level="error",
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if server.started:
            break
        time.sleep(0.05)
    else:
        server.should_exit = True
        pytest.fail("uvicorn did not start within 15s")

    try:
        yield base
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def test_endpoints_work_off_the_dependency_thread(live_server: str) -> None:
    """Each of these opens a store; all must survive the threadpool hop."""
    for path in (
        "/api/overview",
        "/api/sessions",
        f"/api/sessions/{SESSION_ID}",
        f"/api/sessions/{SESSION_ID}/health",
        f"/api/sessions/{SESSION_ID}/contents",
        f"/api/sessions/{SESSION_ID}/commands",
        "/api/tools",
        "/api/tokens",
    ):
        response = httpx.get(live_server + path, timeout=30)
        assert response.status_code == 200, f"{path} -> {response.text[:200]}"


def test_concurrent_requests_do_not_share_a_connection(live_server: str) -> None:
    """Parallel load is what forces dependency and handler onto other threads."""
    results: list[int] = []
    errors: list[BaseException] = []

    def hit() -> None:
        try:
            results.append(
                httpx.get(live_server + "/api/overview", timeout=30).status_code
            )
        except BaseException as exc:  # noqa: BLE001 - surfaced in the assert
            errors.append(exc)

    threads = [threading.Thread(target=hit) for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors, errors
    assert results == [200] * 12
