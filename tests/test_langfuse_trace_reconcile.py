from __future__ import annotations

import httpx

from flume.analysis.langfuse_trace_reconcile import (
    _is_local_url,
    _summarize_trace,
    delete_clickhouse_trace,
    delete_trace,
    fetch_trace,
    wait_until_missing,
)


def test_fetch_and_delete_one_trace_with_public_api() -> None:
    deleted = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal deleted
        assert request.url.path == "/api/public/traces/trace-1"
        if request.method == "GET" and not deleted:
            return httpx.Response(
                200,
                json={
                    "id": "trace-1",
                    "name": "stale-fixture",
                    "timestamp": "2026-04-20T10:00:00.000Z",
                    "observations": [{"id": "obs-1"}],
                },
            )
        if request.method == "DELETE":
            deleted = True
            return httpx.Response(200, json={"message": "Trace deleted successfully"})
        return httpx.Response(404, json={"message": "not found"})

    client = httpx.Client(
        base_url="http://localhost:3000",
        transport=httpx.MockTransport(handler),
    )

    trace = fetch_trace(client, "trace-1")
    assert trace is not None
    assert _summarize_trace(trace).observations == 1

    delete_trace(client, "trace-1")
    assert fetch_trace(client, "trace-1") is None


def test_local_url_guard() -> None:
    assert _is_local_url("http://localhost:3000")
    assert _is_local_url("http://127.0.0.1:3000")
    assert _is_local_url("http://langfuse-web:3000")
    assert not _is_local_url("https://cloud.langfuse.com")


def test_wait_until_missing_allows_eventual_delete_visibility() -> None:
    gets = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal gets
        assert request.method == "GET"
        gets += 1
        if gets < 3:
            return httpx.Response(200, json={"id": "trace-1", "observations": []})
        return httpx.Response(404, json={"message": "not found"})

    client = httpx.Client(
        base_url="http://localhost:3000",
        transport=httpx.MockTransport(handler),
    )

    assert wait_until_missing(client, "trace-1", attempts=3, interval_s=0)


def test_clickhouse_delete_is_trace_scoped(monkeypatch) -> None:
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr("subprocess.run", fake_run)

    delete_clickhouse_trace("a" * 32, container="langfuse-clickhouse-1")

    args, kwargs = calls[0]
    query = args[-1]
    assert args[:3] == ["docker", "exec", "langfuse-clickhouse-1"]
    assert "DELETE WHERE trace_id='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'" in query
    assert "DELETE WHERE id='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'" in query
    assert kwargs["check"] is True
