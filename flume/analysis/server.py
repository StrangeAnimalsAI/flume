"""Local HTTP API + web UI over the session store.

Stdlib-only. One process, read-only queries:

    flume serve                # http://localhost:8321
    flume serve --port 9000 --store-url sqlite:///tmp/x.sqlite3

JSON endpoints (same data the CLI exposes — agents can hit these directly):

    GET /api/overview
    GET /api/sessions?source=&surface=&cwd=&since=7d&limit=50
    GET /api/sessions/{id}
    GET /api/sessions/{id}/contents?kind=thinking&kind=tool_result
    GET /api/tools?source=&since=&top=10
    GET /api/tokens?group_by=source|surface|model|session&source=&since=
    GET /api/search?q=...&kind=thinking&source=&limit=50

The UI at / is a single static file (static/index.html) — no build step.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from flume.store.base import SessionStore, open_store

_STATIC_DIR = Path(__file__).parent / "static"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="flume serve",
        description="Serve the session-store analysis API and web UI.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8321)
    parser.add_argument(
        "--store-url",
        default=None,
        help="Store URL (default: sqlite://~/.flume/store.sqlite3).",
    )
    args = parser.parse_args(argv)

    store = open_store(args.store_url)
    server = ThreadingHTTPServer(
        (args.host, args.port), _handler_class(args.store_url)
    )
    store.close()  # probe only; handlers open per-request connections
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"WARNING: binding {args.host} exposes full session transcripts "
            "(prompts, thinking, tool output) to the network with no "
            "authentication. Only do this on a trusted network.",
            file=sys.stderr,
        )
    print(f"flume UI on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _handler_class(store_url: str | None):
    class Handler(_StoreHandler):
        pass

    Handler.store_url = store_url
    return Handler


class _StoreHandler(BaseHTTPRequestHandler):
    store_url: str | None = None

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path in ("/", "/index.html"):
                self._send_static("index.html")
                return
            if parsed.path.startswith("/api/"):
                # sqlite connections are cheap and per-thread; open per request.
                with open_store(self.store_url) as store:
                    payload = _route(store, parsed.path, query)
                if payload is None:
                    self._send_json({"error": "not found"}, status=404)
                else:
                    self._send_json(payload)
                return
            self._send_json({"error": "not found"}, status=404)
        except Exception as exc:  # noqa: BLE001 - report to client
            self._send_json(
                {"error": f"{type(exc).__name__}: {exc}"}, status=500
            )

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass  # keep stdout quiet; this is a local tool

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # Deliberately NO Access-Control-Allow-Origin: transcripts contain
        # prompts, thinking, and tool output — no browser origin may read
        # them cross-origin, even from localhost.
        self.end_headers()
        self.wfile.write(body)

    def _send_static(self, name: str) -> None:
        path = _STATIC_DIR / name
        if not path.is_file():
            self._send_json({"error": "static file missing"}, status=404)
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _route(
    store: SessionStore,
    path: str,
    query: dict[str, list[str]],
) -> Any:
    if path == "/api/overview":
        return store.overview()
    if path == "/api/sessions":
        return store.list_sessions(
            source=_q(query, "source"),
            surface=_q(query, "surface"),
            cwd_like=_q(query, "cwd"),
            project=_q(query, "project"),
            since_ns=_since_ns(_q(query, "since")),
            top_level_only=_q(query, "top_level") == "1",
            limit=int(_q(query, "limit") or 50),
        )
    match = re.fullmatch(r"/api/sessions/([^/]+)", path)
    if match:
        return store.get_session(match.group(1))
    match = re.fullmatch(r"/api/sessions/([^/]+)/contents", path)
    if match:
        return store.get_contents(match.group(1), kinds=query.get("kind"))
    match = re.fullmatch(r"/api/sessions/([^/]+)/commands", path)
    if match:
        return store.session_commands(match.group(1))
    if path == "/api/tools":
        top = int(_q(query, "top") or 10)
        return store.tool_stats(
            source=_q(query, "source"),
            since_ns=_since_ns(_q(query, "since")),
            slowest=top,
            largest=top,
        )
    if path == "/api/tokens":
        return store.token_stats(
            source=_q(query, "source"),
            since_ns=_since_ns(_q(query, "since")),
            group_by=_q(query, "group_by") or "source",
        )
    if path == "/api/audit/repeats":
        return store.audit_repeats(
            source=_q(query, "source"),
            since_ns=_since_ns(_q(query, "since")),
            limit=int(_q(query, "limit") or 50),
        )
    if path == "/api/audit/whole_file_reads":
        return store.audit_whole_file_reads(
            source=_q(query, "source"),
            since_ns=_since_ns(_q(query, "since")),
            min_chars=int(_q(query, "min_chars") or 50_000),
            limit=int(_q(query, "limit") or 50),
        )
    if path == "/api/audit/toolgaps":
        from flume.analysis.audit import script_clusters

        return script_clusters(
            store,
            since_ns=_since_ns(_q(query, "since")),
            min_sessions=int(_q(query, "min_sessions") or 3),
        )
    if path == "/api/insights":
        if _q(query, "stored") == "1":
            return store.list_findings(limit=int(_q(query, "limit") or 50))
        from flume.analysis.insights import run_insights

        return run_insights(store, since_ns=_since_ns(_q(query, "since") or "7d"))
    if path == "/api/archive":
        from flume.store.archive import open_archive
        from flume.store.config import load_policy

        with open_archive(None) as archive:
            return {
                "stats": archive.stats(),
                "retention": load_policy().describe(),
            }
    if path == "/api/search":
        q = _q(query, "q")
        if not q:
            return []
        return store.search(
            q,
            kinds=query.get("kind"),
            source=_q(query, "source"),
            limit=int(_q(query, "limit") or 50),
        )
    return None


def _q(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return values[0] if values else None


def _since_ns(window: str | None) -> int | None:
    """Bad or missing query windows are ignored, not errors."""
    if not window:
        return None
    from flume.store.config import parse_duration_ns

    try:
        ttl_ns = parse_duration_ns(window)
    except ValueError:
        return None
    if ttl_ns is None:
        return None
    return time.time_ns() - ttl_ns


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
