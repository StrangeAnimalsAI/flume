"""Argument parsing for `flume serve`.

The HTTP client lives in `flume.web`; this only turns flags into the
call that starts it.
"""
from __future__ import annotations

import argparse
import sys

from flume.store.base import open_analyzed_store


def _require_web() -> None:
    """Fail with an install hint rather than a bare ImportError.

    Ingest and analyze stay dependency-free so they can run from hooks and
    cron; the dashboard is the one surface that pulls a framework.
    """
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
    except ImportError:
        raise SystemExit(
            "`flume serve` needs the 'fastapi' and 'uvicorn' packages, which "
            "are optional dependencies. Install them with: "
            "uv pip install 'flume[web]'"
        ) from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="flume serve",
        description="Serve the session-store analysis API and web UI.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8321)
    parser.add_argument(
        "--analyzed-store-url",
        default=None,
        help="Store URL (default: sqlite://~/.flume/store.sqlite3).",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Restart on source changes (development).",
    )
    args = parser.parse_args(argv)

    _require_web()
    import uvicorn

    from flume.web.server import create_app

    # Probe the store before binding: a bad --analyzed-store-url should fail
    # here with a clear error, not on the first request.
    open_analyzed_store(args.analyzed_store_url).close()

    if args.host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"WARNING: binding {args.host} exposes full session transcripts "
            "(prompts, thinking, tool output) to the network with no "
            "authentication. Only do this on a trusted network.",
            file=sys.stderr,
        )
    print(f"flume UI on http://{args.host}:{args.port}")
    print(f"     API docs http://{args.host}:{args.port}/api/docs")
    uvicorn.run(
        create_app(args.analyzed_store_url),
        host=args.host,
        port=args.port,
        log_level="warning",  # keep stdout quiet; this is a local tool
    )
    return 0
