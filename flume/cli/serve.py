"""Argument parsing for `flume serve`.

The HTTP client lives in `flume.web`; this only turns flags into the
call that starts it.
"""
from __future__ import annotations

import argparse
import sys
from http.server import ThreadingHTTPServer

from flume.store.base import open_analyzed_store
from flume.web.server import _handler_class


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
    args = parser.parse_args(argv)

    store = open_analyzed_store(args.analyzed_store_url)
    server = ThreadingHTTPServer(
        (args.host, args.port), _handler_class(args.analyzed_store_url)
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
