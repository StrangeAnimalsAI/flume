"""Argument parsing for `flume harness`.

The harness itself knows nothing about a command line: it exposes
`run_session` and a backend registry, and this module translates flags
into those calls.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from flume.harness.agent import DEFAULT_MODEL, DEFAULT_TRANSCRIPT_DIR, _ingest
from flume.harness.backends import get_backend, names as _backend_names


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="flume harness",
        description="Minimal traced agent: thinking summaries land in the store.",
    )
    parser.add_argument("prompt")
    parser.add_argument(
        "--backend", default="anthropic",
        help="Which model to drive: " + ", ".join(
            f"{b}" for b in _backend_names()),
    )
    parser.add_argument(
        "--model", default=None,
        help="Model id. Required for the openai backend; the anthropic "
        "backend falls back to FLUME_HARNESS_MODEL or a current Opus.",
    )
    parser.add_argument(
        "--base-url", default=None,
        help="openai backend: server base URL (default: "
        "$OPENAI_BASE_URL, else http://localhost:11434/v1 for Ollama).",
    )
    parser.add_argument(
        "--effort", default=None,
        choices=("low", "medium", "high", "xhigh", "max"),
        help="anthropic backend only.",
    )
    parser.add_argument("--system", default=None)
    parser.add_argument("--max-turns", type=int, default=40)
    parser.add_argument("--max-tokens", type=int, default=16000)
    parser.add_argument(
        "--permission-mode", default=None,
        help="SDK backend only (e.g. acceptEdits, bypassPermissions).",
    )
    parser.add_argument("--transcript-dir", type=Path, default=DEFAULT_TRANSCRIPT_DIR)
    parser.add_argument(
        "--no-ingest", action="store_true",
        help="Skip ingesting the transcript into the session store at exit.",
    )
    args = parser.parse_args(argv)

    backend = get_backend(args.backend)
    common = {
        "system": args.system,
        "max_turns": args.max_turns,
        "transcript_dir": args.transcript_dir,
    }
    if backend.name == "claude-sdk":
        import anyio

        path = anyio.run(
            lambda: backend.run(
                args.prompt,
                model=args.model,
                permission_mode=args.permission_mode,
                **common,
            )
        )
    elif backend.name == "openai":
        if not args.model:
            raise SystemExit(
                "--model is required for the openai backend (e.g. "
                "--model qwen3-coder)"
            )

        path = backend.run(
            args.prompt,
            model=args.model,
            base_url=args.base_url
            or os.environ.get("OPENAI_BASE_URL")
            or "http://localhost:11434/v1",
            max_tokens=args.max_tokens,
            **common,
        )
    else:
        path = backend.run(
            args.prompt,
            model=args.model or DEFAULT_MODEL,
            effort=args.effort,
            max_tokens=args.max_tokens,
            **common,
        )
    if not args.no_ingest:
        _ingest(path)
    return 0
