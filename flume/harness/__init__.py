"""Minimal Anthropic-SDK agent harness with full-fidelity tracing.

Exists to recover agent thinking text for audits (INT-439): Claude Code
stopped persisting plaintext thinking ~May 2026, so this harness requests
`thinking: {type: "adaptive", display: "summarized"}` and writes every
turn — thinking summaries, tool I/O, usage — to a JSONL transcript the
store ingests through the `harness` source adapter.
"""

HARNESS_VERSION = "0.1.0"


def main(argv: list[str] | None = None) -> int:
    """Entry point for `flume harness`."""
    from flume.harness.agent import main as _main

    return _main(argv)


__all__ = ["HARNESS_VERSION", "main"]
