"""CLI for source-agnostic transcript auto-ingest."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from agent_telemetry.ingest.claude_code import (
    ClaudeCodeTranscriptSource,
    ingest_claude_code_transcript,
)
from agent_telemetry.ingest.codex import (
    DEFAULT_CODEX_ARCHIVED_ROOT,
    CodexRolloutSource,
    ingest_codex_rollout,
)
from agent_telemetry.ingest.fake import FakeTranscriptSource, fake_ingest
from agent_telemetry.ingest.runner import IngestFunction, run_once
from agent_telemetry.ingest.state import SqliteIngestStateStore
from agent_telemetry.ingest.types import TranscriptSource


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    source, ingest = _source_and_ingest(args, parser)

    with SqliteIngestStateStore(args.state_db) as store:
        if args.loop:
            while True:
                summary = run_once(
                    source=source,
                    store=store,
                    ingest=ingest,
                    quiet_seconds=args.quiet_seconds,
                    dry_run=args.dry_run,
                )
                _print_summary(summary.to_dict())
                time.sleep(args.interval_seconds)

        summary = run_once(
            source=source,
            store=store,
            ingest=ingest,
            quiet_seconds=args.quiet_seconds,
            dry_run=args.dry_run,
        )
        _print_summary(summary.to_dict())
        return 1 if summary.failed else 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-telemetry-auto-ingest",
        description="Discover quiet transcript files and checkpoint ingest state.",
    )
    parser.add_argument(
        "--source",
        default="fake",
        help="Source adapter to use. Currently supported: fake, codex, claude-code.",
    )
    parser.add_argument(
        "--fake-root",
        type=Path,
        help="Fixture directory for --source fake.",
    )
    parser.add_argument(
        "--codex-root",
        action="append",
        type=Path,
        help=(
            "Codex sessions root, rollout file, or fixture root. May be "
            "provided more than once. Defaults to ~/.codex/sessions."
        ),
    )
    parser.add_argument(
        "--include-archived-codex",
        action="store_true",
        help="Also discover ~/.codex/archived_sessions/*.jsonl for --source codex.",
    )
    parser.add_argument(
        "--codex-archived-root",
        type=Path,
        help="Archived Codex sessions root for tests or custom layouts.",
    )
    parser.add_argument(
        "--claude-root",
        action="append",
        type=Path,
        help=(
            "Claude Code projects root, transcript file, or fixture root. May be "
            "provided more than once. Defaults to ~/.claude/projects."
        ),
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:4318/v1/traces",
        help="OTLP-HTTP traces endpoint for real ingest (default: %(default)s).",
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=Path.cwd() / ".agent_telemetry_auto_ingest.sqlite3",
        help="sqlite state path (default: %(default)s).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once",
        action="store_true",
        help="Run one pass and exit. This is the default.",
    )
    mode.add_argument(
        "--loop",
        action="store_true",
        help="Run forever, sleeping between passes.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=60.0,
        help="Loop sleep interval (default: %(default)s).",
    )
    parser.add_argument(
        "--quiet-seconds",
        type=float,
        default=5.0,
        help="Only ingest files whose mtime is at least this old.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report work without mutating ingest state or calling ingest.",
    )
    return parser


def _source_and_ingest(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> tuple[TranscriptSource, IngestFunction]:
    if args.source == "fake":
        if args.fake_root is None:
            parser.error("--fake-root is required for --source fake")
        return FakeTranscriptSource(args.fake_root), fake_ingest

    if args.source == "codex":
        source = CodexRolloutSource(
            args.codex_root,
            include_archived=args.include_archived_codex,
            archived_root=args.codex_archived_root
            if args.codex_archived_root is not None
            else DEFAULT_CODEX_ARCHIVED_ROOT,
        )

        def ingest(request):
            return ingest_codex_rollout(request, endpoint=args.endpoint)

        return source, ingest

    if args.source == "claude-code":
        source = ClaudeCodeTranscriptSource(args.claude_root)

        def ingest(request):
            return ingest_claude_code_transcript(request, endpoint=args.endpoint)

        return source, ingest

    parser.error(
        f"unsupported --source {args.source!r}; supported: fake, codex, claude-code"
    )


def _print_summary(summary: dict[str, object]) -> None:
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
