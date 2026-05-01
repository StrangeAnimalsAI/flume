"""CLI for source-agnostic transcript auto-ingest."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

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
        help="Source adapter to use. Currently supported: fake.",
    )
    parser.add_argument(
        "--fake-root",
        type=Path,
        help="Fixture directory for --source fake.",
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
    if args.source != "fake":
        parser.error(f"unsupported --source {args.source!r}; supported: fake")
    if args.fake_root is None:
        parser.error("--fake-root is required for --source fake")
    return FakeTranscriptSource(args.fake_root), fake_ingest


def _print_summary(summary: dict[str, object]) -> None:
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
