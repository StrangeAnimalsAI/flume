"""CLI for source-agnostic transcript auto-ingest."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from flume.ingest.fake import FakeTranscriptSource, fake_ingest
from flume.ingest.runner import IngestFunction, run_once
from flume.ingest.state import SqliteIngestStateStore
from flume.sources import TranscriptSource, registered
from flume.store.raw import open_raw_store
from flume.store.base import open_analyzed_store
from flume.store.config import load_policy


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.state_db is None:
        args.state_db = Path.home() / ".flume" / "auto-ingest-state.sqlite3"
    # Dry runs must not create the store, raw_store, or their directories;
    # the runner never invokes the ingest function under --dry-run.
    analyzed_store, raw_store = (
        (None, None) if args.dry_run else _open_stores(args)
    )
    try:
        source, ingest = _source_and_ingest(args, parser, analyzed_store, raw_store)
        return _run(args, source, ingest, analyzed_store, raw_store)
    finally:
        if analyzed_store is not None:
            analyzed_store.close()
        if raw_store is not None:
            raw_store.close()


def _open_stores(args: argparse.Namespace):
    raw_store = None if args.no_raw_store else open_raw_store(args.raw_store_url)
    return open_analyzed_store(args.analyzed_store_url), raw_store


def _apply_retention(args: argparse.Namespace, analyzed_store, raw_store) -> None:
    if not args.apply_retention or analyzed_store is None or raw_store is None:
        return
    # Only genuinely deferred import in this module: retention machinery
    # loads solely for --apply-retention. The rest of flume.store is
    # already imported at module scope via flume.ingest.write.
    from flume.store.retention import run_retention

    report = run_retention(
        store=analyzed_store,
        raw_store=raw_store,
        policy=load_policy(),
        sources=[a.name for a in registered()],
        dry_run=args.dry_run,
    )
    _print_summary({"retention": report})


def _run(args: argparse.Namespace, source, ingest, analyzed_store, raw_store) -> int:
    state_db = args.state_db
    if args.dry_run and not Path(state_db).exists():
        # Nothing to read and nothing may be written: stay off disk.
        state_db = ":memory:"
    with SqliteIngestStateStore(state_db) as store:
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
                _apply_retention(args, analyzed_store, raw_store)
                time.sleep(args.interval_seconds)

        summary = run_once(
            source=source,
            store=store,
            ingest=ingest,
            quiet_seconds=args.quiet_seconds,
            dry_run=args.dry_run,
        )
        _print_summary(summary.to_dict())
        _apply_retention(args, analyzed_store, raw_store)
        return 1 if summary.failed else 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flume ingest",
        description="Discover quiet transcript files and checkpoint ingest state.",
    )
    parser.add_argument(
        "--source",
        required=True,
        help=(
            "Source adapter to ingest: claude-code or codex. 'fake' is a "
            "test fixture source and needs --fake-root."
        ),
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
        "--analyzed-store-url",
        default=None,
        help=(
            "Analyzed store URL (default: sqlite://~/.flume/store.sqlite3).\n"
            "Holds the rows derived from parsing."
        ),
    )
    parser.add_argument(
        "--raw-store-url",
        default=None,
        help=(
            "Raw store URL (default: file://~/.flume/raw). Holds the\n"
            "original transcript bytes, captured before parsing."
        ),
    )
    parser.add_argument(
        "--no-raw-store",
        action="store_true",
        help="Skip capturing raw file copies when ingesting to the store.",
    )
    parser.add_argument(
        "--apply-retention",
        action="store_true",
        help=(
            "After each ingest pass, enforce retention TTLs from "
            "~/.flume/config.toml."
        ),
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=None,
        help=(
            "sqlite ingest-state path (default: "
            "~/.flume/auto-ingest-state.sqlite3)."
        ),
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
    analyzed_store=None,
    raw_store=None,
) -> tuple[TranscriptSource, IngestFunction]:
    source_name = args.source
    if source_name == "fake":
        if args.fake_root is None:
            parser.error("--fake-root is required for --source fake")
        return FakeTranscriptSource(args.fake_root), fake_ingest

    from flume.ingest.write import store_ingest_function
    from flume.sources import get_adapter, get_discovery

    try:
        source = get_discovery(source_name, **_discovery_flags(args, source_name))
        adapter = get_adapter(source_name)
    except ValueError as exc:
        # Unknown source, or a push-only source with no discovery —
        # the registry's message already says which.
        parser.error(str(exc))

    return source, store_ingest_function(adapter, analyzed_store, raw_store)


def _discovery_flags(args: argparse.Namespace, source_name: str) -> dict:
    """CLI flags for the sources flume ships, mapped to their `make_source`
    keywords. A config-declared source takes its options from `[sources]`."""
    if source_name == "codex":
        return {
            "roots": args.codex_root,
            "include_archived": args.include_archived_codex,
            "archived_root": args.codex_archived_root,
        }
    if source_name == "claude-code":
        return {"roots": args.claude_root}
    return {}



def _print_summary(summary: dict[str, object]) -> None:
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
