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
from flume.sources import TranscriptSource
from flume.sources.claude_code import ClaudeCodeTranscriptSource
from flume.sources.codex import DEFAULT_CODEX_ARCHIVED_ROOT, CodexRolloutSource


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.state_db is None:
        args.state_db = Path.cwd() / ".flume_auto_ingest.store.sqlite3"
    session_store, archive = _open_backends(args, parser)
    try:
        source, ingest = _source_and_ingest(args, parser, session_store, archive)
        return _run(args, source, ingest, session_store, archive)
    finally:
        if session_store is not None:
            session_store.close()
        if archive is not None:
            archive.close()


def _open_backends(args: argparse.Namespace, parser: argparse.ArgumentParser):
    from flume.store.archive import open_archive
    from flume.store.base import open_store

    archive = None if args.no_raw_archive else open_archive(args.archive_url)
    return open_store(args.store_url), archive


def _apply_retention(args: argparse.Namespace, session_store, archive) -> None:
    if not args.apply_retention or session_store is None or archive is None:
        return
    from flume.sources import adapters
    from flume.store.config import load_policy
    from flume.store.retention import run_retention

    report = run_retention(
        store=session_store,
        archive=archive,
        policy=load_policy(),
        sources=[a.name for a in adapters()],
        dry_run=args.dry_run,
    )
    _print_summary({"retention": report})


def _run(args: argparse.Namespace, source, ingest, session_store, archive) -> int:
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
                _apply_retention(args, session_store, archive)
                time.sleep(args.interval_seconds)

        summary = run_once(
            source=source,
            store=store,
            ingest=ingest,
            quiet_seconds=args.quiet_seconds,
            dry_run=args.dry_run,
        )
        _print_summary(summary.to_dict())
        _apply_retention(args, session_store, archive)
        return 1 if summary.failed else 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flume ingest",
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
        "--backend",
        choices=("store",),
        default="store",
        help=(
            "Ingest sink. The local session store is the only backend; "
            "this flag is kept for service-plist compatibility."
        ),
    )
    parser.add_argument(
        "--store-url",
        default=None,
        help=(
            "Session store URL for --backend store "
            "(default: sqlite://~/.flume/store.sqlite3)."
        ),
    )
    parser.add_argument(
        "--archive-url",
        default=None,
        help=(
            "Raw archive URL for --backend store "
            "(default: file://~/.flume/raw)."
        ),
    )
    parser.add_argument(
        "--no-raw-archive",
        action="store_true",
        help="Skip capturing raw file copies when ingesting to the store.",
    )
    parser.add_argument(
        "--apply-retention",
        action="store_true",
        help=(
            "After each ingest pass, enforce retention TTLs from "
            "~/.flume/config.toml (store backend only)."
        ),
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=None,
        help=(
            "sqlite ingest-state path (default: "
            ".flume_auto_ingest.sqlite3 in the cwd; the store "
            "backend appends .store so the two sinks track independently)."
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
    session_store=None,
    archive=None,
) -> tuple[TranscriptSource, IngestFunction]:
    source_name = _canonical_source(args.source)
    if source_name == "fake":
        if args.fake_root is None:
            parser.error("--fake-root is required for --source fake")
        return FakeTranscriptSource(args.fake_root), fake_ingest

    if source_name == "codex":
        source = CodexRolloutSource(
            args.codex_root,
            include_archived=args.include_archived_codex,
            archived_root=args.codex_archived_root
            if args.codex_archived_root is not None
            else DEFAULT_CODEX_ARCHIVED_ROOT,
        )
        from flume.ingest.write import store_ingest_function
        from flume.sources import get_adapter

        return source, store_ingest_function(
            get_adapter("codex"), session_store, archive
        )

    if source_name == "claude-code":
        source = ClaudeCodeTranscriptSource(args.claude_root)
        from flume.ingest.write import store_ingest_function
        from flume.sources import get_adapter

        return source, store_ingest_function(
            get_adapter("claude-code"), session_store, archive
        )

    parser.error(
        f"unsupported --source {args.source!r}; supported: fake, codex, "
        "claude-code (or vendor aliases anthropic, openai)"
    )


def _canonical_source(name: str) -> str:
    """Vendor aliases resolve through the adapter registry; discovery here
    still needs the concrete source adapter, so map back to its name."""
    if name == "fake":
        return name
    try:
        from flume.sources import get_adapter

        return get_adapter(name).name
    except ValueError:
        return name


def _print_summary(summary: dict[str, object]) -> None:
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
