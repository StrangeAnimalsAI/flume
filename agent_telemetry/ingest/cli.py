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
    if args.state_db is None:
        suffix = ".store" if args.backend == "store" else ""
        args.state_db = Path.cwd() / f".agent_telemetry_auto_ingest{suffix}.sqlite3"
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
    if args.backend != "store":
        if args.apply_retention:
            parser.error("--apply-retention requires --backend store")
        return None, None
    from agent_telemetry.store.archive import open_archive
    from agent_telemetry.store.base import open_store

    archive = None if args.no_raw_archive else open_archive(args.archive_url)
    return open_store(args.store_url), archive


def _apply_retention(args: argparse.Namespace, session_store, archive) -> None:
    if not args.apply_retention or session_store is None or archive is None:
        return
    from agent_telemetry.store.config import load_policy
    from agent_telemetry.store.retention import run_retention

    report = run_retention(
        store=session_store,
        archive=archive,
        policy=load_policy(),
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
        "--backend",
        choices=("otlp", "store"),
        default="otlp",
        help=(
            "Ingest sink: 'otlp' exports spans to --endpoint (Langfuse path); "
            "'store' writes full-fidelity sessions to the local session store."
        ),
    )
    parser.add_argument(
        "--store-url",
        default=None,
        help=(
            "Session store URL for --backend store "
            "(default: sqlite://~/.agent-telemetry/store.sqlite3)."
        ),
    )
    parser.add_argument(
        "--archive-url",
        default=None,
        help=(
            "Raw archive URL for --backend store "
            "(default: file://~/.agent-telemetry/raw)."
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
            "~/.agent-telemetry/config.toml (store backend only)."
        ),
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=None,
        help=(
            "sqlite ingest-state path (default: "
            ".agent_telemetry_auto_ingest.sqlite3 in the cwd; the store "
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
        if session_store is not None:
            from agent_telemetry.store.ingest import store_ingest_function

            return source, store_ingest_function("codex", session_store, archive)

        def ingest(request):
            return ingest_codex_rollout(request, endpoint=args.endpoint)

        return source, ingest

    if source_name == "claude-code":
        source = ClaudeCodeTranscriptSource(args.claude_root)
        if session_store is not None:
            from agent_telemetry.store.ingest import store_ingest_function

            return source, store_ingest_function(
                "claude-code", session_store, archive
            )

        def ingest(request):
            return ingest_claude_code_transcript(request, endpoint=args.endpoint)

        return source, ingest

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
        from agent_telemetry.store.registry import get_adapter

        return get_adapter(name).name
    except ValueError:
        return name


def _print_summary(summary: dict[str, object]) -> None:
    json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
