"""Claude Code JSONL source adapter for transcript auto-ingest."""
from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from agent_telemetry.backfill.claude_code import jsonl_to_spans, trace_id_for_session
from agent_telemetry.backfill.otlp import export_spans_via_otlp
from agent_telemetry.ingest.runner import IngestOutcome, IngestRequest
from agent_telemetry.ingest.types import DiscoveredTranscript

Span = dict[str, Any]
ClaudeCodeExporter = Callable[[list[Span], str, str], None]

DEFAULT_CLAUDE_PROJECTS_ROOT = Path.home() / ".claude" / "projects"


class ClaudeCodeTranscriptSource:
    """Discover canonical Claude Code JSONL transcripts under project roots."""

    source_type = "claude-code"

    def __init__(self, roots: Sequence[Path | str] | None = None) -> None:
        self.roots = tuple(
            Path(root).expanduser().resolve(strict=False)
            for root in (roots or (DEFAULT_CLAUDE_PROJECTS_ROOT,))
        )

    def discover(self) -> Iterable[DiscoveredTranscript]:
        for path in _unique_sorted(self._candidate_paths()):
            metadata = read_transcript_metadata(path)
            session_id = path.stem
            metadata.setdefault("session_id", session_id)
            yield DiscoveredTranscript(
                source_type=self.source_type,
                path=path,
                session_id=session_id,
                trace_id=trace_id_for_session(session_id),
                metadata=metadata,
            )

    def _candidate_paths(self) -> Iterable[Path]:
        for root in self.roots:
            yield from _jsonl_paths(root, recursive=True)


def read_transcript_metadata(path: Path, *, max_lines: int = 200) -> dict[str, Any]:
    """Extract cheap Claude Code metadata without running the full mapper."""
    metadata: dict[str, Any] = {"source": "claude-code"}
    for index, obj in enumerate(_read_jsonl_objects(path)):
        if index >= max_lines:
            break

        _merge_event_metadata(metadata, obj)
        _merge_message_metadata(metadata, obj.get("message"))
        if _has_enough_metadata(metadata):
            break

    return metadata


def ingest_claude_code_transcript(
    request: IngestRequest,
    *,
    endpoint: str,
    exporter: ClaudeCodeExporter = export_spans_via_otlp,
) -> IngestOutcome:
    """Map one Claude Code transcript and export it through the backfill path."""
    spans = jsonl_to_spans(request.transcript.path)
    exporter(spans, endpoint, "claude-code")
    root = spans[0] if spans else {}
    attrs = root.get("attributes") if isinstance(root, dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}
    session_id = _string(attrs.get("session.id")) or request.transcript.session_id
    trace_id = _string(root.get("trace_id")) or request.transcript.trace_id
    return IngestOutcome(session_id=session_id, trace_id=trace_id)


def _merge_event_metadata(metadata: dict[str, Any], obj: dict[str, Any]) -> None:
    event_session_id = _string(obj.get("sessionId"))
    if event_session_id:
        metadata.setdefault("claude_session_id", event_session_id)

    for source_key, metadata_key in (
        ("cwd", "cwd"),
        ("version", "version"),
        ("gitBranch", "git_branch"),
        ("userType", "user_type"),
        ("permissionMode", "permission_mode"),
        ("promptId", "prompt_id"),
        ("agentId", "agent_id"),
        ("slug", "slug"),
    ):
        value = _string(obj.get(source_key))
        if value:
            metadata.setdefault(metadata_key, value)

    entrypoint = _string(obj.get("entrypoint"))
    if entrypoint:
        metadata.setdefault("entrypoint", entrypoint)
        metadata.setdefault("surface", entrypoint)

    is_sidechain = obj.get("isSidechain")
    if isinstance(is_sidechain, bool):
        metadata.setdefault("is_sidechain", is_sidechain)


def _merge_message_metadata(metadata: dict[str, Any], message: Any) -> None:
    if not isinstance(message, dict):
        return
    model = _string(message.get("model"))
    if model:
        metadata.setdefault("model", model)


def _has_enough_metadata(metadata: dict[str, Any]) -> bool:
    return bool(
        metadata.get("claude_session_id")
        and metadata.get("entrypoint")
        and metadata.get("version")
        and metadata.get("model")
    )


def _jsonl_paths(root: Path, *, recursive: bool) -> Iterable[Path]:
    if root.is_file() and root.suffix == ".jsonl":
        yield root
        return
    if not root.is_dir():
        return
    pattern = "**/*.jsonl" if recursive else "*.jsonl"
    yield from root.glob(pattern)


def _unique_sorted(paths: Iterable[Path]) -> list[Path]:
    out: dict[str, Path] = {}
    for path in paths:
        if path.is_file():
            resolved = path.expanduser().resolve(strict=False)
            out[str(resolved)] = resolved
    return [out[key] for key in sorted(out)]


def _read_jsonl_objects(path: Path) -> Iterable[dict[str, Any]]:
    try:
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    yield obj
    except FileNotFoundError:
        return


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
