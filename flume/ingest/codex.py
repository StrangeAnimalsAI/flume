"""Codex rollout source adapter for transcript auto-ingest."""
from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from flume.backfill.codex import rollout_to_spans, trace_id_for_session
from flume.backfill.otlp import export_spans_via_otlp
from flume.ingest.runner import IngestOutcome, IngestRequest
from flume.ingest.types import DiscoveredTranscript

Span = dict[str, Any]
CodexExporter = Callable[[list[Span], str, str], None]

DEFAULT_CODEX_SESSIONS_ROOT = Path.home() / ".codex" / "sessions"
DEFAULT_CODEX_ARCHIVED_ROOT = Path.home() / ".codex" / "archived_sessions"


class CodexRolloutSource:
    """Discover Codex rollout JSONL files under session roots."""

    source_type = "codex"

    def __init__(
        self,
        roots: Sequence[Path | str] | None = None,
        *,
        include_archived: bool = False,
        archived_root: Path | str = DEFAULT_CODEX_ARCHIVED_ROOT,
    ) -> None:
        self.roots = tuple(
            Path(root).expanduser().resolve(strict=False)
            for root in (roots or (DEFAULT_CODEX_SESSIONS_ROOT,))
        )
        self.include_archived = include_archived
        self.archived_root = Path(archived_root).expanduser().resolve(strict=False)

    def discover(self) -> Iterable[DiscoveredTranscript]:
        for path in _unique_sorted(self._candidate_paths()):
            metadata = read_rollout_metadata(path)
            session_id = _string(metadata.get("session_id")) or path.stem
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
        if self.include_archived:
            yield from _jsonl_paths(self.archived_root, recursive=False)


def read_rollout_metadata(path: Path, *, max_lines: int = 200) -> dict[str, Any]:
    """Extract cheap Codex metadata without running the full mapper."""
    metadata: dict[str, Any] = {}
    for index, obj in enumerate(_read_jsonl_objects(path)):
        if index >= max_lines:
            break
        payload = obj.get("payload")
        if not isinstance(payload, dict):
            continue

        if obj.get("type") == "session_meta":
            _merge_session_meta(metadata, payload)
            continue

        if obj.get("type") == "turn_context":
            model = payload.get("model")
            if isinstance(model, str) and model:
                metadata.setdefault("model", model)

        if metadata.get("session_id") and metadata.get("model"):
            break

    return metadata


def ingest_codex_rollout(
    request: IngestRequest,
    *,
    endpoint: str,
    exporter: CodexExporter = export_spans_via_otlp,
) -> IngestOutcome:
    """Map one Codex rollout and export it through the existing OTLP path."""
    spans = rollout_to_spans(request.transcript.path)
    exporter(spans, endpoint, "codex")
    root = spans[0] if spans else {}
    attrs = root.get("attributes") if isinstance(root, dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}
    session_id = _string(attrs.get("session.id")) or request.transcript.session_id
    trace_id = _string(root.get("trace_id")) or request.transcript.trace_id
    return IngestOutcome(session_id=session_id, trace_id=trace_id)


def _merge_session_meta(metadata: dict[str, Any], payload: dict[str, Any]) -> None:
    session_id = payload.get("id")
    if isinstance(session_id, str) and session_id:
        metadata.setdefault("session_id", session_id)

    for source_key, metadata_key in (
        ("originator", "originator"),
        ("cli_version", "cli_version"),
        ("cwd", "cwd"),
        ("model_provider", "model_provider"),
    ):
        value = payload.get(source_key)
        if isinstance(value, str) and value:
            metadata.setdefault(metadata_key, value)

    surface = _surface(payload.get("source"))
    if surface:
        metadata.setdefault("source", surface)
        metadata.setdefault("surface", surface)


def _surface(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict):
        if "subagent" in value:
            return "subagent"
        for key, nested in value.items():
            if isinstance(key, str) and nested:
                return key
    return None


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
