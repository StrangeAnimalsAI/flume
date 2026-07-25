"""Source adapters: the only code in flume that knows any vendor's format.

A source is one coding agent's transcript format plus, when transcripts
are pulled rather than pushed, the knowledge of where its files live.
Each vendor is one module in this package implementing two small
interfaces:

`SourceAdapter` — how to read the format. Three pure functions plus
identity metadata:

    1. `map_spans(path) -> list[Span]` parses a transcript into flume's
       normal form: span dicts named `<source>.interaction` (root),
       `<source>.llm_request` (turns), `<source>.tool` (tool calls). The
       vocabulary is OTel-shaped for historical reasons; it is flume's
       internal interchange format, not an OTel compatibility layer.
    2. `extract_contents(path, session_id) -> list[ContentRow]` walks the
       same file for the full-fidelity layer (thinking, messages,
       untruncated tool payloads), keyed by the same deterministic span
       ids `map_spans` produces so text joins onto the metrics skeleton.
    3. `probe(path) -> dict` (optional) reads at most a few lines for
       cheap hints: hierarchy (parent_session_id, is_subagent), cwd.
    4. `classify_tool(name, args_preview) -> str` (optional) labels one
       tool call `navigation` / `editing` / `subagent` / `bash-other` /
       `other`. Only the source knows its own tool vocabulary — Claude
       Code's `Read` and Codex's `exec_command` mean different things —
       and time attribution is wrong without it.

`TranscriptSource` — where transcripts live. Pull-based sources
(claude-code, codex) implement `discover()`; push-based sources (the
harness ingests its own transcript when a run ends) do not have one.

Contract notes:

- Adapter functions take a local `Path` and must not perform I/O beyond
  reading it. Remote acquisition (S3, a server, ...) belongs in a
  `TranscriptSource` that materializes files locally and hands over
  paths — acquire-then-parse, never parse-at-a-distance.
- Adapters may depend on `flume.store.base` types (`ContentRow`); the
  store never imports this package. Keep it that way.

To add a vendor: write the module, then `register(SourceAdapter(...))`
below. Adapters resolve by source name, or by vendor alias when only one
source has that vendor.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from flume.store.base import ContentRow

Span = dict[str, Any]


@dataclass(frozen=True)
class SourceAdapter:
    """How to read one vendor's transcript format. Pure functions of a file."""

    name: str  # canonical source name, e.g. "claude-code"
    vendor: str  # model vendor, e.g. "anthropic"
    map_spans: Callable[[Path], list[Span]]
    extract_contents: Callable[[Path, str], list[ContentRow]]
    probe: Callable[[Path], dict[str, Any]] | None = None
    classify_tool: Callable[[str | None, str | None], str] | None = None


@dataclass(frozen=True)
class DiscoveredTranscript:
    """A transcript candidate reported by a `TranscriptSource`."""

    source_type: str
    path: Path
    session_id: str | None = None
    trace_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class TranscriptSource(Protocol):
    """Where transcripts of one source live. Discovery only — keep it boring:
    enumerate candidate files and expose ids only when cheap to extract."""

    source_type: str

    def discover(self) -> Iterable[DiscoveredTranscript]:
        """Yield transcript files this source knows about."""


_ADAPTERS: dict[str, SourceAdapter] = {}


def register(adapter: SourceAdapter) -> None:
    _ADAPTERS[adapter.name] = adapter


def get_adapter(name_or_vendor: str) -> SourceAdapter:
    """Resolve an adapter by source name, or by vendor when unambiguous."""
    adapter = _ADAPTERS.get(name_or_vendor)
    if adapter is not None:
        return adapter
    by_vendor = [a for a in _ADAPTERS.values() if a.vendor == name_or_vendor]
    if len(by_vendor) == 1:
        return by_vendor[0]
    if len(by_vendor) > 1:
        names = ", ".join(sorted(a.name for a in by_vendor))
        raise ValueError(
            f"vendor {name_or_vendor!r} is ambiguous; use a source name: {names}"
        )
    known = ", ".join(sorted(_ADAPTERS))
    raise ValueError(f"unknown source {name_or_vendor!r}; known: {known}")


def adapters() -> list[SourceAdapter]:
    return sorted(_ADAPTERS.values(), key=lambda a: a.name)


from flume.sources import claude_code as _claude_code  # noqa: E402
from flume.sources import codex as _codex  # noqa: E402
from flume.sources import harness as _harness  # noqa: E402

register(
    SourceAdapter(
        name="claude-code",
        vendor="anthropic",
        map_spans=_claude_code.jsonl_to_spans,
        extract_contents=_claude_code.extract_contents,
        probe=_claude_code.probe,
        classify_tool=_claude_code.classify_tool,
    )
)
register(
    SourceAdapter(
        name="codex",
        vendor="openai",
        map_spans=_codex.rollout_to_spans,
        extract_contents=_codex.extract_contents,
        probe=_codex.probe,
        classify_tool=_codex.classify_tool,
    )
)
register(
    SourceAdapter(
        name="harness",
        vendor="anthropic",
        map_spans=_harness.harness_to_spans,
        extract_contents=_harness.extract_contents,
        probe=_harness.probe,
        classify_tool=_harness.classify_tool,
    )
)
