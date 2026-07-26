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
(claude-code, codex) implement `discover()`; the harness pushes its own
transcript when a run ends and does not need one.

Contract notes:

- Adapter functions take a local `Path` and must not perform I/O beyond
  reading it. Remote acquisition (S3, a server, ...) belongs in a
  `TranscriptSource` that materializes files locally and hands over
  paths — acquire-then-parse, never parse-at-a-distance.
- Adapters may depend on `flume.store.base` types (`ContentRow`); the
  store never imports this package. Keep it that way.

The registry is data, not code — the same treatment `flume.pricing` gives
model rates. `_DEFAULTS` below ships the sources flume knows, and each
entry is just a name, a vendor, and a module path; nothing is imported
until `get_adapter()` actually resolves to it. A `[sources]` table in
`~/.flume/config.toml` adds or overrides entries, so a third-party source
needs no edit to flume:

    [sources]
    "my-agent" = { vendor = "acme", module = "my_pkg.flume_adapter" }

To add a vendor: write the module, define a module-level `ADAPTER =
SourceAdapter(...)` in it, and list it in `_DEFAULTS` (or in config). The
module owns its own adapter definition, which is what keeps this package
from importing every vendor at import time — and what stops the vendor
modules, which import names from here, from forming a cycle.
"""
from __future__ import annotations

import importlib
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from flume.store.base import ContentRow
from flume.store.config import DEFAULT_CONFIG_PATH, load_toml

Span = dict[str, Any]

# Attributes a vendor module exposes: its adapter, and — for pull-based
# sources only — a factory building its `TranscriptSource`.
ADAPTER_ATTR = "ADAPTER"
DISCOVERY_ATTR = "make_source"


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


@dataclass(frozen=True)
class SourceInfo:
    """What the registry knows about a source without importing its module.

    Enough to answer "which sources exist" and "what vendor is this" — the
    only questions the CLI asks when building `--source` choices — so those
    paths never pay to import a parser they will not run.
    """

    name: str
    vendor: str
    module: str
    # Everything else from the source's config table — discovery roots and
    # whatever else its `make_source` understands. Empty for shipped sources,
    # whose options arrive as CLI flags instead.
    options: dict[str, Any] = field(default_factory=dict)


_DEFAULTS: dict[str, SourceInfo] = {
    "claude-code": SourceInfo(
        "claude-code", "anthropic", "flume.sources.claude_code"
    ),
    "codex": SourceInfo("codex", "openai", "flume.sources.codex"),
    "harness": SourceInfo("harness", "anthropic", "flume.sources.harness"),
}

# Merged registry, memoized against the config file's identity + mtime so a
# rewritten config is picked up without a restart and without re-reading TOML
# on every lookup (`rebuild_stale` resolves an adapter per row).
_cache: tuple[tuple[str, int | None, int | None], dict[str, SourceInfo]] | None = None


def _config_stamp() -> tuple[str, int | None, int | None]:
    """Identity of the config file: path, mtime, size.

    Size is in there because mtime granularity is filesystem-dependent, and
    two edits landing inside one tick would otherwise serve a stale registry.
    """
    path = os.environ.get("FLUME_CONFIG", str(DEFAULT_CONFIG_PATH))
    try:
        stat = Path(path).stat()
        return path, stat.st_mtime_ns, stat.st_size
    except OSError:  # no config file: a valid, stable state
        return path, None, None


def _registry() -> dict[str, SourceInfo]:
    global _cache
    stamp = _config_stamp()
    if _cache is not None and _cache[0] == stamp:
        return _cache[1]
    entries = dict(_DEFAULTS)
    for name, spec in (load_toml().get("sources") or {}).items():
        entries[name] = _parse_entry(name, spec)
    _cache = (stamp, entries)
    return entries


def _parse_entry(name: str, spec: object) -> SourceInfo:
    if not isinstance(spec, dict):
        raise ValueError(
            f"bad [sources] entry for {name!r}: expected a table with "
            "`vendor` and `module`"
        )
    missing = {"vendor", "module"} - set(spec)
    if missing:
        raise ValueError(
            f"[sources] entry {name!r} is missing {', '.join(sorted(missing))}"
        )
    options = {k: v for k, v in spec.items() if k not in ("vendor", "module")}
    return SourceInfo(name, str(spec["vendor"]), str(spec["module"]), options)


def registered() -> list[SourceInfo]:
    """Every known source, by name. Imports no vendor module."""
    return sorted(_registry().values(), key=lambda info: info.name)


def get_adapter(name_or_vendor: str) -> SourceAdapter:
    """Resolve an adapter by source name, or by vendor when unambiguous.

    Imports the vendor module here and nowhere earlier, so a process that
    touches one source never loads the parsers for the others.
    """
    return _load(resolve(name_or_vendor))


def get_discovery(name_or_vendor: str, **overrides: Any) -> TranscriptSource:
    """Build the `TranscriptSource` for a pull-based source.

    Options come from the source's config table, with `overrides` (the CLI's
    flags) layered on top; a None override defers to config, then to the
    source's own default roots. Raises for a push-only source — one whose
    module defines no `make_source`, like the harness.
    """
    info = resolve(name_or_vendor)
    module = _import(info)
    factory = getattr(module, DISCOVERY_ATTR, None)
    if factory is None:
        raise ValueError(
            f"source {info.name!r} has no discovery: its module defines no "
            f"`{DISCOVERY_ATTR}`, so it is push-only and must be ingested by path"
        )
    options = dict(info.options)
    options.update({k: v for k, v in overrides.items() if v is not None})
    return factory(**options)


def resolve(name_or_vendor: str) -> SourceInfo:
    """Registry entry for a source name, or a vendor when unambiguous."""
    entries = _registry()
    info = entries.get(name_or_vendor)
    if info is None:
        by_vendor = [e for e in entries.values() if e.vendor == name_or_vendor]
        if len(by_vendor) == 1:
            info = by_vendor[0]
        elif len(by_vendor) > 1:
            names = ", ".join(sorted(e.name for e in by_vendor))
            raise ValueError(
                f"vendor {name_or_vendor!r} is ambiguous; use a source name: {names}"
            )
        else:
            known = ", ".join(sorted(entries))
            raise ValueError(
                f"unknown source {name_or_vendor!r}; known: {known}"
            )
    return info


def _import(info: SourceInfo):
    try:
        return importlib.import_module(info.module)
    except ImportError as exc:
        raise ValueError(
            f"source {info.name!r} declares module {info.module!r}, which "
            f"failed to import: {exc}"
        ) from None


def _load(info: SourceInfo) -> SourceAdapter:
    module = _import(info)
    adapter = getattr(module, ADAPTER_ATTR, None)
    if not isinstance(adapter, SourceAdapter):
        raise ValueError(
            f"source {info.name!r}: module {info.module!r} defines no "
            f"module-level `{ADAPTER_ATTR}` of type SourceAdapter"
        )
    return adapter
