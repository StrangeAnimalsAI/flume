"""Langfuse-specific span attributes shared by backfill mappers."""
from __future__ import annotations

from typing import Any

Span = dict[str, Any]


def enrich_trace_attrs(
    spans: list[Span],
    *,
    agent_source: str,
    agent_family: str | None = None,
    agent_surface: str | None = None,
) -> list[Span]:
    """Add Langfuse trace metadata/tags to every span in a trace.

    Langfuse recommends propagating trace-level metadata and tags to every
    span so filtering and aggregation work even when views operate over
    observations. The raw OTel `source` attribute is left untouched; these
    keys only add Langfuse-friendly projections.
    """
    family = agent_family or _agent_family(agent_source)
    tags = [f"agent:{agent_source}"]
    if family:
        tags.append(f"family:{family}")
    if agent_surface:
        tags.append(f"surface:{agent_surface}")

    for span in spans:
        attrs = span.setdefault("attributes", {})
        attrs.setdefault("langfuse.trace.metadata.agent_source", agent_source)
        if family:
            attrs.setdefault("langfuse.trace.metadata.agent_family", family)
        if agent_surface:
            attrs.setdefault("langfuse.trace.metadata.agent_surface", agent_surface)
        attrs.setdefault("langfuse.trace.tags", list(tags))
    return spans


def _agent_family(agent_source: str) -> str | None:
    if agent_source == "codex" or agent_source.startswith("codex-"):
        return "codex"
    if agent_source == "claude-code" or agent_source.startswith("claude-code-"):
        return "claude-code"
    return None
