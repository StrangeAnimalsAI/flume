"""OTel-SDK adapter: span-dicts → real OTel Spans with deterministic IDs.

The mapper (`jsonl_to_spans`) emits pure data: hex trace/span IDs, nanosecond
timestamps, attributes. This module turns that data into live SDK `_Span`
objects routed through whatever `SpanProcessor` the caller attaches — an
`InMemorySpanExporter` in tests, a `BatchSpanProcessor(OTLPSpanExporter(...))`
in the CLI.

Deterministic IDs matter because backfill is rerunnable: replaying the same
JSONL must produce the same trace on the wire. We bypass the tracer's
IdGenerator and hand a pre-built `SpanContext` to `_Span` directly.
"""
from __future__ import annotations

from typing import Any

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, _Span
from opentelemetry.trace import SpanContext, SpanKind, Status, StatusCode, TraceFlags

Span = dict[str, Any]

_SAMPLED = TraceFlags(0x01)


def export_spans_via_otlp(
    span_dicts: list[Span],
    endpoint: str,
    source: str,
) -> None:
    """Send span-dicts to an OTLP-HTTP traces endpoint.

    Shared across the per-source backfill CLIs (INT-432 Claude Code,
    INT-433 Codex, and whatever lands next). Each mapper already tags its
    spans with a `source` attribute; that same value is mirrored into the
    Resource here so Langfuse can split by source without reading every span.
    """
    # Imports deferred so CLI `--dry-run` paths stay pure-Python.
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create(
        {
            "service.name": "agent-telemetry-backfill",
            "source": source,
        }
    )
    exporter = OTLPSpanExporter(endpoint=endpoint)
    processor = BatchSpanProcessor(exporter)
    try:
        export_span_dicts(span_dicts, resource, processor)
    finally:
        processor.shutdown()


def export_span_dicts(
    span_dicts: list[Span],
    resource: Resource,
    span_processor: SpanProcessor,
) -> list[_Span]:
    """Convert span-dicts to SDK spans and push them through `span_processor`.

    Returns the list of constructed (and ended) `_Span` objects, which is
    useful for tests — production callers ignore the return value and let the
    processor/exporter do the work.
    """
    built: list[_Span] = []
    for sd in span_dicts:
        span = _build_span(sd, resource, span_processor)
        built.append(span)
    return built


def _build_span(
    sd: Span,
    resource: Resource,
    span_processor: SpanProcessor,
) -> _Span:
    ctx = SpanContext(
        trace_id=int(sd["trace_id"], 16),
        span_id=int(sd["span_id"], 16),
        is_remote=False,
        trace_flags=_SAMPLED,
    )
    parent_ctx: SpanContext | None = None
    parent_hex = sd.get("parent_span_id")
    if parent_hex:
        parent_ctx = SpanContext(
            trace_id=ctx.trace_id,
            span_id=int(parent_hex, 16),
            is_remote=False,
            trace_flags=_SAMPLED,
        )

    attributes = {
        k: v for k, v in (sd.get("attributes") or {}).items() if v is not None
    }

    span = _Span(
        name=sd["name"],
        context=ctx,
        parent=parent_ctx,
        resource=resource,
        attributes=attributes,
        span_processor=span_processor,
        kind=SpanKind.INTERNAL,
    )
    span.start(start_time=sd["start_unix_nano"])
    status_str = sd.get("status") or "OK"
    if status_str == "ERROR":
        span.set_status(Status(StatusCode.ERROR))
    else:
        span.set_status(Status(StatusCode.OK))
    span.end(end_time=sd["end_unix_nano"])
    return span
