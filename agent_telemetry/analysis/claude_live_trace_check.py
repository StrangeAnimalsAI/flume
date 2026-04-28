"""Check native Claude Code live traces for missing interaction grouping.

Usage:
    uv run python -m agent_telemetry.analysis.claude_live_trace_check \
        [--langfuse-url http://localhost:3000] [--limit 100] [--pages 5]

Credentials default to `infra/langfuse/.env` in this repo, matching the
parity checker. The check ignores synthetic Claude-shaped smoke spans that do
not carry `session.id`. Exit code is 0 when recent Claude session observations
are grouped under `claude_code.interaction` roots and 1 when child spans are
split into top-level traces or sessions span multiple trace IDs.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
LANGFUSE_ENV = REPO_ROOT / "infra" / "langfuse" / ".env"

CHILD_SPANS = {
    "claude_code.llm_request",
    "claude_code.tool",
    "claude_code.tool.blocked_on_user",
    "claude_code.tool.execution",
    "claude_code.hook",
}


@dataclass(frozen=True)
class LangfuseCreds:
    url: str
    public_key: str
    secret_key: str


@dataclass(frozen=True)
class ClaudeObservation:
    trace_id: str
    trace_name: str
    observation_id: str
    name: str
    parent_id: str
    session_id: str
    source: str


@dataclass(frozen=True)
class GroupingSummary:
    observation_count: int
    trace_count: int
    session_count: int
    name_counts: Counter[str]
    source_counts: Counter[str]
    traces_missing_interaction: set[str]
    orphan_child_observations: list[ClaudeObservation]
    sessions_split_across_traces: dict[str, set[str]]

    @property
    def has_grouping_issues(self) -> bool:
        return bool(
            self.traces_missing_interaction
            or self.orphan_child_observations
            or self.sessions_split_across_traces
        )


def _load_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _metadata(item: dict[str, Any]) -> dict[str, Any]:
    metadata = item.get("metadata") or {}
    return metadata if isinstance(metadata, dict) else {}


def _attrs(item: dict[str, Any]) -> dict[str, Any]:
    attrs = _metadata(item).get("attributes") or {}
    return attrs if isinstance(attrs, dict) else {}


def _resource_attrs(item: dict[str, Any]) -> dict[str, Any]:
    attrs = _metadata(item).get("resourceAttributes") or {}
    return attrs if isinstance(attrs, dict) else {}


def _first_present(*values: Any, default: str = "unknown") -> str:
    for value in values:
        if value:
            return str(value)
    return default


def _observation_record(
    trace: dict[str, Any],
    observation: dict[str, Any],
) -> ClaudeObservation | None:
    name = str(observation.get("name") or "")
    if not name.startswith("claude_code."):
        return None

    attrs = _attrs(observation)
    resource_attrs = _resource_attrs(observation)
    trace_resource_attrs = _resource_attrs(trace)
    trace_metadata = _metadata(trace)
    source = _first_present(
        attrs.get("langfuse.trace.metadata.agent_source"),
        resource_attrs.get("source"),
        trace_metadata.get("agent_source"),
        trace_resource_attrs.get("source"),
    )

    return ClaudeObservation(
        trace_id=str(trace.get("id") or ""),
        trace_name=str(trace.get("name") or ""),
        observation_id=str(observation.get("id") or ""),
        name=name,
        parent_id=str(observation.get("parentObservationId") or ""),
        session_id=_first_present(
            attrs.get("session.id"),
            resource_attrs.get("session.id"),
            trace_resource_attrs.get("session.id"),
            default="unknown",
        ),
        source=source,
    )


def summarize(records: list[ClaudeObservation]) -> GroupingSummary:
    by_trace: dict[str, list[ClaudeObservation]] = defaultdict(list)
    by_session: dict[str, set[str]] = defaultdict(set)
    name_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    orphan_child_observations: list[ClaudeObservation] = []

    for record in records:
        by_trace[record.trace_id].append(record)
        if record.session_id != "unknown":
            by_session[record.session_id].add(record.trace_id)
        name_counts[record.name] += 1
        source_counts[record.source] += 1
        if record.name in CHILD_SPANS and not record.parent_id:
            orphan_child_observations.append(record)

    traces_missing_interaction = {
        trace_id
        for trace_id, trace_records in by_trace.items()
        if not any(r.name == "claude_code.interaction" for r in trace_records)
    }
    sessions_split_across_traces = {
        session_id: trace_ids
        for session_id, trace_ids in by_session.items()
        if len(trace_ids) > 1
    }

    return GroupingSummary(
        observation_count=len(records),
        trace_count=len(by_trace),
        session_count=len(by_session),
        name_counts=name_counts,
        source_counts=source_counts,
        traces_missing_interaction=traces_missing_interaction,
        orphan_child_observations=orphan_child_observations,
        sessions_split_across_traces=sessions_split_across_traces,
    )


def _fetch_recent_traces(
    client: httpx.Client,
    *,
    pages: int,
    limit: int,
) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []
    for page in range(1, pages + 1):
        response = client.get("/api/public/traces", params={"limit": limit, "page": page})
        response.raise_for_status()
        data = response.json().get("data") or []
        if not data:
            break
        traces.extend(data)
    return traces


def _fetch_trace(client: httpx.Client, trace_id: str) -> dict[str, Any]:
    response = client.get(f"/api/public/traces/{trace_id}")
    response.raise_for_status()
    return response.json()


def fetch_claude_observations(
    creds: LangfuseCreds,
    *,
    pages: int,
    limit: int,
) -> list[ClaudeObservation]:
    records: list[ClaudeObservation] = []
    with httpx.Client(
        base_url=creds.url.rstrip("/"),
        auth=(creds.public_key, creds.secret_key),
        timeout=30.0,
    ) as client:
        for row in _fetch_recent_traces(client, pages=pages, limit=limit):
            trace_id = row.get("id")
            if not trace_id:
                continue
            trace = _fetch_trace(client, str(trace_id))
            observations = trace.get("observations") or []
            if not isinstance(observations, list):
                continue
            for observation in observations:
                if not isinstance(observation, dict):
                    continue
                record = _observation_record(trace, observation)
                if record is not None and record.session_id != "unknown":
                    records.append(record)
    return records


def format_summary(summary: GroupingSummary, *, sample_limit: int = 10) -> str:
    lines = [
        (
            f"claude_observations={summary.observation_count} "
            f"traces={summary.trace_count} sessions={summary.session_count}"
        ),
        "span_names:",
    ]
    for name, count in summary.name_counts.most_common():
        lines.append(f"  {count:5d}  {name}")

    lines.append("sources:")
    for source, count in summary.source_counts.most_common():
        lines.append(f"  {count:5d}  {source}")

    lines.append(
        f"traces_missing_interaction={len(summary.traces_missing_interaction)}"
    )
    lines.append(
        f"orphan_child_observations={len(summary.orphan_child_observations)}"
    )
    lines.append(
        f"sessions_split_across_traces={len(summary.sessions_split_across_traces)}"
    )

    if summary.sessions_split_across_traces:
        lines.append("split_session_samples:")
        for session_id, trace_ids in list(summary.sessions_split_across_traces.items())[
            :sample_limit
        ]:
            joined = ", ".join(sorted(trace_ids))
            lines.append(f"  session.id={session_id} traces={joined}")

    if summary.orphan_child_observations:
        lines.append("orphan_child_samples:")
        for record in summary.orphan_child_observations[:sample_limit]:
            lines.append(
                "  "
                f"trace={record.trace_id} obs={record.observation_id} "
                f"name={record.name} session.id={record.session_id} "
                f"source={record.source}"
            )

    if not summary.has_grouping_issues:
        lines.append("claude_live_grouping=ok")
    else:
        lines.append("claude_live_grouping=split_or_missing_roots")
    return "\n".join(lines)


def run(creds: LangfuseCreds, *, pages: int, limit: int) -> int:
    records = fetch_claude_observations(creds, pages=pages, limit=limit)
    summary = summarize(records)
    print(format_summary(summary))
    return 1 if summary.has_grouping_issues else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--langfuse-url", default="http://localhost:3000")
    parser.add_argument("--public-key", default=os.getenv("LANGFUSE_PUBLIC_KEY"))
    parser.add_argument("--secret-key", default=os.getenv("LANGFUSE_SECRET_KEY"))
    parser.add_argument("--env-file", type=Path, default=LANGFUSE_ENV)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--pages", type=int, default=5)
    args = parser.parse_args(argv)

    env = _load_env(args.env_file)
    public_key = args.public_key or env.get("LANGFUSE_INIT_PROJECT_PUBLIC_KEY")
    secret_key = args.secret_key or env.get("LANGFUSE_INIT_PROJECT_SECRET_KEY")
    if not public_key or not secret_key:
        print(
            "missing Langfuse credentials; pass --public-key/--secret-key or set "
            "LANGFUSE_INIT_PROJECT_{PUBLIC,SECRET}_KEY in --env-file",
            file=sys.stderr,
        )
        return 2

    return run(
        LangfuseCreds(args.langfuse_url, public_key, secret_key),
        pages=args.pages,
        limit=args.limit,
    )


if __name__ == "__main__":
    raise SystemExit(main())
