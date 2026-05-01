"""Check recent Langfuse traces for live Codex telemetry.

Usage:
    uv run python -m agent_telemetry.analysis.codex_live_trace_check \
        [--langfuse-url http://localhost:3000] [--since-minutes 60]

Credentials default to `infra/langfuse/.env` in this repo, matching the other
analysis checks. Exit code is 0 when at least one recent Codex trace is found
and 1 otherwise.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
LANGFUSE_ENV = REPO_ROOT / "infra" / "langfuse" / ".env"


@dataclass(frozen=True)
class LangfuseCreds:
    url: str
    public_key: str
    secret_key: str


@dataclass(frozen=True)
class CodexTrace:
    trace_id: str
    name: str
    timestamp: str
    session_id: str
    service_name: str
    agent_source: str
    agent_family: str
    observation_names: Counter[str]
    analysis_fields: Counter[str] = field(default_factory=Counter)
    vocabulary_candidates: Counter[str] = field(default_factory=Counter)


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


def _tags(item: dict[str, Any]) -> list[str]:
    tags = item.get("tags") or _metadata(item).get("tags") or []
    return tags if isinstance(tags, list) else []


def _timestamp(item: dict[str, Any]) -> str:
    return str(item.get("timestamp") or item.get("createdAt") or item.get("updatedAt") or "")


def _parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_codex_trace(item: dict[str, Any]) -> bool:
    metadata = _metadata(item)
    attrs = _attrs(item)
    resource_attrs = _resource_attrs(item)
    service_name = str(resource_attrs.get("service.name") or "")
    return (
        str(item.get("name") or "").startswith("codex")
        or metadata.get("agent_source") == "codex"
        or metadata.get("agent_family") == "codex"
        or attrs.get("langfuse.trace.metadata.agent_source") == "codex"
        or attrs.get("langfuse.trace.metadata.agent_family") == "codex"
        or service_name.startswith("codex")
        or "agent:codex" in _tags(item)
    )


def _is_smoke_trace(item: dict[str, Any]) -> bool:
    metadata = _metadata(item)
    attrs = metadata.get("attributes") or {}
    resource_attrs = _resource_attrs(item)
    session_id = attrs.get("session.id")
    service_name = resource_attrs.get("service.name")
    return str(session_id or "").startswith("codex-smoke") or service_name == "codex-smoke"


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


_LLM_OBSERVATION_NAMES = {
    "model_client.stream_responses_websocket",
    "responses_websocket.stream_request",
    "run_sampling_request",
    "stream_request",
    "try_run_sampling_request",
}

_TOKEN_USAGE_KEYS = {
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.cache_read.input_tokens",
    "gen_ai.usage.cache_read_input_tokens",
    "gen_ai.usage.cache_creation.input_tokens",
    "gen_ai.usage.cache_creation_input_tokens",
}

_PROMPT_PAYLOAD_KEYS = {
    "prompt",
    "prompt.text",
    "user.prompt",
    "user_prompt",
    "quest",
    "transcript",
    "message.content",
    "input_text",
    "output_text",
}


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _has_session_id(attrs: dict[str, Any]) -> bool:
    return _has_value(attrs.get("session.id")) or _has_value(attrs.get("conversation.id"))


def _has_turn_id(attrs: dict[str, Any]) -> bool:
    return _has_value(attrs.get("turn.id")) or _has_value(attrs.get("turn_id"))


def _has_token_usage(item: dict[str, Any]) -> bool:
    usage = item.get("usage") or item.get("usageDetails") or {}
    if isinstance(usage, dict):
        usage_keys = (
            "input",
            "output",
            "total",
            "inputTokens",
            "outputTokens",
            "totalTokens",
        )
        if any(_number(usage.get(key)) > 0 for key in usage_keys):
            return True

    attrs = _attrs(item)
    return any(_number(attrs.get(key)) > 0 for key in _TOKEN_USAGE_KEYS)


def _has_prompt_payload(item: dict[str, Any]) -> bool:
    if _has_value(item.get("input")) or _has_value(item.get("output")):
        return True

    attrs = _attrs(item)
    for key, value in attrs.items():
        if key.lower() in _PROMPT_PAYLOAD_KEYS and _has_value(value):
            return True
    return False


def _has_duration(item: dict[str, Any]) -> bool:
    start = _parse_time(str(item.get("startTime") or item.get("start_time") or ""))
    end = _parse_time(str(item.get("endTime") or item.get("end_time") or ""))
    return start is not None and end is not None and end >= start


def _is_llm_request_candidate(item: dict[str, Any]) -> bool:
    attrs = _attrs(item)
    name = str(item.get("name") or "")
    return (
        name in _LLM_OBSERVATION_NAMES
        or str(item.get("type") or "").upper() == "GENERATION"
        and _has_value(attrs.get("model"))
        or _has_token_usage(item)
    )


def _is_tool_call_candidate(item: dict[str, Any]) -> bool:
    attrs = _attrs(item)
    name = str(item.get("name") or "")
    if name == "mcp.tools.call" and (
        _has_value(attrs.get("tool.name")) or _has_value(attrs.get("tool.call_id"))
    ):
        return True
    return _has_value(attrs.get("tool_name")) and _has_value(attrs.get("call_id"))


def _analysis_counts(item: dict[str, Any]) -> tuple[Counter[str], Counter[str]]:
    attrs = _attrs(item)
    fields: Counter[str] = Counter()
    vocabulary: Counter[str] = Counter()
    mapped_vocabulary: set[str] = set()

    if _has_session_id(attrs):
        fields["session_id_observations"] += 1
    if _has_turn_id(attrs):
        fields["turn_id_observations"] += 1
    if _is_llm_request_candidate(item):
        fields["llm_request_candidates"] += 1
        mapped_vocabulary.add("codex.llm_request")
    if _is_tool_call_candidate(item):
        fields["tool_call_candidates"] += 1
        mapped_vocabulary.add("codex.tool")
    if _has_token_usage(item):
        fields["token_usage_observations"] += 1
    if _has_prompt_payload(item):
        fields["prompt_payload_observations"] += 1
    if _has_duration(item):
        fields["duration_observations"] += 1

    name = str(item.get("name") or "")
    if name in {"codex.interaction", "codex.llm_request", "codex.tool"}:
        mapped_vocabulary.add(name)
    vocabulary.update(mapped_vocabulary)
    return fields, vocabulary


def fetch_codex_traces(
    creds: LangfuseCreds,
    *,
    pages: int,
    limit: int,
    since: datetime | None,
    include_smoke: bool,
) -> list[CodexTrace]:
    traces: list[CodexTrace] = []
    with httpx.Client(
        base_url=creds.url.rstrip("/"),
        auth=(creds.public_key, creds.secret_key),
        timeout=30.0,
    ) as client:
        for row in _fetch_recent_traces(client, pages=pages, limit=limit):
            if not _is_codex_trace(row):
                continue
            if not include_smoke and _is_smoke_trace(row):
                continue
            timestamp = _parse_time(_timestamp(row))
            if since is not None and (timestamp is None or timestamp < since):
                continue

            trace_id = str(row.get("id") or "")
            if not trace_id:
                continue
            full = _fetch_trace(client, trace_id)
            observations = full.get("observations") or []
            counts: Counter[str] = Counter()
            analysis_fields: Counter[str] = Counter()
            vocabulary_candidates: Counter[str] = Counter()
            agent_sources: Counter[str] = Counter()
            agent_families: Counter[str] = Counter()
            session_ids: Counter[str] = Counter()
            service_names: Counter[str] = Counter()
            for obs in observations:
                if not isinstance(obs, dict):
                    continue
                counts[str(obs.get("name") or "unknown")] += 1
                obs_fields, obs_vocabulary = _analysis_counts(obs)
                analysis_fields.update(obs_fields)
                vocabulary_candidates.update(obs_vocabulary)
                attrs = _attrs(obs)
                resource_attrs = _resource_attrs(obs)
                if attrs.get("langfuse.trace.metadata.agent_source"):
                    agent_sources[str(attrs["langfuse.trace.metadata.agent_source"])] += 1
                if attrs.get("langfuse.trace.metadata.agent_family"):
                    agent_families[str(attrs["langfuse.trace.metadata.agent_family"])] += 1
                if attrs.get("session.id"):
                    session_ids[str(attrs["session.id"])] += 1
                if resource_attrs.get("service.name"):
                    service_names[str(resource_attrs["service.name"])] += 1

            metadata = _metadata(row)
            attrs = metadata.get("attributes") or {}
            resource_attrs = _resource_attrs(row)
            traces.append(
                CodexTrace(
                    trace_id=trace_id,
                    name=str(row.get("name") or ""),
                    timestamp=_timestamp(row),
                    session_id=str(
                        attrs.get("session.id")
                        or (session_ids.most_common(1)[0][0] if session_ids else "unknown")
                    ),
                    service_name=str(
                        resource_attrs.get("service.name")
                        or (service_names.most_common(1)[0][0] if service_names else "unknown")
                    ),
                    agent_source=str(
                        metadata.get("agent_source")
                        or (agent_sources.most_common(1)[0][0] if agent_sources else "unknown")
                    ),
                    agent_family=str(
                        metadata.get("agent_family")
                        or (agent_families.most_common(1)[0][0] if agent_families else "unknown")
                    ),
                    observation_names=counts,
                    analysis_fields=analysis_fields,
                    vocabulary_candidates=vocabulary_candidates,
                )
            )
    return sorted(traces, key=lambda trace: trace.timestamp, reverse=True)


def format_summary(traces: list[CodexTrace], *, sample_limit: int = 5) -> str:
    observation_totals: Counter[str] = Counter()
    source_totals: Counter[str] = Counter()
    family_totals: Counter[str] = Counter()
    analysis_totals: Counter[str] = Counter()
    vocabulary_totals: Counter[str] = Counter()
    for trace in traces:
        observation_totals.update(trace.observation_names)
        analysis_totals.update(trace.analysis_fields)
        vocabulary_totals.update(trace.vocabulary_candidates)
        source_totals[trace.agent_source] += 1
        family_totals[trace.agent_family] += 1

    lines = [f"codex_traces={len(traces)}"]
    lines.append("agent_sources:")
    for source, count in source_totals.most_common():
        lines.append(f"  {count:5d}  {source}")
    lines.append("agent_families:")
    for family, count in family_totals.most_common():
        lines.append(f"  {count:5d}  {family}")
    lines.append("observation_names:")
    for name, count in observation_totals.most_common():
        lines.append(f"  {count:5d}  {name}")
    lines.append("analysis_field_presence:")
    for key in (
        "session_id_observations",
        "turn_id_observations",
        "llm_request_candidates",
        "tool_call_candidates",
        "token_usage_observations",
        "prompt_payload_observations",
        "duration_observations",
    ):
        lines.append(f"  {analysis_totals.get(key, 0):5d}  {key}")
    lines.append("backfill_vocabulary_candidates:")
    for key in ("codex.interaction", "codex.llm_request", "codex.tool"):
        lines.append(f"  {vocabulary_totals.get(key, 0):5d}  {key}")

    if traces:
        if (
            vocabulary_totals.get("codex.interaction", 0) == 0
            or analysis_totals.get("prompt_payload_observations", 0) == 0
        ):
            lines.append("session_analysis_assessment=partial_live_signal")
            lines.append(
                "session_analysis_recommendation=use_rollout_backfill_for_canonical_session_tool_llm_view"
            )
        else:
            lines.append("session_analysis_assessment=live_signal_may_be_sufficient")
        lines.append("latest_codex_samples:")
        for trace in traces[:sample_limit]:
            obs = ", ".join(
                f"{name}={count}" for name, count in trace.observation_names.most_common()
            )
            lines.append(
                "  "
                f"time={trace.timestamp} trace={trace.trace_id} name={trace.name} "
                f"session.id={trace.session_id} service.name={trace.service_name} "
                f"agent_source={trace.agent_source} agent_family={trace.agent_family} "
                f"observations=[{obs}]"
            )
        lines.append("codex_live_traces=present")
    else:
        lines.append("codex_live_traces=missing")
    return "\n".join(lines)


def run(
    creds: LangfuseCreds,
    *,
    pages: int,
    limit: int,
    since_minutes: int,
    include_smoke: bool,
) -> int:
    since = datetime.now(UTC) - timedelta(minutes=since_minutes) if since_minutes else None
    traces = fetch_codex_traces(
        creds,
        pages=pages,
        limit=limit,
        since=since,
        include_smoke=include_smoke,
    )
    print(format_summary(traces))
    return 0 if traces else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--langfuse-url", default="http://localhost:3000")
    parser.add_argument("--public-key", default=os.getenv("LANGFUSE_PUBLIC_KEY"))
    parser.add_argument("--secret-key", default=os.getenv("LANGFUSE_SECRET_KEY"))
    parser.add_argument("--env-file", type=Path, default=LANGFUSE_ENV)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--pages", type=int, default=5)
    parser.add_argument(
        "--since-minutes",
        type=int,
        default=60,
        help="Only count Codex traces newer than this many minutes. Use 0 for all scanned traces.",
    )
    parser.add_argument(
        "--include-smoke",
        action="store_true",
        help="Include synthetic codex-smoke traces in the result.",
    )
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
        since_minutes=args.since_minutes,
        include_smoke=args.include_smoke,
    )


if __name__ == "__main__":
    raise SystemExit(main())
