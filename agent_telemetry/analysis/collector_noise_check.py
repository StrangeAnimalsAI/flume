"""Check recent Langfuse traces for Docker/buildx collector noise.

Usage:
    uv run python -m agent_telemetry.analysis.collector_noise_check \
        [--langfuse-url http://localhost:3000] [--limit 100] [--pages 5]

Credentials default to `infra/langfuse/.env` in this repo, matching the
parity checker. Exit code is 0 when no known Docker/buildx noise signatures
are present and 1 when any are found.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
LANGFUSE_ENV = REPO_ROOT / "infra" / "langfuse" / ".env"

NOISE_NAME_RE = re.compile(r"^/?moby\.(buildkit|filesync|auth)\.")
NOISE_SERVICE_RE = re.compile(r"^(docker|buildx|buildkit|moby)($|[._-])")


@dataclass(frozen=True)
class LangfuseCreds:
    url: str
    public_key: str
    secret_key: str


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


def _attrs(item: dict[str, Any]) -> dict[str, Any]:
    metadata = item.get("metadata") or {}
    attrs = metadata.get("attributes") or {}
    return attrs if isinstance(attrs, dict) else {}


def _resource_attrs(item: dict[str, Any]) -> dict[str, Any]:
    metadata = item.get("metadata") or {}
    resource = metadata.get("resourceAttributes") or {}
    return resource if isinstance(resource, dict) else {}


def _source(trace: dict[str, Any], observations: list[dict[str, Any]]) -> str:
    metadata = trace.get("metadata") or {}
    candidates = [
        metadata.get("agent_source"),
        _resource_attrs(trace).get("source"),
    ]
    for obs in observations:
        attrs = _attrs(obs)
        candidates.extend(
            [
                attrs.get("langfuse.trace.metadata.agent_source"),
                _resource_attrs(obs).get("source"),
            ]
        )
    return next((str(v) for v in candidates if v), "unknown")


def _service(trace: dict[str, Any], obs: dict[str, Any]) -> str:
    attrs = _attrs(obs)
    candidates = [
        attrs.get("service.name"),
        attrs.get("resource.service.name"),
        _resource_attrs(obs).get("service.name"),
        _resource_attrs(trace).get("service.name"),
    ]
    return next((str(v) for v in candidates if v), "unknown")


def _is_noise(name: str, service: str) -> bool:
    return bool(
        NOISE_NAME_RE.search(name)
        or NOISE_SERVICE_RE.search(service)
        or (name == "build" and NOISE_SERVICE_RE.search(service))
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


def run(creds: LangfuseCreds, *, pages: int, limit: int) -> int:
    summary: Counter[tuple[str, str]] = Counter()
    noisy: Counter[tuple[str, str, str]] = Counter()
    trace_count = 0
    observation_count = 0

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
                observations = []

            trace_count += 1
            observation_count += len(observations)
            source = _source(trace, observations)

            if not observations:
                service = _resource_attrs(trace).get("service.name") or "unknown"
                name = str(trace.get("name") or "")
                summary[(source, str(service))] += 1
                if _is_noise(name, str(service)):
                    noisy[(source, str(service), name)] += 1
                continue

            for obs in observations:
                name = str(obs.get("name") or "")
                service = _service(trace, obs)
                summary[(source, service)] += 1
                if _is_noise(name, service):
                    noisy[(source, service, name)] += 1

    print(f"traces_checked={trace_count} observations_checked={observation_count}")
    print("summary_by_source_service:")
    for (source, service), count in summary.most_common():
        print(f"  {count:5d}  source={source} service.name={service}")

    if not noisy:
        print("docker_buildx_noise=0")
        return 0

    print("docker_buildx_noise:")
    for (source, service, name), count in noisy.most_common():
        print(f"  {count:5d}  source={source} service.name={service} span={name}")
    return 1


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
