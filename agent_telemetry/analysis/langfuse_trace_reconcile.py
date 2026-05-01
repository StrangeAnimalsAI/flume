"""Delete one local Langfuse trace so deterministic backfill can recreate it.

This is intentionally narrow: it targets local development reconciliation after
a mapper bug fix, where Langfuse kept first-ingested observation fields for a
deterministic trace/span ID. It does not re-ingest by itself; delete the stale
trace here, then rerun the appropriate backfill command.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
LANGFUSE_ENV = REPO_ROOT / "infra" / "langfuse" / ".env"
TRACE_ID_RE = re.compile(r"^[0-9a-fA-F]{32}$")


@dataclass(frozen=True)
class LangfuseCreds:
    url: str
    public_key: str
    secret_key: str


@dataclass(frozen=True)
class TraceSummary:
    trace_id: str
    name: str
    timestamp: str
    observations: int


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


def _is_local_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.hostname in {"localhost", "127.0.0.1", "::1", "langfuse-web"}


def _summarize_trace(trace: dict[str, Any]) -> TraceSummary:
    observations = trace.get("observations") or []
    return TraceSummary(
        trace_id=str(trace.get("id") or ""),
        name=str(trace.get("name") or "unknown"),
        timestamp=str(trace.get("timestamp") or trace.get("createdAt") or "unknown"),
        observations=len(observations) if isinstance(observations, list) else 0,
    )


def fetch_trace(client: httpx.Client, trace_id: str) -> dict[str, Any] | None:
    response = client.get(f"/api/public/traces/{trace_id}")
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def delete_trace(client: httpx.Client, trace_id: str) -> None:
    response = client.delete(f"/api/public/traces/{trace_id}")
    response.raise_for_status()


def delete_clickhouse_trace(trace_id: str, *, container: str) -> None:
    if not TRACE_ID_RE.fullmatch(trace_id):
        raise ValueError("trace_id must be a 32-character hex string")
    query = (
        "ALTER TABLE default.observations "
        f"DELETE WHERE trace_id='{trace_id}' SETTINGS mutations_sync=1; "
        "ALTER TABLE default.traces "
        f"DELETE WHERE id='{trace_id}' SETTINGS mutations_sync=1;"
    )
    subprocess.run(
        [
            "docker",
            "exec",
            container,
            "clickhouse-client",
            "--multiquery",
            "--query",
            query,
        ],
        check=True,
        text=True,
        capture_output=True,
    )


def wait_until_missing(
    client: httpx.Client,
    trace_id: str,
    *,
    attempts: int = 20,
    interval_s: float = 0.5,
) -> bool:
    for _ in range(attempts):
        if fetch_trace(client, trace_id) is None:
            return True
        time.sleep(interval_s)
    return False


def reconcile_trace(
    creds: LangfuseCreds,
    trace_id: str,
    *,
    confirm: bool,
    clickhouse_container: str,
) -> int:
    with httpx.Client(
        base_url=creds.url.rstrip("/"),
        auth=(creds.public_key, creds.secret_key),
        timeout=30.0,
    ) as client:
        trace = fetch_trace(client, trace_id)
        if trace is None:
            print(f"trace={trace_id} status=missing")
            return 1

        summary = _summarize_trace(trace)
        print(
            "trace_found "
            f"trace={summary.trace_id} name={summary.name} "
            f"timestamp={summary.timestamp} observations={summary.observations}"
        )

        if not confirm:
            print("dry_run=true")
            print("pass --yes to delete exactly this trace from local Langfuse")
            return 0

        delete_trace(client, trace_id)
        try:
            delete_clickhouse_trace(trace_id, container=clickhouse_container)
        except (ValueError, subprocess.CalledProcessError) as exc:
            print(
                f"trace={trace_id} clickhouse_delete_status=failed error={exc}",
                file=sys.stderr,
            )
            return 1

        if not wait_until_missing(client, trace_id):
            print(
                f"trace={trace_id} delete_status=failed_still_present_after_wait",
                file=sys.stderr,
            )
            return 1

        print(f"trace={trace_id} delete_status=deleted verify_missing=true")
        print("next_step=rerun the deterministic backfill for the original source file")
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m agent_telemetry.analysis.langfuse_trace_reconcile",
        description=__doc__,
    )
    parser.add_argument("trace_id", help="Langfuse trace ID to delete and recreate.")
    parser.add_argument(
        "--langfuse-url",
        default=os.getenv("LANGFUSE_URL", "http://localhost:3000"),
    )
    parser.add_argument("--public-key", default=os.getenv("LANGFUSE_PUBLIC_KEY"))
    parser.add_argument("--secret-key", default=os.getenv("LANGFUSE_SECRET_KEY"))
    parser.add_argument("--env-file", type=Path, default=LANGFUSE_ENV)
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete the trace. Without this, only prints the trace summary.",
    )
    parser.add_argument(
        "--allow-non-localhost",
        action="store_true",
        help="Allow deletion against a non-local Langfuse URL.",
    )
    parser.add_argument(
        "--clickhouse-container",
        default="langfuse-clickhouse-1",
        help="Local Langfuse ClickHouse container name (default: %(default)s).",
    )
    args = parser.parse_args(argv)

    if not _is_local_url(args.langfuse_url) and not args.allow_non_localhost:
        print(
            "refusing non-local Langfuse URL without --allow-non-localhost",
            file=sys.stderr,
        )
        return 2

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

    return reconcile_trace(
        LangfuseCreds(args.langfuse_url, public_key, secret_key),
        args.trace_id,
        confirm=args.yes,
        clickhouse_container=args.clickhouse_container,
    )


if __name__ == "__main__":
    raise SystemExit(main())
