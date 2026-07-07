"""Retention cycle: enforce raw/analyzed TTLs from RetentionPolicy.

Raw blobs expire on `captured_at_ns`; analyzed sessions on `ended_at_ns`.
"forever" tiers are never touched. The cycle is idempotent and safe to run
from cron/launchd, the auto-ingest loop, or by hand (`retention run`).
"""
from __future__ import annotations

import time
from typing import Any

from agent_telemetry.store.archive import RawArchive
from agent_telemetry.store.base import SessionStore
from agent_telemetry.store.config import RetentionPolicy
from agent_telemetry.store.registry import adapters


def run_retention(
    *,
    store: SessionStore,
    archive: RawArchive,
    policy: RetentionPolicy,
    now_ns: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    now = time.time_ns() if now_ns is None else now_ns
    report: dict[str, Any] = {"dry_run": dry_run, "raw": {}, "analyzed": {}}

    for adapter in adapters():
        source = adapter.name

        raw_ttl = policy.raw_ttl_ns(source)
        if raw_ttl is None:
            report["raw"][source] = {"ttl": "forever", "deleted": 0}
        else:
            expired = archive.expired(source, now - raw_ttl)
            if not dry_run:
                for entry in expired:
                    archive.delete(entry)
            report["raw"][source] = {
                "ttl_ns": raw_ttl,
                "deleted": len(expired),
                "freed_bytes": sum(e.size_bytes for e in expired),
            }

        analyzed_ttl = policy.analyzed_ttl_ns(source)
        if analyzed_ttl is None:
            report["analyzed"][source] = {"ttl": "forever", "deleted": 0}
        else:
            session_ids = store.prune_sessions(
                source=source,
                before_ns=now - analyzed_ttl,
                dry_run=dry_run,
            )
            report["analyzed"][source] = {
                "ttl_ns": analyzed_ttl,
                "deleted": len(session_ids),
            }

    return report
