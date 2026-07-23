"""Pluggable local session store — the system of record for agent sessions.

The backfill mappers stay the source of truth for structure and metrics;
this package adds a storage backend that keeps FULL session fidelity
(thinking blocks, untruncated tool payloads) and a query surface for
audits — CLI for agents, HTTP API + web UI for humans.
"""
from flume.store.base import SessionStore, open_store

__all__ = ["SessionStore", "open_store"]
