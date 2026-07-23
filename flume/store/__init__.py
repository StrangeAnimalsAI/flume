"""Pluggable session store — the system of record for agent sessions.

The engine: schema and storage interface (`base`), the sqlite backend
(`sqlite`), normal-form-to-rows assembly (`bundle`), the immutable raw
archive (`archive`), and retention (`retention`, `config`). Source
formats live in `flume.sources`; the write path in `flume.ingest`;
query-side analytics and the web viewer in `flume.analysis`. This
package imports none of them — swap the backend via `open_store(url)`
and nothing upstream changes.
"""
from flume.store.base import SessionStore, open_store

__all__ = ["SessionStore", "open_store"]
