"""Durable sqlite state for transcript auto-ingest."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from agent_telemetry.ingest.fingerprint import FileFingerprint
from agent_telemetry.ingest.types import DiscoveredTranscript


class IngestStatus(StrEnum):
    PENDING = "pending"
    ACTIVE = "active"
    INGESTING = "ingesting"
    INGESTED = "ingested"
    FAILED = "failed"


@dataclass(frozen=True)
class IngestRecord:
    source_type: str
    path: str
    fingerprint: str
    sha256: str
    size_bytes: int
    mtime_ns: int
    session_id: str | None
    trace_id: str | None
    metadata: dict[str, Any]
    first_seen_at: str
    last_seen_at: str
    last_ingested_at: str | None
    status: IngestStatus
    error: str | None
    attempts: int
    updated_at: str

    @property
    def path_obj(self) -> Path:
        return Path(self.path)


class SqliteIngestStateStore:
    """Tiny sqlite-backed checkpoint store.

    Rows are keyed by source type and absolute path. Re-ingest decisions use
    the content fingerprint, so touching a file without changing bytes does
    not create duplicate Langfuse writes.
    """

    def __init__(self, path: Path | str) -> None:
        if str(path) == ":memory:":
            self.path = Path(":memory:")
            sqlite_path = ":memory:"
        else:
            self.path = Path(path).expanduser().resolve(strict=False)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            sqlite_path = str(self.path)
        self._conn = sqlite3.connect(sqlite_path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "SqliteIngestStateStore":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def get(self, source_type: str, path: Path | str) -> IngestRecord | None:
        row = self._conn.execute(
            """
            SELECT * FROM transcript_ingest_state
            WHERE source_type = ? AND path = ?
            """,
            (source_type, _normal_path(path)),
        ).fetchone()
        return _record(row) if row else None

    def list_records(self) -> list[IngestRecord]:
        rows = self._conn.execute(
            """
            SELECT * FROM transcript_ingest_state
            ORDER BY source_type, path
            """
        ).fetchall()
        return [_record(row) for row in rows]

    def observe(
        self,
        transcript: DiscoveredTranscript,
        fingerprint: FileFingerprint,
        *,
        now: float,
    ) -> IngestRecord:
        """Record discovery and reset changed files to pending."""
        path = _normal_path(transcript.path)
        ts = _iso(now)
        existing = self.get(transcript.source_type, path)
        if existing is None:
            self._conn.execute(
                """
                INSERT INTO transcript_ingest_state (
                    source_type, path, fingerprint, sha256, size_bytes, mtime_ns,
                    session_id, trace_id, metadata_json, first_seen_at, last_seen_at,
                    last_ingested_at, status, error, attempts, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, 0, ?)
                """,
                (
                    transcript.source_type,
                    path,
                    fingerprint.identity,
                    fingerprint.sha256,
                    fingerprint.size_bytes,
                    fingerprint.mtime_ns,
                    transcript.session_id,
                    transcript.trace_id,
                    _json_metadata(dict(transcript.metadata)),
                    ts,
                    ts,
                    IngestStatus.PENDING.value,
                    ts,
                ),
            )
        elif existing.fingerprint != fingerprint.identity:
            self._conn.execute(
                """
                UPDATE transcript_ingest_state
                SET fingerprint = ?, sha256 = ?, size_bytes = ?, mtime_ns = ?,
                    session_id = ?, trace_id = ?, metadata_json = ?, last_seen_at = ?,
                    status = ?, error = NULL, attempts = 0, updated_at = ?
                WHERE source_type = ? AND path = ?
                """,
                (
                    fingerprint.identity,
                    fingerprint.sha256,
                    fingerprint.size_bytes,
                    fingerprint.mtime_ns,
                    transcript.session_id,
                    transcript.trace_id,
                    _json_metadata(dict(transcript.metadata)),
                    ts,
                    IngestStatus.PENDING.value,
                    ts,
                    transcript.source_type,
                    path,
                ),
            )
        else:
            self._conn.execute(
                """
                UPDATE transcript_ingest_state
                SET sha256 = ?, size_bytes = ?, mtime_ns = ?,
                    session_id = COALESCE(session_id, ?),
                    trace_id = COALESCE(trace_id, ?),
                    metadata_json = ?, last_seen_at = ?, updated_at = ?
                WHERE source_type = ? AND path = ?
                """,
                (
                    fingerprint.sha256,
                    fingerprint.size_bytes,
                    fingerprint.mtime_ns,
                    transcript.session_id,
                    transcript.trace_id,
                    _json_metadata(dict(transcript.metadata)),
                    ts,
                    ts,
                    transcript.source_type,
                    path,
                ),
            )
        self._conn.commit()
        record = self.get(transcript.source_type, path)
        if record is None:  # pragma: no cover - guarded by schema constraints.
            raise RuntimeError("failed to read observed ingest record")
        return record

    def mark_active(self, record: IngestRecord, *, now: float, reason: str) -> IngestRecord:
        return self._update_status(record, IngestStatus.ACTIVE, now=now, error=reason)

    def mark_ingesting(self, record: IngestRecord, *, now: float) -> IngestRecord:
        ts = _iso(now)
        self._conn.execute(
            """
            UPDATE transcript_ingest_state
            SET status = ?, error = NULL, attempts = attempts + 1, updated_at = ?
            WHERE source_type = ? AND path = ?
            """,
            (IngestStatus.INGESTING.value, ts, record.source_type, record.path),
        )
        self._conn.commit()
        updated = self.get(record.source_type, record.path)
        if updated is None:  # pragma: no cover
            raise RuntimeError("failed to read ingesting record")
        return updated

    def mark_ingested(
        self,
        record: IngestRecord,
        *,
        now: float,
        session_id: str | None = None,
        trace_id: str | None = None,
    ) -> IngestRecord:
        ts = _iso(now)
        self._conn.execute(
            """
            UPDATE transcript_ingest_state
            SET status = ?, error = NULL, last_ingested_at = ?,
                session_id = COALESCE(?, session_id),
                trace_id = COALESCE(?, trace_id),
                updated_at = ?
            WHERE source_type = ? AND path = ?
            """,
            (
                IngestStatus.INGESTED.value,
                ts,
                session_id,
                trace_id,
                ts,
                record.source_type,
                record.path,
            ),
        )
        self._conn.commit()
        updated = self.get(record.source_type, record.path)
        if updated is None:  # pragma: no cover
            raise RuntimeError("failed to read ingested record")
        return updated

    def mark_failed(self, record: IngestRecord, *, now: float, error: str) -> IngestRecord:
        return self._update_status(record, IngestStatus.FAILED, now=now, error=error)

    def _update_status(
        self,
        record: IngestRecord,
        status: IngestStatus,
        *,
        now: float,
        error: str | None,
    ) -> IngestRecord:
        ts = _iso(now)
        self._conn.execute(
            """
            UPDATE transcript_ingest_state
            SET status = ?, error = ?, updated_at = ?
            WHERE source_type = ? AND path = ?
            """,
            (status.value, error, ts, record.source_type, record.path),
        )
        self._conn.commit()
        updated = self.get(record.source_type, record.path)
        if updated is None:  # pragma: no cover
            raise RuntimeError("failed to read updated ingest record")
        return updated

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_ingest_state (
                source_type TEXT NOT NULL,
                path TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                session_id TEXT,
                trace_id TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_ingested_at TEXT,
                status TEXT NOT NULL,
                error TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_type, path)
            )
            """
        )
        columns = {
            row["name"]
            for row in self._conn.execute(
                "PRAGMA table_info(transcript_ingest_state)"
            ).fetchall()
        }
        if "metadata_json" not in columns:
            self._conn.execute(
                """
                ALTER TABLE transcript_ingest_state
                ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'
                """
            )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS transcript_ingest_state_status_idx
            ON transcript_ingest_state (status)
            """
        )
        self._conn.commit()


def _normal_path(path: Path | str) -> str:
    return str(Path(path).expanduser().resolve(strict=False))


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _json_metadata(metadata: dict[str, Any]) -> str:
    return json.dumps(metadata, sort_keys=True)


def _metadata(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return obj if isinstance(obj, dict) else {}


def _record(row: sqlite3.Row) -> IngestRecord:
    return IngestRecord(
        source_type=row["source_type"],
        path=row["path"],
        fingerprint=row["fingerprint"],
        sha256=row["sha256"],
        size_bytes=row["size_bytes"],
        mtime_ns=row["mtime_ns"],
        session_id=row["session_id"],
        trace_id=row["trace_id"],
        metadata=_metadata(row["metadata_json"]),
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        last_ingested_at=row["last_ingested_at"],
        status=IngestStatus(row["status"]),
        error=row["error"],
        attempts=row["attempts"],
        updated_at=row["updated_at"],
    )
