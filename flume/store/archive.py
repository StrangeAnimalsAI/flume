"""Durable raw archive: immutable copies of source session files.

The analyzed store can always be rebuilt; the raw files cannot — the agent
apps prune them on their own schedule. The archive captures each ingested
file as a gzip blob plus a manifest row, versioned by content hash: if a
session file grows and is re-ingested, a new version is captured and the
old one kept (retention decides how long).

Layout (filesystem backend, default `~/.flume/raw`):

    raw/
      manifest.sqlite3
      blobs/<source>/<YYYY-MM>/<session_id>.<sha8>.jsonl.gz

Pluggable like the session store: `open_archive(url)` with `file://<dir>`.
A future S3/remote backend implements the same interface.
"""
from __future__ import annotations

import gzip
import hashlib
import os
import re
import sqlite3
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ArchiveEntry:
    id: int
    source: str
    session_id: str
    sha256: str
    size_bytes: int
    blob_path: str
    original_path: str
    original_mtime_ns: int | None
    captured_at_ns: int


class RawArchive(ABC):
    """Write-once raw file archive with content-hash versioning."""

    @abstractmethod
    def capture(
        self,
        source: str,
        session_id: str,
        path: Path,
        *,
        mtime_ns: int | None = None,
        data: bytes | None = None,
    ) -> ArchiveEntry | None:
        """Store a copy of `path` (or of `data`, when the caller already
        read the file — one snapshot for capture, hash, and parse).
        Returns None if this exact content (source, session_id, sha256)
        is already archived."""

    @abstractmethod
    def versions(self, session_id: str) -> list[ArchiveEntry]:
        """All archived versions of a session, oldest first."""

    @abstractmethod
    def restore(self, entry: ArchiveEntry, dest: Path) -> Path:
        """Decompress a blob to `dest` and return it."""

    @abstractmethod
    def expired(self, source: str, before_ns: int) -> list[ArchiveEntry]:
        """Entries for `source` captured before the cutoff."""

    @abstractmethod
    def delete(self, entry: ArchiveEntry) -> None:
        """Remove a blob and its manifest row."""

    @abstractmethod
    def stats(self) -> list[dict[str, Any]]:
        """Per-source blob counts and byte totals."""

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> "RawArchive":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


DEFAULT_ARCHIVE_URL = "file://~/.flume/raw"


def open_archive(url: str | None = None) -> RawArchive:
    resolved = url or DEFAULT_ARCHIVE_URL
    if resolved.startswith("file://"):
        return FsRawArchive(resolved[len("file://") :])
    raise ValueError(f"unsupported archive url {resolved!r}; expected file://<dir>")


_MANIFEST_SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    blob_path TEXT NOT NULL,
    original_path TEXT NOT NULL,
    original_mtime_ns INTEGER,
    captured_at_ns INTEGER NOT NULL,
    UNIQUE (source, session_id, sha256)
);
CREATE INDEX IF NOT EXISTS idx_blobs_session ON blobs (session_id);
CREATE INDEX IF NOT EXISTS idx_blobs_source_captured ON blobs (source, captured_at_ns);
"""


def _fs_name(value: str) -> str:
    """Filesystem-safe component: ids come from transcript CONTENT, which is
    untrusted — a crafted session id must not become a path traversal."""
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", value)
    safe = re.sub(r"\.{2,}", "_", safe).lstrip(".")
    return safe or "_"


class FsRawArchive(RawArchive):
    def __init__(self, root: str | Path) -> None:
        self.root = Path(str(root)).expanduser()
        self.blob_root = self.root / "blobs"
        self.blob_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        manifest = self.root / "manifest.sqlite3"
        self._conn = sqlite3.connect(str(manifest))
        os.chmod(manifest, 0o600)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_MANIFEST_SCHEMA)

    def capture(
        self,
        source: str,
        session_id: str,
        path: Path,
        *,
        mtime_ns: int | None = None,
        data: bytes | None = None,
    ) -> ArchiveEntry | None:
        """Capture one immutable copy. Pass `data` when the caller already
        read the file so capture, sha, and parse share one snapshot."""
        if data is None:
            data = path.read_bytes()
        sha = hashlib.sha256(data).hexdigest()
        existing = self._conn.execute(
            "SELECT id, blob_path FROM blobs "
            "WHERE source=? AND session_id=? AND sha256=?",
            (source, session_id, sha),
        ).fetchone()
        if existing:
            # Manifest hit is only trustworthy if the blob is really there;
            # an interrupted delete must not permanently block recapture.
            blob = self.blob_root / existing["blob_path"]
            if not blob.is_file():
                blob.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                with open(blob, "wb") as fh:
                    with gzip.GzipFile(fileobj=fh, mode="wb", mtime=0) as gz:
                        gz.write(data)
                os.chmod(blob, 0o600)
            return None

        captured_ns = time.time_ns()
        month = datetime.fromtimestamp(
            captured_ns / 1e9, tz=timezone.utc
        ).strftime("%Y-%m")
        rel = (
            Path(_fs_name(source)) / month / f"{_fs_name(session_id)}.{sha[:8]}.jsonl.gz"
        )
        blob = self.blob_root / rel
        if not blob.resolve().is_relative_to(self.blob_root.resolve()):
            raise ValueError(f"blob path escapes archive root: {rel}")
        blob.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # mtime=0 keeps the gzip output deterministic for identical input.
        with open(blob, "wb") as fh:
            with gzip.GzipFile(fileobj=fh, mode="wb", mtime=0) as gz:
                gz.write(data)
        os.chmod(blob, 0o600)

        with self._conn:
            cur = self._conn.execute(
                """
                INSERT INTO blobs (source, session_id, sha256, size_bytes,
                    blob_path, original_path, original_mtime_ns, captured_at_ns)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source,
                    session_id,
                    sha,
                    len(data),
                    str(rel),
                    str(path),
                    mtime_ns,
                    captured_ns,
                ),
            )
        return self._entry_by_id(cur.lastrowid)

    def versions(self, session_id: str) -> list[ArchiveEntry]:
        rows = self._conn.execute(
            "SELECT * FROM blobs WHERE session_id=? ORDER BY captured_at_ns",
            (session_id,),
        ).fetchall()
        return [_entry(row) for row in rows]

    def restore(self, entry: ArchiveEntry, dest: Path) -> Path:
        blob = self.blob_root / entry.blob_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(blob, "rb") as gz:
            data = gz.read()
        sha = hashlib.sha256(data).hexdigest()
        if sha != entry.sha256:
            raise ValueError(
                f"archive blob corrupt for {entry.session_id}: "
                f"sha256 {sha[:12]}... != manifest {entry.sha256[:12]}..."
            )
        dest.write_bytes(data)
        return dest

    def expired(self, source: str, before_ns: int) -> list[ArchiveEntry]:
        rows = self._conn.execute(
            "SELECT * FROM blobs WHERE source=? AND captured_at_ns < ?",
            (source, before_ns),
        ).fetchall()
        return [_entry(row) for row in rows]

    def delete(self, entry: ArchiveEntry) -> None:
        blob = self.blob_root / entry.blob_path
        blob.unlink(missing_ok=True)
        with self._conn:
            self._conn.execute("DELETE FROM blobs WHERE id=?", (entry.id,))

    def stats(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT source, COUNT(*) AS blobs,
                   COUNT(DISTINCT session_id) AS sessions,
                   SUM(size_bytes) AS raw_bytes,
                   MIN(captured_at_ns) AS oldest_ns,
                   MAX(captured_at_ns) AS newest_ns
            FROM blobs GROUP BY source ORDER BY source
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self._conn.close()

    def _entry_by_id(self, row_id: int | None) -> ArchiveEntry:
        row = self._conn.execute(
            "SELECT * FROM blobs WHERE id=?", (row_id,)
        ).fetchone()
        return _entry(row)


def _entry(row: sqlite3.Row) -> ArchiveEntry:
    return ArchiveEntry(
        id=row["id"],
        source=row["source"],
        session_id=row["session_id"],
        sha256=row["sha256"],
        size_bytes=row["size_bytes"],
        blob_path=row["blob_path"],
        original_path=row["original_path"],
        original_mtime_ns=row["original_mtime_ns"],
        captured_at_ns=row["captured_at_ns"],
    )
