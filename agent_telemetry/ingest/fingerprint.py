"""File fingerprinting and quiet-file detection for transcript ingest."""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileFingerprint:
    """Content identity plus stat data captured during one file read."""

    size_bytes: int
    mtime_ns: int
    sha256: str
    stable: bool

    @property
    def identity(self) -> str:
        """Stable re-ingest key. Mtime is intentionally excluded."""
        return f"sha256:{self.sha256}:size:{self.size_bytes}"

    @property
    def mtime_seconds(self) -> float:
        return self.mtime_ns / 1_000_000_000


def fingerprint_file(path: Path, *, chunk_size: int = 1024 * 1024) -> FileFingerprint:
    """Hash a file and report whether it changed while being read."""
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    after = path.stat()
    stable = (
        before.st_size == after.st_size
        and before.st_mtime_ns == after.st_mtime_ns
    )
    return FileFingerprint(
        size_bytes=after.st_size,
        mtime_ns=after.st_mtime_ns,
        sha256=digest.hexdigest(),
        stable=stable,
    )


def is_quiet(
    fingerprint: FileFingerprint,
    *,
    quiet_seconds: float,
    now: float | None = None,
) -> bool:
    """Return true when a file has been unchanged for the quiet window."""
    if quiet_seconds < 0:
        raise ValueError("quiet_seconds must be >= 0")
    current = time.time() if now is None else now
    return fingerprint.stable and current - fingerprint.mtime_seconds >= quiet_seconds
