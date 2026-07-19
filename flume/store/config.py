"""Retention configuration for the raw archive and analyzed store.

Read from `~/.flume/config.toml` (override path with the
FLUME_CONFIG env var). Durations are "forever" or `<N><unit>`
with unit h/d/w. Example:

    [retention]
    raw = "forever"        # default for raw blobs
    analyzed = "forever"   # default for analyzed sessions

    [retention.raw_overrides]
    codex = "30d"          # per-source override

    [retention.analyzed_overrides]
    codex = "90d"

Missing file or missing keys mean "forever" — nothing is deleted unless
explicitly configured.
"""
from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG_PATH = Path.home() / ".flume" / "config.toml"

_FOREVER = None  # sentinel: no expiry


@dataclass(frozen=True)
class RetentionPolicy:
    raw_default_ns: int | None = _FOREVER
    analyzed_default_ns: int | None = _FOREVER
    raw_overrides_ns: dict[str, int | None] = field(default_factory=dict)
    analyzed_overrides_ns: dict[str, int | None] = field(default_factory=dict)

    def raw_ttl_ns(self, source: str) -> int | None:
        return self.raw_overrides_ns.get(source, self.raw_default_ns)

    def analyzed_ttl_ns(self, source: str) -> int | None:
        return self.analyzed_overrides_ns.get(source, self.analyzed_default_ns)

    def describe(self) -> dict[str, object]:
        return {
            "raw": _fmt(self.raw_default_ns),
            "analyzed": _fmt(self.analyzed_default_ns),
            "raw_overrides": {k: _fmt(v) for k, v in self.raw_overrides_ns.items()},
            "analyzed_overrides": {
                k: _fmt(v) for k, v in self.analyzed_overrides_ns.items()
            },
        }


def load_policy(path: Path | None = None) -> RetentionPolicy:
    resolved = path or Path(
        os.environ.get("FLUME_CONFIG", str(DEFAULT_CONFIG_PATH))
    )
    if not resolved.is_file():
        return RetentionPolicy()
    with open(resolved, "rb") as fh:
        doc = tomllib.load(fh)
    section = doc.get("retention") or {}
    return RetentionPolicy(
        raw_default_ns=parse_duration_ns(section.get("raw", "forever")),
        analyzed_default_ns=parse_duration_ns(section.get("analyzed", "forever")),
        raw_overrides_ns={
            k: parse_duration_ns(v)
            for k, v in (section.get("raw_overrides") or {}).items()
        },
        analyzed_overrides_ns={
            k: parse_duration_ns(v)
            for k, v in (section.get("analyzed_overrides") or {}).items()
        },
    )


def parse_duration_ns(value: str) -> int | None:
    """"forever" -> None; "30d"/"12h"/"2w" -> nanoseconds."""
    text = str(value).strip().lower()
    if text in ("forever", "keep", "infinite", ""):
        return _FOREVER
    match = re.fullmatch(r"(\d+)([hdw])", text)
    if not match:
        raise ValueError(
            f"bad retention duration {value!r}; use 'forever' or <N>h/<N>d/<N>w"
        )
    n, unit = int(match.group(1)), match.group(2)
    seconds = n * {"h": 3600, "d": 86400, "w": 604800}[unit]
    return seconds * 1_000_000_000


def _fmt(ttl_ns: int | None) -> str:
    if ttl_ns is None:
        return "forever"
    days = ttl_ns / 86_400_000_000_000
    if days >= 7 and days % 7 == 0:
        return f"{int(days // 7)}w"
    if days >= 1:
        return f"{days:g}d"
    return f"{ttl_ns / 3_600_000_000_000:g}h"
