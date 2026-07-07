"""Storage interface for agent session data.

A `SessionStore` persists one `SessionBundle` per session file and answers
the analysis queries the CLI/API expose. Implementations are pluggable via
`open_store(url)`; the default is sqlite. A new backend (Postgres, DuckDB,
a remote API) implements the abstract methods and registers a URL scheme.

Content rows are the full-fidelity layer: thinking blocks, complete tool
arguments/results, and message texts that the OTel/Langfuse path truncates
or redacts. They reference the same deterministic span ids the mappers
emit, so the metrics skeleton and the full text always join cleanly.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

CONTENT_KINDS = (
    "thinking",
    "user_message",
    "assistant_message",
    "tool_arguments",
    "tool_result",
)


@dataclass(frozen=True)
class ContentRow:
    """One full-fidelity text item tied to a span."""

    span_id: str | None
    kind: str  # one of CONTENT_KINDS
    seq: int
    text: str
    ts_ns: int | None = None


@dataclass(frozen=True)
class SessionBundle:
    """Everything the store persists for one session, ready to write."""

    session: dict[str, Any]
    turns: list[dict[str, Any]]
    tool_calls: list[dict[str, Any]]
    contents: list[ContentRow] = field(default_factory=list)


class SessionStore(ABC):
    """Write + query interface every backend implements."""

    @abstractmethod
    def ingest_session(self, bundle: SessionBundle) -> None:
        """Persist a session, replacing any prior rows for its session_id."""

    @abstractmethod
    def overview(self) -> dict[str, Any]:
        """Corpus-level counts and totals, grouped by source."""

    @abstractmethod
    def list_sessions(
        self,
        *,
        source: str | None = None,
        surface: str | None = None,
        cwd_like: str | None = None,
        project: str | None = None,
        since_ns: int | None = None,
        top_level_only: bool = False,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Session rows, newest first, each with a `children` count."""

    @abstractmethod
    def get_session(self, session_id: str) -> dict[str, Any] | None:
        """One session row with turns, tool calls, child sessions, and a
        `family` rollup (this session + direct children), or None."""

    def session_commands(self, session_id: str) -> list[dict[str, Any]]:
        """Per-prompt breakdown: each user prompt with the turns, tool
        calls, tokens, and wall time it triggered (until the next prompt).

        Backend-agnostic default built on get_session/get_contents."""
        session = self.get_session(session_id)
        if session is None:
            return []
        prompts: list[dict[str, Any]] = []
        last_ts = None
        for row in self.get_contents(session_id, kinds=["user_message"]):
            text = (row.get("text") or "").strip()
            ts = row.get("ts_ns")
            if not text or ts is None or text.startswith("<"):
                continue  # harness/system payloads are not user commands
            if last_ts is not None and ts == last_ts:
                continue  # multi-block prompt: keep first block only
            prompts.append({"ts_ns": ts, "text": text})
            last_ts = ts
        commands: list[dict[str, Any]] = []
        for index, prompt in enumerate(prompts):
            start = prompt["ts_ns"]
            end = (
                prompts[index + 1]["ts_ns"]
                if index + 1 < len(prompts)
                else None
            )

            def _within(row: dict[str, Any]) -> bool:
                ts = row.get("started_at_ns") or 0
                return ts >= start and (end is None or ts < end)

            turns = [t for t in session["turns"] if _within(t)]
            tools = [t for t in session["tool_calls"] if _within(t)]
            ended = max(
                [start, *(t.get("ended_at_ns") or 0 for t in turns)],
            )
            commands.append(
                {
                    "prompt": prompt["text"][:300],
                    "started_at_ns": start,
                    "duration_ms": max(0, (ended - start) // 1_000_000),
                    "turns": len(turns),
                    "tool_calls": len(tools),
                    "input_tokens": sum(t["input_tokens"] or 0 for t in turns),
                    "output_tokens": sum(t["output_tokens"] or 0 for t in turns),
                    "cache_read_tokens": sum(
                        t["cache_read_tokens"] or 0 for t in turns
                    ),
                    "thinking_chars": sum(
                        t["thinking_chars"] or 0 for t in turns
                    ),
                }
            )
        return commands

    @abstractmethod
    def get_contents(
        self,
        session_id: str,
        *,
        kinds: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Full-fidelity content rows for a session, in transcript order."""

    @abstractmethod
    def tool_stats(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        slowest: int = 10,
        largest: int = 10,
    ) -> dict[str, Any]:
        """Per-tool aggregates plus repeated-call, slowest-N, largest-N rollups."""

    @abstractmethod
    def token_stats(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        group_by: str = "source",
    ) -> list[dict[str, Any]]:
        """Token/cache economy grouped by source, surface, model, or session."""

    @abstractmethod
    def search(
        self,
        query: str,
        *,
        kinds: list[str] | None = None,
        source: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Full-text search across thinking, messages, and tool payloads."""

    @abstractmethod
    def audit_repeats(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Identical tool calls repeated within one session, worst first.
        Rows include `byte_identical`: True when every stored result for the
        group is the same text — provably zero-information re-work."""

    @abstractmethod
    def audit_whole_file_reads(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        min_chars: int = 50_000,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Unranged Read calls returning at least `min_chars` characters."""

    @abstractmethod
    def tool_argument_rows(
        self,
        *,
        tool_names: list[str],
        since_ns: int | None = None,
        like: str | None = None,
    ) -> list[dict[str, Any]]:
        """(session_id, text) rows of full tool arguments, for offline
        clustering (e.g. throwaway-script detection)."""

    @abstractmethod
    def upsert_findings(self, findings: list[dict[str, Any]]) -> None:
        """Persist insight findings. Rows are keyed by (kind, fingerprint):
        a recurring finding updates last_seen/occurrences/metric on the
        existing row instead of duplicating, so trends stay visible."""

    @abstractmethod
    def list_findings(
        self,
        *,
        kind: str | None = None,
        active_within_ns: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Stored findings, most severe first, then by metric."""

    @abstractmethod
    def prune_sessions(
        self,
        *,
        source: str,
        before_ns: int,
        dry_run: bool = False,
    ) -> list[str]:
        """Delete sessions (and their rows) that ended before the cutoff.
        Returns the affected session ids; with dry_run, deletes nothing."""

    @abstractmethod
    def close(self) -> None:
        """Release resources."""

    def __enter__(self) -> "SessionStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


DEFAULT_STORE_URL = "sqlite://~/.agent-telemetry/store.sqlite3"


def open_store(url: str | None = None) -> SessionStore:
    """Open a store by URL. `sqlite://<path>` is the only built-in scheme."""
    resolved = url or DEFAULT_STORE_URL
    if resolved.startswith("sqlite://"):
        from agent_telemetry.store.sqlite import SqliteSessionStore

        return SqliteSessionStore(resolved[len("sqlite://") :])
    raise ValueError(
        f"unsupported store url {resolved!r}; expected sqlite://<path>"
    )
