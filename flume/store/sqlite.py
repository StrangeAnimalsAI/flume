"""SQLite implementation of AnalyzedStore.

One file, no server, FTS5 full-text search over thinking/messages/tool
payloads when available (falls back to LIKE otherwise). Re-ingesting a
session deletes and rewrites its rows inside one transaction, so updated
transcripts actually update rather than being deduped by span id with
stale fields (INT-455).
"""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from flume.store.base import ContentRow, SessionBundle, AnalyzedStore

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    trace_id TEXT,
    source TEXT,
    surface TEXT,
    cwd TEXT,
    project TEXT,
    is_subagent INTEGER DEFAULT 0,
    parent_session_id TEXT,
    git_branch TEXT,
    model TEXT,
    version TEXT,
    started_at_ns INTEGER,
    ended_at_ns INTEGER,
    wall_ms INTEGER,
    active_ms INTEGER,
    turn_count INTEGER,
    tool_call_count INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_creation_tokens INTEGER,
    reasoning_tokens INTEGER,
    thinking_blocks INTEGER,
    thinking_chars INTEGER,
    first_user_message TEXT,
    file_path TEXT,
    raw_sha256 TEXT,
    pipeline_version INTEGER,
    ingested_at_ns INTEGER,
    metadata TEXT,
    experiment TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_source_started
    ON sessions (source, started_at_ns);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions (parent_session_id);

CREATE TABLE IF NOT EXISTS turns (
    span_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    turn_index INTEGER,
    model TEXT,
    started_at_ns INTEGER,
    ended_at_ns INTEGER,
    duration_ms INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_creation_tokens INTEGER,
    reasoning_tokens INTEGER,
    thinking_chars INTEGER,
    text_chars INTEGER
);
CREATE INDEX IF NOT EXISTS idx_turns_session ON turns (session_id, started_at_ns);

CREATE TABLE IF NOT EXISTS tool_calls (
    span_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    turn_span_id TEXT,
    name TEXT,
    args_hash TEXT,
    args_preview TEXT,
    started_at_ns INTEGER,
    ended_at_ns INTEGER,
    duration_ms INTEGER,
    is_error INTEGER,
    result_chars INTEGER
);
CREATE INDEX IF NOT EXISTS idx_tools_session ON tool_calls (session_id, started_at_ns);
CREATE INDEX IF NOT EXISTS idx_tools_name ON tool_calls (name);
-- Analytic indexes: exploratory queries rank by result size / duration and
-- group by (session, name, args) for dedup. Without these, each such query
-- full-scans the whole tool_calls table.
CREATE INDEX IF NOT EXISTS idx_tools_name_chars ON tool_calls (name, result_chars);
CREATE INDEX IF NOT EXISTS idx_tools_name_dur ON tool_calls (name, duration_ms);
CREATE INDEX IF NOT EXISTS idx_tools_chars ON tool_calls (result_chars);
CREATE INDEX IF NOT EXISTS idx_tools_dedup ON tool_calls (session_id, name, args_hash);

CREATE TABLE IF NOT EXISTS experiments (
    name TEXT PRIMARY KEY,
    hypothesis TEXT,
    source TEXT,
    project TEXT,
    started_at_ns INTEGER NOT NULL,
    ended_at_ns INTEGER,
    created_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS findings (
    kind TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    severity INTEGER NOT NULL,
    title TEXT NOT NULL,
    detail TEXT,
    metric REAL,
    unit TEXT,
    action TEXT,
    evidence TEXT,
    first_seen_ns INTEGER NOT NULL,
    last_seen_ns INTEGER NOT NULL,
    occurrences INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (kind, fingerprint)
);

CREATE TABLE IF NOT EXISTS contents (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL,
    span_id TEXT,
    kind TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ts_ns INTEGER,
    text TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_contents_session ON contents (session_id, seq);
CREATE INDEX IF NOT EXISTS idx_contents_kind ON contents (kind);
CREATE INDEX IF NOT EXISTS idx_contents_span ON contents (span_id);
"""

_FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS contents_fts USING fts5(
    text,
    content='contents',
    content_rowid='id'
);
CREATE TRIGGER IF NOT EXISTS contents_ai AFTER INSERT ON contents BEGIN
    INSERT INTO contents_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS contents_ad AFTER DELETE ON contents BEGIN
    INSERT INTO contents_fts(contents_fts, rowid, text)
        VALUES ('delete', old.id, old.text);
END;
"""


class SqliteAnalyzedStore(AnalyzedStore):
    def __init__(self, path: str | Path, *, readonly: bool = False) -> None:
        db_path = Path(str(path)).expanduser()
        if readonly:
            # A reader must be a pure reader: no migrations, no view
            # recreation, no FTS DDL — GETs must not take write locks or
            # mutate schema. Requires a database a writer already created.
            self._conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            self._conn.row_factory = sqlite3.Row
            self._fts = bool(
                self._conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='contents_fts'"
                ).fetchone()
            )
            return
        if str(db_path) != ":memory:":
            db_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        if str(db_path) != ":memory:":
            # Full-fidelity transcripts: private regardless of umask. Set
            # before WAL/SHM exist so the sidecars inherit the mode.
            os.chmod(db_path, 0o600)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._ensure_views()
        self._fts = self._init_fts()

    def _ensure_views(self) -> None:
        from flume.store.taxonomy import view_sql

        # Recreate so a taxonomy change takes effect on next open.
        with self._conn:
            self._conn.execute("DROP VIEW IF EXISTS tool_calls_ext")
            self._conn.execute(view_sql())

    def _init_fts(self) -> bool:
        try:
            self._conn.executescript(_FTS_SCHEMA)
            return True
        except sqlite3.OperationalError:
            return False

    # -- write ------------------------------------------------------------

    def ingest_session(self, bundle: SessionBundle) -> None:
        session_id = bundle.session["session_id"]
        session = dict(bundle.session)
        # session_id is the sole primary key; until it is source-qualified,
        # refuse to let one source silently overwrite another's session.
        existing = self._conn.execute(
            "SELECT source FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if (
            existing
            and existing["source"]
            and session.get("source")
            and existing["source"] != session["source"]
        ):
            raise ValueError(
                f"session id collision: {session_id!r} already ingested from "
                f"source {existing['source']!r}; refusing to overwrite with "
                f"{session['source']!r}"
            )
        session["experiment"] = self._experiment_tags(
            session.get("source"),
            session.get("project"),
            session.get("started_at_ns"),
        )
        with self._conn:
            self._delete_session_rows(session_id)
            self._insert("sessions", session)
            for turn in bundle.turns:
                self._insert("turns", {**turn, "session_id": session_id})
            for tool in bundle.tool_calls:
                self._insert("tool_calls", {**tool, "session_id": session_id})
            for row in bundle.contents:
                self._insert("contents", _content_dict(session_id, row))

    def _delete_session_rows(self, session_id: str) -> None:
        for table in ("contents", "tool_calls", "turns", "sessions"):
            self._conn.execute(
                f"DELETE FROM {table} WHERE session_id = ?", (session_id,)
            )

    def _insert(self, table: str, row: dict[str, Any]) -> None:
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        self._conn.execute(
            f"INSERT INTO {table} ({cols}) VALUES ({marks})",
            tuple(row.values()),
        )

    # -- read -------------------------------------------------------------

    def overview(self) -> dict[str, Any]:
        from flume.store.bundle import PIPELINE_VERSION

        totals = self.row(
            """
            SELECT COUNT(*) AS sessions,
                   COALESCE(SUM(pipeline_version < ?), 0) AS stale_sessions,
                   COALESCE(SUM(turn_count), 0) AS turns,
                   COALESCE(SUM(tool_call_count), 0) AS tool_calls,
                   COALESCE(SUM(thinking_blocks), 0) AS thinking_blocks,
                   COALESCE(SUM(input_tokens), 0) AS input_tokens,
                   COALESCE(SUM(output_tokens), 0) AS output_tokens,
                   COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
                   COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
                   COALESCE(SUM(wall_ms), 0) AS wall_ms,
                   COALESCE(SUM(active_ms), 0) AS active_ms,
                   MIN(started_at_ns) AS first_started_at_ns,
                   MAX(ended_at_ns) AS last_ended_at_ns
            FROM sessions
            """,
            (PIPELINE_VERSION,),
        )
        by_source = self.rows(
            """
            SELECT source, COUNT(*) AS sessions,
                   COALESCE(SUM(turn_count), 0) AS turns,
                   COALESCE(SUM(tool_call_count), 0) AS tool_calls,
                   COALESCE(SUM(thinking_blocks), 0) AS thinking_blocks,
                   COALESCE(SUM(output_tokens), 0) AS output_tokens
            FROM sessions GROUP BY source ORDER BY sessions DESC
            """
        )
        return {"totals": totals, "by_source": by_source}

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
        where, params = _filters(
            source=source, surface=surface, cwd_like=cwd_like, since_ns=since_ns
        )
        conditions = []
        if top_level_only:
            conditions.append("is_subagent = 0")
        if project:
            conditions.append("project = ?")
            params = (*params, project)
        if conditions:
            extra = " AND ".join(conditions)
            where = f"{where} AND {extra}" if where else f"WHERE {extra}"
        return self.rows(
            f"""
            SELECT s.*, (
                SELECT COUNT(*) FROM sessions c
                WHERE c.parent_session_id = s.session_id
            ) AS children
            FROM sessions s {where}
            ORDER BY started_at_ns DESC LIMIT ?
            """,
            (*params, limit),
        )

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        session = self.row(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        )
        if not session or session.get("session_id") is None:
            return None
        session["turns"] = self.rows(
            "SELECT * FROM turns WHERE session_id = ? ORDER BY started_at_ns",
            (session_id,),
        )
        session["tool_calls"] = self.rows(
            "SELECT * FROM tool_calls WHERE session_id = ? ORDER BY started_at_ns",
            (session_id,),
        )
        session["children"] = self.rows(
            """
            SELECT * FROM sessions WHERE parent_session_id = ?
            ORDER BY started_at_ns
            """,
            (session_id,),
        )
        session["family"] = _family_rollup(session, session["children"])
        return session

    def get_contents(
        self,
        session_id: str,
        *,
        kinds: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM contents WHERE session_id = ?"
        params: list[Any] = [session_id]
        if kinds:
            sql += f" AND kind IN ({', '.join('?' for _ in kinds)})"
            params.extend(kinds)
        sql += " ORDER BY seq"
        return self.rows(sql, tuple(params))

    def tool_stats(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        slowest: int = 10,
        largest: int = 10,
    ) -> dict[str, Any]:
        join = "JOIN sessions s ON s.session_id = t.session_id"
        where, params = _filters(
            source=source, since_ns=since_ns, prefix="s."
        )
        per_tool = self.rows(
            f"""
            SELECT t.name, COUNT(*) AS calls,
                   SUM(t.is_error) AS errors,
                   COALESCE(SUM(t.duration_ms), 0) AS total_ms,
                   COALESCE(AVG(t.duration_ms), 0) AS avg_ms,
                   MAX(t.duration_ms) AS max_ms,
                   COALESCE(SUM(t.result_chars), 0) AS total_result_chars
            FROM tool_calls t {join} {where}
            GROUP BY t.name ORDER BY calls DESC
            """,
            params,
        )
        repeats = self.rows(
            f"""
            SELECT t.session_id, t.name, t.args_preview, COUNT(*) AS calls
            FROM tool_calls t {join} {where}
            GROUP BY t.session_id, t.name, t.args_hash
            HAVING COUNT(*) > 1
            ORDER BY calls DESC LIMIT 25
            """,
            params,
        )
        slowest_rows = self.rows(
            f"""
            SELECT t.session_id, t.name, t.args_preview, t.duration_ms, t.is_error
            FROM tool_calls t {join} {where}
            ORDER BY t.duration_ms DESC LIMIT ?
            """,
            (*params, slowest),
        )
        largest_rows = self.rows(
            f"""
            SELECT t.session_id, t.name, t.args_preview, t.result_chars
            FROM tool_calls t {join} {where}
            ORDER BY t.result_chars DESC LIMIT ?
            """,
            (*params, largest),
        )
        return {
            "per_tool": per_tool,
            "repeated_calls": repeats,
            "slowest": slowest_rows,
            "largest_results": largest_rows,
        }

    def token_stats(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        group_by: str = "source",
    ) -> list[dict[str, Any]]:
        column = {
            "source": "source",
            "surface": "surface",
            "model": "model",
            "session": "session_id",
        }.get(group_by)
        if column is None:
            raise ValueError(f"unsupported group_by {group_by!r}")
        where, params = _filters(source=source, since_ns=since_ns)
        return self.rows(
            f"""
            SELECT {column} AS grp, COUNT(*) AS sessions,
                   COALESCE(SUM(input_tokens), 0) AS input_tokens,
                   COALESCE(SUM(output_tokens), 0) AS output_tokens,
                   COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
                   COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
                   COALESCE(SUM(reasoning_tokens), 0) AS reasoning_tokens,
                   CASE WHEN SUM(input_tokens) + SUM(cache_read_tokens) > 0
                        THEN ROUND(1.0 * SUM(cache_read_tokens)
                             / (SUM(input_tokens) + SUM(cache_read_tokens)), 4)
                        ELSE 0 END AS cache_hit_ratio
            FROM sessions {where}
            GROUP BY {column} ORDER BY output_tokens DESC
            """,
            params,
        )

    def search(
        self,
        query: str,
        *,
        kinds: list[str] | None = None,
        source: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if self._fts:
            base = (
                "SELECT c.session_id, c.span_id, c.kind, c.seq, c.ts_ns, "
                "snippet(contents_fts, 0, '[', ']', ' … ', 24) AS snippet, "
                "s.source, s.cwd "
                "FROM contents_fts "
                "JOIN contents c ON c.id = contents_fts.rowid "
                "JOIN sessions s ON s.session_id = c.session_id "
            )
            conditions.append("contents_fts MATCH ?")
            params.append(query)
        else:
            base = (
                "SELECT c.session_id, c.span_id, c.kind, c.seq, c.ts_ns, "
                "substr(c.text, 1, 240) AS snippet, s.source, s.cwd "
                "FROM contents c "
                "JOIN sessions s ON s.session_id = c.session_id "
            )
            conditions.append("c.text LIKE ?")
            params.append(f"%{query}%")
        if kinds:
            conditions.append(f"c.kind IN ({', '.join('?' for _ in kinds)})")
            params.extend(kinds)
        if source:
            conditions.append("s.source = ?")
            params.append(source)
        sql = base + "WHERE " + " AND ".join(conditions) + " LIMIT ?"
        params.append(limit)
        return self.rows(sql, tuple(params))

    def audit_repeats(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        where, params = _filters(source=source, since_ns=since_ns, prefix="s.")
        # Two-phase: group on tool_calls only (no text), then check
        # byte-identity with targeted contents lookups for the returned
        # groups. A single joined GROUP BY over result text reads GBs.
        groups = self.rows(
            f"""
            SELECT t.session_id, s.source, t.name, t.args_hash,
                   MIN(t.args_preview) AS args_preview,
                   COUNT(*) AS calls,
                   COALESCE(SUM(t.duration_ms), 0) AS total_ms
            FROM tool_calls t
            JOIN sessions s ON s.session_id = t.session_id
            {where}
            GROUP BY t.session_id, t.name, t.args_hash
            HAVING COUNT(*) > 1
            ORDER BY calls DESC LIMIT ?
            """,
            (*params, limit),
        )
        for group in groups:
            # INDEXED BY pins both lookups: without it the planner drifts to
            # the kind/name indexes and scans every tool_result text in the
            # table — minutes instead of milliseconds on a multi-GB store.
            row = self.row(
                """
                SELECT COUNT(c.text) AS results,
                       COUNT(DISTINCT c.text) AS distinct_results
                FROM contents c INDEXED BY idx_contents_span
                WHERE c.kind = 'tool_result' AND c.span_id IN (
                    SELECT span_id FROM tool_calls INDEXED BY idx_tools_session
                    WHERE session_id = ? AND name = ? AND args_hash = ?
                )
                """,
                (group["session_id"], group["name"], group["args_hash"]),
            ) or {}
            results = row.get("results") or 0
            distinct = row.get("distinct_results") or 0
            group["distinct_results"] = distinct
            group["byte_identical"] = bool(results > 1 and distinct == 1)
            group.pop("args_hash", None)
        return groups

    def audit_whole_file_reads(
        self,
        *,
        source: str | None = None,
        since_ns: int | None = None,
        min_chars: int = 50_000,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        where, params = _filters(source=source, since_ns=since_ns, prefix="s.")
        clause = "WHERE" if not where else f"{where} AND"
        return self.rows(
            f"""
            SELECT t.session_id, s.source, t.args_preview, t.result_chars,
                   t.duration_ms, t.started_at_ns
            FROM tool_calls t JOIN sessions s ON s.session_id = t.session_id
            {clause} t.name = 'Read' AND t.result_chars >= ?
                AND t.args_preview NOT LIKE '%offset%'
                AND t.args_preview NOT LIKE '%limit%'
            ORDER BY t.result_chars DESC LIMIT ?
            """,
            (*params, min_chars, limit),
        )

    def tool_argument_rows(
        self,
        *,
        tool_names: list[str],
        source: str | None = None,
        since_ns: int | None = None,
        like: str | None = None,
    ) -> list[dict[str, Any]]:
        marks = ", ".join("?" for _ in tool_names)
        sql = (
            "SELECT c.session_id, c.text "
            "FROM contents c "
            "JOIN tool_calls t ON t.span_id = c.span_id "
            "JOIN sessions s ON s.session_id = c.session_id "
            f"WHERE c.kind = 'tool_arguments' AND t.name IN ({marks})"
        )
        params: list[Any] = list(tool_names)
        if source is not None:
            sql += " AND s.source = ?"
            params.append(source)
        if since_ns is not None:
            sql += " AND s.started_at_ns >= ?"
            params.append(since_ns)
        if like:
            sql += " AND c.text LIKE ?"
            params.append(like)
        return self.rows(sql, tuple(params))

    # -- experiments --------------------------------------------------------
    #
    # An experiment is a named time window, optionally scoped to a source
    # and/or project. Sessions whose start falls inside an experiment's
    # window (and match its scope) carry its name in sessions.experiment —
    # comma-joined when windows overlap. Tags are recomputed from the
    # experiments table alone, so retagging is idempotent and re-ingesting
    # a session never loses its tag.

    def create_experiment(
        self,
        name: str,
        *,
        hypothesis: str | None = None,
        source: str | None = None,
        project: str | None = None,
        started_at_ns: int | None = None,
        ended_at_ns: int | None = None,
    ) -> dict[str, Any]:

        now = time.time_ns()
        row = {
            "name": name,
            "hypothesis": hypothesis,
            "source": source,
            "project": project,
            "started_at_ns": started_at_ns or now,
            "ended_at_ns": ended_at_ns,
            "created_at_ns": now,
        }
        with self._conn:
            self._insert("experiments", row)
        self.retag_experiments()
        return row

    def end_experiment(self, name: str, ended_at_ns: int | None = None) -> dict[str, Any]:

        experiment = self.get_experiment(name)
        if experiment is None:
            raise KeyError(f"no experiment named {name!r}")
        with self._conn:
            self._conn.execute(
                "UPDATE experiments SET ended_at_ns = ? WHERE name = ?",
                (ended_at_ns or time.time_ns(), name),
            )
        self.retag_experiments()
        return self.get_experiment(name)  # type: ignore[return-value]

    def get_experiment(self, name: str) -> dict[str, Any] | None:
        return self.row("SELECT * FROM experiments WHERE name = ?", (name,))

    def list_experiments(self) -> list[dict[str, Any]]:
        return self.rows(
            """
            SELECT e.*, (
                SELECT COUNT(*) FROM sessions s
                WHERE s.is_subagent = 0
                    AND ',' || COALESCE(s.experiment, '') || ',' LIKE '%,' || e.name || ',%'
            ) AS sessions
            FROM experiments e ORDER BY e.started_at_ns DESC
            """
        )

    def experiment_session_ids(self, name: str) -> list[str]:
        return [
            row["session_id"]
            for row in self.rows(
                """
                SELECT session_id FROM sessions
                WHERE is_subagent = 0
                    AND ',' || COALESCE(experiment, '') || ',' LIKE ?
                ORDER BY started_at_ns
                """,
                (f"%,{name},%",),
            )
        ]

    def retag_experiments(self) -> int:
        """Recompute sessions.experiment for every session. Returns rows changed."""
        changed = 0
        rows = self.rows(
            "SELECT session_id, source, project, started_at_ns, experiment FROM sessions"
        )
        with self._conn:
            for row in rows:
                tags = self._experiment_tags(
                    row["source"], row["project"], row["started_at_ns"]
                )
                if tags != row["experiment"]:
                    self._conn.execute(
                        "UPDATE sessions SET experiment = ? WHERE session_id = ?",
                        (tags, row["session_id"]),
                    )
                    changed += 1
        return changed

    def _experiment_tags(
        self,
        source: str | None,
        project: str | None,
        started_at_ns: int | None,
    ) -> str | None:
        if started_at_ns is None:
            return None
        matched = [
            row["name"]
            for row in self.rows(
                "SELECT name, source, project, started_at_ns, ended_at_ns "
                "FROM experiments ORDER BY name"
            )
            if started_at_ns >= row["started_at_ns"]
            and (row["ended_at_ns"] is None or started_at_ns <= row["ended_at_ns"])
            and (row["source"] is None or row["source"] == source)
            and (row["project"] is None or row["project"] == project)
        ]
        return ",".join(matched) if matched else None

    def upsert_findings(self, findings: list[dict[str, Any]]) -> None:

        now = time.time_ns()
        with self._conn:
            for f in findings:
                self._conn.execute(
                    """
                    INSERT INTO findings (kind, fingerprint, severity, title,
                        detail, metric, unit, action, evidence,
                        first_seen_ns, last_seen_ns, occurrences)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT (kind, fingerprint) DO UPDATE SET
                        severity = excluded.severity,
                        title = excluded.title,
                        detail = excluded.detail,
                        metric = excluded.metric,
                        unit = excluded.unit,
                        action = excluded.action,
                        evidence = excluded.evidence,
                        last_seen_ns = excluded.last_seen_ns,
                        occurrences = occurrences + 1
                    """,
                    (
                        f["kind"], f["fingerprint"], f["severity"], f["title"],
                        f.get("detail"), f.get("metric"), f.get("unit"),
                        f.get("action"), f.get("evidence"), now, now,
                    ),
                )

    def list_findings(
        self,
        *,
        kind: str | None = None,
        active_within_ns: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        conditions, params = [], []
        if kind:
            conditions.append("kind = ?")
            params.append(kind)
        if active_within_ns is not None:

            conditions.append("last_seen_ns >= ?")
            params.append(time.time_ns() - active_within_ns)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return self.rows(
            f"""
            SELECT * FROM findings {where}
            ORDER BY severity, metric DESC LIMIT ?
            """,
            (*params, limit),
        )

    def stale_sessions(
        self,
        current_version: int,
        *,
        source: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT session_id, source, file_path, raw_sha256,
                   pipeline_version, metadata
            FROM sessions
            WHERE pipeline_version < ?
        """
        params: list[Any] = [current_version]
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY started_at_ns DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self.rows(sql, tuple(params))

    def prune_sessions(
        self,
        *,
        source: str,
        before_ns: int,
        dry_run: bool = False,
    ) -> list[str]:
        rows = self.rows(
            "SELECT session_id FROM sessions WHERE source = ? AND ended_at_ns < ?",
            (source, before_ns),
        )
        session_ids = [row["session_id"] for row in rows]
        if dry_run:
            return session_ids
        with self._conn:
            for session_id in session_ids:
                self._delete_session_rows(session_id)
        return session_ids

    def close(self) -> None:
        self._conn.close()

    # -- helpers ----------------------------------------------------------

    # -- SqlReadable ------------------------------------------------------
    #
    # Public because the analysis layer depends on them by name (see
    # flume.store.base.SqlReadable for why analytics declare a SQL
    # capability instead of growing AnalyzedStore).

    def rows(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self._conn.execute(sql, params).fetchall()]

    def row(self, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        found = self._conn.execute(sql, params).fetchone()
        return dict(found) if found is not None else None


# SQLite caps a single string around 1 GB; a content row anywhere near that
# is pathological (the raw store keeps the true bytes regardless).
_CONTENT_TEXT_MAX = 50_000_000


_ROLLUP_KEYS = (
    "turn_count",
    "tool_call_count",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "reasoning_tokens",
    "thinking_blocks",
    "active_ms",
    "wall_ms",
)


def _family_rollup(
    session: dict[str, Any],
    children: list[dict[str, Any]],
) -> dict[str, Any]:
    rollup = {key: session.get(key) or 0 for key in _ROLLUP_KEYS}
    for child in children:
        for key in _ROLLUP_KEYS:
            rollup[key] += child.get(key) or 0
    rollup["sessions"] = 1 + len(children)
    return rollup


def _content_dict(session_id: str, row: ContentRow) -> dict[str, Any]:
    text = row.text
    if len(text) > _CONTENT_TEXT_MAX:
        omitted = len(text) - _CONTENT_TEXT_MAX
        text = (
            text[:_CONTENT_TEXT_MAX]
            + f"\n...[store cap: {omitted} chars omitted; full text in raw store]"
        )
    return {
        "session_id": session_id,
        "span_id": row.span_id,
        "kind": row.kind,
        "seq": row.seq,
        "ts_ns": row.ts_ns,
        "text": text,
    }


def _filters(
    *,
    source: str | None = None,
    surface: str | None = None,
    cwd_like: str | None = None,
    since_ns: int | None = None,
    prefix: str = "",
) -> tuple[str, tuple]:
    conditions: list[str] = []
    params: list[Any] = []
    if source:
        conditions.append(f"{prefix}source = ?")
        params.append(source)
    if surface:
        conditions.append(f"{prefix}surface = ?")
        params.append(surface)
    if cwd_like:
        conditions.append(f"{prefix}cwd LIKE ?")
        params.append(f"%{cwd_like}%")
    if since_ns is not None:
        conditions.append(f"{prefix}started_at_ns >= ?")
        params.append(since_ns)
    if not conditions:
        return "", ()
    return "WHERE " + " AND ".join(conditions), tuple(params)
