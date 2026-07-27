"""Local HTTP API + web UI over the session store.

    flume serve                # http://localhost:8321
    flume serve --port 9000

Read-only throughout: every route opens its own SQLite connection in readonly
mode, so a GET can neither migrate the schema nor take a write lock while an
ingest is running.

The UI at / is a static file plus vendored assets — no build step. The JSON
endpoints under /api are the same data the CLI exposes, and FastAPI publishes
their schema at /api/docs so an agent can discover them rather than guess.
"""
from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from flume.store.base import AnalyzedStore, open_analyzed_store

_STATIC_DIR = Path(__file__).parent / "static"

# Set by create_app; the store URL is process-wide, not per-request.
_STORE_URL: str | None = None


def _get_store() -> Iterator[AnalyzedStore]:
    """One readonly connection per request — sqlite handles are cheap.

    Readonly is load-bearing, not just tidy: it keeps a browser refresh from
    running migrations or blocking a concurrent `flume analyze ingest`.
    """
    with open_analyzed_store(_STORE_URL, readonly=True) as store:
        yield store


Store = Annotated[AnalyzedStore, Depends(_get_store)]


def _since_ns(window: str | None) -> int | None:
    """Bad or missing query windows are ignored, not errors."""
    if not window:
        return None
    from flume.store.config import parse_duration_ns

    try:
        ttl_ns = parse_duration_ns(window)
    except ValueError:
        return None
    if ttl_ns is None:
        return None
    return time.time_ns() - ttl_ns


def create_app(store_url: str | None = None) -> FastAPI:
    global _STORE_URL
    _STORE_URL = store_url

    app = FastAPI(
        title="flume",
        description="Local-first telemetry for coding agents.",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        redoc_url=None,
    )

    # Deliberately NO CORSMiddleware: transcripts contain prompts, thinking,
    # and tool output. No browser origin may read them cross-origin, even from
    # localhost. Adding CORS here would hand every page you have open a reader
    # for your entire session history.

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(_STATIC_DIR / "index.html")

    @app.get("/api/overview")
    def overview(store: Store) -> Any:
        return store.overview()

    @app.get("/api/sessions")
    def list_sessions(
        store: Store,
        source: str | None = None,
        surface: str | None = None,
        cwd: str | None = None,
        project: str | None = None,
        since: str | None = None,
        top_level: bool = False,
        limit: Annotated[int, Query(ge=1, le=1000)] = 50,
    ) -> Any:
        return store.list_sessions(
            source=source,
            surface=surface,
            cwd_like=cwd,
            project=project,
            since_ns=_since_ns(since),
            top_level_only=top_level,
            limit=limit,
        )

    @app.get("/api/sessions/{session_id}")
    def get_session(store: Store, session_id: str) -> Any:
        session = store.get_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="session not found")
        return session

    @app.get("/api/sessions/{session_id}/contents")
    def session_contents(
        store: Store,
        session_id: str,
        kind: Annotated[list[str] | None, Query()] = None,
    ) -> Any:
        return store.get_contents(session_id, kinds=kind)

    @app.get("/api/sessions/{session_id}/commands")
    def session_commands(store: Store, session_id: str) -> Any:
        return store.session_commands(session_id)

    @app.get("/api/tools")
    def tools(
        store: Store,
        source: str | None = None,
        since: str | None = None,
        top: Annotated[int, Query(ge=1, le=200)] = 10,
    ) -> Any:
        return store.tool_stats(
            source=source, since_ns=_since_ns(since), slowest=top, largest=top
        )

    @app.get("/api/tokens")
    def tokens(
        store: Store,
        source: str | None = None,
        since: str | None = None,
        group_by: str = "source",
    ) -> Any:
        return store.token_stats(
            source=source, since_ns=_since_ns(since), group_by=group_by
        )

    @app.get("/api/audit/repeats")
    def audit_repeats(
        store: Store,
        source: str | None = None,
        since: str | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> Any:
        return store.audit_repeats(
            source=source, since_ns=_since_ns(since), limit=limit
        )

    @app.get("/api/audit/whole_file_reads")
    def audit_whole_file_reads(
        store: Store,
        source: str | None = None,
        since: str | None = None,
        min_chars: int = 50_000,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> Any:
        return store.audit_whole_file_reads(
            source=source,
            since_ns=_since_ns(since),
            min_chars=min_chars,
            limit=limit,
        )

    @app.get("/api/audit/toolgaps")
    def audit_toolgaps(
        store: Store, since: str | None = None, min_sessions: int = 3
    ) -> Any:
        from flume.analysis.audit import script_clusters

        return script_clusters(
            store, since_ns=_since_ns(since), min_sessions=min_sessions
        )

    @app.get("/api/insights")
    def insights(
        store: Store,
        since: str = "7d",
        stored: bool = False,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> Any:
        if stored:
            return store.list_findings(limit=limit)
        from flume.analysis.insights import run_insights

        # A GET must not write: refreshing the page shouldn't bump finding
        # occurrence counts (persistence happens in the CLI).
        return run_insights(store, since_ns=_since_ns(since), persist=False)

    @app.get("/api/raw_store")
    def raw_store() -> Any:
        from flume.store.config import load_policy
        from flume.store.raw import open_raw_store

        with open_raw_store(None) as raw:
            return {"stats": raw.stats(), "retention": load_policy().describe()}

    @app.get("/api/search")
    def search(
        store: Store,
        q: str | None = None,
        kind: Annotated[list[str] | None, Query()] = None,
        source: str | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> Any:
        if not q:
            return []
        return store.search(q, kinds=kind, source=source, limit=limit)

    # Mounted last so it cannot shadow an /api route. StaticFiles resolves and
    # confines paths itself, which is why assets are served through it rather
    # than a hand-rolled handler on a server whose whole job is exposing
    # transcripts.
    #
    # Mount only if it exists — never mkdir here. create_app() must not write
    # into the package directory, which is read-only under a system install.
    assets = _STATIC_DIR / "assets"
    if assets.is_dir():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    return app
