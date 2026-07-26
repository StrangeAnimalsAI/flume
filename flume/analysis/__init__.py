"""Query-side analysis over the session store: insight detectors,
navigation-time attribution, experiment comparison, audit heuristics,
hook-event joins, the analyze CLI, and the web viewer.

Backend coupling, stated plainly: the CLI and server run on the portable
`AnalyzedStore` interface, while detectors, navigation-time attribution and
experiment comparison additionally require `SqlReadable` — a store that
answers ad-hoc SQL. That requirement is declared, not smuggled: each entry
point calls `require_sql` and fails with a message naming the feature.

The alternative — promoting every detector aggregation to a `AnalyzedStore`
method — would roughly double the interface with single-caller queries and
make a second backend harder to write. See `flume.store.base.SqlReadable`.
"""
from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    """Entry point for `flume analyze`."""
    from flume.analysis.cli import main as _main

    return _main(argv)


def serve(argv: list[str] | None = None) -> int:
    """Entry point for `flume serve`."""
    from flume.analysis.server import main as _main

    return _main(argv)


__all__ = ["main", "serve"]
