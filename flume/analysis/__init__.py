"""Query-side analysis over the session store: insight detectors,
navigation-time attribution, experiment comparison, audit heuristics,
hook-event joins, the analyze CLI, and the web viewer.

Backend coupling, stated plainly: the CLI and server run on the portable
`SessionStore` interface, while detectors, navigation-time attribution and
experiment comparison additionally require `SqlReadable` — a store that
answers ad-hoc SQL. That requirement is declared, not smuggled: each entry
point calls `require_sql` and fails with a message naming the feature.

The alternative — promoting every detector aggregation to a `SessionStore`
method — would roughly double the interface with single-caller queries and
make a second backend harder to write. See `flume.store.base.SqlReadable`.
"""
