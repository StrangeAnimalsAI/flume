"""Query-side analysis over the session store: insight detectors,
navigation-time attribution, experiment comparison, audit heuristics,
hook-event joins, the analyze CLI, and the web viewer.

Honesty note on backend coupling: the CLI and server consume the public
`SessionStore` interface, but several detectors (insights, navtime,
experiments) still reach into the sqlite backend's private row helpers
(`_all`/`_one`) for queries the interface does not yet expose. Promoting
those queries to named `SessionStore` methods is the path to true
backend neutrality; until then, these modules require the sqlite store.
"""
