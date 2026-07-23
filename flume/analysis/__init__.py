"""Query-side analysis over the session store: insight detectors, navigation-time
attribution, experiment comparison, audit heuristics, hook-event joins, and the
analyze CLI. Everything here consumes the SessionStore interface — never SQL
directly — so it works against any store backend."""
