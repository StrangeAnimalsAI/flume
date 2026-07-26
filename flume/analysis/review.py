"""The periodic efficiency review, assembled without a model.

A scheduled "how did my agents do this week" review is mostly deterministic:
run the detectors, work out which findings are new, diff each recurring
finding's metric against its stored value, score the active experiments,
tally hook compliance. Only the write-up needs a model.

Splitting it that way is what makes the review portable. Whatever runs it —
a Claude Code scheduled task, a Codex automation, cron, launchd — invokes
one command and hands the JSON to whichever agent it likes, so the schedule
and the agent are the runtime's business and none of flume's.

Defaults come from `[review]` in ~/.flume/config.toml:

    [review]
    since = "7d"              # review window
    severity_max = 2          # report severity <= this (1 is worst)
    growth = 0.25             # flag recurring findings whose metric grew this much
    baseline_days = 30        # experiment comparison baseline
    min_sessions = 10         # below this, an experiment arm is directional only

Ordering matters in one place: stored findings are read BEFORE the detectors
run, because running them overwrites each finding's metric with the current
value. The pre-read is the only record of what the metric was last time.
"""
from __future__ import annotations

from typing import Any

from flume.store.base import require_sql
from flume.store.config import load_toml

DEFAULTS: dict[str, Any] = {
    "since": "7d",
    "severity_max": 2,
    "growth": 0.25,
    "baseline_days": 30,
    "min_sessions": 10,
}


def load_review_config(path=None) -> dict[str, Any]:
    """`[review]` from config, over the shipped defaults."""
    settings = dict(DEFAULTS)
    section = load_toml(path).get("review") or {}
    for key, value in section.items():
        if key in settings:
            settings[key] = value
    return settings


def _key(finding: dict[str, Any]) -> tuple[str, str]:
    return (str(finding.get("kind")), str(finding.get("fingerprint")))


def _growth(previous: float | None, current: float | None) -> float | None:
    """Fractional change, or None when it isn't meaningful to compute."""
    if previous is None or current is None or previous == 0:
        return None
    return (current - previous) / abs(previous)


def run_review(
    store,
    *,
    since_ns: int | None,
    severity_max: int = 2,
    growth: float = 0.25,
    baseline_days: int = 30,
    min_sessions: int = 10,
    source: str | None = None,
) -> dict[str, Any]:
    """Assemble the review. Persists findings, as `insights` does; the rest
    is read-only."""
    from flume.analysis.experiments import compare_experiment
    from flume.analysis.hooks import hook_events, hooks_summary
    from flume.analysis.insights import run_insights

    require_sql(store, "review")

    # Before the detectors overwrite them (see module docstring).
    previous = {_key(f): f for f in store.list_findings(limit=10_000)}

    findings = [dict(f) for f in run_insights(store, since_ns=since_ns, source=source)]
    ranked = [f for f in findings if int(f.get("severity", 99)) <= severity_max]

    new, grown = [], []
    for finding in ranked:
        prior = previous.get(_key(finding))
        # "New" means the store had never recorded this (kind, fingerprint)
        # before this run — which the pre-read answers exactly. Deriving it
        # from first_seen_ns instead would depend on the review's cadence
        # matching its window, and misfile anything sitting on the boundary.
        # The first run on a fresh store reports everything as new, correctly.
        if prior is None:
            new.append(finding)
            continue
        change = _growth(prior.get("metric"), finding.get("metric"))
        if change is not None and change >= growth:
            grown.append({**finding, "previous_metric": prior.get("metric"),
                          "growth": round(change, 4)})

    experiments = []
    for experiment in store.list_experiments():
        if experiment.get("ended_at_ns"):  # stopped: not part of this week's board
            continue
        try:
            comparison = compare_experiment(
                store, experiment["name"], baseline_days=baseline_days
            )
        except KeyError:
            continue
        # `measured` is per group (baseline vs experiment); the weaker arm
        # decides whether the comparison can carry weight.
        measured = [_as_int(g.get("measured")) for g in comparison.get("groups") or []]
        comparison["directional_only"] = not measured or min(measured) < min_sessions
        experiments.append(comparison)

    events = hook_events(store, since_ns=since_ns, limit=10_000)

    return {
        "window": {"since_ns": since_ns, "severity_max": severity_max,
                   "growth_threshold": growth},
        "new_findings": new,
        "grown_findings": grown,
        "experiments": experiments,
        "hooks": hooks_summary(events),
        "counts": {
            # `considered` distinguishes a quiet week from a broken run: zero
            # new and zero grown out of eleven findings is a real result.
            "considered": len(ranked),
            "detected": len(findings),
            "new": len(new),
            "grown": len(grown),
            "experiments": len(experiments),
            "hook_events": len(events),
        },
    }


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
