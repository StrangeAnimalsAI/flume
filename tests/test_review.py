"""The assembled periodic review.

The point of `analyze review` is that everything except the write-up is
deterministic, so whichever runtime schedules it — a Claude Code scheduled
task, a Codex automation, cron — gets identical input. These cover the
classification rules that make the output trustworthy: what counts as new,
what counts as grown, which experiments make the board, and whether an
arm has enough sessions to be more than directional.
"""
from __future__ import annotations

from pathlib import Path

from flume.analysis.review import DEFAULTS, load_review_config, run_review
from flume.store.sqlite import SqliteSessionStore

SEC_NS = 1_000_000_000
BASE_NS = 1_780_000_000 * SEC_NS


def _store(tmp_path: Path) -> SqliteSessionStore:
    return SqliteSessionStore(tmp_path / "store.sqlite3")


def test_config_layers_over_defaults(tmp_path, monkeypatch) -> None:
    assert load_review_config(tmp_path / "absent.toml") == DEFAULTS

    cfg = tmp_path / "config.toml"
    cfg.write_text('[review]\nsince = "30d"\ngrowth = 0.5\n')
    monkeypatch.setenv("FLUME_CONFIG", str(cfg))
    settings = load_review_config()
    assert settings["since"] == "30d"
    assert settings["growth"] == 0.5
    assert settings["severity_max"] == DEFAULTS["severity_max"]  # untouched


def test_unknown_config_keys_are_ignored(tmp_path) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text('[review]\nsince = "1d"\nnonsense = 42\n')
    settings = load_review_config(cfg)
    assert settings["since"] == "1d"
    assert "nonsense" not in settings


def test_review_separates_new_from_grown(tmp_path) -> None:
    """A finding already stored is not new; it is grown only if its metric
    moved past the threshold. Both must survive `insights` overwriting the
    stored metric mid-run."""
    with _store(tmp_path) as store:
        store.upsert_findings([
            {"kind": "toolgap", "fingerprint": "old-and-flat", "severity": 1,
             "title": "flat", "detail": "", "metric": 10.0, "unit": "n",
             "action": "", "evidence": "{}"},
            {"kind": "toolgap", "fingerprint": "old-and-growing", "severity": 1,
             "title": "growing", "detail": "", "metric": 10.0, "unit": "n",
             "action": "", "evidence": "{}"},
        ])

        # Same fingerprints come back with one metric moved well past +25%.
        def fake_insights(_store, **_kwargs):
            return [
                {"kind": "toolgap", "fingerprint": "old-and-flat", "severity": 1,
                 "title": "flat", "metric": 10.4, "first_seen_ns": BASE_NS},
                {"kind": "toolgap", "fingerprint": "old-and-growing", "severity": 1,
                 "title": "growing", "metric": 18.0, "first_seen_ns": BASE_NS},
                {"kind": "toolgap", "fingerprint": "brand-new", "severity": 2,
                 "title": "fresh", "metric": 3.0,
                 "first_seen_ns": BASE_NS + 10 * SEC_NS},
                {"kind": "toolgap", "fingerprint": "too-mild", "severity": 4,
                 "title": "ignored", "metric": 1.0, "first_seen_ns": BASE_NS},
            ]

        import flume.analysis.insights as insights_mod

        original = insights_mod.run_insights
        insights_mod.run_insights = fake_insights
        try:
            result = run_review(store, since_ns=BASE_NS, severity_max=2, growth=0.25)
        finally:
            insights_mod.run_insights = original

    new = {f["fingerprint"] for f in result["new_findings"]}
    grown = {f["fingerprint"] for f in result["grown_findings"]}
    assert new == {"brand-new"}
    assert grown == {"old-and-growing"}  # +80%; the +4% one is not reported
    assert result["counts"]["considered"] == 3  # severity 4 filtered out
    assert result["counts"]["detected"] == 4
    assert result["grown_findings"][0]["previous_metric"] == 10.0


def test_stopped_experiments_are_off_the_board(tmp_path) -> None:
    with _store(tmp_path) as store:
        store.create_experiment(
            "running", hypothesis="h", source="claude-code", started_at_ns=BASE_NS
        )
        store.create_experiment(
            "finished", hypothesis="h", source="claude-code",
            started_at_ns=BASE_NS, ended_at_ns=BASE_NS + 60 * SEC_NS,
        )
        result = run_review(store, since_ns=BASE_NS)

    names = {e["experiment"]["name"] for e in result["experiments"]}
    assert names == {"running"}


def test_thin_experiment_arm_is_flagged_directional(tmp_path) -> None:
    """The weaker arm decides: a lopsided comparison is not evidence."""
    with _store(tmp_path) as store:
        store.create_experiment(
            "thin", hypothesis="h", source="claude-code", started_at_ns=BASE_NS
        )
        result = run_review(store, since_ns=BASE_NS, min_sessions=10)

    assert result["experiments"][0]["directional_only"] is True
