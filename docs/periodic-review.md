# Periodic review

A scheduled "how did my agents do" review, written so any agent runtime can
run it. The data gathering is deterministic and lives in flume; only the
write-up needs a model.

## Run it

```sh
uv run flume analyze --json review
```

Defaults come from `[review]` in `~/.flume/config.toml`; every one has a
CLI flag that overrides it.

```toml
[review]
since = "7d"           # review window
severity_max = 2       # report severity <= this (1 is worst)
growth = 0.25          # flag recurring findings whose metric grew this much
baseline_days = 30     # experiment comparison baseline
min_sessions = 10      # below this, an experiment arm is directional only
```

## What comes back

| key | meaning |
|---|---|
| `new_findings` | never recorded before this run |
| `grown_findings` | recurring, metric up by at least `growth`; carries `previous_metric` and `growth` |
| `experiments` | active experiments only, each with `groups` and `directional_only` |
| `hooks` | per-hook rollup: events, heeded, bypassed |
| `counts` | `detected` / `considered` / `new` / `grown` — `considered` is post-severity-filter |

`new` and `grown` both zero with a non-zero `considered` is a real result: a
quiet week, not a broken run. Say so rather than padding.

## The report

Ask the model for, in this order, under ~40 lines:

1. **New findings** — severity 1 and 2 only, each with its recommended action.
2. **Growing findings** — those past the growth threshold, with before → after.
3. **Experiment scoreboard** — movement in `nav_share_median`, `duplicate_reads`,
   `read_used_share`, `cache_hit`. Anything with `directional_only: true` is
   suggestive, not evidence; label it that way.
4. **Hook compliance** — nudge counts, heeded vs bypassed.
5. **One line: the biggest win available now.**

Lead with what changed, not totals.

Two constraints worth passing through: the review is read-only apart from the
findings persistence `insights` performs — it must not start or stop
experiments or edit hooks or settings. And `analyze cost` prices Codex/OpenAI
turns at API list rates while Codex may be running on a flat subscription, so
that share is API-equivalent, not cash.

## Scheduling it

flume does not schedule anything — it has no opinion about which agent writes
the report. Point whatever you already use at the two steps above:

- **Claude Code** — a scheduled task under `~/.claude/scheduled-tasks/`.
- **Codex** — an automation under `~/.codex/automations/`.
- **cron / launchd** — run the command, pipe the JSON to any agent CLI.

Each is the same two steps: run the command, hand the JSON over with this
file as the spec. Keeping the spec here rather than inside one runtime's
config is what stops the two copies drifting.
