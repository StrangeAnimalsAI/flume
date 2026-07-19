# agent-telemetry

Local-first telemetry for coding agents. It captures full sessions from
**Claude Code** and **Codex** — thinking, tool calls, token/cache
economy, timings — into a SQLite store *you own*, then answers questions
about cost, tool usage, and agent behavior that hosted dashboards can't:
where navigation time goes, which tools are missing, what's being re-done,
what a session actually cost.

Built after deciding Langfuse wasn't the right system of record: the OTLP
path redacts thinking to counts and caps payloads, so this keeps the full
fidelity in a store with a stable query interface instead.

## Architecture

Three layers, each independently rebuildable:

1. **Raw archive** (`~/.agent-telemetry/raw`) — every ingested transcript
   captured as a gzip blob, content-hash-versioned, *before* parsing. The
   agent apps prune their own transcripts; this doesn't.
2. **Analyzed store** (`~/.agent-telemetry/store.sqlite3`) — the relational
   + FTS5 layer: sessions, turns, tool calls, and full-fidelity content
   rows (thinking, messages, untruncated tool I/O). Rebuildable from the
   raw archive via `analyze rebuild --stale` when the pipeline changes.
3. **Declarative retention** (`~/.agent-telemetry/config.toml`) — per-tier,
   per-source TTLs (default: keep forever).

Sources are pluggable adapters (`claude-code`, `codex`, and a traced
`harness`) — the vendor is just an argument.

## Interfaces

| Command | What it does |
|---|---|
| `agent-telemetry-analyze` | Query CLI (below). `--json` on any subcommand. |
| `agent-telemetry-serve` | Web UI + JSON API on `:8321` (sessions, tools, tokens, insights, audit). |
| `agent-telemetry-auto-ingest` | Daemon: watches transcript dirs, ingests quiet files on a loop. |
| `agent-telemetry-harness` | Minimal traced agent that captures summarized thinking (see below). |

### analyze subcommands

```sh
agent-telemetry-analyze overview                 # corpus totals by source
agent-telemetry-analyze sessions --project X     # sessions, newest first
agent-telemetry-analyze show <session-id>        # turns, tools, economy
agent-telemetry-analyze thinking <session-id>    # full thinking blocks
agent-telemetry-analyze tools --since 7d         # per-tool, repeats, slowest
agent-telemetry-analyze tokens --group-by model
agent-telemetry-analyze cost --since 24h --group-by model   # cache-aware $
agent-telemetry-analyze search "navigat* codebase" --kind thinking
agent-telemetry-analyze insights --since 7d      # gap detectors (below)
agent-telemetry-analyze audit {repeats,bigreads,toolgaps}   # waste queries
agent-telemetry-analyze rebuild --stale          # re-ingest behind-version rows
agent-telemetry-analyze experiment {start,stop,list,compare}   # tag + measure
agent-telemetry-analyze hooks                    # nudge/denial interventions
```

### Insights

`insights` runs gap detectors over a window and persists ranked, deduped
findings: `toolgap` (throwaway scripts → durable CLIs), `repeat_waste`
(byte-identical re-work), `schema_loop` (StructuredOutput retry grinding),
`error_hotspot`, `context_flood`, `idle_gap_churn`, `marathon_session`,
`premium_grind`, `docnav_ignored`.

### Provenance & rebuild

Every session row records the `raw_sha256` it was built from and a
`pipeline_version`. Bump `PIPELINE_VERSION` when the mappers/extractors
change, then `rebuild --stale` re-ingests older rows from the raw archive
— so improvements to the pipeline retroactively apply to old sessions.

### Harness (thinking capture)

Claude Code stopped persisting plaintext thinking (~May 2026). The harness
requests summarized thinking and writes it into the store:

```sh
agent-telemetry-harness "why is retention skipping codex blobs?" --backend sdk
```

`--backend sdk` drives the Agent SDK under your Claude plan login (thinking
summaries, plan-billed); `--backend api` uses the raw Anthropic API.

## Install & run

```sh
uv venv && uv pip install -e .
agent-telemetry-analyze overview
agent-telemetry-serve            # http://localhost:8321
```

For unattended use, run the idempotent `--once` ingest commands from cron,
launchd, or your scheduler of choice.

## Development

```sh
uv sync --frozen
uv run ruff check .
uv run pytest -q
uv build
```

CI runs the same lint and test checks on Python 3.11, 3.12, and 3.13, then
builds the source distribution and wheel. Tests are offline and use synthetic
transcripts; they do not read your local agent history or require Langfuse.

## Data

Everything lives under `~/.agent-telemetry/` (store, raw archive, config,
logs) — outside this repo, and gitignored where it isn't. There is also a
Langfuse OTLP path (`--backend otlp`) retained for comparison, but the
store is the audit path of record.
