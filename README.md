# flume

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

1. **Raw archive** (`~/.flume/raw`) — every ingested transcript
   captured as a gzip blob, content-hash-versioned, *before* parsing. The
   agent apps prune their own transcripts; this doesn't.
2. **Analyzed store** (`~/.flume/store.sqlite3`) — the relational
   + FTS5 layer: sessions, turns, tool calls, and full-fidelity content
   rows (thinking, messages, untruncated tool I/O). Rebuildable from the
   raw archive via `analyze rebuild --stale` when the pipeline changes.
3. **Declarative retention** (`~/.flume/config.toml`) — per-tier,
   per-source TTLs (default: keep forever):

   ```toml
   [retention]
   raw = "forever"
   analyzed = "forever"

   [retention.raw_overrides]
   codex = "30d"
   ```

The same file carries `[pricing]` (add or reprice any model, including
local ones at zero) and `[insights]` (which files mark an agent-readable
index). Nothing about pricing or tooling conventions is baked into the
code.

Source caveat the store cannot fix: recent Claude Code builds often persist
thinking blocks as encrypted signatures, and Codex `reasoning` items are
always encrypted — for those sessions the store has counts, not text.
Plaintext thinking is captured whole wherever it exists (see the harness
below for recovering it going forward).

Sources are pluggable adapters (`claude-code`, `codex`, and a traced
`harness`) — the vendor is just an argument. Nothing in the pipeline
assumes a particular model or vendor: prices come from a config table you
extend, tool vocabularies come from each source adapter, and the harness
drives whichever backend you point it at, including a local model. A core
install carries no model-vendor dependency at all.

The package layout mirrors the pipeline, and dependencies point one way
(`cli → ingest → sources → store`):

- `flume/sources/` — one module per vendor: format mapping, full-fidelity
  extraction, and discovery. The only code that knows any vendor's format.
- `flume/ingest/` — the source-agnostic pipeline: discovery loop, durable
  checkpoints, and the archive-then-persist write path.
- `flume/store/` — the engine: storage interface, sqlite backend, raw
  archive, retention. Swappable via `open_store(url)` / `open_archive(url)`;
  it never imports the layers above.
- `flume/analysis/` — insight detectors, experiment comparison, and the
  `analyze` CLI. Runs on the `SessionStore` interface, plus an explicitly
  declared `SqlReadable` capability for the detectors that need ad-hoc
  SQL — so a non-SQL backend gets a clear error naming the feature rather
  than a broken query.
- `flume/harness/` — the traced agent app; its transcript format registers
  in `sources` like any vendor.

## Interfaces

| Command | What it does |
|---|---|
| `flume analyze` | Query CLI (below). `--json` on any subcommand. |
| `flume serve` | Web UI + JSON API on `:8321` (sessions, tools, tokens, insights, audit). |
| `flume ingest` | Daemon: watches transcript dirs, ingests quiet files on a loop. |
| `flume harness` | Minimal traced agent that captures summarized thinking (see below). |

### analyze subcommands

```sh
flume analyze overview                 # corpus totals by source
flume analyze sessions --project X     # sessions, newest first
flume analyze show <session-id>        # turns, tools, economy
flume analyze thinking <session-id>    # full thinking blocks
flume analyze tools --since 7d         # per-tool, repeats, slowest
flume analyze tokens --group-by model
flume analyze cost --since 24h --group-by model   # cache-aware $
flume analyze search "navigat* codebase" --kind thinking
flume analyze insights --since 7d      # gap detectors (below)
flume analyze audit {repeats,bigreads,toolgaps}   # waste queries
flume analyze rebuild --stale          # re-ingest behind-version rows
flume analyze experiment {start,stop,list,compare}   # tag + measure
flume analyze hooks                    # nudge/denial interventions
```

### Insights

`insights` runs gap detectors over a window and persists ranked, deduped
findings: `toolgap` (throwaway scripts → durable CLIs), `repeat_waste`
(byte-identical re-work), `schema_loop` (StructuredOutput retry grinding),
`error_hotspot`, `context_flood`, `idle_gap_churn`, `marathon_session`,
`premium_grind`, `index_ignored`.

Detectors are source- and vendor-agnostic: they find agentic-coding
pathologies (duplicate calls, idle-gap cache churn, navigation grind),
not Claude-specific ones. `analyze insights --source X` scopes to one
source; the default covers all of them.

### Provenance & rebuild

Every session row records the `raw_sha256` it was built from and a
`pipeline_version`. Bump `PIPELINE_VERSION` when the mappers/extractors
change, then `rebuild --stale` re-ingests older rows from the raw archive
— so improvements to the pipeline retroactively apply to old sessions.

### Harness (thinking capture)

> **Warning:** the harness is an agent. It gives the model a Bash tool and
> executes the commands the model chooses with your privileges, in your
> working directory, with no sandbox or approval step. Run it only in
> directories you'd let an agent loose in.

Claude Code stopped persisting plaintext thinking (~May 2026). The harness
runs its own traced agent loop and writes whatever reasoning the model
exposes into the store:

```sh
flume harness "why is retention skipping codex blobs?" --backend claude-sdk
flume harness "..." --backend openai --model qwen3-coder   # local via Ollama
```

Backends are pluggable, and the transcript format is the contract — a
session run against a local model is ingested and analyzed exactly like a
hosted one:

| `--backend` | Drives | Needs |
|---|---|---|
| `anthropic` | Anthropic Messages API, pay-per-token | `flume[anthropic]` |
| `claude-sdk` | Claude Agent SDK on a plan login, full tool suite | `flume[claude-sdk]` |
| `openai` | Any OpenAI-compatible `/v1/chat/completions` server — hosted OpenAI, Ollama, llama.cpp, vLLM | nothing (stdlib) |

Point the `openai` backend anywhere with `--base-url` (default is Ollama's
`http://localhost:11434/v1`).

## Install & run

```sh
uv venv && uv pip install -e .          # core: ingest + analyze, no model SDKs
uv pip install -e '.[harness]'         # + the Anthropic harness backends
flume analyze overview
flume serve            # http://localhost:8321
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
transcripts; they do not read your local agent history.

## Data

Everything lives under `~/.flume/` (store, raw archive, config,
logs) — outside this repo, and gitignored where it isn't. The store is
the audit path of record. (An OTLP→Langfuse export path existed for
comparison during development and was removed in July 2026.)
