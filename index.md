# agent-telemetry

Unified telemetry for coding agents — Claude Code (CLI + desktop), Codex (CLI + desktop + IDE), and a future custom harness — with two sinks: a **local session store** (full fidelity, own UI, audit-oriented) and Langfuse via OTel (legacy path).

## Start Here

- [goal.md](goal.md) — What this is, what it's not, what it has to deliver.
- [reports/spend-analysis-2026-07-02.md](reports/spend-analysis-2026-07-02.md) — where the token/time spend goes; ranked wins.

## Session Store (own backend — full fidelity)

The store keeps what the Langfuse path deliberately drops: **full thinking text** (when the source persists it), **untruncated tool arguments/results** (no 60 KB cap), and complete message texts — all joined to the same deterministic span ids the mappers emit. Backend is pluggable via `SessionStore` (`agent_telemetry/store/base.py`); the default is a single SQLite file with FTS5 search at `~/.agent-telemetry/store.sqlite3`. Re-ingesting a changed transcript **replaces** its rows (unlike Langfuse OTel first-write-wins, INT-455).

### Three layers, one writer protocol

1. **Raw archive** (`~/.agent-telemetry/raw/`) — immutable gzip copies of every ingested source file, versioned by content hash, captured *before* parsing so unparseable files are still preserved. `agent-telemetry-analyze raw stats|versions|restore`.
2. **Analyzed store** — the structured sessions/turns/tools/contents layer described above.
3. **Retention** — per-tier, per-source TTLs in `~/.agent-telemetry/config.toml`; missing config means keep forever. Enforced by `agent-telemetry-analyze retention run` (or `--apply-retention` on the auto-ingest loop):

```toml
[retention]
raw = "forever"
analyzed = "forever"

[retention.raw_overrides]
codex = "30d"
```

Vendors are just arguments: every source registers a `SourceAdapter` (`agent_telemetry/store/registry.py`) with `map_spans` + `extract_contents`, and the entire pipeline downstream is adapter-agnostic. `--source anthropic` and `--source openai` resolve to claude-code and codex; adding Gemini or a custom harness is one registry entry plus the two mapper functions.

```bash
# Ingest everything (reuses the auto-ingest state machine; safe to re-run / cron):
agent-telemetry-auto-ingest --source claude-code --backend store --once
agent-telemetry-auto-ingest --source codex --backend store --once --include-archived-codex

# Analysis CLI (add --json for agent/script consumption):
agent-telemetry-analyze overview
agent-telemetry-analyze sessions --source claude-code --since 7d
agent-telemetry-analyze show <session-id>
agent-telemetry-analyze thinking <session-id>          # full thought process
agent-telemetry-analyze tools --since 7d               # repeats, slowest, largest
agent-telemetry-analyze tokens --group-by model        # cache split preserved
agent-telemetry-analyze search "navigat*" --kind thinking
agent-telemetry-analyze audit repeats --since 30d      # identical calls, byte-identical proof
agent-telemetry-analyze audit bigreads --since 30d     # unranged whole-file Reads
agent-telemetry-analyze audit toolgaps --since 12w     # recurring throwaway scripts

# Web UI + JSON API (http://localhost:8321; endpoints under /api/*):
agent-telemetry-serve
```

Source caveats the store cannot fix: recent Claude Code versions often persist thinking blocks with empty text + signature (encrypted at source), and Codex `reasoning` items are always encrypted — for those sessions the store has counts, not text. Plaintext thinking is captured whole wherever it exists.

## Orientation

This repo replaces the ad-hoc JSONL parser at `crypto-analysis/scripts/analyze_sessions.py` with a single telemetry pipeline that covers every agent surface on the machine. Two ingestion paths:

- **Live.** Agents emit OTel → local Collector → Langfuse. Configured via env vars (Claude Code) and `~/.codex/config.toml` (Codex).
- **Backfill.** Existing session files (`~/.claude/projects/*.jsonl`, `~/.codex/sessions/*`) parsed and replayed as OTel spans with original timestamps and deterministic IDs, so re-runs are idempotent.

The confidence check: recomputing every metric from `analyze_sessions.py` against Langfuse data must match the local-parse numbers for the same session. Until parity holds, trust the JSONL.

## Claude Code Auto-Ingest

Claude Code CLI and desktop sessions write canonical transcript JSONL files under `~/.claude/projects/**/*.jsonl`, including sidechain/subagent transcripts nested below a session directory. These files are the replay source for Claude transcript structure, timing, token usage, tool calls, cwd, version, git branch, and the exposed `entrypoint` surface such as `cli` or `claude-desktop`.

Use the auto-ingest CLI to discover quiet Claude Code transcript files and checkpoint their state before exporting:

```bash
agent-telemetry-auto-ingest --source claude-code --once --dry-run
```

Dry-run mode lists pending or skipped Claude Code files with session id, trace id, path, mtime, fingerprint, entrypoint metadata, and reason without writing to Langfuse. Real ingest reuses the existing Claude Code JSONL backfill mapper and OTLP exporter, preserving the `claude_code.interaction`, `claude_code.llm_request`, and `claude_code.tool` vocabulary plus Langfuse source metadata/tags. Pass one or more `--claude-root` values for fixture/custom roots; by default discovery walks `~/.claude/projects`.

After ingest, inspect the canonical Claude Code trace in Langfuse for transcript detail. The root `claude_code.interaction` observation shows a preview-sized list of user requests in Input and assistant-visible transcript counts/previews in Output. Each `claude_code.llm_request` observation shows the relevant user/tool-result request slice in Input and visible assistant text/tool-call slice in Output. Each `claude_code.tool` observation uses Input for the tool name/arguments and Output for the bounded tool result. Hidden/private thinking blocks remain metadata/counts only.

This canonical JSONL ingest is separate from native live Claude OTel. Use it when transcript-level grouping and deterministic replay matter. The live path still goes through the local Collector, but live Claude grouping fixes and launchd/Docker sidecar packaging are intentionally outside this auto-ingest path.

## Codex Auto-Ingest

Codex CLI, Desktop, and IDE sessions write canonical rollout JSONL files under `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`; archived sessions live under `~/.codex/archived_sessions/*.jsonl`. These rollout files are the canonical source for Codex prompt, transcript, tool-call, token, timing, cwd, model, and source-surface data.

Use the auto-ingest CLI to discover quiet rollout files and checkpoint their state before exporting:

```bash
agent-telemetry-auto-ingest --source codex --once --dry-run
```

Dry-run mode lists pending or skipped Codex files with session id, path, mtime, fingerprint, source metadata, and reason without writing to Langfuse. Real ingest reuses the existing Codex rollout mapper and OTLP exporter, preserving the `codex.interaction`, `codex.llm_request`, and `codex.tool` vocabulary plus Langfuse source metadata/tags. Add `--include-archived-codex` to include `~/.codex/archived_sessions/*.jsonl`, or pass one or more `--codex-root` values for fixture/custom roots.

After ingest, inspect the canonical Codex trace in Langfuse rather than the live app-server trace for transcript detail. The root `codex.interaction` observation shows session-level user requests and assistant-visible transcript counts in Input/Output. Each `codex.llm_request` observation shows the relevant visible request/response slice for that model response. Each `codex.tool` observation uses Input for the tool name/arguments and Output for the bounded tool result. Opaque reasoning rollout items are counted as metadata only; encrypted/private reasoning text is not exposed.

## Layout

- `agent_telemetry/backfill/` — parsers that turn historical session files into OTel spans.
- `agent_telemetry/store/` — pluggable session store (SQLite default), full-fidelity content extraction, analysis CLI, HTTP API + web UI.
- `agent_telemetry/ingest/` — durable source-agnostic auto-ingest state machine and source adapters.
- `recipes/` — per-source env/config recipes for live agents, including Codex `trace_exporter` config for the local collector.
- `agent_telemetry/analysis/` — parity check + reproductions of the `analyze_sessions.py` metrics against Langfuse.
- `infra/langfuse/` — local self-hosted Langfuse v3 stack (docker-compose).
- `infra/collector/` — local OTel Collector (docker-compose). Single fan-in point on `http://localhost:4318` for backfill + live recipes; forwards to Langfuse with auth.
- `tests/` — mapping tests with small JSONL fixtures.

## Status

Phase 0 — mapping the Claude Code JSONL format to OTel spans, offline and testable. No network yet.
