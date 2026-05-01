# agent-telemetry

Unified OpenTelemetry telemetry for coding agents — Claude Code (CLI + desktop), Codex (CLI + desktop + IDE), and a future custom harness — shipped to Langfuse for cross-surface analysis.

## Start Here

- [goal.md](goal.md) — What this is, what it's not, what it has to deliver.

## Orientation

This repo replaces the ad-hoc JSONL parser at `crypto-analysis/scripts/analyze_sessions.py` with a single telemetry pipeline that covers every agent surface on the machine. Two ingestion paths:

- **Live.** Agents emit OTel → local Collector → Langfuse. Configured via env vars (Claude Code) and `~/.codex/config.toml` (Codex).
- **Backfill.** Existing session files (`~/.claude/projects/*.jsonl`, `~/.codex/sessions/*`) parsed and replayed as OTel spans with original timestamps and deterministic IDs, so re-runs are idempotent.

The confidence check: recomputing every metric from `analyze_sessions.py` against Langfuse data must match the local-parse numbers for the same session. Until parity holds, trust the JSONL.

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
- `agent_telemetry/ingest/` — durable source-agnostic auto-ingest state machine and source adapters.
- `recipes/` — per-source env/config recipes for live agents, including Codex `trace_exporter` config for the local collector.
- `agent_telemetry/analysis/` — parity check + reproductions of the `analyze_sessions.py` metrics against Langfuse.
- `infra/langfuse/` — local self-hosted Langfuse v3 stack (docker-compose).
- `infra/collector/` — local OTel Collector (docker-compose). Single fan-in point on `http://localhost:4318` for backfill + live recipes; forwards to Langfuse with auth.
- `tests/` — mapping tests with small JSONL fixtures.

## Status

Phase 0 — mapping the Claude Code JSONL format to OTel spans, offline and testable. No network yet.
