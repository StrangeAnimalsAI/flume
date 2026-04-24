# agent-telemetry

Unified OpenTelemetry telemetry for coding agents — Claude Code (CLI + desktop), Codex (CLI + desktop + IDE), and a future custom harness — shipped to Langfuse for cross-surface analysis.

## Start Here

- [goal.md](goal.md) — What this is, what it's not, what it has to deliver.

## Orientation

This repo replaces the ad-hoc JSONL parser at `crypto-analysis/scripts/analyze_sessions.py` with a single telemetry pipeline that covers every agent surface on the machine. Two ingestion paths:

- **Live.** Agents emit OTel → local Collector → Langfuse. Configured via env vars (Claude Code) and `~/.codex/config.toml` (Codex).
- **Backfill.** Existing session files (`~/.claude/projects/*.jsonl`, `~/.codex/sessions/*`) parsed and replayed as OTel spans with original timestamps and deterministic IDs, so re-runs are idempotent.

The confidence check: recomputing every metric from `analyze_sessions.py` against Langfuse data must match the local-parse numbers for the same session. Until parity holds, trust the JSONL.

## Layout

- `agent_telemetry/backfill/` — parsers that turn historical session files into OTel spans.
- `agent_telemetry/live/` — per-source env/config recipes for live agents. *(not yet)*
- `agent_telemetry/analysis/` — parity check + reproductions of the `analyze_sessions.py` metrics against Langfuse.
- `infra/langfuse/` — local self-hosted Langfuse v3 stack (docker-compose).
- `infra/collector/` — local OTel Collector (docker-compose). Single fan-in point on `http://localhost:4318` for backfill + live recipes; forwards to Langfuse with auth.
- `tests/` — mapping tests with small JSONL fixtures.

## Status

Phase 0 — mapping the Claude Code JSONL format to OTel spans, offline and testable. No network yet.
