# Goal

> **Historical (v0, superseded).** This was the original goal, written when
> Langfuse was the intended store. The v0 success condition was met, and the
> project then replaced Langfuse with the local sqlite session store —
> full-fidelity, no payload caps, queried via `flume analyze`. The Langfuse
> OTLP path and its infra were removed in July 2026. Kept as a record of
> what v0 set out to prove.

One telemetry pipeline that covers every coding-agent surface on the machine, shipped to Langfuse, with enough fidelity to answer the questions the current JSONL parser (`crypto-analysis/scripts/analyze_sessions.py`) answers today — across sources, not just Claude Code.

## What this has to deliver

- **Cross-source comparisons.** cli vs. desktop, Claude Code vs. Codex vs. custom harness, config A vs. config B on the same task. The current parser does this for Claude Code only; the new pipeline does it for everything.
- **Tool-call economy.** Per tool: name, args, duration, is_error, output size. Same repeated-call detection (same tool + same args), same slowest-N and largest-N rollups.
- **Token economy.** Per turn: input, output, cache-read, cache-create. Per session: cache-hit ratio. The Langfuse ingest has to preserve the cache split — an aggregate input-tokens number is useless.
- **Time economy.** Wall time vs. active time. Active time requires the per-turn duration signal, not just trace start/end.
- **Backfill.** Historical JSONL (Claude Code) and rollouts (Codex) must replay into Langfuse with original timestamps and deterministic IDs, so the parity check can diff a local-parse report against a Langfuse-derived report for the same session and expect zero drift on shared metrics.

## What this is not

- Not a Langfuse alternative. Langfuse is the store and the UI; this repo is the pipe and the per-source recipes.
- Not a general-purpose OTel distribution. Scope is coding agents whose output is "what the code looks like" — enough to reason about navigation cost, tool preference, and cache behavior, not enough to replace application observability.
- Not a CLAUDE.md-style convention enforcer or contract. The `.no-slop.toml` comes after the first real module earns it.

## Non-goals for v0

- No live ingestion for ChatGPT Desktop's "codex mode" — separate surface, no documented `~/.codex/` integration, skip until someone files a path in.
- No mitigations for the 60 KB Claude Code OTel tool-content truncation. Backfill uses the full JSONL payload; live is accepted as lossy on the long tail.

## Success condition for v0

Given one Claude Code JSONL and one Codex session rollout, the backfill writes OTel spans that, when ingested into Langfuse, let a parity-check script reproduce every number in `analyze_sessions.py` for that session, with the cache/token/timing breakdown intact.
