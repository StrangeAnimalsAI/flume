# Claude Code CLI -> local OTel Collector (agent-telemetry).
#
# Source from your shell to point `claude` at the local Collector running on
# http://localhost:4318. The Collector forwards to self-hosted Langfuse with
# project-scoped basic auth, so this script intentionally carries NO secrets.
#
# Usage (one-shot, current shell only):
#   source /Users/james/Code/tools/agent-telemetry/recipes/claude-code-cli.env.sh
#   claude -p "say hi"
#
# Usage (persistent, every new shell):
#   echo 'source /Users/james/Code/tools/agent-telemetry/recipes/claude-code-cli.env.sh' >> ~/.zshrc
#
# Prerequisites:
#   - Collector running:  cd infra/collector && docker compose up -d
#   - Langfuse running:   cd infra/langfuse  && docker compose up -d
#   See infra/collector/README.md.

# 1. Master switch. Required for any telemetry to leave the process.
export CLAUDE_CODE_ENABLE_TELEMETRY=1

# 2. Traces are still beta as of 2026-04. Without this flag, only metrics +
#    logs/events emit; no spans. Both names are accepted by Claude Code; we set
#    the documented one.
export CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1

# 3. Pick the three exporter signals. `otlp` for all so they fan into the
#    Collector on the same endpoint.
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_TRACES_EXPORTER=otlp

# 4. OTLP endpoint + protocol. Collector exposes 4318 (HTTP) and 4317 (gRPC);
#    we use HTTP/protobuf because the Collector's HTTP path is what the
#    backfill CLIs already exercise (consistent fan-in).
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318

# 5. Reveal tool arguments + content in spans/events. Without these, Bash
#    commands, file paths, MCP tool names, etc. all redact to placeholders,
#    and the cross-source attribute alignment work has nothing to align on.
#    OTEL_LOG_TOOL_CONTENT requires tracing (covered by the beta flag above)
#    and is capped at 60 KB per attribute.
export OTEL_LOG_TOOL_DETAILS=1
export OTEL_LOG_TOOL_CONTENT=1

# 6. Faster export intervals than the docs defaults (60s metrics / 5s logs).
#    The CLI lifecycle is short; long intervals lose data on quick `-p` runs.
export OTEL_METRIC_EXPORT_INTERVAL=10000
export OTEL_LOGS_EXPORT_INTERVAL=2000
export OTEL_TRACES_EXPORT_INTERVAL=2000

# 7. Tag the source so `claude-code-cli` vs `claude-code-desktop` stays
#    distinguishable in Langfuse. No spaces, comma-separated key=value.
export OTEL_RESOURCE_ATTRIBUTES=source=claude-code-cli

# Note: OTEL_LOG_USER_PROMPTS is intentionally NOT set. Prompt text is
# captured by length only by default; flip it on per-shell if you need the
# verbatim prompt for a specific debugging session.
