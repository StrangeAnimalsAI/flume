# Live-ingest recipes

Per-source env / config to point real, running coding-agent surfaces at the
local OTel Collector. The Collector is the prerequisite — see
[`infra/collector/README.md`](../infra/collector/README.md) for bring-up.

```
agent (process)  ->  http://localhost:4318  ->  collector  ->  Langfuse
```

These recipes hold no secrets. The Collector carries the Langfuse basic-auth
header (`infra/collector/.env`); recipes only tell the agent where to send
OTLP.

## Files

| File                         | Surface           | Mechanism                                            |
| ---------------------------- | ----------------- | ---------------------------------------------------- |
| `claude-code-cli.env.sh`     | `claude` in TTY   | `source` from `~/.zshrc` (or once per shell)         |
| `claude-code-desktop.plist`  | Claude Desktop.app| LaunchAgent that runs `launchctl setenv` per var     |
| `codex-config.toml`          | Codex CLI/Desktop | Copy `[otel]` into `~/.codex/config.toml` after review |

The CLI script and the desktop plist set the **same** env vars with one
deliberate difference: `OTEL_RESOURCE_ATTRIBUTES=source=claude-code-cli` vs
`source=claude-code-desktop`. The collector preserves that raw OTel resource
attribute and also promotes it into Langfuse-visible
`metadata.agent_source`, `metadata.agent_family`, `metadata.agent_surface`,
and tags.

## CLI: source-and-go

```bash
source recipes/claude-code-cli.env.sh
echo "say hi" | claude -p
```

Persistent (every shell):

```bash
echo 'source /Users/james/Code/tools/agent-telemetry/recipes/claude-code-cli.env.sh' >> ~/.zshrc
```

To turn it off without editing `~/.zshrc`, `unset CLAUDE_CODE_ENABLE_TELEMETRY`
in the current shell.

## Desktop: load, then relaunch

The desktop app inherits env from the launchd user-domain session, not your
zsh. Loading the plist runs `launchctl setenv` for each variable; you must
**quit and relaunch Claude Desktop** for the new vars to take effect (a
running app already has the old, empty env baked in).

```bash
cp recipes/claude-code-desktop.plist \
   ~/Library/LaunchAgents/com.jameshtimmins.agent-telemetry-claude-code-desktop.plist
launchctl load -w ~/Library/LaunchAgents/com.jameshtimmins.agent-telemetry-claude-code-desktop.plist

launchctl getenv OTEL_RESOURCE_ATTRIBUTES   # source=claude-code-desktop
osascript -e 'quit app "Claude"'
open -a "Claude"
```

Unload commands and full uninstall steps are in the plist's leading
comment. **The plist is not auto-loaded** — review it before `launchctl load`.

## Endpoint flip: collector vs direct-to-Langfuse

Both recipes target the Collector at `http://localhost:4318`. Two reasons
to bypass:

- **Debugging the Collector itself.** Send straight to Langfuse to confirm
  the agent emits the OTLP shape Langfuse expects, then add the Collector
  back in.
- **Collector down.** Same idea, fallback path.

Direct-to-Langfuse needs basic auth that the Collector normally injects.
Override per-shell:

```bash
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' infra/langfuse/.env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' infra/langfuse/.env | cut -d= -f2)
AUTH="Basic $(printf '%s:%s' "$PK" "$SK" | base64)"

source recipes/claude-code-cli.env.sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:3000/api/public/otel
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=$AUTH"
```

Note the Langfuse base path is `/api/public/otel`; signal-specific endpoints
(`/v1/traces`, `/v1/logs`, `/v1/metrics`) hang off it. The Collector hides
that detail; bypassing means you wear it.

## What the env vars actually do

Source the script then `env | grep -E '^(CLAUDE|OTEL)_'` to inspect. Quick
notes per var:

- `CLAUDE_CODE_ENABLE_TELEMETRY=1` — master switch. Without it, no signals.
- `CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1` — turns on the spans pipeline.
  Traces are documented as **beta** as of 2026-04. Without this, only
  metrics + log/event signals emit (no `claude_code.interaction` /
  `claude_code.llm_request` / `claude_code.tool` spans).
- `OTEL_METRICS_EXPORTER=otlp`, `OTEL_LOGS_EXPORTER=otlp`,
  `OTEL_TRACES_EXPORTER=otlp` — fan all three signals into the Collector.
- `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`,
  `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` — Collector's HTTP
  receiver.
- `OTEL_LOG_TOOL_DETAILS=1` — un-redacts Bash commands, file paths, MCP
  tool names in `tool_result` events and span attrs.
- `OTEL_LOG_TOOL_CONTENT=1` — un-redacts tool input/output bodies in span
  events, capped at 60 KB. Requires tracing.
- `OTEL_METRIC_EXPORT_INTERVAL=10000`, `OTEL_LOGS_EXPORT_INTERVAL=2000`,
  `OTEL_TRACES_EXPORT_INTERVAL=2000` — shorter than the 60s/5s/5s docs
  defaults so quick `claude -p` runs flush before exit.
- `OTEL_RESOURCE_ATTRIBUTES=source=claude-code-{cli,desktop}` — the
  cross-source discriminator.

`OTEL_LOG_USER_PROMPTS` and `OTEL_LOG_RAW_API_BODIES` are intentionally NOT
set — flip them on per-shell when you need verbatim prompt or full Messages
API bodies.

Claude Code's native live spans emit token attrs as `input_tokens`,
`output_tokens`, `cache_read_tokens`, and `cache_creation_tokens`. The
collector preserves those raw fields and also copies them into the
backfill-compatible `gen_ai.usage.*` fields so live and replayed Claude
sessions use the same analysis vocabulary in Langfuse.

## Codex: global config traces

Codex reads live OTel configuration from `~/.codex/config.toml`. The current
Codex config reference separates logs and traces:

- `otel.exporter` controls the log exporter.
- `otel.trace_exporter` controls the trace exporter.

The local collector currently has a traces pipeline only. For live Codex
sessions to appear in Langfuse, the Codex config must set `trace_exporter`
and target the collector's HTTP traces endpoint:

```toml
[otel]
environment = "local"
log_user_prompt = false
trace_exporter = { otlp-http = { endpoint = "http://localhost:4318/v1/traces", protocol = "binary" } }
```

The same snippet is tracked in [`codex-config.toml`](codex-config.toml).
Copy it into the user's global `~/.codex/config.toml` only after manager
review. This repo intentionally does not edit that file.

Do not add `otel.exporter` for Codex logs as part of this trace recipe. The
collector logs/metrics pipeline work is tracked separately in INT-582.

### Codex Langfuse smoke test

After the collector and Langfuse are running, start a fresh Codex session
with the global config above, wait a few seconds for export, then check recent
Langfuse traces for `codex.*` names:

```bash
set -a
source infra/langfuse/.env
for page in 1 2 3 4 5; do
  curl -sS \
    -u "$LANGFUSE_INIT_PROJECT_PUBLIC_KEY:$LANGFUSE_INIT_PROJECT_SECRET_KEY" \
    "http://localhost:3000/api/public/traces?limit=100&page=$page"
done | jq -s 'map(.data // []) | add | map(select((.name // "") | startswith("codex"))) | length'
```

Expected result: a non-zero count. In the Langfuse UI, open
`http://localhost:3000`, choose project `agent-telemetry-proj`, and filter
recent traces by names starting with `codex`. A healthy live session should
include a `codex.interaction` root trace with request and tool observations
when the live exporter emits them.

## Source in Langfuse

For new ingests, source appears in two places:

- Raw OTel compatibility: `metadata.resourceAttributes.source`.
- Langfuse filtering/scanning: `metadata.agent_source`,
  `metadata.agent_family`, optional `metadata.agent_surface`, and `tags`.

Claude live recipes set `source=claude-code-cli` or
`source=claude-code-desktop`, so the collector can derive
`agent_family=claude-code` and `agent_surface=cli|desktop`. Backfill mappers
stamp the Langfuse attributes directly on every replayed span while preserving
their existing raw `source` attribute/resource.

Codex live export does not currently have a repo-owned way to add a
`source` resource attribute without editing the user's global Codex config.
The collector therefore falls back to `codex.*` span names and sets
`agent_source=codex`, `agent_family=codex`, and matching tags when those spans
arrive.

## See also

- [`infra/collector/README.md`](../infra/collector/README.md) — Collector
  bring-up, health check, smoke test.
- [`infra/langfuse/README.md`](../infra/langfuse/README.md) — Langfuse
  stack.
- Claude Code OTel reference: <https://code.claude.com/docs/en/monitoring-usage>
- Codex config reference: <https://developers.openai.com/codex/config-reference>
