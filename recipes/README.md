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

The CLI script and the desktop plist set the **same** env vars with one
deliberate difference: `OTEL_RESOURCE_ATTRIBUTES=source=claude-code-cli` vs
`source=claude-code-desktop`. That's how cross-source comparisons stay
honest — a Langfuse filter on `resource.source` separates them cleanly even
though they share the binary, model, and account.

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

## See also

- [`infra/collector/README.md`](../infra/collector/README.md) — Collector
  bring-up, health check, smoke test.
- [`infra/langfuse/README.md`](../infra/langfuse/README.md) — Langfuse
  stack.
- Claude Code OTel reference: <https://code.claude.com/docs/en/monitoring-usage>
