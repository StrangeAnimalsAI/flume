# Live-ingest recipes

Per-source env / config to point real, running coding-agent surfaces at the
local OTel Collector. The Collector is the prerequisite — see
[`infra/collector/README.md`](../infra/collector/README.md) for bring-up.

```
agent traces  ->  http://localhost:4318  ->  collector  ->  Langfuse
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
echo "source ${PWD}/recipes/claude-code-cli.env.sh" >> ~/.zshrc
```

To turn it off without editing `~/.zshrc`, `unset CLAUDE_CODE_ENABLE_TELEMETRY`
in the current shell.

### CLI trace grouping caveat

Claude Code's trace docs say each user prompt starts a
`claude_code.interaction` span, with `claude_code.llm_request`,
`claude_code.tool`, and related spans as children. They also say Agent SDK and
non-interactive `claude -p` runs honor inbound `TRACEPARENT` / `TRACESTATE`,
while interactive CLI sessions ignore inbound `TRACEPARENT`.

In local Langfuse audits against Claude Code 2.1.122, live CLI telemetry can
still split a single `session.id` across multiple trace IDs: one trace may
contain `claude_code.interaction` plus a child request, while later
`claude_code.llm_request` spans from the same session appear as top-level
traces with no parent. That means this is not just a Langfuse display issue;
the child spans arrive without parent linkage and, in observed cases, with
different trace IDs.

Until Claude's native exporter emits stable interaction grouping for every
live CLI span, inspect native live sessions by `session.id` as well as by
trace ID. Use this check to summarize recent Langfuse data and flag split
sessions or orphan child spans:

```bash
uv run python -m agent_telemetry.analysis.claude_live_trace_check
```

The collector should not synthesize missing `claude_code.interaction` roots:
repairing this safely would require stateful, session-aware grouping by
`session.id` and time window, then rewriting trace/parent IDs. That belongs
in an offline analysis/backfill layer or upstream exporter behavior, not in
the stateless local collector pipeline.

## Desktop: load, then relaunch

The desktop app inherits env from the launchd user-domain session, not your
zsh. Loading the plist runs `launchctl setenv` for each variable; you must
**quit and relaunch Claude Desktop** for the new vars to take effect (a
running app already has the old, empty env baked in).

```bash
cp recipes/claude-code-desktop.plist \
   ~/Library/LaunchAgents/io.agent-telemetry-claude-code-desktop.plist
launchctl load -w ~/Library/LaunchAgents/io.agent-telemetry-claude-code-desktop.plist

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

Note the Langfuse base path is `/api/public/otel`; the trace endpoint is
`/v1/traces`. The Collector hides that detail; bypassing means you wear it.

## What the env vars actually do

Source the script then `env | grep -E '^(CLAUDE|OTEL)_'` to inspect. Quick
notes per var:

- `CLAUDE_CODE_ENABLE_TELEMETRY=1` — master switch. Without it, no signals.
- `CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1` — turns on the spans pipeline.
  Traces are documented as **beta** as of 2026-04. Without this, no
  `claude_code.interaction` / `claude_code.llm_request` /
  `claude_code.tool` spans.
- `OTEL_TRACES_EXPORTER=otlp` — sends trace spans to the Collector.
- `OTEL_METRICS_EXPORTER=none`, `OTEL_LOGS_EXPORTER=none` — intentionally
  disabled. The local Langfuse OTLP stack does not expose a supported,
  inspectable logs path (`/v1/logs` returns 404), and raw OTLP metrics are
  not visible in the Langfuse trace/session UI or public trace API. Langfuse
  product metrics should be read from ingested traces/observations instead.
- `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`,
  `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` — Collector's HTTP
  receiver.
- `OTEL_LOG_TOOL_DETAILS=1` — un-redacts Bash commands, file paths, MCP
  tool names in `tool_result` events and span attrs.
- `OTEL_LOG_TOOL_CONTENT=1` — un-redacts tool input/output bodies in span
  events, capped at 60 KB. Requires tracing.
- `OTEL_TRACES_EXPORT_INTERVAL=2000` — shorter than the 5s docs default so
  quick `claude -p` runs flush before exit.
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

## Signal Support

| Signal | Sender recipe | Collector pipeline | Langfuse visibility |
| ------ | ------------- | ------------------ | ------------------- |
| Traces | enabled       | forwarded          | Trace/session list, observations, public traces API |
| Logs   | disabled      | not configured     | Unsupported here; direct `/api/public/otel/v1/logs` returns 404 |
| Metrics| disabled      | not configured     | Use Langfuse Metrics API over trace/observation data, not raw OTLP metric points |

For trace checks, open `http://localhost:3000` and inspect the trace/session
list, or query `GET /api/public/traces`. Logs and raw OTLP metrics are
disabled at the Claude sender so they are not silently dropped by the
trace-only Collector.

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

The collector also drops known Docker/buildx/BuildKit infrastructure traces
before source metadata projection. That prevents subprocess spans in
BuildKit's `moby.buildkit.*`, `moby.filesync.*`, and `moby.auth.*`
namespaces, or resources whose `service.name` identifies Docker/buildx, from
showing up as Codex or Claude Code sessions when they inherit agent OTEL env
vars. After changing the filter, run
`uv run python -m agent_telemetry.analysis.collector_noise_check` to summarize
recent Langfuse traces by source/service and confirm the noise is absent.

The collector classifies Codex-owned live services before copying raw
`resource.source`. This keeps `service.name=codex-app-server` and
`service.name=codex_exec` traces filterable as Codex even when those processes
inherit `OTEL_RESOURCE_ATTRIBUTES=source=claude-code-cli` from a shell.

### Codex Langfuse smoke test

After the collector and Langfuse are running, start a fresh Codex session
with the global config above, wait a few seconds for export, then check recent
Langfuse traces for `service.name=codex-app-server` classified as Codex:

```bash
docker exec langfuse-langfuse-web-1 node -e '
const pk = process.env.LANGFUSE_INIT_PROJECT_PUBLIC_KEY;
const sk = process.env.LANGFUSE_INIT_PROJECT_SECRET_KEY;
const auth = "Basic " + Buffer.from(`${pk}:${sk}`).toString("base64");
const base = "http://langfuse-web:3000";
const first = (...xs) => xs.find((v) => v !== undefined && v !== null && v !== "") || "unknown";
const md = (x) => (x && x.metadata) || {};
const attrs = (x) => md(x).attributes || {};
const res = (x) => md(x).resourceAttributes || {};
(async () => {
  const list = await fetch(`${base}/api/public/traces?limit=50`, {headers: {Authorization: auth}});
  const rows = (await list.json()).data || [];
  const out = [];
  for (const row of rows) {
    const detail = await fetch(`${base}/api/public/traces/${row.id}`, {headers: {Authorization: auth}});
    const trace = await detail.json();
    const observations = Array.isArray(trace.observations) ? trace.observations : [];
    const service = first(res(trace)["service.name"], res(row)["service.name"], ...observations.map((o) => res(o)["service.name"]));
    if (service !== "codex-app-server") continue;
    out.push({
      id: trace.id,
      name: trace.name,
      source: first(res(trace).source, res(row).source, ...observations.map((o) => res(o).source)),
      agent_source: first(md(trace).agent_source, md(row).agent_source, ...observations.map((o) => attrs(o)["langfuse.trace.metadata.agent_source"])),
      agent_family: first(md(trace).agent_family, md(row).agent_family, ...observations.map((o) => attrs(o)["langfuse.trace.metadata.agent_family"])),
    });
  }
  console.log(JSON.stringify(out.slice(0, 5), null, 2));
})().catch((err) => { console.error(err); process.exit(1); });
'
```

Expected result for fresh traces: `source` may still show the inherited raw
resource value, but `agent_source` and `agent_family` should both be `codex`.
In the Langfuse UI, open `http://localhost:3000`, choose project
`agent-telemetry-proj`, and filter recent traces by `metadata.agent_source =
codex` or tag `agent:codex`.

### Live Codex app-server vs rollout backfill

Live Codex app-server telemetry is useful for freshness checks and partial
turn/tool/model evidence, but it is not yet the canonical session analysis
view that rollout backfill provides.

Observed live app-server traces expose `turn.id`/`turn_id` on turn and model
spans. Some tool spans, especially `mcp.tools.call`, also expose
`session.id`/`conversation.id`, `turn.id`, `tool.name`, and tool call IDs.
The runtime `thread.id` attribute is an OS/runtime worker thread identity and
must not be treated as a Codex session.

For LLM activity, live traces expose model/streaming candidates such as
`model_client.stream_responses_websocket`, `run_sampling_request`, and
`try_run_sampling_request`. Token counts currently appear on `handle_responses`
as `gen_ai.usage.*` attributes, not as Langfuse usage totals. Tool activity can
be read from `mcp.tools.call` spans and direct tool-name spans with call IDs.
Internal spans such as `list_all_tools`, `build_tool_call`, and
`handle_tool_call` are app-server mechanics, not faithful `codex.tool`
records by themselves.

As of the INT-695 check, recent live traces did not include Langfuse
`input`/`output` payloads or prompt/transcript text attributes, even when they
included model, turn, tool, duration, and token metadata. Use
`agent_telemetry.analysis.codex_live_trace_check` to audit the current live
shape. Use Codex rollout backfill for the canonical
`codex.interaction` / `codex.llm_request` / `codex.tool` session view until
live telemetry carries prompt/transcript payloads and a session-root span.

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

Codex live app-server traces may inherit raw `source=claude-code-cli` from
the launching shell. The collector leaves that raw resource attribute intact
for compatibility, but `service.name` values beginning with `codex` win for
Langfuse metadata: `agent_source=codex`, `agent_family=codex`, and matching
tags. If no Codex service name is present, the collector still falls back to
`codex.*` span names.

## See also

- [`infra/collector/README.md`](../infra/collector/README.md) — Collector
  bring-up, health check, smoke test.
- [`infra/langfuse/README.md`](../infra/langfuse/README.md) — Langfuse
  stack.
- Claude Code OTel reference: <https://code.claude.com/docs/en/monitoring-usage>
- Codex config reference: <https://developers.openai.com/codex/config-reference>
