# Local OTel Collector

Single fan-in point for every agent-telemetry sender on this machine — the
backfill CLIs today, live Claude Code / Codex recipes (INT-437, INT-438)
tomorrow. Everything speaks OTLP to `http://localhost:4318`; the collector
batches, enriches, and forwards to the local Langfuse stack with auth.

The file checks into git; real secrets live in `.env` which is repo-wide
gitignored via `infra/**/.env`.

## Services

| Name            | Image                                              | Host port |
| --------------- | -------------------------------------------------- | --------- |
| otel-collector  | `otel/opentelemetry-collector-contrib:0.112.0`     | `127.0.0.1:4317` (gRPC), `127.0.0.1:4318` (HTTP) |

The collector reaches Langfuse at `http://host.docker.internal:3000` from
inside the container, not via the `langfuse_default` network. Keeps the two
compose projects independent.

## Setup

```bash
cd infra/collector
cp .env.example .env

# Fill in .env. Construct the basic-auth header with the Langfuse project
# keys from infra/langfuse/.env:
PK=$(grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' ../langfuse/.env | cut -d= -f2)
SK=$(grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' ../langfuse/.env | cut -d= -f2)
AUTH="Basic $(printf '%s:%s' "$PK" "$SK" | base64)"
# Paste $AUTH into LANGFUSE_BASIC_AUTH and set HOST_NAME.

docker compose up -d
docker compose ps
# otel-collector running; 127.0.0.1:4317-4318 bound.
```

## Signal support

This local Collector intentionally forwards traces only.

| Signal | Collector pipeline | Langfuse support in this stack |
| ------ | ------------------ | ------------------------------ |
| Traces | yes                | Visible in the trace/session UI and `/api/public/traces` |
| Logs   | no                 | Unsupported here; direct `/api/public/otel/v1/logs` returns 404 |
| Metrics| no                 | Langfuse product metrics derive from traces/observations, not raw OTLP metric points |

The Claude live recipes set `OTEL_LOGS_EXPORTER=none` and
`OTEL_METRICS_EXPORTER=none` so logs/metrics are not silently sent to a
collector that will not forward them.

## Config validation

Validate the Collector config without starting the local collector or reading
real `.env` secrets:

```bash
infra/collector/validate-config.sh
```

The script runs the pinned `otel/opentelemetry-collector-contrib:0.112.0`
image with dummy env vars and executes `validate
--config=/etc/otelcol-contrib/config.yaml`.

## Recent noise check

After restarting the collector and running a Docker/buildx-heavy agent task,
check recent Langfuse traces for leftover infrastructure noise:

```bash
uv run python -m agent_telemetry.analysis.collector_noise_check
```

The check reads Langfuse credentials from `infra/langfuse/.env` by default
or from `--public-key` / `--secret-key`, summarizes recent observations by
`source` and `service.name`, and exits non-zero if it still sees known
Docker/buildx signatures.

## Claude live grouping check

Native Claude Code live traces may split one `session.id` across multiple
Langfuse trace IDs when child spans arrive without a
`claude_code.interaction` parent. Summarize recent Claude observations with:

```bash
uv run python -m agent_telemetry.analysis.claude_live_trace_check
```

This is a detection check, not a collector repair path. The collector keeps
trace processing stateless; safely rebuilding missing interaction roots would
require session-aware grouping and trace/parent ID rewriting.

## Smoke test

Send a hand-crafted OTLP JSON span to the collector and verify it lands in
Langfuse:

```bash
NOW_NS=$(python3 -c 'import time; print(int(time.time()*1e9))')
END_NS=$((NOW_NS + 1_000_000_000))
cat > /tmp/otlp-span.json <<JSON
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"collector-smoke"}}]},"scopeSpans":[{"scope":{"name":"smoke"},"spans":[{"traceId":"cccccccccccccccccccccccccccccccc","spanId":"dddddddddddddddd","name":"collector-smoke-span","kind":1,"startTimeUnixNano":"${NOW_NS}","endTimeUnixNano":"${END_NS}","status":{"code":1}}]}]}]}
JSON

curl -sS -i -X POST \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/otlp-span.json \
  http://localhost:4318/v1/traces
```

Expect `HTTP/1.1 200 OK` from the collector. A few seconds later the trace
shows up in the Langfuse UI under project `agent-telemetry-proj`. Note:
the trace id is what you sent to the collector, not what the collector
generated — deterministic IDs round-trip cleanly.

## Backfill via the collector

The backfill CLIs default to `http://localhost:4318/v1/traces` — that's the
collector. No flag needed:

```bash
uv run python -m agent_telemetry.backfill.claude_code \
  --path ~/.claude/projects/<project>/<session>.jsonl
```

Override with `--endpoint http://localhost:3000/api/public/otel/v1/traces`
to skip the collector and hit Langfuse directly (useful for debugging the
collector itself without taking it out of the loop).

## LaunchAgent — start at login

A LaunchAgent plist ships under `launchd/`. It is NOT auto-loaded. Install
manually when you want the collector up at every login:

```bash
cp launchd/com.jameshtimmins.agent-telemetry-collector.plist ~/Library/LaunchAgents/
launchctl load -w ~/Library/LaunchAgents/com.jameshtimmins.agent-telemetry-collector.plist

# Verify:
launchctl list | grep agent-telemetry-collector
tail -f /tmp/agent-telemetry-collector.stdout.log
```

Stop / unload:

```bash
launchctl unload ~/Library/LaunchAgents/com.jameshtimmins.agent-telemetry-collector.plist
rm ~/Library/LaunchAgents/com.jameshtimmins.agent-telemetry-collector.plist
```

The plist runs `docker compose ... up -d` with a hard-coded
`WorkingDirectory`. If you move the checkout, edit that path before
reloading.

## Operations

```bash
docker compose logs -f otel-collector      # see receiver/exporter logs
docker compose ps                          # status
docker compose down                        # stop (no volumes to preserve)
```

## What the config does

Pipeline: `otlp receivers → memory_limiter → filter/drop_non_agent_infra →
resource/enrich → transform/claude_code_genai → transform/langfuse_source →
transform/pii → batch → otlphttp/langfuse`.

- **`filter/drop_non_agent_infra`** drops Docker/buildx/BuildKit internal
  traces that can inherit the agent OTEL environment from tool subprocesses
  and otherwise appear as agent sessions in Langfuse. It filters spans whose
  names or `rpc.service` attrs are in BuildKit's `moby.buildkit.*`,
  `moby.filesync.*`, or `moby.auth.*` gRPC namespaces, plus spans whose
  resource `service.name` is `docker`, `buildx`, `buildkit`, `moby`, or those
  names followed by `.`, `_`, or `-`. Real Claude Code and Codex agent spans
  use `claude_code.*` / `codex.*` names or source metadata and are preserved.
- **`resource/enrich`** tags `host.name` (from the `HOST_NAME` env var),
  fills `service.instance.id` if a sender didn't set one, and upserts
  `deployment.environment=local` + `telemetry.collector.name`. Does NOT
  touch the `source` attribute — backfill and live recipes own that.
- **`transform/claude_code_genai`** copies native Claude Code live token
  attrs into the same `gen_ai.usage.*` keys used by backfill. It preserves
  raw native attrs (`input_tokens`, `output_tokens`, `cache_read_tokens`,
  `cache_creation_tokens`) and only fills missing canonical attrs:
  `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`,
  `gen_ai.usage.cache_read_input_tokens`, and
  `gen_ai.usage.cache_creation_input_tokens`.
- **`transform/langfuse_source`** copies sender identity into Langfuse
  trace metadata on every span:
  `langfuse.trace.metadata.agent_source`,
  `langfuse.trace.metadata.agent_family`, optional
  `langfuse.trace.metadata.agent_surface`, and `langfuse.trace.tags`. It
  preserves raw `resource.source` and falls back to known span name prefixes
  (`codex.*`, `claude_code.*`) when a sender did not set `source`.
- **`transform/pii`** is a stub. It runs a no-op OTTL statement today; real
  redactions land as a one-line change when we need them.
- **`batch`** uses the 5s / 8192-span OTel default.
- **`memory_limiter`** caps at 512 MiB.
- **Logs/metrics** are not configured. Senders that need Langfuse-visible
  data should put it on trace spans/observations until this stack has a
  supported, inspectable logs or raw-metrics destination.

## Known rough edges

- **macOS `base64` and the auth header.** `printf ... | base64` on macOS
  emits without a trailing newline, which is what we want. On Linux, pipe
  through `tr -d '\n'` to match — a stray newline in the `Authorization`
  header will fail auth with an opaque 401.
- **Docker Desktop must be running** for the LaunchAgent to succeed. If
  Docker isn't up at login, the agent's `docker compose up -d` exits
  non-zero and the collector isn't available until you manually bring it
  up. No retry loop; this is dev.
- **Collector version is pinned** to `0.112.0`. Bumping is cheap but the
  OTTL dialect has shifted between minor releases; re-smoke the config
  when upgrading.

## Secrets hygiene

- Real secrets live in `infra/collector/.env` — gitignored by the repo-root
  rule `infra/**/.env`.
- `infra/collector/.env.example` is the tracked template.
- Never `git add infra/collector/.env`. Verify with
  `git check-ignore -v infra/collector/.env` before every commit.
