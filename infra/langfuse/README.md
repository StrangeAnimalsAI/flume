# Self-hosted Langfuse (local)

A v0 Langfuse v3 stack for local development. The `flume` backfill
CLI and (eventually) the live OTel Collector push spans here at
`http://localhost:3000/api/public/otel/v1/traces`.

Not production. See the upstream
[docker-compose self-hosting guide](https://langfuse.com/self-hosting/docker-compose)
for real deployments.

## Services

| Service         | Image                                        | Host port         |
| --------------- | -------------------------------------------- | ----------------- |
| langfuse-web    | `docker.io/langfuse/langfuse:3`              | `3000`            |
| langfuse-worker | `docker.io/langfuse/langfuse-worker:3`       | `127.0.0.1:3030`  |
| postgres        | `docker.io/postgres:17`                      | `127.0.0.1:55432` |
| clickhouse      | `docker.io/clickhouse/clickhouse-server`     | `127.0.0.1:8123`, `127.0.0.1:9000` |
| redis           | `docker.io/redis:7`                          | `127.0.0.1:56379` |
| minio           | `cgr.dev/chainguard/minio`                   | `9090` (S3 API), `127.0.0.1:9091` (console) |

### Port remaps (deviations from canonical)

The canonical compose publishes Postgres on host `5432` and Redis on host
`6379`. Both are already in use on this machine by native daemons, so they are
remapped to `55432` and `56379` respectively. The containers still speak
`5432`/`6379` internally on the compose network — only the host-side binding
changed.

## Setup

```bash
cd infra/langfuse
cp .env.example .env
# Edit .env. Every `replace-me` must be replaced.
#   - NEXTAUTH_SECRET / SALT:   openssl rand -base64 32
#   - ENCRYPTION_KEY:           openssl rand -hex 32   (exactly 64 hex chars)
#   - *_PASSWORD, REDIS_AUTH:   openssl rand -hex 24   (keep URL-safe)
#   - DATABASE_URL:             must match POSTGRES_PASSWORD
#   - LANGFUSE_INIT_PROJECT_PUBLIC_KEY:  pk-lf-$(openssl rand -hex 16)
#   - LANGFUSE_INIT_PROJECT_SECRET_KEY:  sk-lf-$(openssl rand -hex 32)
#   - LANGFUSE_INIT_USER_PASSWORD:       openssl rand -base64 18
docker compose up -d
```

First boot takes ~60s: postgres initializes, langfuse-web runs 391 Prisma
migrations, then the seed block (`LANGFUSE_INIT_*`) creates the org, project,
user, and API-key pair from the `.env` values. Ready when:

```bash
curl -s http://localhost:3000/api/public/health
# {"status":"OK","version":"3.170.0"}
```

UI: <http://localhost:3000>. Sign in with `LANGFUSE_INIT_USER_EMAIL` /
`LANGFUSE_INIT_USER_PASSWORD` from `.env`.

## First-boot seeding vs. UI click-through

The stack uses the `LANGFUSE_INIT_*` env vars, so no UI click-through is
needed on first boot — one org (`flume-org`), one project
(`flume-proj`), one user, and one API-key pair are seeded directly.
These vars are **ignored on subsequent boots** once the records exist in
postgres; rotating them after the fact has no effect.

If you need a second project (e.g., to isolate backfill from live ingestion),
do it through the UI: Settings → API Keys → Create new key pair. The
basic-auth construction is the same.

## Basic-auth for OTLP ingestion

Langfuse's `/api/public/otel/v1/traces` endpoint authenticates with HTTP Basic
using the project's public key as username and secret key as password:

```bash
PK="$LANGFUSE_INIT_PROJECT_PUBLIC_KEY"   # pk-lf-...
SK="$LANGFUSE_INIT_PROJECT_SECRET_KEY"   # sk-lf-...
AUTH=$(printf '%s:%s' "$pk" "$sk" | base64)
# Send as: Authorization: Basic $AUTH
```

For the backfill CLI (INT-432), pass the header via the OTel SDK's standard
env var:

```bash
export OTEL_EXPORTER_OTLP_TRACES_HEADERS="Authorization=Basic $AUTH"
uv run python -m flume.backfill.claude_code \
  --path ~/.claude/projects/<project>/<session>.jsonl \
  --endpoint http://localhost:3000/api/public/otel/v1/traces
```

## Smoke tests (as run during INT-434 bring-up)

### (a) Hand-crafted OTLP JSON

```bash
NOW_NS=$(python3 -c 'import time; print(int(time.time()*1e9))')
END_NS=$((NOW_NS + 1_000_000_000))
cat > /tmp/otlp-span.json <<JSON
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"int-434-smoke"}}]},"scopeSpans":[{"scope":{"name":"int-434-curl"},"spans":[{"traceId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","spanId":"bbbbbbbbbbbbbbbb","name":"int-434-smoke-span","kind":1,"startTimeUnixNano":"${NOW_NS}","endTimeUnixNano":"${END_NS}","attributes":[{"key":"gen_ai.system","value":{"stringValue":"anthropic"}}],"status":{"code":1}}]}]}]}
JSON
curl -sS -i -X POST \
  -H "Authorization: Basic $AUTH" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/otlp-span.json \
  http://localhost:3000/api/public/otel/v1/traces
```

Expect `HTTP/1.1 200 OK` with a JSON body announcing an `otel-ingestion-job`
queued against `projectId: flume-proj`. The trace appears in the UI
a few seconds later.

### (b) Real backfill via the INT-432 CLI

```bash
uv run python -m flume.backfill.claude_code \
  --path ~/.claude/projects/<project>/<session>.jsonl \
  --endpoint http://localhost:3000/api/public/otel/v1/traces
```

The CLI uses `BatchSpanProcessor`, which logs export failures but does not
crash on 4xx/5xx. To see the response code explicitly, re-run with
`OTEL_PYTHON_LOG_LEVEL=debug` and a Python `logging.basicConfig(level=DEBUG)`
shim — urllib3 will log the `POST .../v1/traces HTTP/1.1 200 898` line.

## Reconcile a stale deterministic trace

Langfuse ingestion is first-write-wins for deterministic trace/observation
IDs in this local stack. Replaying the same backfill after a mapper fix is
idempotent for new data, but it does not reliably update already-ingested
observation fields such as `startTime` and `endTime`. INT-455 reproduced this
with a Claude Code trace first written by the pre-INT-436 timestamp mapper:
the fixed backfill emitted `.827Z`, while the existing Langfuse observation
kept the older `.826Z` value.

Do not wipe the whole local Langfuse database for this. Delete just the stale
trace, then rerun the deterministic backfill for the original source file:

```bash
# Dry-run: prints the trace name, timestamp, and observation count.
uv run python -m flume.analysis.langfuse_trace_reconcile \
  e08b95ad6fd560012ff085a57be21609

# Delete exactly that trace from local Langfuse.
uv run python -m flume.analysis.langfuse_trace_reconcile \
  e08b95ad6fd560012ff085a57be21609 --yes

# Recreate it from the fixed mapper.
uv run python -m flume.backfill.claude_code \
  --path ~/.claude/projects/<project>/<session>.jsonl \
  --endpoint http://localhost:3000/api/public/otel/v1/traces
```

The helper defaults to `http://localhost:3000`, reads credentials from
`infra/langfuse/.env`, refuses non-local URLs unless explicitly overridden,
uses a trace-scoped ClickHouse mutation against the local
`langfuse-clickhouse-1` container, and verifies the trace is missing after
deletion. The public trace DELETE endpoint alone is not enough in this stack:
the trace can still be reconstructed from ClickHouse observation rows. The
parity checker also prints a stale-ingest diagnostic when only known timestamp
fields look like old first-write data.

## Operations

```bash
docker compose logs -f langfuse-web        # NextAuth / ingestion logs
docker compose logs -f langfuse-worker     # ClickhouseWriter, queue processing
docker compose ps                          # status
docker compose down                        # stop (keeps volumes)
docker compose down -v                     # WIPE (drops postgres/clickhouse/minio data)
```

## Secrets hygiene

- Real secrets live in `infra/langfuse/.env` which is gitignored via the
  repo-root `.gitignore` rule `infra/**/.env`.
- `infra/langfuse/.env.example` is the tracked template.
- Never `git add infra/langfuse/.env`. If you slip: `git rm --cached
  infra/langfuse/.env` and verify with `git check-ignore -v
  infra/langfuse/.env`.

## Known rough edges

- **`dev@localhost` fails email validation.** Langfuse's zod schema rejects
  TLD-less addresses. Use `dev@localhost.test` or similar.
- **`POSTGRES_PASSWORD` must be URL-safe.** It gets embedded into
  `DATABASE_URL`. `openssl rand -base64` values contain `/` and `+` which need
  percent-encoding; `openssl rand -hex` sidesteps that.
- **`DATABASE_URL` must be explicit.** The compose default hard-codes
  `postgres:postgres@...`, so rotating `POSTGRES_PASSWORD` without updating
  `DATABASE_URL` produces a P1000 auth failure loop in `langfuse-web`.
- **MinIO publishes `9090:9000` on all interfaces**, not localhost. This
  matches the canonical compose — the intent is that
  `LANGFUSE_S3_MEDIA_UPLOAD_ENDPOINT` points to `http://localhost:9090` so
  browsers can fetch pre-signed URLs — but be aware on a shared network.
