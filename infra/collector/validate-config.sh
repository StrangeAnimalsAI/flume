#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
config_file="${script_dir}/config.yaml"
image="otel/opentelemetry-collector-contrib:0.112.0"

docker run --rm \
  -v "${config_file}:/etc/otelcol-contrib/config.yaml:ro" \
  -e HOST_NAME=collector-config-test \
  -e LANGFUSE_BASIC_AUTH="Basic test" \
  -e LANGFUSE_OTLP_ENDPOINT=http://host.docker.internal:3000/api/public/otel \
  "${image}" \
  validate --config=/etc/otelcol-contrib/config.yaml
