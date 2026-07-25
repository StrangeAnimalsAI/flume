"""OpenAI-compatible backend: any server speaking /v1/chat/completions.

Covers hosted OpenAI, Ollama, llama.cpp's server, vLLM, LM Studio, and the
various proxies — they all implement the same chat-completions shape, so
one backend reaches all of them. Point it somewhere with `--base-url`:

    flume harness "..." --backend openai --base-url http://localhost:11434/v1 \\
        --model qwen3-coder

Written against `urllib` rather than an SDK: the wire format is small and
stable, and a telemetry tool has no business dragging a second vendor SDK
into its install for one endpoint.

Reasoning capture: models in this family expose reasoning inconsistently —
some return a `reasoning_content` field, some `reasoning`, most nothing at
all. Whatever is present is recorded as a thinking block; when nothing is,
the transcript simply has no thinking rows. That is a source limitation,
recorded honestly, exactly as with Codex's encrypted reasoning.

Tool-calling quality varies more than the wire format does. Verified
against Ollama 2026-07: a well-formed `tool_calls` response round-trips
correctly, but the same model on the same prompt sometimes emits tool-call
syntax the host cannot parse, and the server then returns billed output
tokens with neither text nor `tool_calls`. That is not recoverable here —
the tokens never reach the client — so the turn is annotated rather than
silently dropped (see `_degenerate_note`). Prefer models with first-class
tool support when the run depends on tools.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from flume.harness import HARNESS_VERSION
from flume.harness.transcript import TranscriptWriter

DEFAULT_BASE_URL = "http://localhost:11434/v1"
DEFAULT_TRANSCRIPT_DIR = Path("~/.flume/harness")

# The bash tool in OpenAI function-calling shape. Kept deliberately close to
# the Anthropic backend's single-tool surface so transcripts are comparable
# across backends.
BASH_TOOL = {
    "type": "function",
    "function": {
        "name": "bash",
        "description": "Run a shell command and return its combined output.",
        "parameters": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The command to run."}
            },
            "required": ["command"],
        },
    },
}


def run_openai_session(
    prompt: str,
    *,
    model: str,
    base_url: str = DEFAULT_BASE_URL,
    api_key: str | None = None,
    system: str | None = None,
    max_turns: int = 40,
    max_tokens: int = 16000,
    bash_timeout: int = 120,
    transcript_dir: Path = DEFAULT_TRANSCRIPT_DIR,
    post: Any = None,
    echo: bool = True,
) -> Path:
    """Run one session against an OpenAI-compatible server.

    `post(url, payload, headers) -> dict` is injectable for tests; the
    default posts JSON over urllib."""
    if post is None:
        post = _post_json
    key = api_key or os.environ.get("OPENAI_API_KEY") or "not-needed"
    url = base_url.rstrip("/") + "/chat/completions"

    session_id = str(uuid.uuid4())
    cwd = str(Path.cwd())
    path = transcript_dir.expanduser() / f"{session_id}.jsonl"

    messages: list[dict[str, Any]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    with TranscriptWriter(path) as log:
        log.write({
            "type": "session_meta",
            "session_id": session_id,
            "model": model,
            "cwd": cwd,
            "backend": "openai",
            "base_url": base_url,
            "harness_version": HARNESS_VERSION,
        })
        log.write({"type": "user", "text": prompt})

        stop_reason = None
        turns = 0
        for _ in range(max_turns):
            payload = {
                "model": model,
                "messages": messages,
                "tools": [BASH_TOOL],
                "max_tokens": max_tokens,
            }
            t0 = time.monotonic()
            body = post(url, payload, {"Authorization": f"Bearer {key}"})
            duration_ms = int((time.monotonic() - t0) * 1000)
            turns += 1

            choice = (body.get("choices") or [{}])[0]
            message = choice.get("message") or {}
            stop_reason = choice.get("finish_reason")
            text = message.get("content") or ""
            # Reasoning field name varies by server; take whichever exists.
            thinking = message.get("reasoning_content") or message.get("reasoning") or ""
            tool_calls = message.get("tool_calls") or []

            if echo and text:
                print(text, end="", flush=True)

            content: list[dict[str, Any]] = []
            if thinking:
                content.append({"type": "thinking", "thinking": thinking})
            if text:
                content.append({"type": "text", "text": text})
            for call in tool_calls:
                fn = call.get("function") or {}
                content.append({
                    "type": "tool_use",
                    "id": call.get("id") or "",
                    "name": fn.get("name") or "?",
                    "input": _decode_arguments(fn.get("arguments")),
                })

            usage = body.get("usage") or {}
            event: dict[str, Any] = {
                "type": "assistant",
                "model": body.get("model") or model,
                "stop_reason": stop_reason,
                "duration_ms": duration_ms,
                "usage": _usage(usage),
                "content": content,
            }
            # A turn with billed output but nothing parseable means the
            # server produced tokens it could not classify — in practice a
            # model emitting tool-call syntax its host cannot parse, which
            # is common with smaller local models. Say so instead of ending
            # the session with an empty transcript and no explanation.
            note = _degenerate_note(content, usage)
            if note:
                event["note"] = note
                print(f"\n[warning] {note}", file=sys.stderr, flush=True)
            log.write(event)
            messages.append(_assistant_message(text, tool_calls))

            if not tool_calls:
                break

            for call in tool_calls:
                fn = call.get("function") or {}
                args = _decode_arguments(fn.get("arguments"))
                output, is_error, tool_ms = run_bash(
                    args, timeout=bash_timeout, cwd=cwd, echo=echo
                )
                log.write({
                    "type": "tool_result",
                    "tool_use_id": call.get("id") or "",
                    "name": fn.get("name") or "?",
                    "duration_ms": tool_ms,
                    "is_error": is_error,
                    "output": output,
                })
                # Chat-completions wants one tool message per call.
                messages.append({
                    "role": "tool",
                    "tool_call_id": call.get("id") or "",
                    "content": output,
                })

        log.write({"type": "end", "stop_reason": stop_reason, "turns": turns})
    if echo:
        print(f"\n[transcript: {path}]")
    return path


def _degenerate_note(content: list[dict[str, Any]], usage: dict[str, Any]) -> str | None:
    """Explain a turn that billed output tokens but yielded nothing usable."""
    if content:
        return None
    produced = int(usage.get("completion_tokens") or 0)
    if produced <= 0:
        return None
    return (
        f"the server billed {produced} output tokens but returned no text and "
        "no tool calls — the model likely emitted tool-call syntax its host "
        "could not parse. Try a model with first-class tool support, or drop "
        "--backend openai for one that does."
    )


def _assistant_message(text: str, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": text or None}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return message


def _usage(usage: dict[str, Any]) -> dict[str, int]:
    """Normalize to the store's exclusive-input vocabulary.

    OpenAI-shaped usage reports prompt_tokens INCLUSIVE of the cached
    subset, same as Codex rollouts — subtract so cache-hit math stays
    uniform across every source."""
    total_input = int(usage.get("prompt_tokens") or 0)
    details = usage.get("prompt_tokens_details") or {}
    cached = int(details.get("cached_tokens") or 0)
    return {
        "input_tokens": max(total_input - cached, 0),
        "output_tokens": int(usage.get("completion_tokens") or 0),
        "cache_read_input_tokens": cached,
        "cache_creation_input_tokens": 0,
    }


def _decode_arguments(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {"command": raw}
    return decoded if isinstance(decoded, dict) else {}


def _post_json(url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict:
    data = json.dumps(payload).encode()
    request = urllib.request.Request(url, data=data, method="POST")
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:500]
        raise SystemExit(
            f"{url} returned {exc.code}: {detail}"
        ) from None
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"cannot reach {url}: {exc.reason}. Is the server running?"
        ) from None


def run_bash(
    tool_input: dict[str, Any], *, timeout: int, cwd: str, echo: bool
) -> tuple[str, bool, int]:
    """Shared with the Anthropic backend — same execution semantics."""
    from flume.harness.agent import _run_bash

    return _run_bash(tool_input, timeout=timeout, cwd=cwd, echo=echo)
