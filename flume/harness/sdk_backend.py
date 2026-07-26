"""Agent SDK backend: plan-billed sessions with thinking summaries.

The Agent SDK (claude-agent-sdk) drives the real Claude Code CLI under the
user's OAuth login, so sessions draw from the Claude plan instead of
pay-per-token API. It passes `thinking: {type: "adaptive", display:
"summarized"}` through to the API (verified empirically 2026-07-07 —
undocumented for adaptive, but ThinkingBlocks come back populated), while
the CLI's own persisted transcript still carries empty thinking. This
backend therefore captures the live stream into the harness transcript
format.

Two impedance mismatches vs the API backend, both absorbed here/adapter:
- The CLI also writes its normal claude-code transcript, which the ingest
  daemon picks up separately. The harness uses a `harness-<cli-session>`
  id so the two sources never fight over one sessions row.
- Per-turn usage is not exposed on stream messages; the run's totals from
  ResultMessage go on the `end` event, and the adapter attributes them to
  the last turn when per-turn numbers are absent.

Message handling is duck-typed on type names (AssistantMessage,
ThinkingBlock, ...) so tests can stub the stream and SDK type moves don't
break us.
"""
from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any, AsyncIterator, Callable

from flume.harness import HARNESS_VERSION
from flume.harness.transcript import TranscriptWriter

DEFAULT_TRANSCRIPT_DIR = Path("~/.flume/harness")

THINKING_CONFIG = {"type": "adaptive", "display": "summarized"}


def _kind(obj: Any) -> str:
    return type(obj).__name__


def _block_dict(block: Any) -> dict[str, Any] | None:
    kind = _kind(block)
    if kind == "ThinkingBlock":
        return {"type": "thinking", "thinking": getattr(block, "thinking", "") or ""}
    if kind == "TextBlock":
        return {"type": "text", "text": getattr(block, "text", "") or ""}
    if kind == "ToolUseBlock":
        return {
            "type": "tool_use",
            "id": getattr(block, "id", ""),
            "name": getattr(block, "name", "?"),
            "input": getattr(block, "input", {}) or {},
        }
    return None


def _result_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text") or "")
            else:
                parts.append(str(item))
        return "\n".join(parts)
    return "" if content is None else str(content)


async def run_sdk_session(
    prompt: str,
    *,
    model: str | None = None,
    system: str | None = None,
    max_turns: int = 40,
    permission_mode: str | None = None,
    transcript_dir: Path = DEFAULT_TRANSCRIPT_DIR,
    query_fn: Callable[..., AsyncIterator[Any]] | None = None,
    echo: bool = True,
) -> Path:
    """Run one Agent SDK session; return the harness transcript path."""
    if query_fn is None:
        from claude_agent_sdk import ClaudeAgentOptions, query

        options = ClaudeAgentOptions(max_turns=max_turns)
        options.thinking = THINKING_CONFIG
        if model:
            options.model = model
        if system:
            options.system_prompt = system
        if permission_mode:
            options.permission_mode = permission_mode
        stream = query(prompt=prompt, options=options)
    else:
        stream = query_fn(prompt=prompt)

    session_id = f"harness-{uuid.uuid4()}"
    cwd = str(Path.cwd())
    path = transcript_dir.expanduser() / f"{session_id}.jsonl"

    with TranscriptWriter(path) as log:
        log.write({
            "type": "session_meta",
            "session_id": session_id,
            "backend": "claude-sdk",
            "model": model,
            "cwd": cwd,
            "harness_version": HARNESS_VERSION,
        })
        log.write({"type": "user", "text": prompt})

        last_stamp = time.monotonic()
        stop_reason = None
        turns = 0
        totals: dict[str, Any] | None = None
        async for message in stream:
            kind = _kind(message)
            now = time.monotonic()
            if kind == "SystemMessage":
                cli_session = (getattr(message, "data", None) or {}).get("session_id")
                if cli_session:
                    log.write({"type": "cli_session", "cli_session_id": cli_session})
            elif kind == "AssistantMessage":
                blocks = [
                    b for b in map(_block_dict, getattr(message, "content", []) or [])
                    if b is not None
                ]
                if echo:
                    for b in blocks:
                        if b["type"] == "text":
                            print(b["text"], end="", flush=True)
                        elif b["type"] == "tool_use":
                            print(f"\n[{b['name']}] {str(b['input'])[:120]}", flush=True)
                usage = getattr(message, "usage", None)
                log.write({
                    "type": "assistant",
                    "model": getattr(message, "model", None) or model,
                    "duration_ms": int((now - last_stamp) * 1000),
                    "usage": dict(usage) if isinstance(usage, dict) else None,
                    "content": blocks,
                })
                turns += 1
            elif kind == "UserMessage":
                content = getattr(message, "content", None)
                if isinstance(content, list):
                    for item in content:
                        if _kind(item) == "ToolResultBlock" or (
                            isinstance(item, dict) and item.get("type") == "tool_result"
                        ):
                            get = item.get if isinstance(item, dict) else (
                                lambda key, _i=item: getattr(_i, key, None)
                            )
                            log.write({
                                "type": "tool_result",
                                "tool_use_id": get("tool_use_id") or "",
                                "is_error": bool(get("is_error")),
                                "output": _result_text(get("content")),
                            })
            elif kind == "ResultMessage":
                stop_reason = getattr(message, "subtype", None) or "end_turn"
                usage = getattr(message, "usage", None)
                if isinstance(usage, dict):
                    totals = {
                        "input_tokens": usage.get("input_tokens") or 0,
                        "output_tokens": usage.get("output_tokens") or 0,
                        "cache_read_input_tokens":
                            usage.get("cache_read_input_tokens") or 0,
                        "cache_creation_input_tokens":
                            usage.get("cache_creation_input_tokens") or 0,
                    }
            last_stamp = now

        log.write({
            "type": "end",
            "stop_reason": stop_reason,
            "turns": turns,
            "usage": totals,
        })
    if echo:
        print(f"\n[transcript: {path}]")
    return path
