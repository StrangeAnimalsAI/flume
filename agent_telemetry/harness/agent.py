"""Minimal agent loop on the Anthropic SDK with full-fidelity tracing.

The one capability Claude Code cannot provide: `thinking: {type:
"adaptive", display: "summarized"}` — readable reasoning summaries,
persisted verbatim to the transcript and (by default) ingested straight
into the session store at the end of the run.

    agent-telemetry-harness "why is retention skipping codex blobs?"

Tool surface is deliberately minimal: the Anthropic-defined bash tool.
This is a tracer for audits, not a Claude Code replacement.
"""
from __future__ import annotations

import argparse
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

from agent_telemetry.harness import HARNESS_VERSION
from agent_telemetry.harness.transcript import TranscriptWriter, content_block_dict

DEFAULT_MODEL = "claude-opus-4-8"
DEFAULT_TRANSCRIPT_DIR = Path("~/.agent-telemetry/harness")
BASH_TOOL = {"type": "bash_20250124", "name": "bash"}
_OUTPUT_CAP = 100_000  # chars of tool output kept in context/transcript


def run_session(
    prompt: str,
    *,
    model: str = DEFAULT_MODEL,
    effort: str | None = None,
    system: str | None = None,
    max_turns: int = 40,
    max_tokens: int = 16000,
    bash_timeout: int = 120,
    transcript_dir: Path = DEFAULT_TRANSCRIPT_DIR,
    client: Any = None,
    echo: bool = True,
) -> Path:
    """Run one agent session; return the transcript path."""
    if client is None:
        import anthropic

        client = anthropic.Anthropic()

    session_id = str(uuid.uuid4())
    cwd = str(Path.cwd())
    path = transcript_dir.expanduser() / f"{session_id}.jsonl"

    request: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "tools": [BASH_TOOL],
        # The reason this harness exists: readable reasoning summaries.
        # The API default is display: "omitted" (empty thinking text).
        "thinking": {"type": "adaptive", "display": "summarized"},
    }
    if effort:
        request["output_config"] = {"effort": effort}
    if system:
        request["system"] = system

    with TranscriptWriter(path) as log:
        log.write({
            "type": "session_meta",
            "session_id": session_id,
            "model": model,
            "effort": effort,
            "cwd": cwd,
            "harness_version": HARNESS_VERSION,
        })
        log.write({"type": "user", "text": prompt})
        messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]

        stop_reason = None
        turns = 0
        for _ in range(max_turns):
            t0 = time.monotonic()
            with client.messages.stream(messages=messages, **request) as stream:
                if echo:
                    for text in stream.text_stream:
                        print(text, end="", flush=True)
                response = stream.get_final_message()
            duration_ms = int((time.monotonic() - t0) * 1000)
            turns += 1
            stop_reason = response.stop_reason
            usage = response.usage
            log.write({
                "type": "assistant",
                "model": response.model,
                "stop_reason": stop_reason,
                "duration_ms": duration_ms,
                "usage": {
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens or 0,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens or 0,
                },
                "content": [content_block_dict(b) for b in response.content],
            })
            # Echo blocks back unchanged — required for thinking continuity.
            messages.append({"role": "assistant", "content": response.content})

            if stop_reason == "pause_turn":
                continue
            if stop_reason != "tool_use":
                break

            results = []
            for block in response.content:
                if getattr(block, "type", None) != "tool_use":
                    continue
                output, is_error, tool_ms = _run_bash(
                    block.input, timeout=bash_timeout, cwd=cwd, echo=echo
                )
                log.write({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "name": block.name,
                    "duration_ms": tool_ms,
                    "is_error": is_error,
                    "output": output,
                })
                results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": output,
                    "is_error": is_error,
                })
            # All results for one assistant turn go in ONE user message.
            messages.append({"role": "user", "content": results})

        log.write({"type": "end", "stop_reason": stop_reason, "turns": turns})
    if echo:
        print(f"\n[transcript: {path}]")
    return path


def _run_bash(
    tool_input: dict[str, Any], *, timeout: int, cwd: str, echo: bool
) -> tuple[str, bool, int]:
    if tool_input.get("restart"):
        return "(session state is per-command; nothing to restart)", False, 0
    command = tool_input.get("command") or ""
    if echo:
        print(f"\n$ {command}", flush=True)
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            ["bash", "-c", command],
            capture_output=True, text=True, timeout=timeout, cwd=cwd,
        )
        output = proc.stdout + proc.stderr
        is_error = proc.returncode != 0
    except subprocess.TimeoutExpired:
        output, is_error = f"(timed out after {timeout}s)", True
    tool_ms = int((time.monotonic() - t0) * 1000)
    if len(output) > _OUTPUT_CAP:
        output = output[:_OUTPUT_CAP] + f"\n... ({len(output) - _OUTPUT_CAP} chars truncated)"
    return output or "(no output)", is_error, tool_ms


def _ingest(path: Path) -> None:
    from agent_telemetry.store.archive import open_archive
    from agent_telemetry.store.base import open_store
    from agent_telemetry.store.ingest import ingest_path

    with open_store() as store, open_archive() as archive:
        outcome = ingest_path(store, "harness", path, archive=archive)
    if outcome is not None:
        print(f"[ingested: {outcome.session_id}]")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="agent-telemetry-harness",
        description="Minimal traced agent: thinking summaries land in the store.",
    )
    parser.add_argument("prompt")
    parser.add_argument(
        "--backend", default="api", choices=("api", "sdk"),
        help="api: raw Anthropic SDK, pay-per-token, bash tool only. "
        "sdk: Agent SDK on the Claude plan login, full Claude Code tool suite.",
    )
    parser.add_argument("--model", default=None)
    parser.add_argument(
        "--effort", default=None,
        choices=("low", "medium", "high", "xhigh", "max"),
        help="API backend only.",
    )
    parser.add_argument("--system", default=None)
    parser.add_argument("--max-turns", type=int, default=40)
    parser.add_argument("--max-tokens", type=int, default=16000)
    parser.add_argument(
        "--permission-mode", default=None,
        help="SDK backend only (e.g. acceptEdits, bypassPermissions).",
    )
    parser.add_argument("--transcript-dir", type=Path, default=DEFAULT_TRANSCRIPT_DIR)
    parser.add_argument(
        "--no-ingest", action="store_true",
        help="Skip ingesting the transcript into the session store at exit.",
    )
    args = parser.parse_args(argv)

    if args.backend == "sdk":
        import anyio

        from agent_telemetry.harness.sdk_backend import run_sdk_session

        path = anyio.run(
            lambda: run_sdk_session(
                args.prompt,
                model=args.model,
                system=args.system,
                max_turns=args.max_turns,
                permission_mode=args.permission_mode,
                transcript_dir=args.transcript_dir,
            )
        )
    else:
        path = run_session(
            args.prompt,
            model=args.model or DEFAULT_MODEL,
            effort=args.effort,
            system=args.system,
            max_turns=args.max_turns,
            max_tokens=args.max_tokens,
            transcript_dir=args.transcript_dir,
        )
    if not args.no_ingest:
        _ingest(path)
    return 0
