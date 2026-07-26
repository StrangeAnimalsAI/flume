"""Harness backends: what model flume itself drives.

A backend runs one traced agent session and writes a transcript in the
harness format (see flume/harness/transcript.py). **The transcript format is
the contract** — `flume.sources.harness` maps it to spans and content rows
without knowing or caring which backend produced it, so a session run on a
local model is analyzed exactly like one run on a hosted API.

Three ship today:

    anthropic   Anthropic Messages API, pay-per-token, bash tool. The only
                one that captures readable reasoning summaries.
    claude-sdk  Claude Agent SDK on a plan login, full Claude Code tools.
    openai      Any OpenAI-compatible /v1/chat/completions server — hosted
                OpenAI, Ollama, llama.cpp, vLLM, LM Studio, proxies.

Model-vendor SDKs are optional dependencies: a core install carries none,
and a backend reports what to install when its dependency is absent. Adding
a backend means writing the session function and registering it here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

# (extra, import name) for the dependency each backend needs, so a missing
# one produces an install hint instead of a bare ImportError.
_REQUIREMENTS = {
    "anthropic": ("anthropic", "anthropic"),
    "claude-sdk": ("claude-sdk", "claude_agent_sdk"),
    "openai": (None, None),  # stdlib only
}


@dataclass(frozen=True)
class Backend:
    name: str
    describe: str
    run: Callable[..., Any]
    is_async: bool = False


def _anthropic_backend() -> Backend:
    from flume.harness.agent import run_session

    return Backend(
        name="anthropic",
        describe="Anthropic Messages API (pay-per-token; captures thinking summaries)",
        run=run_session,
    )


def _claude_sdk_backend() -> Backend:
    from flume.harness.sdk_backend import run_sdk_session

    return Backend(
        name="claude-sdk",
        describe="Claude Agent SDK on a plan login (full Claude Code tool suite)",
        run=run_sdk_session,
        is_async=True,
    )


def _openai_backend() -> Backend:
    from flume.harness.openai_compat import run_openai_session

    return Backend(
        name="openai",
        describe="Any OpenAI-compatible server (OpenAI, Ollama, llama.cpp, vLLM)",
        run=run_openai_session,
    )


_BACKENDS: dict[str, Callable[[], Backend]] = {
    "anthropic": _anthropic_backend,
    "claude-sdk": _claude_sdk_backend,
    "openai": _openai_backend,
}


def names() -> list[str]:
    return sorted(_BACKENDS)


def get_backend(name: str) -> Backend:
    factory = _BACKENDS.get(name)
    if factory is None:
        raise SystemExit(
            f"unknown backend {name!r}; known: {', '.join(names())}"
        )
    extra, module = _REQUIREMENTS.get(name, (None, None))
    if module is not None:
        try:
            __import__(module)
        except ImportError:
            raise SystemExit(
                f"backend {name!r} needs the {module!r} package, which is "
                f"an optional dependency. Install it with: "
                f"uv pip install 'flume[{extra}]'"
            ) from None
    return factory()
