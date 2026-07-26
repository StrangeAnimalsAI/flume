"""flume — the single command-line interface.

This is the only CLI surface. Each subsystem publishes its entry point
through its own package `__init__`, and this module maps a command name to
one of them; nothing here reaches into a subsystem's internals, and no
subsystem is separately invokable. Adding a command means one row in
`_COMMANDS` and one function exported from that package.

Resolution is deferred on purpose, and it is a real deferral rather than a
habit: `flume ingest` must not import the analysis stack, and `flume
analyze` must not import the harness or its optional model SDKs.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Command:
    """One subcommand: what it does, and where its entry point lives."""

    description: str
    module: str
    attr: str


# Declared once. The previous shape listed every command twice — a dict for
# the help text and an if/elif chain for the import — so a new command could
# be documented but unresolvable, or resolvable but invisible in --help.
_COMMANDS: dict[str, Command] = {
    "analyze": Command(
        "query and analyze the session store", "flume.analysis", "main"
    ),
    "serve": Command(
        "serve the store UI/API over HTTP", "flume.analysis", "serve"
    ),
    "ingest": Command(
        "auto-ingest agent session files (see --source)", "flume.ingest", "main"
    ),
    "harness": Command(
        "run the instrumented agent harness", "flume.harness", "main"
    ),
}


def _resolve(name: str) -> Callable[[list[str] | None], int]:
    command = _COMMANDS[name]
    return getattr(importlib.import_module(command.module), command.attr)


def _usage() -> str:
    lines = ["usage: flume <command> [args]", "", "commands:"]
    lines += [f"  {name:<10}{c.description}" for name, c in _COMMANDS.items()]
    lines.append("")
    lines.append("run `flume <command> --help` for command-specific usage")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(_usage())
        return 0
    command, rest = args[0], args[1:]
    if command not in _COMMANDS:
        print(f"flume: unknown command {command!r}\n\n{_usage()}", file=sys.stderr)
        return 2
    return _resolve(command)(rest)


if __name__ == "__main__":
    raise SystemExit(main())
