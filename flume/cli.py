"""flume — single entry point dispatching to the subsystem CLIs.

Each subcommand delegates to a subsystem's own argparse main, so
`flume analyze --help` etc. show the full per-subsystem usage.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence

_SUBCOMMANDS: dict[str, str] = {
    "analyze": "query and analyze the session store",
    "serve": "serve the store UI/API over HTTP",
    "ingest": "auto-ingest Claude Code and Codex session files",
    "harness": "run the instrumented agent harness",
}


def _resolve(name: str) -> Callable[[list[str] | None], int]:
    if name == "analyze":
        from flume.store.cli import main
    elif name == "serve":
        from flume.store.server import main
    elif name == "ingest":
        from flume.ingest.cli import main
    elif name == "harness":
        from flume.harness.agent import main
    else:  # pragma: no cover - guarded by caller
        raise KeyError(name)
    return main


def _usage() -> str:
    lines = ["usage: flume <command> [args]", "", "commands:"]
    lines += [f"  {name:<10}{desc}" for name, desc in _SUBCOMMANDS.items()]
    lines.append("")
    lines.append("run `flume <command> --help` for command-specific usage")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(_usage())
        return 0
    command, rest = args[0], args[1:]
    if command not in _SUBCOMMANDS:
        print(f"flume: unknown command {command!r}\n\n{_usage()}", file=sys.stderr)
        return 2
    return _resolve(command)(rest)


if __name__ == "__main__":
    raise SystemExit(main())
