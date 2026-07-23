"""Tool taxonomy: classify a tool call by kind and vendor.

Exploratory analysis constantly needs "group by tool kind" (mcp vs shell vs
web vs file...) and "which MCP vendor". Without a stored classification, every
query hand-writes `name LIKE 'mcp__%'` and `IN ('WebFetch', ...)`. This
centralizes the rules once, in Python (`tool_kind`/`tool_vendor`) and as SQL
CASE expressions (`kind_sql`/`vendor_sql`) that back the `tool_calls_ext` view
— keep the two in sync.
"""
from __future__ import annotations

_SHELL = ("Bash", "exec_command", "write_stdin")
_WEB = ("WebFetch", "WebSearch")
_SUBAGENT = ("Agent", "Task", "spawn_agent", "wait_agent")
_FILE = ("Read", "Glob", "Grep", "LS")
_EDIT = ("Edit", "Write", "MultiEdit", "NotebookEdit", "apply_patch")


def tool_kind(name: str | None) -> str:
    if not name:
        return "other"
    if name.startswith("mcp__"):
        return "mcp"
    if name in _SHELL:
        return "shell"
    if name in _WEB:
        return "web"
    if name in _SUBAGENT:
        return "subagent"
    if name in _FILE:
        return "file"
    if name in _EDIT:
        return "edit"
    return "other"


def tool_vendor(name: str | None) -> str | None:
    """MCP server segment (linear, notion, or a server uuid); None otherwise."""
    if not name or not name.startswith("mcp__"):
        return None
    rest = name[len("mcp__"):]
    end = rest.find("__")
    return rest[:end] if end > 0 else rest or None


def _in(values: tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


def kind_sql(col: str = "name") -> str:
    return (
        f"CASE WHEN {col} LIKE 'mcp__%' THEN 'mcp' "
        f"WHEN {col} IN {_in(_SHELL)} THEN 'shell' "
        f"WHEN {col} IN {_in(_WEB)} THEN 'web' "
        f"WHEN {col} IN {_in(_SUBAGENT)} THEN 'subagent' "
        f"WHEN {col} IN {_in(_FILE)} THEN 'file' "
        f"WHEN {col} IN {_in(_EDIT)} THEN 'edit' "
        "ELSE 'other' END"
    )


def vendor_sql(col: str = "name") -> str:
    # After 'mcp__' (5 chars → substr start 6), take up to the next '__'.
    return (
        f"CASE WHEN {col} LIKE 'mcp__%' THEN "
        f"CASE WHEN instr(substr({col}, 6), '__') > 0 "
        f"THEN substr(substr({col}, 6), 1, instr(substr({col}, 6), '__') - 1) "
        f"ELSE substr({col}, 6) END ELSE NULL END"
    )


def view_sql() -> str:
    return (
        "CREATE VIEW IF NOT EXISTS tool_calls_ext AS SELECT tc.*, "
        f"{kind_sql('name')} AS kind, {vendor_sql('name')} AS vendor, "
        "CAST(result_chars / 4 AS INTEGER) AS result_tokens_est "
        "FROM tool_calls tc"
    )
