"""Parity check: reconstruct source-level transcript metrics from Langfuse.

This is the MVP confidence gate. We parse the JSONL locally via
`claude_transcript.analyze_session`, fetch the same session's trace +
observations from Langfuse, reconstruct the report shape purely from what
Langfuse preserved, and diff.

Usage:
    uv run python -m agent_telemetry.analysis.parity_check <jsonl> \
        [--trace-id <hex32>] \
        [--langfuse-url http://localhost:3000] \
        [--public-key pk-lf-...] [--secret-key sk-lf-...]

Credentials default to `infra/langfuse/.env` in this repo.

Exit code: 0 if every metric matches within tolerance, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from agent_telemetry.analysis.claude_transcript import (
    analyze_session,
    is_nav_tool,
    repeat_key,
    summarize_input,
)
from agent_telemetry.backfill.claude_code import _trace_id

REPO_ROOT = Path(__file__).resolve().parents[2]
LANGFUSE_ENV = REPO_ROOT / "infra" / "langfuse" / ".env"
# ±1 ms tolerance on wall/active time (ns↔s rounding).
TIMING_EPSILON_S = 0.001


def _load_langfuse_env(path: Path = LANGFUSE_ENV) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


# ---------- Langfuse fetch -------------------------------------------------


@dataclass(frozen=True)
class LangfuseCreds:
    url: str
    public_key: str
    secret_key: str


def fetch_trace(creds: LangfuseCreds, trace_id: str) -> dict[str, Any]:
    """Fetch one trace plus all its observations from Langfuse.

    Observations come embedded in the `/traces/{id}` response (a full dump),
    which is what we want — no pagination to fight with.
    """
    with httpx.Client(
        base_url=creds.url.rstrip("/"),
        auth=(creds.public_key, creds.secret_key),
        timeout=30.0,
    ) as client:
        r = client.get(f"/api/public/traces/{trace_id}")
        r.raise_for_status()
        return r.json()


# ---------- Reconstruction -------------------------------------------------


def _to_int(x: Any) -> int:
    """Langfuse returns OTLP attributes as strings; cast safely."""
    if x is None:
        return 0
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, (int, float)):
        return int(x)
    try:
        return int(x)
    except (TypeError, ValueError):
        try:
            return int(float(x))
        except (TypeError, ValueError):
            return 0


def _to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, str):
        return x.strip().lower() == "true"
    return bool(x)


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _attrs(obs: dict[str, Any]) -> dict[str, Any]:
    md = obs.get("metadata") or {}
    return md.get("attributes") or {}


def reconstruct_report(
    trace: dict[str, Any],
    session_id: str,
    repeat_key_fn: Any,
    is_nav_fn: Any,
    summarize_fn: Any,
) -> dict[str, Any]:
    """Rebuild the analyze_session() shape using only Langfuse data."""
    observations = trace.get("observations") or []
    root_attrs: dict[str, Any] = {}
    turns: list[dict[str, Any]] = []
    tools: list[dict[str, Any]] = []

    for obs in observations:
        name = obs.get("name")
        a = _attrs(obs)
        if name == "claude_code.interaction":
            root_attrs = a
        elif name == "claude_code.llm_request":
            turns.append({"obs": obs, "attrs": a})
        elif name == "claude_code.tool":
            tools.append({"obs": obs, "attrs": a})

    # Time economy. Prefer the root observation's startTime/endTime over the
    # trace-level timestamp: Langfuse fixes the trace row's `timestamp` on
    # first ingest (immutable even if spans are re-sent), while observations
    # upsert by span_id. Root start/end are what the mapper controls.
    root_obs: dict[str, Any] | None = None
    for obs in observations:
        if (
            obs.get("parentObservationId") is None
            and obs.get("name") == "claude_code.interaction"
        ):
            root_obs = obs
            break
    if root_obs:
        first_ts_raw = root_obs.get("startTime")
        last_ts_raw = root_obs.get("endTime")
    else:
        first_ts_raw = trace.get("timestamp")
        last_ts_raw = None

    # Wall time: prefer observation-derived delta (precise to the ms); fall
    # back to trace.latency (float seconds).
    wall_s = round(float(trace.get("latency") or 0.0), 1)
    first_dt = _parse_iso(first_ts_raw)
    last_dt = _parse_iso(last_ts_raw)
    if first_dt and last_dt:
        wall_s = round((last_dt - first_dt).total_seconds(), 1)

    active_ms = 0
    for t in turns:
        # Prefer the explicit attr the mapper sets (matches analyze_sessions'
        # sum of turn_duration events).
        ms = _to_int(t["attrs"].get("claude_code.duration_ms"))
        if ms:
            active_ms += ms
            continue
        # Fallback: span duration.
        start = _parse_iso(t["obs"].get("startTime"))
        end = _parse_iso(t["obs"].get("endTime"))
        if start and end:
            active_ms += int((end - start).total_seconds() * 1000)
    active_s = round(active_ms / 1000.0, 1)

    # Token economy (per-turn GenAI attrs).
    tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_create": 0}
    thinking_chars = 0
    text_chars = 0
    for t in turns:
        a = t["attrs"]
        tokens["input"] += _to_int(a.get("gen_ai.usage.input_tokens"))
        tokens["output"] += _to_int(a.get("gen_ai.usage.output_tokens"))
        tokens["cache_read"] += _to_int(a.get("gen_ai.usage.cache_read_input_tokens"))
        tokens["cache_create"] += _to_int(
            a.get("gen_ai.usage.cache_creation_input_tokens")
        )
        thinking_chars += _to_int(a.get("claude_code.thinking_chars"))
        text_chars += _to_int(a.get("claude_code.text_chars"))
    total_in_like = tokens["input"] + tokens["cache_read"] + tokens["cache_create"]
    cache_hit = tokens["cache_read"] / total_in_like if total_in_like else 0.0

    # Tool economy.
    tool_calls: list[dict[str, Any]] = []
    for t in tools:
        a = t["attrs"]
        args_raw = a.get("tool.arguments") or "{}"
        try:
            inp = json.loads(args_raw)
        except (TypeError, json.JSONDecodeError):
            inp = {}
        duration_s = _to_int(a.get("tool.duration_ms")) / 1000.0
        tool_calls.append(
            {
                "name": a.get("tool.name") or "?",
                "input": inp,
                "duration_s": duration_s,
                "is_error": _to_bool(a.get("tool.is_error")),
                "result_chars": _to_int(a.get("tool.result_chars")),
            }
        )

    by_tool: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "total_s": 0.0, "errors": 0, "total_result_chars": 0}
    )
    for tc in tool_calls:
        b = by_tool[tc["name"]]
        b["count"] += 1
        b["total_s"] += tc["duration_s"]
        if tc["is_error"]:
            b["errors"] += 1
        b["total_result_chars"] += tc["result_chars"]

    repeats: dict[str, int] = defaultdict(int)
    for tc in tool_calls:
        k = repeat_key_fn(tc["name"], tc["input"])
        if k is not None:
            repeats[" | ".join(str(x) for x in k)] += 1
    repeats_filtered = {k: v for k, v in repeats.items() if v > 1}

    slowest = sorted(tool_calls, key=lambda x: -x["duration_s"])[:10]
    largest = sorted(tool_calls, key=lambda x: -x["result_chars"])[:10]

    # turns_user can't be perfectly reconstructed from Langfuse (user events
    # aren't spans). We approximate with a best-effort value; the caller
    # tolerates drift here and flags it.
    turns_user_reconstructed: int | None = None

    tool_out_chars = sum(v["total_result_chars"] for v in by_tool.values())
    nav_chars_all = sum(
        v["total_result_chars"] for name, v in by_tool.items() if is_nav_fn(name)
    )

    first_ts = first_ts_raw
    last_ts = last_ts_raw

    entrypoint_str = root_attrs.get("entrypoint")
    # The local parser produces a {entrypoint: count} dict. Langfuse only
    # carries a single entrypoint string on the root. We report the single
    # value, count unknown — the diff layer will normalize.
    entrypoints = (
        {entrypoint_str: turns_user_reconstructed or 0} if entrypoint_str else {}
    )

    return {
        "session_id": session_id,
        "entrypoints": entrypoints,
        "entrypoint_single": entrypoint_str,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "wall_time_s": wall_s,
        "active_time_s": active_s,
        "tool_out_chars": tool_out_chars,
        "nav_chars_all": nav_chars_all,
        "turns_assistant": len(turns),
        "turns_user": turns_user_reconstructed,
        "tool_calls": len(tool_calls),
        "tool_errors": sum(1 for tc in tool_calls if tc["is_error"]),
        "tokens": tokens,
        "cache_hit_ratio": round(cache_hit, 4),
        "thinking_chars": thinking_chars,
        "text_out_chars": text_chars,
        "by_tool": {k: dict(v) for k, v in by_tool.items()},
        "repeats": repeats_filtered,
        "slowest_tools": [
            {
                "name": s["name"],
                "duration_s": round(s["duration_s"], 2),
                "result_chars": s["result_chars"],
                "is_error": s["is_error"],
                "summary": summarize_fn(s["name"], s["input"]),
            }
            for s in slowest
        ],
        "largest_results": [
            {
                "name": s["name"],
                "result_chars": s["result_chars"],
                "duration_s": round(s["duration_s"], 2),
                "summary": summarize_fn(s["name"], s["input"]),
            }
            for s in largest
        ],
    }


# ---------- Diff -----------------------------------------------------------


@dataclass
class DiffRow:
    metric: str
    local: Any
    langfuse: Any
    ok: bool

    @property
    def status(self) -> str:
        return "OK" if self.ok else "DRIFT"


def _eq_timing(a: float, b: float) -> bool:
    return abs(float(a) - float(b)) <= TIMING_EPSILON_S


def _eq_exact(a: Any, b: Any) -> bool:
    return a == b


def _slowest_key(entry: dict[str, Any]) -> tuple:
    # Set-equality key: ignore ordering and rounding-sensitive float.
    return (
        entry.get("name"),
        round(float(entry.get("duration_s") or 0), 2),
        int(entry.get("result_chars") or 0),
        bool(entry.get("is_error", False)),
        entry.get("summary"),
    )


def _largest_key(entry: dict[str, Any]) -> tuple:
    return (
        entry.get("name"),
        int(entry.get("result_chars") or 0),
        round(float(entry.get("duration_s") or 0), 2),
        entry.get("summary"),
    )


def diff_reports(local: dict[str, Any], lf: dict[str, Any]) -> list[DiffRow]:
    """Yield a stable, deterministic list of comparison rows."""
    rows: list[DiffRow] = []

    def add(metric: str, a: Any, b: Any, eq: Any = _eq_exact) -> None:
        rows.append(DiffRow(metric, a, b, eq(a, b)))

    # Timing.
    add("wall_time_s", local["wall_time_s"], lf["wall_time_s"], _eq_timing)
    add("active_time_s", local["active_time_s"], lf["active_time_s"], _eq_timing)
    add(
        "first_ts",
        local.get("first_ts"),
        lf.get("first_ts"),
        lambda a, b: _parse_iso(a) == _parse_iso(b),
    )
    add(
        "last_ts",
        local.get("last_ts"),
        lf.get("last_ts"),
        lambda a, b: _parse_iso(a) == _parse_iso(b),
    )

    # Tokens.
    for k in ("input", "output", "cache_read", "cache_create"):
        add(f"tokens.{k}", local["tokens"][k], lf["tokens"][k])
    add("cache_hit_ratio", local["cache_hit_ratio"], lf["cache_hit_ratio"])

    # Turns / calls.
    add("turns_assistant", local["turns_assistant"], lf["turns_assistant"])
    # turns_user is only tracked locally; Langfuse can't reconstruct it from
    # spans alone. If lf is None, skip comparison (report separately).
    if lf.get("turns_user") is not None:
        add("turns_user", local["turns_user"], lf["turns_user"])
    add("tool_calls", local["tool_calls"], lf["tool_calls"])
    add("tool_errors", local["tool_errors"], lf["tool_errors"])

    # Thinking / text.
    add("thinking_chars", local["thinking_chars"], lf["thinking_chars"])
    add("text_out_chars", local["text_out_chars"], lf["text_out_chars"])

    # Nav / out.
    add("tool_out_chars", local["tool_out_chars"], lf["tool_out_chars"])
    add("nav_chars_all", local["nav_chars_all"], lf["nav_chars_all"])

    # Per-tool.
    tool_names = sorted(set(local["by_tool"]) | set(lf["by_tool"]))
    for name in tool_names:
        l_stats = local["by_tool"].get(name) or {
            "count": 0,
            "total_s": 0.0,
            "errors": 0,
            "total_result_chars": 0,
        }
        f_stats = lf["by_tool"].get(name) or {
            "count": 0,
            "total_s": 0.0,
            "errors": 0,
            "total_result_chars": 0,
        }
        add(f"by_tool[{name}].count", l_stats["count"], f_stats["count"])
        # total_s is a float sum; allow mild fuzz (sums of ms→s can round).
        add(
            f"by_tool[{name}].total_s",
            round(l_stats["total_s"], 2),
            round(f_stats["total_s"], 2),
            lambda a, b: abs(a - b) <= 0.01,
        )
        add(f"by_tool[{name}].errors", l_stats["errors"], f_stats["errors"])
        add(
            f"by_tool[{name}].total_result_chars",
            l_stats["total_result_chars"],
            f_stats["total_result_chars"],
        )

    # Repeats.
    repeat_keys = sorted(set(local["repeats"]) | set(lf["repeats"]))
    for k in repeat_keys:
        add(
            f"repeats[{k}]",
            local["repeats"].get(k, 0),
            lf["repeats"].get(k, 0),
        )

    # Slowest / largest — set equality on the top-10.
    local_slow = {_slowest_key(e) for e in local["slowest_tools"]}
    lf_slow = {_slowest_key(e) for e in lf["slowest_tools"]}
    add(
        "slowest_tools (set)",
        f"{len(local_slow)} entries",
        f"{len(lf_slow)} entries",
        lambda a, b: local_slow == lf_slow,
    )
    local_large = {_largest_key(e) for e in local["largest_results"]}
    lf_large = {_largest_key(e) for e in lf["largest_results"]}
    add(
        "largest_results (set)",
        f"{len(local_large)} entries",
        f"{len(lf_large)} entries",
        lambda a, b: local_large == lf_large,
    )

    # Entrypoint. Local is a dict; Langfuse gets a single string.
    local_ep_keys = set((local.get("entrypoints") or {}).keys())
    lf_ep = lf.get("entrypoint_single")
    add(
        "entrypoint",
        sorted(local_ep_keys),
        [lf_ep] if lf_ep else [],
        lambda a, b: a == b,
    )

    return rows


def stale_ingest_diagnostics(local: dict[str, Any], lf: dict[str, Any]) -> list[str]:
    """Return operator hints for known Langfuse first-write-wins drift shapes."""

    def timestamp_delta_ms(a: Any, b: Any) -> float | None:
        left = _parse_iso(a)
        right = _parse_iso(b)
        if left is None or right is None:
            return None
        return abs((left - right).total_seconds() * 1000.0)

    messages: list[str] = []
    for metric in ("first_ts", "last_ts"):
        local_value = local.get(metric)
        lf_value = lf.get(metric)
        if local_value == lf_value:
            continue
        delta_ms = timestamp_delta_ms(local_value, lf_value)
        if delta_ms is not None and 0 < delta_ms <= 1.0:
            messages.append(
                f"{metric} differs by {delta_ms:.3f} ms; this matches the "
                "pre-INT-436 float timestamp drift that Langfuse can retain "
                "after deterministic re-ingest."
            )
            continue
        if local_value and lf_value is None:
            messages.append(
                f"{metric} is present locally but missing from Langfuse; this "
                "can happen when an older observation with the same span ID "
                "won first write before the mapper emitted that field."
            )

    if messages:
        messages.append(
            "Suggested local-dev reconciliation: delete this single Langfuse "
            "trace with `python -m agent_telemetry.analysis.langfuse_trace_reconcile "
            "<trace-id> --yes`, then rerun the backfill for the source file."
        )
    return messages


def format_diff(rows: Iterable[DiffRow]) -> str:
    rows = list(rows)
    name_w = max((len(r.metric) for r in rows), default=10)
    name_w = max(name_w, 20)
    local_w = max((len(str(r.local)) for r in rows), default=8)
    local_w = min(max(local_w, 8), 40)
    lf_w = max((len(str(r.langfuse)) for r in rows), default=8)
    lf_w = min(max(lf_w, 8), 40)

    lines = []
    header = f"{'metric':<{name_w}}  {'local':<{local_w}}  {'langfuse':<{lf_w}}  diff"
    lines.append(header)
    lines.append("-" * len(header))
    for r in rows:
        local_s = str(r.local)
        if len(local_s) > local_w:
            local_s = local_s[: local_w - 1] + "…"
        lf_s = str(r.langfuse)
        if len(lf_s) > lf_w:
            lf_s = lf_s[: lf_w - 1] + "…"
        lines.append(
            f"{r.metric:<{name_w}}  {local_s:<{local_w}}  {lf_s:<{lf_w}}  {r.status}"
        )
    return "\n".join(lines)


# ---------- CLI ------------------------------------------------------------


def run_parity(
    jsonl_path: Path,
    trace_id: str | None,
    creds: LangfuseCreds,
) -> tuple[list[DiffRow], dict[str, Any], dict[str, Any]]:
    local_report = analyze_session(jsonl_path)
    session_id = jsonl_path.stem
    tid = trace_id or _trace_id(session_id)
    trace = fetch_trace(creds, tid)
    lf_report = reconstruct_report(
        trace,
        session_id,
        repeat_key,
        is_nav_tool,
        summarize_input,
    )
    rows = diff_reports(local_report, lf_report)
    return rows, local_report, lf_report


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m agent_telemetry.analysis.parity_check",
        description=__doc__,
    )
    parser.add_argument("jsonl", type=Path, help="Claude Code JSONL transcript.")
    parser.add_argument(
        "--trace-id",
        default=None,
        help="Langfuse trace ID (default: derived from session_id).",
    )
    parser.add_argument(
        "--langfuse-url",
        default=os.environ.get("LANGFUSE_URL", "http://localhost:3000"),
    )
    parser.add_argument(
        "--public-key",
        default=os.environ.get("LANGFUSE_PUBLIC_KEY"),
    )
    parser.add_argument(
        "--secret-key",
        default=os.environ.get("LANGFUSE_SECRET_KEY"),
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=LANGFUSE_ENV,
        help="Path to infra/langfuse/.env (default: %(default)s).",
    )
    args = parser.parse_args(argv)

    env = _load_langfuse_env(args.env_file)
    pk = args.public_key or env.get("LANGFUSE_INIT_PROJECT_PUBLIC_KEY")
    sk = args.secret_key or env.get("LANGFUSE_INIT_PROJECT_SECRET_KEY")
    if not pk or not sk:
        print(
            "error: Langfuse credentials not found. Set --public-key/--secret-key "
            "or ensure infra/langfuse/.env has LANGFUSE_INIT_PROJECT_{PUBLIC,SECRET}_KEY.",
            file=sys.stderr,
        )
        return 2

    creds = LangfuseCreds(url=args.langfuse_url, public_key=pk, secret_key=sk)
    if not args.jsonl.exists():
        print(f"error: {args.jsonl} does not exist", file=sys.stderr)
        return 2

    rows, _local, _lf = run_parity(args.jsonl, args.trace_id, creds)
    print(format_diff(rows))
    diagnostics = stale_ingest_diagnostics(_local, _lf)
    if diagnostics:
        print("\nDiagnostics:")
        for diagnostic in diagnostics:
            print(f"- {diagnostic}")
    drifts = [r for r in rows if not r.ok]
    if drifts:
        print(f"\n{len(drifts)} metric(s) drifted.")
        return 1
    print("\nAll metrics match within tolerance.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli())
