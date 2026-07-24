# Agent navigation benchmark ("agent CI")

Standing correctness + cost check for navigation tooling and contract
changes. Cost-shaped metrics alone can't catch quality erosion — in the
2026-07-08 baseline run, the *cheaper* condition missed a relevant file.
Re-run this after changing repo-nav, `_docnav` playbooks, nav contract
text in CLAUDE.md/AGENTS.md, or nudge hooks. ~$0.10/run on haiku scouts.

## Protocol

Launch each task twice as `scout` subagents (same model both conditions):

- **docnav condition** — prompt prepends the navigation contract: read
  `<repo>/_docnav/index.md` first, grep the symbols maps, trust the map,
  `repo-nav read --around` only for implementation bodies; no tree grep,
  no whole-file reads.
- **control condition** — "navigate with standard tools however you judge
  best; do NOT use anything under _docnav/ or the repo-nav command."

Agents must return ONLY file:line references (plus a one-line note each),
so answers are mechanically scorable.

## Tasks and ground truth

Build the task set against your own repos — the shapes below are what
matters, not the specific targets. Verify ground truth before scoring —
line numbers drift. Refresh with a grep; update your table when they move.

| Task shape | Example ask | Ground truth looks like |
|---|---|---|
| T1 cross-language | a UI banner: HTML definition + every JS show/hide site | `panel.html:<n>`; `panel.js:<n>,<n>`. Refresh: `grep -n '<banner-id>' path/to/panel.html path/to/panel.js` |
| T2 symbol lookup | definition, signature, and return shape of one named function | `pkg/module.py:<n>` |
| T3 flow trace | an HTTP view plus the helper it calls to resolve its input | `views.py:<n>` (the view), `:<n>` (the helper) |
| T4 inventory (recall test) | EVERY element id matching a pattern, plus their JS togglers | an exhaustive list — recall is the score, so ground truth must be complete |

## Scoring

Per agent, from the subagent usage stats and the store (subagents ingest
automatically; find them via `sessions --all` under the parent session):

- **correct** — all ground-truth refs present (T4: count elements found /5)
- **tool calls**, **wall seconds**, **tokens** (from Agent tool usage)
- **call precision** — classify each tool call target/map/search/offtarget
  against ground truth (see the `_docnav`-aware classifier in
  `flume/analysis/navtime.py` plus the task's target file set)

## Baseline (2026-07-08, haiku scouts)

| Metric | docnav | control |
|---|---|---|
| correctness | 4/4 (T4: 5/5 elements) | 3.5/4 (T4: 4/5, one hedged) |
| tool calls | 31 | 21 |
| wall time | 127.7s | 93.8s (contract then required confirm-reads; since removed) |
| tokens | 46.0k | 88.4k |
| on-task call rate | 90% | 100% |

Interpretation guardrails: single-turn scouts hide context-compounding
(the token column is the one that matters for long sessions); n=4 pairs
is directional. Log each rerun by tagging a `bench-<change>` experiment
window so results live in the store next to everything else.
